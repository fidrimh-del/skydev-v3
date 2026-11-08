"""crazyrun_clan.py — CRAZY RUN MASSAL untuk bot clan_bots.

Turunan crazyrun_diag.py (yang terbukti: RunCount naik, quest trigger
terkirim, statistik terkonfirmasi) untuk SEMUA bot clan sekaligus,
dengan LOOP per bot.

Alur per bot (satu koneksi TCP, loop di dalamnya):
  login -> TCP login -> tunggu jendela login (tbugv/JoinedRoom)
  -> untuk tiap loop (1..LOOPS):
       Count (RunCount) -> JoinGame (GameId HANYA dari 'crazy-run-joined' f2)
       -> tunggu Start -> GameEnd -> QuestTrigger -> Count-sesudah
       -> Statistik total_matches -> sukses bila RunCount naik
          (atau PlayerResult diterima); kalau gagal, RETRY 1x
  -> LeaveGame -> HASIL per bot.

Energi: KOREKSI Bartes 2026-11-08 07:10 WIB — energi TIDAK membatasi
jumlah lomba (di game bisa main sepuasnya; energi hanya untuk reward).
Jadi loop TIDAK berhenti saat energi habis; energi hanya dicatat
untuk info. Loop berhenti hanya bila diminta selesai atau koneksi
benar-benar gagal.

Env / arg:
  SUPABASE_URL, SUPABASE_SERVICE_KEY (baca clan_bots)
  CLAN_ID (default BiackHat, hanya untuk filter baca)
  JUMLAH (default 29), LOOPS (default 1), WORKERS (default 6)
  RUN_TIME (default 12), WAIT_START (default 10), MAX_RETRY (default 2)
  COMMAND (default 'crazy-run')
  PARTISI (default 0), TOTAL_PARTISI (default 1) — untuk matrix:
    bot diurutkan by email, lalu diambil bots[partisi::total_partisi]
    (disjoint, deterministik). Leg matrix ke-i memproses partisi i.
Keluar: exit 0 bila semua bot menyelesaikan semua loop, else 1.
"""
import argparse
import asyncio
import concurrent.futures
import os
import socket
import struct
import sys
import time
import uuid

import lookup_uuid as L

# CrazyRunRequestEventType
EV_JOIN = 0
EV_FINISH = 1
EV_STATE = 2
EV_DOOR = 3
EV_COUNT = 4
EV_RECHARGE = 5
EV_LEAVE = 6

QUEST_CMD = "quests"
QUEST_EVENT_TRIGGER = 3
QUEST_TRIGGER_NAME = "CRAZY_RUN_FINISHED"
CRAZY_RUN_QUEST_ID = "48fca873-32d5-4d01-bc2c-094ee2a32daf"

STAT_CMD = "user_statistics"
STAT_EVENT_UPDATE_NUMBER = 1

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")


def log(*a, **k):
    print(*a, **k, flush=True)


# ---------- protobuf Crazy Run ----------

def build_cr_request(event_id):
    return b"\x08" + L.varint(event_id)


def build_cr_end(game_id, run_time, is_xp=False):
    g = game_id.encode("utf-8")
    return (b"\x08" + L.varint(EV_FINISH)
            + b"\x12" + L.varint(len(g)) + g
            + b"\x1d" + struct.pack("<f", float(run_time))
            + b"\x20" + L.varint(1 if is_xp else 0))


# ---------- protobuf Quest Trigger ----------

def _pb_value_number(num):
    return b"\x11" + struct.pack("<d", float(num))


def _pb_value_string(s):
    b = s.encode("utf-8")
    return b"\x1a" + L.varint(len(b)) + b


def _pb_value_bool(flag):
    return b"\x20" + L.varint(1 if flag else 0)


def _pb_map_entry(key, value_bytes):
    k = key.encode("utf-8")
    return (b"\x0a" + L.varint(len(k)) + k
            + b"\x12" + L.varint(len(value_bytes)) + value_bytes)


def build_quest_trigger(run_time, quest_id=CRAZY_RUN_QUEST_ID):
    attrs = [
        ("activeQuestId", _pb_value_string(quest_id)),
        ("timeSpent", _pb_value_number(run_time)),
        ("starsCollected", _pb_value_number(0)),
        ("fellInWater", _pb_value_bool(False)),
        ("playerCount", _pb_value_number(1)),
        ("wasWithPet", _pb_value_bool(False)),
        ("powerUpUsed", _pb_value_number(0)),
    ]
    out = b"\x08" + L.varint(QUEST_EVENT_TRIGGER)
    t = QUEST_TRIGGER_NAME.encode("utf-8")
    out += b"\x12" + L.varint(len(t)) + t
    for key, val in attrs:
        entry = _pb_map_entry(key, val)
        out += b"\x1a" + L.varint(len(entry)) + entry
    return out


# ---------- protobuf User Statistics ----------

def build_stat_update(stat_id, value):
    sid = stat_id.encode("utf-8")
    num = (b"\x0a" + L.varint(len(sid)) + sid
           + b"\x10" + L.varint(int(value)))
    req_id = str(uuid.uuid4()).encode("utf-8")
    return (b"\x08" + L.varint(STAT_EVENT_UPDATE_NUMBER)
            + b"\x12" + L.varint(len(num)) + num
            + b"\x1a" + L.varint(len(req_id)) + req_id)


def parse_protobuf_fields(data):
    out = []
    i = 0
    n = len(data)
    while i < n:
        tag, m = L.read_varint(data, i)
        if tag is None:
            break
        i += m
        field_no = tag >> 3
        wt = tag & 7
        try:
            if wt == 0:
                v, m2 = L.read_varint(data, i)
                if v is None:
                    break
                out.append((field_no, "varint", v))
                i += m2
            elif wt == 2:
                ln, m2 = L.read_varint(data, i)
                if ln is None or i + m2 + ln > n:
                    break
                raw = data[i + m2:i + m2 + ln]
                try:
                    v = raw.decode("utf-8")
                except Exception:
                    v = raw.hex()
                out.append((field_no, "string/bytes", v))
                i += m2 + ln
            elif wt == 5:
                if i + 4 > n:
                    break
                v = struct.unpack("<f", data[i:i + 4])[0]
                out.append((field_no, "float32", v))
                i += 4
            elif wt == 1:
                if i + 8 > n:
                    break
                v = struct.unpack("<d", data[i:i + 8])[0]
                out.append((field_no, "float64", v))
                i += 8
            else:
                break
        except Exception:
            break
    return out


def decode_room_event(frame):
    """Frame RoomEvent Crazy Run = tipe 0x08 (pola crazyrun_diag.py)."""
    if len(frame) < 2 or frame[0] != 0x08:
        return None, None
    ln, m = L.read_varint(frame, 1)
    if ln is None:
        return None, None
    name = frame[1 + m:1 + m + ln].decode("utf-8", "replace")
    return name, frame[1 + m + ln:]


def _is_tbugv(frame):
    if len(frame) >= 2 and frame[0] == 0x05:
        ln, m = L.read_varint(frame, 1)
        if ln is not None and ln < 0x80 and frame[1 + m:1 + m + ln] == b"tbugv":
            return True
    return False


def _is_joinedroom(frame):
    return len(frame) >= 6 and frame[0] == 0x06 and frame[1] == 1


class FrameReader:
    """Sama seperti crazyrun_diag.py: buffer persisten + auto-pong."""

    def __init__(self, sock):
        self.s = sock
        self.buf = bytearray()

    def read_until(self, pred, seconds):
        deadline = time.time() + seconds
        self.s.settimeout(0.5)
        while time.time() < deadline:
            while len(self.buf) >= 4:
                ln = struct.unpack(">I", self.buf[:4])[0]
                if ln > 262144:
                    return None, False
                if len(self.buf) - 4 < ln:
                    break
                fr = bytes(self.buf[4:4 + ln])
                del self.buf[:4 + ln]
                if fr == b"\x01":
                    try:
                        self.s.sendall(struct.pack(">I", 1) + b"\x02")
                    except Exception:
                        pass
                    continue
                if pred(fr):
                    return fr, False
            try:
                chunk = self.s.recv(65536)
            except socket.timeout:
                continue
            except Exception:
                return None, True
            if chunk == b"":
                return None, True
            self.buf += chunk
        return None, False

    def drain(self, seconds):
        out = []
        deadline = time.time() + seconds
        self.s.settimeout(0.5)
        while time.time() < deadline:
            while len(self.buf) >= 4:
                ln = struct.unpack(">I", self.buf[:4])[0]
                if ln > 262144:
                    return out
                if len(self.buf) - 4 < ln:
                    break
                fr = bytes(self.buf[4:4 + ln])
                del self.buf[:4 + ln]
                if fr == b"\x01":
                    try:
                        self.s.sendall(struct.pack(">I", 1) + b"\x02")
                    except Exception:
                        pass
                    continue
                out.append(fr)
            try:
                chunk = self.s.recv(65536)
            except socket.timeout:
                continue
            except Exception:
                return out
            if chunk == b"":
                return out
            self.buf += chunk
        return out


# ---------- baca bot dari clan_bots ----------

def read_bots(jumlah, clan_id):
    from curl_cffi import requests as creq
    url = (f"{SUPABASE_URL}/rest/v1/clan_bots"
           f"?select=email,password,device_id,nickname,clan_id"
           f"&order=id.asc&limit={jumlah}")
    headers = {"apikey": SUPABASE_KEY,
               "Authorization": f"Bearer {SUPABASE_KEY}"}
    r = creq.get(url, headers=headers, timeout=20)
    if r.status_code != 200:
        log(f"[!] Baca clan_bots gagal HTTP {r.status_code}: "
            f"{str(r.text)[:200]}")
        return []
    bots = []
    for row in r.json():
        if not row.get("email") or not row.get("password"):
            continue
        if clan_id and row.get("clan_id") not in (None, "", clan_id):
            continue
        bots.append({
            "email": row["email"],
            "password": row["password"],
            "device_id": row.get("device_id") or "",
            "nickname": row.get("nickname") or "",
        })
    return bots


# ---------- TCP flow: N loop Crazy Run dalam 1 koneksi (sinkron) ----------

def tcp_crazyrun_loops(jwt, host, port, command, run_time, wait_start,
                       loops, max_retry):
    """Jalankan `loops` lomba Crazy Run dalam satu koneksi TCP.
    Kembalikan dict: loops_done, runcount_awal/akhir, energy_awal/akhir,
    gagal (list alasan)."""
    out = {"loops_done": 0, "runcount_awal": None, "runcount_akhir": None,
           "energy_awal": None, "energy_akhir": None,
           "gagal": [], "error": ""}
    # TCP connect dengan retry (koneksi kadang ditolak sesaat).
    sock = None
    for percobaan in range(1, 4):
        sock, _pid = L.tcp_connect(host, port)
        if sock is not None:
            break
        log(f"    tcp_connect percobaan {percobaan} gagal, coba lagi...")
        time.sleep(2)
    if sock is None:
        out["error"] = "tcp_connect"
        return out
    try:
        sock.sendall(L.build_login_frame(jwt))
    except Exception as exc:
        out["error"] = f"login_frame: {exc}"
        return out
    fr = FrameReader(sock)

    # State yang dikumpulkan dari event selama drain.
    state = {"energy": None, "runcount": None, "game_id": None,
             "player_result": False}

    def _catat(frame):
        name, data = decode_room_event(frame)
        if name is None or not data:
            return
        fields = parse_protobuf_fields(data)
        d = dict((f, v) for f, _w, v in fields)
        if name == "crazy-run-count" and 2 in d and 3 in d:
            state["energy"] = d[2]
            state["runcount"] = d[3]
        if name == "crazy-run-result":
            state["player_result"] = True
            if 9 in d:
                state["runcount"] = d[9]

    def kirim(event_id, data_pb, label):
        try:
            sock.sendall(L.build_room_event_varint(command, data_pb))
            return True
        except Exception:
            return False

    def drain_cat(seconds):
        for f in fr.drain(seconds):
            _catat(f)

    # --- tunggu jendela login ---
    got, closed = fr.read_until(
        lambda f: _is_tbugv(f) or _is_joinedroom(f), 3)
    if closed:
        out["error"] = "closed_saat_login"
        try:
            sock.close()
        except Exception:
            pass
        return out
    if got is not None and _is_tbugv(got):
        time.sleep(1)

    # --- Count awal (energi + RunCount awal) ---
    if not kirim(EV_COUNT, build_cr_request(EV_COUNT), "Count-awal"):
        out["error"] = "kirim_count_awal"
        try:
            sock.close()
        except Exception:
            pass
        return out
    drain_cat(4)
    out["runcount_awal"] = state["runcount"]
    out["energy_awal"] = state["energy"]

    for loop_i in range(1, loops + 1):
        # Energi dicatat untuk info saja — TIDAK menghentikan loop
        # (Bartes 2026-11-08: di game bisa main sepuasnya, energi
        # hanya untuk reward, bukan syarat main).
        rc_before = state["runcount"]
        ok = False
        for attempt in range(1, max_retry + 1):
            state["game_id"] = None
            state["player_result"] = False
            # JoinGame — drain 10 dtk (server bisa lambat membalas
            # 'crazy-run-joined' saat banyak bot join bersamaan).
            if not kirim(EV_JOIN, build_cr_request(EV_JOIN), "JoinGame"):
                out["gagal"].append(f"loop{loop_i} kirim Join gagal")
                break
            # GameId HANYA dari 'crazy-run-joined' f2.
            frames = fr.drain(10)
            gid = None
            for f in frames:
                _catat(f)
                name, data = decode_room_event(f)
                if name != "crazy-run-joined" or not data:
                    continue
                for fno, wt, v in parse_protobuf_fields(data):
                    if fno == 2 and wt == "string/bytes" and isinstance(v, str):
                        gid = v
                        break
                if gid:
                    break
            if not gid:
                log(f"    loop {loop_i} percobaan {attempt}: "
                    f"GameId tidak ditemukan.")
                if attempt < max_retry:
                    continue
                out["gagal"].append(f"loop{loop_i} tanpa GameId")
                break
            # Tunggu Start
            drain_cat(wait_start)
            # GameEnd
            if not kirim(EV_FINISH,
                         build_cr_end(gid, run_time, False), "GameEnd"):
                out["gagal"].append(f"loop{loop_i} kirim GameEnd gagal")
                break
            drain_cat(6)
            # QuestTrigger
            try:
                sock.sendall(L.build_room_event_varint(
                    QUEST_CMD, build_quest_trigger(run_time)))
            except Exception:
                pass
            drain_cat(4)
            # Count-sesudah
            if not kirim(EV_COUNT, build_cr_request(EV_COUNT),
                         "Count-sesudah"):
                out["gagal"].append(f"loop{loop_i} kirim Count gagal")
                break
            drain_cat(4)
            rc_after = state["runcount"]
            # Statistik total_matches = RunCount sesudah.
            if rc_after is not None:
                try:
                    sock.sendall(L.build_room_event_varint(
                        STAT_CMD, build_stat_update("crazy_run_total_matches",
                                                    rc_after)))
                except Exception:
                    pass
                drain_cat(4)
            # Sukses bila RunCount naik ATAU PlayerResult diterima.
            naik = (rc_before is not None and rc_after is not None
                    and rc_after > rc_before)
            if naik or state["player_result"]:
                ok = True
                out["runcount_akhir"] = rc_after
                out["energy_akhir"] = state["energy"]
                break
            log(f"    loop {loop_i} percobaan {attempt}: RunCount tidak "
                f"naik (sebelum={rc_before} sesudah={rc_after}).")
        if ok:
            out["loops_done"] += 1
            log(f"    loop {loop_i}/{loops}: BERHASIL "
                f"(RunCount={out['runcount_akhir']}).")
        else:
            # Cek false-negative: kadang lomba SEBENARNYA terhitung
            # (RunCount naik) tapi konfirmasi (PlayerResult / Count)
            # tidak tertangkap dalam drain. Kirim Count terakhir:
            # kalau RunCount naik dari sebelum loop, anggap BERHASIL.
            if kirim(EV_COUNT, build_cr_request(EV_COUNT), "Count-cek"):
                drain_cat(4)
                rc_cek = state["runcount"]
                if (rc_before is not None and rc_cek is not None
                        and rc_cek > rc_before):
                    out["loops_done"] += 1
                    out["runcount_akhir"] = rc_cek
                    out["energy_akhir"] = state["energy"]
                    log(f"    loop {loop_i}/{loops}: BERHASIL (cek akhir: "
                        f"RunCount {rc_before}->{rc_cek}, konfirmasi "
                        f"sebelumnya terlewat).")
                    continue
            out["gagal"].append(f"loop{loop_i} gagal total")
            # Lanjut ke loop berikut (koneksi mungkin masih hidup);
            # Count loop berikut akan memastikan.
            log(f"    loop {loop_i}/{loops}: GAGAL total, lanjut loop berikut.")
    # LeaveGame
    try:
        sock.sendall(L.build_room_event_varint(
            command, build_cr_request(EV_LEAVE)))
    except Exception:
        pass
    drain_cat(2)
    if out["runcount_akhir"] is None:
        out["runcount_akhir"] = state["runcount"]
    if out["energy_akhir"] is None:
        out["energy_akhir"] = state["energy"]
    try:
        sock.close()
    except Exception:
        pass
    return out


# ---------- satu bot (async HTTP login + executor TCP) ----------

_exec_pool = None


async def crazyrun_satu_bot(idx, total, bot, command, run_time, wait_start,
                            loops, max_retry):
    nick = bot.get("nickname") or bot["email"][:6]
    email = bot["email"]
    try:
        from curl_cffi.requests import AsyncSession
        async with AsyncSession(impersonate="chrome110",
                                verify=False) as session:
            jwt, alloc = await L.login_and_allocate(
                email, bot["password"], bot["device_id"], session)
            if not alloc:
                log(f"[{idx}/{total}] {nick}: login/alokasi gagal.")
                return {"nick": nick, "ok": False,
                        "alasan": "login/alokasi gagal"}
            host, port = L.find_host_port(alloc)
            if not jwt or not host or not port:
                log(f"[{idx}/{total}] {nick}: host/port kosong.")
                return {"nick": nick, "ok": False,
                        "alasan": "host/port kosong"}
    except Exception as exc:
        log(f"[{idx}/{total}] {nick}: exception login "
            f"({type(exc).__name__}: {str(exc)[:120]}).")
        return {"nick": nick, "ok": False,
                "alasan": f"login exception {type(exc).__name__}"}
    loop = asyncio.get_running_loop()
    res = await loop.run_in_executor(
        _exec_pool, tcp_crazyrun_loops, jwt, host, port,
        command, run_time, wait_start, loops, max_retry)
    if res.get("error"):
        log(f"[{idx}/{total}] {nick}: GAGAL ({res['error']}).")
        return {"nick": nick, "ok": False, "alasan": res["error"],
                "loops_done": 0, "loops_minta": loops}
    done = res["loops_done"]
    if done >= loops:
        log(f"[{idx}/{total}] {nick}: SELESAI {done}/{loops} loop "
            f"(RunCount {res['runcount_awal']}->{res['runcount_akhir']}).")
        return {"nick": nick, "ok": True, "alasan": "",
                "loops_done": done, "loops_minta": loops,
                "runcount_awal": res["runcount_awal"],
                "runcount_akhir": res["runcount_akhir"]}
    alasan = "; ".join(res["gagal"]) or "loop tidak selesai"
    log(f"[{idx}/{total}] {nick}: GAGAL {done}/{loops} loop ({alasan}).")
    return {"nick": nick, "ok": False, "alasan": alasan,
            "loops_done": done, "loops_minta": loops,
            "runcount_awal": res["runcount_awal"],
            "runcount_akhir": res["runcount_akhir"]}


async def main():
    global _exec_pool
    ap = argparse.ArgumentParser()
    ap.add_argument("--clan-id",
                    default=os.getenv("CLAN_ID",
                                      "8ae19c43-c9f3-4bbd-8da6-ac2e1b6af320"))
    ap.add_argument("--jumlah", type=int,
                    default=int(os.getenv("JUMLAH", "29")))
    ap.add_argument("--loops", type=int,
                    default=int(os.getenv("LOOPS", "1")))
    ap.add_argument("--workers", type=int,
                    default=int(os.getenv("WORKERS", "6")))
    ap.add_argument("--run-time", type=float,
                    default=float(os.getenv("RUN_TIME", "12")))
    ap.add_argument("--wait-start", type=float,
                    default=float(os.getenv("WAIT_START", "10")))
    ap.add_argument("--max-retry", type=int,
                    default=int(os.getenv("MAX_RETRY", "2")))
    ap.add_argument("--max-bot-retry", type=int,
                    default=int(os.getenv("MAX_BOT_RETRY", "5")))
    ap.add_argument("--command",
                    default=os.getenv("CRAZYRUN_COMMAND", "crazy-run"))
    ap.add_argument("--partisi", type=int,
                    default=int(os.getenv("PARTISI", "0")))
    ap.add_argument("--total-partisi", type=int,
                    default=int(os.getenv("TOTAL_PARTISI", "1")))
    args = ap.parse_args()
    try:
        L.apply_panel_versions()
    except Exception:
        pass
    bots = read_bots(args.jumlah, args.clan_id)
    # Partisi matrix: urut by email (deterministik), ambil stride.
    if args.total_partisi > 1:
        bots = sorted(bots, key=lambda b: b["email"])
        bots = bots[args.partisi::args.total_partisi]
        log(f"[*] Partisi {args.partisi}/{args.total_partisi}: "
            f"{len(bots)} bot di leg ini.")
    log(f"[*] CRAZY RUN MASSAL: {len(bots)} bot x {args.loops} loop "
        f"(workers={args.workers}, run_time={args.run_time}, "
        f"max_retry={args.max_retry})")
    if not bots:
        log("[!] Tidak ada bot di clan_bots — batal.")
        return 1
    _exec_pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=args.workers)
    sem = asyncio.Semaphore(args.workers)

    async def _run(i, bot):
        async with sem:
            try:
                # Stagger: sebar start bot dalam satu leg supaya Join
                # tidak menumpuk bersamaan di satu IP (server membatasi
                # Join serentak; 1 bot gagal per leg di run 42431603771
                # karena 'tanpa GameId').
                await asyncio.sleep((i % args.workers) * 2)
                # Retry sampai sukses (Bartes 2026-11-08 08:00 WIB):
                # bot yang gagal diulang dari awal (login baru + TCP
                # baru) sampai berhasil, maks MAX_BOT_RETRY percobaan.
                # Kalau sampai batas masih gagal, berarti bukan masalah
                # sesaat (akun/server) dan harus dilaporkan, bukan
                # diulang tanpa henti.
                best = None
                for percobaan in range(1, args.max_bot_retry + 1):
                    res = await crazyrun_satu_bot(
                        i, len(bots), bot, args.command, args.run_time,
                        args.wait_start, args.loops, args.max_retry)
                    if best is None or (res["loops_done"]
                                        > best["loops_done"]):
                        best = res
                    if res["ok"]:
                        break
                    if percobaan < args.max_bot_retry:
                        log(f"[{i}/{len(bots)}] "
                            f"{bot.get('nickname') or ''}: retry bot "
                            f"(percobaan ke-{percobaan + 1}/"
                            f"{args.max_bot_retry}) setelah 5 dtk...")
                        await asyncio.sleep(5)
                return best
            except Exception as exc:
                log(f"[{i}/{len(bots)}] {bot.get('nickname') or ''}: "
                    f"exception ({type(exc).__name__}).")
                return {"nick": bot.get("nickname") or "", "ok": False,
                        "alasan": f"exception {type(exc).__name__}",
                        "loops_done": 0, "loops_minta": args.loops}

    t0 = time.time()
    results = await asyncio.gather(
        *[_run(i + 1, b) for i, b in enumerate(bots)])
    total_minta = sum(r["loops_minta"] for r in results)
    total_done = sum(r["loops_done"] for r in results)
    sukses = [r for r in results if r["ok"]]
    gagal = [r for r in results if not r["ok"]]
    dt = time.time() - t0
    log("")
    log(f"[HASIL] CRAZY RUN MASSAL: Bot sukses {len(sukses)}/{len(bots)} | "
        f"Loop selesai {total_done}/{total_minta} ({dt:.0f}s)")
    for r in gagal:
        log(f"  GAGAL: {r['nick']} — {r['alasan']} "
            f"({r['loops_done']}/{r['loops_minta']} loop)")
    log("[*] Catatan: energi hanya untuk reward (bukan batas main) — "
        "bot bisa main sebanyak apa pun (Bartes 2026-11-08).")
    return 0 if len(sukses) >= len(bots) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
