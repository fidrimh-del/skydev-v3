"""clan_join.py — GABUNG MASSAL bot clan_bots ke satu clan.

Alur per bot (pola clan_diag.py yang terbukti, PKXDSHOP #760 masuk):
  baca bot dari tabel panel `clan_bots`
  -> login (+ alokasi activity world Bartes, opsional)
  -> TCP login -> tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)
  -> coba masuk world (auto-join; kalau gagal tetap lanjut kirim clan)
  -> RoomEvent 'clan' JoinClan (EventId=2, ClanId)
  -> drain 5 dtk (tangkap respons JoinClan bila ada) -> HASIL.

PENTING (terbukti 2026-11-08): server memproses JoinClan secara
DIAM-DIAM tanpa respons RoomEvent. Jadi 'terkirim' di sini = JoinClan
berhasil DIKIRIM via socket, BUKAN konfirmasi masuk clan. Verifikasi
akhir = cek daftar anggota clan di game (Bartes).
Join bisa butuh persetujuan admin clan (RequestJoin = permintaan
bergabung, bukan langsung masuk) — tergantung setting clan.

Env / arg:
  SUPABASE_URL, SUPABASE_SERVICE_KEY (baca clan_bots)
  CLAN_ID (default BiackHat 8ae19c43-c9f3-4bbd-8da6-ac2e1b6af320)
  CLAN_NAME (default BiackHat, hanya untuk log)
  ACTIVITY_ID (default world Bartes 3e7cec3f-1ae9-4ba2-a19b-a6339e0f9b23)
  JOIN_WORLD (default '1': coba masuk world dulu; '0': langsung JoinClan)
  JUMLAH (default 29), WORKERS (default 6)
Keluar: exit 0 bila terkirim >= jumlah, else 1.
"""
import argparse
import asyncio
import concurrent.futures
import os
import socket
import struct
import sys
import time

import lookup_uuid as L

CLAN_CMD = "clan"
EV_JOIN = 2

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")


def log(*a, **k):
    print(*a, **k, flush=True)


# ---------- protobuf Clan ----------

def build_join_clan(clan_id):
    """JoinClanRequest: f1 EventId=2, f2 ClanId (string)."""
    c = clan_id.encode("utf-8")
    return (b"\x08" + L.varint(EV_JOIN)
            + b"\x12" + L.varint(len(c)) + c)


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


# ---------- TCP frame (pola like_profile_probe.py) ----------

def _is_tbugv(frame):
    return len(frame) >= 2 and frame[0] == 0x03 and frame[1] == 0x74


def _is_joinedroom(frame):
    return len(frame) >= 6 and frame[0] == 0x06 and frame[1] == 1


class FrameReader:
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


def decode_room_event(frame):
    try:
        if len(frame) < 3 or frame[0] != 0x03:
            return None, None
        i = 1
        ln, m = L.read_varint(frame, i)
        if ln is None:
            return None, None
        i += m
        name = frame[i:i + ln].decode("utf-8", "replace")
        i += ln
        if i >= len(frame):
            return name, b""
        dln, m2 = L.read_varint(frame, i)
        if dln is None:
            return name, b""
        i += m2
        return name, frame[i:i + dln]
    except Exception:
        return None, None


# ---------- login + alokasi ----------

async def login_for_activity(session, email, password, device_id,
                             activity_id):
    """Login + alokasi ACTIVITY (pola activity_standby.py).
    Kembalikan (jwt, host, port, gsid)."""
    jwt, _body = await L.login_and_allocate(email, password, device_id,
                                            session)
    if not jwt:
        return None, None, None, None
    import json as _json
    for attempt, extra in enumerate([
            {"userToken": jwt,
             "gameServerProtocolVersion": L.GAME_PROTOCOL,
             "isPrivate": False, "activityId": activity_id},
            {"userToken": jwt,
             "gameServerProtocolVersion": L.GAME_PROTOCOL,
             "isPrivate": False, "activityId": activity_id,
             "datacenterId": "blr1"},
    ], 1):
        b = _json.dumps(extra, separators=(",", ":")).encode("utf-8")
        try:
            r = await session.post(L.LOBBY_URL + "/lobby/allocation",
                                   data=b, headers=L.HEADERS_BASE.copy(),
                                   timeout=20)
            body = r.json()
        except Exception as e:
            log(f"[-] Alokasi activity percobaan {attempt} gagal: {e}")
            continue
        host, port = L.find_host_port(body)
        gsid = body.get("gameServerId", "") if isinstance(body, dict) else ""
        if host and port:
            return jwt, host, port, gsid
    return jwt, None, None, None


# ---------- TCP flow (sinkron, jalan di executor) ----------

def tcp_join_flow(jwt, host, port, gsid, clan_id, activity_id):
    """Kirim JoinClan via TCP. Kembalikan dict hasil."""
    out = {"join_sent": False, "world_joined": False,
           "join_response": False, "error": ""}
    sock, _pid = L.tcp_connect(host, port)
    if sock is None:
        out["error"] = "tcp_connect"
        return out
    try:
        sock.sendall(L.build_login_frame(jwt))
    except Exception as exc:
        out["error"] = f"login_frame: {exc}"
        return out
    fr = FrameReader(sock)
    got, closed = fr.read_until(
        lambda f: _is_tbugv(f) or _is_joinedroom(f), 3)
    if closed:
        out["error"] = "closed_saat_login"
        try:
            sock.close()
        except Exception:
            pass
        return out
    room = None
    if got is not None and _is_tbugv(got):
        time.sleep(1)
    elif got is not None:
        room = True
    if activity_id:
        if not room and gsid:
            try:
                room = L.tcp_join_only(sock, gsid)
            except Exception:
                room = None
        out["world_joined"] = bool(room)
        if room:
            try:
                for name in ["init_pos", "init_day_night", "preset",
                             "fetch_events"]:
                    sock.sendall(L.build_room_event_varint(name, b""))
                time.sleep(0.3)
                sock.sendall(L.build_room_event_varint("activity/get", b""))
            except Exception:
                pass
            time.sleep(2)
    try:
        sock.sendall(L.build_room_event_varint(
            CLAN_CMD, build_join_clan(clan_id)))
    except Exception as exc:
        out["error"] = f"kirim_join: {exc}"
        try:
            sock.close()
        except Exception:
            pass
        return out
    out["join_sent"] = True
    frames = fr.drain(5)
    for f in frames:
        name, data = decode_room_event(f)
        if name == "clan" and data:
            d = dict((fn, v) for fn, _w, v in parse_protobuf_fields(data))
            if d.get(1) == 1:
                out["join_response"] = True
    try:
        sock.close()
    except Exception:
        pass
    return out


# ---------- baca bot dari clan_bots ----------

def read_bots(jumlah, clan_id):
    """Baca bot dari tabel panel clan_bots via PostgREST (service key).
    Kembalikan list dict {email, password, device_id, nickname}."""
    from curl_cffi import requests as creq
    url = (f"{SUPABASE_URL}/rest/v1/clan_bots"
           f"?select=email,password,device_id,nickname"
           f"&order=id.asc&limit={jumlah}")
    headers = {"apikey": SUPABASE_KEY,
               "Authorization": f"Bearer {SUPABASE_KEY}"}
    r = creq.get(url, headers=headers, timeout=20)
    if r.status_code != 200:
        log(f"[!] Baca clan_bots gagal HTTP {r.status_code}: "
            f"{str(r.text)[:200]}")
        return []
    rows = r.json()
    bots = []
    for row in rows:
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


# ---------- satu bot (async HTTP + executor TCP) ----------

_exec_pool = None


async def join_satu_bot(idx, total, bot, clan_id, activity_id, join_world):
    nick = bot.get("nickname") or bot["email"][:6]
    email = bot["email"]
    try:
        from curl_cffi.requests import AsyncSession
        async with AsyncSession(impersonate="chrome110",
                                verify=False) as session:
            if join_world and activity_id:
                jwt, host, port, gsid = await login_for_activity(
                    session, email, bot["password"],
                    bot["device_id"], activity_id)
            else:
                jwt, alloc = await L.login_and_allocate(
                    email, bot["password"], bot["device_id"], session)
                if not alloc:
                    log(f"[{idx}/{total}] {nick}: login/alokasi gagal.")
                    return False
                host, port = L.find_host_port(alloc)
                gsid = ""
            if not jwt or not host or not port:
                log(f"[{idx}/{total}] {nick}: login/alokasi gagal "
                    f"(host/port kosong).")
                return False
    except Exception as exc:
        log(f"[{idx}/{total}] {nick}: exception login "
            f"({type(exc).__name__}: {str(exc)[:120]}).")
        return False
    loop = asyncio.get_running_loop()
    res = await loop.run_in_executor(
        _exec_pool, tcp_join_flow, jwt, host, port, gsid,
        clan_id, activity_id if join_world else "")
    if res["join_sent"]:
        resp = " +RESPONS JOIN" if res["join_response"] else ""
        log(f"[{idx}/{total}] {nick}: JoinClan TERKIRIM"
            f" (world={'ya' if res['world_joined'] else 'tidak'}){resp}")
        return True
    log(f"[{idx}/{total}] {nick}: JoinClan GAGAL ({res['error']}).")
    return False


async def main():
    global _exec_pool
    ap = argparse.ArgumentParser()
    ap.add_argument("--clan-id",
                    default=os.getenv("CLAN_ID",
                                      "8ae19c43-c9f3-4bbd-8da6-ac2e1b6af320"))
    ap.add_argument("--clan-name", default=os.getenv("CLAN_NAME", "BiackHat"))
    ap.add_argument("--activity-id",
                    default=os.getenv(
                        "ACTIVITY_ID",
                        "3e7cec3f-1ae9-4ba2-a19b-a6339e0f9b23"))
    ap.add_argument("--jumlah", type=int,
                    default=int(os.getenv("JUMLAH", "29")))
    ap.add_argument("--workers", type=int,
                    default=int(os.getenv("WORKERS", "6")))
    ap.add_argument("--join-world",
                    default=os.getenv("JOIN_WORLD", "1"))
    args = ap.parse_args()
    join_world = str(args.join_world).strip().lower() not in (
        "0", "false", "no", "")
    try:
        L.apply_panel_versions()
    except Exception:
        pass
    bots = read_bots(args.jumlah, args.clan_id)
    log(f"[*] JOIN CLAN MASSAL: {len(bots)} bot -> clan "
        f"'{args.clan_name}' ({args.clan_id}) "
        f"(join_world={'ya' if join_world else 'tidak'}, "
        f"workers={args.workers})")
    if not bots:
        log("[!] Tidak ada bot di clan_bots — batal.")
        return 1
    _exec_pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=args.workers)
    sem = asyncio.Semaphore(args.workers)

    async def _run(i, bot):
        async with sem:
            try:
                return await join_satu_bot(
                    i, len(bots), bot, args.clan_id,
                    args.activity_id, join_world)
            except Exception as exc:
                log(f"[{i}/{len(bots)}] {bot.get('nickname') or ''}: "
                    f"exception ({type(exc).__name__}).")
                return False

    t0 = time.time()
    results = await asyncio.gather(
        *[_run(i + 1, b) for i, b in enumerate(bots)])
    terkirim = sum(1 for r in results if r)
    gagal = len(bots) - terkirim
    dt = time.time() - t0
    log(f"\n[HASIL] JOIN CLAN: Diminta: {len(bots)} | Terkirim: {terkirim} "
        f"| Gagal: {gagal} ({dt:.1f}s)")
    log("[*] Catatan: 'Terkirim' = JoinClan berhasil dikirim; server "
        "tidak membalas JoinClan, jadi verifikasi akhir = cek daftar "
        "anggota clan di game.")
    return 0 if terkirim >= len(bots) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
