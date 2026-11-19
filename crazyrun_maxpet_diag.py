"""Loop Crazy Run sampai pet MAX — SATU bot, SATU sesi TCP.

Alur (sesuai dump_3, diverifikasi 2026-11-19):
  login -> TCP login -> baca XP pet SEBELUM (RoomEvent "pet" PageRequest,
    PetData f13 = ExperiencePoints)
  -> ACTIVATE pet (RoomEvent "pet", ActivatePetRequest: f1 EventId=4,
    f2 petId) — ini "equip pet" versi bot: pet jadi ActivePet (user
    variable 'ap'), syarat WasWithPet() true
  -> Crazy Run: Count -> JoinGame -> tunggu Start -> GameEnd dengan
    IsThereXpToReceive=TRUE (EndCrazyRunRequest f4) -> baca PlayerResult
    (QuestReward: XpEarned + CurrentPetXp)
  -> QuestTrigger CRAZY_RUN_FINISHED dengan wasWithPet=TRUE
  -> Count SESUDAH -> baca XP pet SESUDAH -> HASIL (delta XP).

Mode cepat (--fast) v2 (perbaikan 2026-11-19, atas perintah Bartes):
  v1 (drain 2/2/1/1, tanpa Count sama sekali) TIDAK PERNAH beri XP di
  PlayerResult (run 44471366600: RUN 1 RunCount naik tapi QuestReward
  kosong, RUN 2 tanpa PlayerResult) — padahal XP +30-nya ternyata
  mendarat BELAKANGAN (terlihat di tes normal 13:22: XP awal 60,
  bukan 30). v2: Count SEKALI di awal (diduga menegakkan state sesi
  Crazy Run di server — semua run yang pernah beri +30 mengirim Count
  sebelum JoinGame pertama), drain 4/4/3/2, tunggu baca-ulang stall
  8 dtk. Count per-run tetap dilewati (hemat 12 dtk/run); target
  ~15-20 dtk/run vs ~36 dtk normal.

Pet default: FalconNeon pkxdshop 378425aa-4d7e-4f14-84bd-4ac551c6c1cf
(stage 2, training gym-nya baru selesai 2026-11-19 10:20).

Kredensial TIDAK disimpan di file ini (input workflow / env).
"""
import argparse
import asyncio
import json
import os
import socket
import struct
import sys
import time
import urllib.request
import uuid

import lookup_uuid as L

VERBOSE = True

EV_JOIN = 0
EV_FINISH = 1
EV_COUNT = 4
EV_LEAVE = 6

PET_CMD = "pet"
PET_EV_ACTIVATE = 4
FALCONNEON = "378425aa-4d7e-4f14-84bd-4ac551c6c1cf"


def vlog(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs, flush=True)


def _kirim_hasil_cepat(kind, email, result):
    """POST hasil diag ke /api/pet-result (jalur cepat panel).
    Gagal kirim TIDAK menggagalkan diag (log tetap jadi fallback).
    Secret dari env PET_RESULT_SECRET (GitHub secret)."""
    secret = os.getenv("PET_RESULT_SECRET", "")
    if not secret:
        return
    try:
        data = json.dumps({"kind": kind, "email": email,
                           "result": result, "secret": secret}).encode()
        req = urllib.request.Request(
            "https://pkxd.shop/api/pet-result", data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=20) as resp:
            resp.read()
        print("[+] hasil cepat terkirim ke /api/pet-result")
    except Exception as exc:
        print(f"[*] kirim hasil cepat gagal (log tetap jadi fallback): {exc}")


def build_cr_request(event_id):
    return b"\x08" + L.varint(event_id)


def build_cr_end(game_id, run_time, is_xp=True):
    g = game_id.encode("utf-8")
    return (b"\x08" + L.varint(EV_FINISH)
            + b"\x12" + L.varint(len(g)) + g
            + b"\x1d" + struct.pack("<f", float(run_time))
            + b"\x20" + L.varint(1 if is_xp else 0))


def build_pet_page_request(page_size=100, page_key=""):
    """PageRequest utk list pet (dari pet_list_diag.py)."""
    inner = b"\x08" + L.varint(0) + b"\x12" + L.varint(0)
    pk = page_key.encode("utf-8")
    out = b"\x08" + L.varint(0)
    out += b"\x12" + L.varint(len(pk)) + pk
    out += b"\x18" + L.varint(page_size)
    out += b"\x22" + L.varint(len(inner)) + inner
    return out


def build_pet_activate(pet_id):
    """ActivatePetRequest: f1 EventId=4, f2 petId (string)."""
    p = pet_id.encode("utf-8")
    return b"\x08" + L.varint(PET_EV_ACTIVATE) + b"\x12" + L.varint(len(p)) + p


def build_evo_request():
    """GetPetEvolutionTable: f1 EventId=13 (PetRequestEventId)."""
    return bytes.fromhex("080d")


def build_stageup_request(pet_id):
    """StageUpRequest: f1 EventId=14, f2 petId."""
    p = pet_id.encode("utf-8")
    return b"\x08" + L.varint(14) + b"\x12" + L.varint(len(p)) + p


def parse_evo(data):
    """PetEvolutionTableResponse: f2 repeated PetEvolutionStatus
    {f1 Level, f2 Stage, f3 MinXp}. Kembalikan list dict."""
    out = []
    for fno, wt, v in _raw_fields(data):
        if fno == 2 and wt == 2:
            st = {"level": None, "stage": None, "min_xp": None}
            for f2, w2, v2 in _raw_fields(bytes(v)):
                if f2 == 1 and w2 == 0:
                    st["level"] = v2
                elif f2 == 2 and w2 == 0:
                    st["stage"] = v2
                elif f2 == 3 and w2 == 0:
                    st["min_xp"] = v2
            if st["level"] is not None:
                out.append(st)
    return out


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


def build_quest_trigger(run_time, quest_id="48fca873-32d5-4d01-bc2c-094ee2a32daf",
                        was_with_pet=True):
    attrs = [
        ("activeQuestId", _pb_value_string(quest_id)),
        ("timeSpent", _pb_value_number(run_time)),
        ("starsCollected", _pb_value_number(0)),
        ("fellInWater", _pb_value_bool(False)),
        ("playerCount", _pb_value_number(1)),
        ("wasWithPet", _pb_value_bool(was_with_pet)),
        ("powerUpUsed", _pb_value_number(0)),
    ]
    out = b"\x08" + L.varint(3)
    t = b"CRAZY_RUN_FINISHED"
    out += b"\x12" + L.varint(len(t)) + t
    for key, val in attrs:
        entry = _pb_map_entry(key, val)
        out += b"\x1a" + L.varint(len(entry)) + entry
    return out


def _raw_fields(buf):
    """Yield (field_no, wire_type, value) generik (tahan truncated)."""
    i = 0
    n = len(buf)
    while i < n:
        tag = buf[i]
        i += 1
        field = tag >> 3
        wire = tag & 7
        if wire == 0:
            val = 0
            shift = 0
            while i < n:
                b = buf[i]
                i += 1
                val |= (b & 0x7F) << shift
                shift += 7
                if not (b & 0x80):
                    break
            yield field, wire, val
        elif wire == 2:
            ln = 0
            shift = 0
            while i < n:
                b = buf[i]
                i += 1
                ln |= (b & 0x7F) << shift
                shift += 7
                if not (b & 0x80):
                    break
            if ln < 0 or i + ln > n:
                break
            chunk = buf[i:i + ln]
            i += ln
            yield field, wire, chunk
        elif wire == 5:
            raw = buf[i:i + 4]
            if len(raw) < 4:
                break
            i += 4
            yield field, wire, struct.unpack("<f", raw)[0]
        elif wire == 1:
            raw = buf[i:i + 8]
            if len(raw) < 8:
                break
            i += 8
            yield field, wire, struct.unpack("<d", raw)[0]
        else:
            break


def parse_protobuf_fields(data):
    out = []
    for fno, wt, v in _raw_fields(data):
        if wt == 2:
            try:
                v = bytes(v).decode("utf-8")
            except Exception:
                v = bytes(v).hex()
            out.append((fno, "string/bytes", v))
        elif wt == 0:
            out.append((fno, "varint", v))
        elif wt == 5:
            out.append((fno, "float32", v))
        elif wt == 1:
            out.append((fno, "float64", v))
    return out


def _parse_petdata(data):
    """PetData: f3 PetId, f4 DisplayName, f6 Stage, f12 Type,
    f13 ExperiencePoints (varint)."""
    pet = {"pet_id": None, "display_name": None, "type": None,
           "owner_id": None, "stage": None, "xp": None}
    for f3, w3, v3 in _raw_fields(bytes(data)):
        if f3 == 1 and w3 == 2:
            try:
                pet["owner_id"] = bytes(v3).decode("utf-8")
            except Exception:
                pass
        elif f3 == 3 and w3 == 2:
            try:
                pet["pet_id"] = bytes(v3).decode("utf-8")
            except Exception:
                pass
        elif f3 == 4 and w3 == 2:
            try:
                pet["display_name"] = bytes(v3).decode("utf-8")
            except Exception:
                pass
        elif f3 == 6 and w3 == 0:
            pet["stage"] = v3
        elif f3 == 12 and w3 == 0:
            pet["type"] = v3
        elif f3 == 13 and w3 == 0:
            pet["xp"] = v3
    return pet


def extract_pets_from_event(data):
    pets = []
    for fno, wt, v in _raw_fields(data):
        if fno == 1 and wt == 0 and v == 8:
            for f2, w2, v2 in _raw_fields(data):
                if f2 == 4 and w2 == 2:
                    p = _parse_petdata(bytes(v2))
                    if p["pet_id"]:
                        pets.append(p)
            return pets
        if fno == 3 and wt == 2:
            for f2, w2, v2 in _raw_fields(bytes(v)):
                if f2 == 4 and w2 == 2:
                    p = _parse_petdata(bytes(v2))
                    if p["pet_id"]:
                        pets.append(p)
    return pets


def decode_room_event(frame):
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


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("CRAZYRUN_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("CRAZYRUN_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("CRAZYRUN_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--pet-id",
                    default=os.getenv("CRAZYRUN_PET_ID", FALCONNEON))
    ap.add_argument("--command",
                    default=os.getenv("CRAZYRUN_COMMAND", "crazy-run"))
    ap.add_argument("--time", type=float,
                    default=float(os.getenv("CRAZYRUN_TIME", "12")))
    ap.add_argument("--wait-start", type=float,
                    default=float(os.getenv("CRAZYRUN_WAIT_START", "10")))
    ap.add_argument("--max-runs", type=int,
                    default=int(os.getenv("MAXPET_MAX_RUNS", "12")))
    ap.add_argument("--target-xp", type=int,
                    default=int(os.getenv("MAXPET_TARGET_XP", "320")))
    ap.add_argument("--fast", default=os.getenv("MAXPET_FAST", "0"),
                    help="Mode cepat v2: Count sekali di awal + drain "
                         "4/4/3/2, tanpa Count per run, XP dari QuestReward "
                         "(1/true = aktif)")
    args = ap.parse_args()
    args.fast = str(args.fast).lower() in ("1", "true", "yes", "y")
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    pet_id = args.pet_id
    print(f"[*] Loop Crazy Run sampai MAX — bot {args.email} | pet {pet_id} "
          f"| target XP {args.target_xp} | max {args.max_runs} run")
    L.QUIET = False
    state = {"xp": None, "stage": None, "energy": None, "run_count": None,
             "runs_done": 0, "stageups": 0, "stop_reason": "",
             "xp_from_result": False}
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        user_jwt, alloc = await L.login_and_allocate(
            args.email, args.password, args.device_id, session)
        if not alloc:
            print("[!] login/alokasi gagal.")
            return 1
        host, port = L.find_host_port(alloc)
        if not host or not port:
            print("[!] host/port tidak ketemu di allocation.")
            return 1
        print(f"[*] alokasi OK: {host}:{port}")
        sock, _pid = L.tcp_connect(host, port)
        if sock is None:
            print("[!] TCP connect gagal.")
            return 1
        try:
            sock.sendall(L.build_login_frame(user_jwt))
        except Exception as exc:
            print(f"[!] kirim login frame gagal: {exc}")
            return 1
        fr = FrameReader(sock)
        print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
        got, closed = fr.read_until(
            lambda f: _is_tbugv(f) or _is_joinedroom(f), 3)
        if closed:
            print("[!] koneksi tertutup saat tunggu login.")
            return 1
        if got is not None and _is_tbugv(got):
            time.sleep(1)

        def kirim_room(cmd, data_pb, label):
            print(f"[*] kirim {label} via RoomEvent '{cmd}' ...")
            try:
                sock.sendall(L.build_room_event_varint(cmd, data_pb))
                return True
            except Exception as exc:
                print(f"[!] kirim {label} gagal: {exc}")
                return False

        def catat(frames, label=""):
            """Update state dari frame yang diterima."""
            for f in frames:
                name, data = decode_room_event(f)
                if name is None or not data:
                    continue
                fields = parse_protobuf_fields(data)
                if name == "crazy-run-count":
                    d = dict((fn, v) for fn, _w, v in fields)
                    if 2 in d and 3 in d:
                        state["energy"] = d[2]
                        state["run_count"] = d[3]
                        print(f"[+] Count: RunCount={d[3]} energi={d[2]}")
                elif name == "crazy-run-result":
                    d = dict((fn, v) for fn, _w, v in fields)
                    if d.get(9) is not None:
                        state["run_count"] = d[9]
                    print(f"[+] PlayerResult: RunCount={d.get(9)} "
                          f"RunTime={d.get(7)}")
                    # QuestReward (f4): f3 XpEarned, f4 CurrentPetXp —
                    # sumber XP tercepat & otoritatif (terbukti di tes 1
                    # run 44453020630: f3=30, f4=30). Dipakai mode cepat
                    # agar tak perlu baca daftar pet tiap run.
                    qrb = d.get(4)
                    if isinstance(qrb, (bytes, bytearray)):
                        try:
                            qr = dict((fn, v) for fn, _w, v
                                      in parse_protobuf_fields(bytes(qrb)))
                            if qr.get(4) is not None:
                                state["xp"] = qr[4]
                                state["xp_from_result"] = True
                                print(f"[+] QuestReward: XpEarned={qr.get(3)} "
                                      f"CurrentPetXp={qr.get(4)}")
                        except Exception:
                            pass
                elif name in ("pet", "pets"):
                    for p in extract_pets_from_event(data):
                        if p["pet_id"] == pet_id:
                            state["xp"] = p["xp"]
                            state["stage"] = p["stage"]
                            print(f"[+] pet: xp={p['xp']} stage={p['stage']} "
                                  f"type={p['type']}")

        def baca_xp_pet():
            req = build_pet_page_request()
            for cmd in ("pet", "pets"):
                try:
                    sock.sendall(L.build_room_event_varint(cmd, req))
                except Exception:
                    continue
                print(f"[*] PageRequest pet via '{cmd}', "
                      f"tunggu {3 if args.fast else 6} dtk...")
                catat(fr.drain(3 if args.fast else 6))
                if state["xp"] is not None:
                    return state["xp"]
                break
            print(f"[!] pet {pet_id} tidak ketemu di daftar.")
            return state["xp"]

        def baca_count():
            if not kirim_room(args.command, build_cr_request(EV_COUNT),
                              "GetUserData Count"):
                return None
            catat(fr.drain(12))
            return state["energy"]

        def stage_up():
            print(f"[*] kirim StageUp pet {pet_id} via RoomEvent 'pet' ...")
            try:
                sock.sendall(L.build_room_event_varint(
                    PET_CMD, build_stageup_request(pet_id)))
            except Exception as exc:
                print(f"[!] kirim StageUp gagal: {exc}")
                return False
            frames = fr.drain(3 if args.fast else 5)
            for f in frames:
                name, data = decode_room_event(f)
                if name in ("pet", "pets") and data:
                    print(f"[+] respons 'pet' StageUp ({len(data)} B): "
                          f"{data.hex()[:300]}")
            before = state["stage"]
            baca_xp_pet()
            ok = state["stage"] is not None and state["stage"] != before
            print(f"[+] StageUp: stage {before} -> {state['stage']} "
                  f"({'OK' if ok else 'BELUM BERUBAH'})")
            if ok:
                state["stageups"] += 1
            return ok

        def aktifkan_pet():
            """Activate pet (EventId=4) — WAJIB sebelum run pertama.

            BUG 2026-11-19 (run 44470482774, pet type 2 98befe5e):
            build_pet_activate() ADA tapi tidak pernah dipanggil — run
            terhitung (RunCount 20->22) tapi XP +0, lalu stall 2x.
            FalconNeon lolos di loop 11:06 karena sudah diaktifkan tes
            petxp 10:41 (ActivePet tersimpan di server antar-sesi).
            Sukses = respons 'pet' f1=12 ActivatePetResponse, f2=1
            (terbukti run 44453020630: payload 08 0c 10 01)."""
            print(f"[*] kirim ActivatePet {pet_id} via RoomEvent 'pet' ...")
            try:
                sock.sendall(L.build_room_event_varint(
                    PET_CMD, build_pet_activate(pet_id)))
            except Exception as exc:
                print(f"[!] kirim ActivatePet gagal: {exc}")
                return False
            frames = fr.drain(3 if args.fast else 5)
            ok = False
            for f in frames:
                name, data = decode_room_event(f)
                if name in ("pet", "pets") and data:
                    d = dict((fn, v) for fn, _w, v
                             in parse_protobuf_fields(data))
                    if d.get(1) == 12 and d.get(2) == 1:
                        ok = True
                    print(f"[+] respons 'pet' Activate ({len(data)} B): "
                          f"{data.hex()[:120]}")
            print(f"[+] ActivatePet: "
                  f"{'OK' if ok else 'TIDAK ADA RESPONS SUKSES'}")
            return ok

        # --- aktivasi pet (WAJIB, sebelum run pertama) ---
        aktif_ok = aktifkan_pet()
        if not aktif_ok:
            print("[*] Aktivasi belum sukses — coba sekali lagi.")
            time.sleep(2 if args.fast else 5)
            aktif_ok = aktifkan_pet()
        if not aktif_ok:
            state["stop_reason"] = ("AKTIVASI PET GAGAL — pet tidak aktif, "
                                    "run tidak akan beri XP")

        # --- ukur awal ---
        baca_xp_pet()
        # Count SEKALI di awal — JUGA di mode cepat (perbaikan fast v2,
        # 2026-11-19). Fast v1 tidak pernah kirim Count dan tidak pernah
        # dapat XP di PlayerResult (run 44471366600), sementara SEMUA run
        # yang pernah beri +30 mengirim Count sebelum JoinGame pertama —
        # Count diduga menegakkan state sesi Crazy Run di server, bukan
        # sekadar baca energi. Hemat 12 dtk/run tetap berlaku: Count
        # per-run tetap dilewati di mode cepat.
        baca_count()
        # Mode cepat (--fast): drain() menunggu PENUH tiap jendela, jadi
        # tiap detik yang dipotong langsung menghemat waktu run.
        # Fast v2: drain v1 (2/2/1/1) terlalu pendek — PlayerResult RUN 2
        # tidak pernah tiba dan QuestReward RUN 1 tidak ada di jendela
        # 2 dtk (XP-nya baru mendarat belakangan). Naikkan ke 4/4/3/2:
        # masih ~2x lebih cepat dari normal, tapi cukup lama agar server
        # sempat memproses tiap langkah.
        d_join = 4 if args.fast else 6
        d_end = 4 if args.fast else 6
        d_quest = 3 if args.fast else 4
        d_leave = 2 if args.fast else 2
        print(f"[*] AWAL: xp={state['xp']} stage={state['stage']} "
              f"energi={state['energy']} RunCount={state['run_count']}")

        # --- loop ---
        while state["runs_done"] < args.max_runs and aktif_ok:
            if (state["xp"] is not None and state["xp"] >= args.target_xp
                    and state["stage"] == 4):
                state["stop_reason"] = "MAX TERCAPAI (xp>=320, stage 4)"
                break
            # Energi TIDAK dipakai sebagai batas (koreksi Bartes
            # 2026-11-19 10:59: energi hanya untuk reward harian,
            # main Crazy Run bisa tanpa batas). Energi tetap dicatat
            # sebagai info saja.
            # stage-up dulu bila XP sudah di cap stage saat ini
            if state["xp"] is not None and state["stage"] == 2 \
                    and state["xp"] >= 120:
                print("[*] XP 120 di stage Baby — StageUp ke Child dulu.")
                if not stage_up():
                    print("[*] StageUp belum berubah — coba sekali lagi.")
                    time.sleep(2 if args.fast else 5)
                    if not stage_up():
                        state["stop_reason"] = "StageUp Baby->Child GAGAL"
                        break
                continue
            if state["xp"] is not None and state["stage"] == 3 \
                    and state["xp"] >= 320:
                print("[*] XP 320 di stage Child — StageUp ke Teen dulu.")
                if not stage_up():
                    print("[*] StageUp belum berubah — coba sekali lagi.")
                    time.sleep(2 if args.fast else 5)
                    if not stage_up():
                        state["stop_reason"] = "StageUp Child->Teen GAGAL"
                        break
                continue
            xp_before = state["xp"]
            # JoinGame
            if not kirim_room(args.command, build_cr_request(EV_JOIN),
                              "JoinGame"):
                state["stop_reason"] = "kirim JoinGame gagal"
                break
            frames = fr.drain(d_join)
            catat(frames)
            gid = None
            for f in frames:
                name, data = decode_room_event(f)
                if name != "crazy-run-joined" or not data:
                    continue
                for fno, wt, v in parse_protobuf_fields(data):
                    if fno == 2 and wt == "string/bytes" \
                            and isinstance(v, str):
                        gid = v
                        break
                if gid:
                    break
            print(f"[*] tunggu Start ({args.wait_start} dtk)...")
            catat(fr.drain(args.wait_start))
            # GameEnd isXp=TRUE
            if not kirim_room(args.command,
                              build_cr_end(gid or "", args.time, True),
                              f"GameEnd(time={args.time}, isXp=TRUE)"):
                state["stop_reason"] = "kirim GameEnd gagal"
                break
            catat(fr.drain(d_end))
            # QuestTrigger wasWithPet=TRUE
            try:
                sock.sendall(L.build_room_event_varint(
                    "quests",
                    build_quest_trigger(args.time, was_with_pet=True)))
            except Exception as exc:
                print(f"[!] kirim QuestTrigger gagal: {exc}")
            catat(fr.drain(d_quest))
            # Leave + ukur ulang
            kirim_room(args.command, build_cr_request(EV_LEAVE), "LeaveGame")
            catat(fr.drain(d_leave))
            if not args.fast:
                baca_count()
            # Mode cepat: XP sudah dari QuestReward (PlayerResult) —
            # lewati baca daftar pet; bila tidak ada, fallback ke daftar.
            if not (args.fast and state.get("xp_from_result")):
                baca_xp_pet()
            state["xp_from_result"] = False
            state["runs_done"] += 1
            gained = (None if xp_before is None or state["xp"] is None
                      else state["xp"] - xp_before)
            if gained == 0 and state["xp"] is not None \
                    and state["xp"] < args.target_xp:
                # Pet list lambat update (run 44455410904: baca pertama
                # 60, baca berikutnya 90; run fast 44471366600: +30 baru
                # terlihat di tes berikutnya) — tunggu 8 dtk + baca ulang
                # sebelum menyimpulkan stall (fast v2: jangan simpulkan
                # stall terlalu cepat).
                print("[!] XP tampak tidak naik — tunggu 8 dtk, "
                      "baca ulang.")
                time.sleep(8)
                baca_xp_pet()
                gained = (None if xp_before is None or state["xp"] is None
                          else state["xp"] - xp_before)
            print(f"[=] RUN {state['runs_done']}: xp {xp_before} -> "
                  f"{state['xp']} (+{gained}) | stage={state['stage']} | "
                  f"energi={state['energy']} | RunCount={state['run_count']}")
            if gained == 0 and state["xp"] is not None \
                    and state["xp"] < args.target_xp:
                # Benar-benar stall: coba StageUp, tapi JANGAN berhenti
                # bila StageUp tidak mengubah stage (bisa jadi cap
                # bukan di angka itu / XP telat) — lanjut run berikut;
                # bila 2 run berturut-turut stall, loop max-runs yang
                # menghentikan.
                print("[!] XP tetap tidak naik — coba StageUp sekali.")
                stage_up()
                state["stalls"] = state.get("stalls", 0) + 1
                if state["stalls"] >= 2:
                    state["stop_reason"] = "XP STALL 2x berturut-turut"
                    break
            else:
                state["stalls"] = 0
        if not state["stop_reason"]:
            state["stop_reason"] = f"max-runs ({args.max_runs}) tercapai"
        try:
            sock.close()
        except Exception:
            pass

    print("=" * 56)
    print("HASIL LOOP CRAZY RUN SAMPAI MAX")
    print(f"  Pet: {pet_id}")
    print(f"  Run dilakukan: {state['runs_done']} | StageUp sukses: "
          f"{state['stageups']}")
    print(f"  XP akhir: {state['xp']} (target {args.target_xp}) | "
          f"stage akhir: {state['stage']}")
    print(f"  Energi akhir: {state['energy']} | "
          f"RunCount: {state['run_count']}")
    print(f"  Berhenti karena: {state['stop_reason']}")
    print(f"  Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 56)
    # Baris JSON mesin (untuk panel Pet Level): satu baris, mudah diparse
    # dari log GitHub Actions tanpa regex rapuh.
    hasil = {"email": args.email, "pet_id": pet_id,
             "runs_done": state["runs_done"],
             "stageups": state["stageups"], "xp": state["xp"],
             "stage": state["stage"], "run_count": state["run_count"],
             "stop_reason": state["stop_reason"],
             "seconds": round(time.time() - t0)}
    print("PETLEVEL_JSON: " + json.dumps(hasil, ensure_ascii=False))
    # Jalur cepat: POST hasil ke /api/pet-result agar panel tidak perlu
    # menunggu arsip log GitHub. Secret dari env PET_RESULT_SECRET
    # (GitHub secret, diteruskan workflow).
    _kirim_hasil_cepat("petlevel", args.email, hasil)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
