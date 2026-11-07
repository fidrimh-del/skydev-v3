"""Diag Crazy Run (mini-game) — SATU bot, SEKALI jalan.

Alur yang diuji (sesuai dump_31_ijrw.cs, ToxicBirdCrazyRunMechanic):
  login -> TCP login -> GetUserData (RunCount/Energy, SEBELUM)
  -> JoinGame (dapat GameId + CanStartGame) -> tunggu Start
  -> GameEnd (finish, kirim time + gameId) -> baca PlayerResult
  -> GetUserData lagi (RunCount SESUDAH) -> LeaveGame -> HASIL.

Protokol (dari dump):
  RoomEvent command: <COMMAND> (input, default "crazy_run").
  CrazyRunRequestEventType: Join=0, Finish=1, State=2, DoorBroken=3,
    Count=4, Recharge=5, Leave=6.
  CrazyRunRequestMessage: field 1 = EventId (varint).
  EndCrazyRunRequest: field 1 = EventId (varint, =1), field 2 = GameId
    (string), field 3 = Time (float32), field 4 = IsThereXpToReceive (bool).
  BreakDoorCrazyRunRequest: field 1 = EventId (=3), field 2 = GameId
    (string), field 3 = DoorIndex (varint).
  Respons (server -> client): Join=0, Start=1, Gamestate=2, Finish=3,
    Doorbroken=4, Gametimedout=5, Count=6, Playerresult=7.
  CrazyRunUserData: EnergyCounter (float), RunCount (int).
  CrazyRunJoinGameResponse: CanStartGame (bool), LobbyTime (float),
    GameId (string).
  CrazyRunPlayerResultResponse: GameId, UserId, RoomId, RunTime (float),
    RunCount (int), QuestReward, GivenReward (Money), UsedBoost.

CATATAN: nilai RoomEvent command BELUM terkonfirmasi dari dump
(field CrazyRunCommand diset di runtime, bukan const). Skrip ini
mencatat SEMUA RoomEvent mentah yang diterima, jadi kalau command
salah (server tidak merespons), log akan menunjukkannya dan command
bisa diganti lewat input workflow tanpa ubah skrip.

Kredensial TIDAK disimpan di file ini. Bot diambil dari input
workflow / env: CRAZYRUN_EMAIL, CRAZYRUN_PASSWORD, CRAZYRUN_DEVICE_ID.
Akun ini TIDAK ada di panel DB (akun Gmail mandiri), jadi tidak bisa
diambil dari accounts/named_bots. Device_id boleh dikosongkan — dipakai
nilai bawaan probe.
"""
import argparse
import asyncio
import os
import socket
import struct
import sys
import time

import lookup_uuid as L

VERBOSE = True

# CrazyRunRequestEventType
EV_JOIN = 0
EV_FINISH = 1
EV_STATE = 2
EV_DOOR = 3
EV_COUNT = 4
EV_RECHARGE = 5
EV_LEAVE = 6


def vlog(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs, flush=True)


# ---------- protobuf Crazy Run (bangun manual, pola build_like_protobuf) ----------

def build_cr_request(event_id):
    """CrazyRunRequestMessage: field 1 = EventId (varint)."""
    return b"\x08" + L.varint(event_id)


def build_cr_end(game_id, run_time, is_xp=True):
    """EndCrazyRunRequest: f1 EventId=1, f2 GameId (string),
    f3 Time (float32), f4 IsThereXpToReceive (bool)."""
    g = game_id.encode("utf-8")
    return (b"\x08" + L.varint(EV_FINISH)
            + b"\x12" + L.varint(len(g)) + g
            + b"\x1d" + struct.pack("<f", float(run_time))
            + b"\x20" + L.varint(1 if is_xp else 0))


def build_cr_door(game_id, door_index):
    """BreakDoorCrazyRunRequest: f1 EventId=3, f2 GameId, f3 DoorIndex."""
    g = game_id.encode("utf-8")
    return (b"\x08" + L.varint(EV_DOOR)
            + b"\x12" + L.varint(len(g)) + g
            + b"\x18" + L.varint(int(door_index)))


def parse_protobuf_fields(data):
    """Parser protobuf generik (best-effort): kembalikan list
    (field_no, wire_type, value). Dipakai untuk mengintip isi respons."""
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
    """Kembalikan (nama_event, data_protobuf) untuk frame RoomEvent 0x08."""
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
    """Sama seperti like_profile_probe.py: buffer persisten + auto-pong."""

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
        """Kumpulkan SEMUA frame selama `seconds` detik (untuk log)."""
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


async def crazyrun_once(session, email, password, device_id, command,
                        run_time, wait_start):
    """Satu percobaan penuh. Kembalikan dict hasil (untuk HASIL)."""
    hasil = {"email": email, "command": command,
             "count_sebelum": None, "count_sesudah": None,
             "energy_sebelum": None, "energy_sesudah": None,
             "game_id": None, "can_start": None,
             "player_result": None, "events": []}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil
    host, port = L.find_host_port(alloc)
    if not host or not port:
        print("[!] host/port tidak ketemu di allocation.")
        return hasil
    print(f"[*] alokasi OK: {host}:{port}")
    sock, _pid = L.tcp_connect(host, port)
    if sock is None:
        print("[!] TCP connect gagal.")
        return hasil
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        print(f"[!] kirim login frame gagal: {exc}")
        return hasil
    fr = FrameReader(sock)

    def _catat(frame):
        name, data = decode_room_event(frame)
        if name is None:
            return
        hasil["events"].append((name, data.hex() if data else ""))
        vlog(f"[evt] RoomEvent name={name!r} data_hex={data.hex() if data else ''}")
        if not data:
            return
        fields = parse_protobuf_fields(data)
        for fno, wt, v in fields:
            vlog(f"      field {fno} ({wt}): {v!r}")
        # Tangkap RunCount/energi dari crazy-run-count (f2=energi float,
        # f3=RunCount varint). Yang pertama = sebelum, kedua = sesudah.
        if name == "crazy-run-count":
            d = dict((f, v) for f, _w, v in fields)
            if 2 in d and 3 in d:
                if hasil["count_sebelum"] is None:
                    hasil["energy_sebelum"] = d[2]
                    hasil["count_sebelum"] = d[3]
                    print(f"[+] Count SEBELUM: RunCount={d[3]} energi={d[2]}")
                else:
                    hasil["energy_sesudah"] = d[2]
                    hasil["count_sesudah"] = d[3]
                    print(f"[+] Count SESUDAH: RunCount={d[3]} energi={d[2]}")
        # Tangkap RunCount dari crazy-run-result (f9=RunCount, f7=RunTime).
        if name == "crazy-run-result":
            d = dict((f, v) for f, _w, v in fields)
            hasil["player_result"] = {"run_count": d.get(9),
                                      "run_time": d.get(7)}
            print(f"[+] PlayerResult: RunCount={d.get(9)} RunTime={d.get(7)}")

    # --- tunggu jendela login (pola WAIT-LOGIN): tbugv atau JoinedRoom ---
    print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
    got, closed = fr.read_until(
        lambda f: _is_tbugv(f) or _is_joinedroom(f), 3)
    if closed:
        print("[!] koneksi tertutup saat tunggu login.")
        return hasil
    if got is not None and _is_tbugv(got):
        print("[*] tbugv diterima — observasi 1 dtk lagi.")
        time.sleep(1)
    elif got is not None:
        print("[*] JoinedRoom diterima.")
    else:
        print("[*] timeout jendela login (koneksi hidup) — lanjut kirim.")

    def kirim(event_id, data_pb, label):
        print(f"[*] kirim {label} (EventId={event_id}) via RoomEvent "
              f"'{command}' ...")
        try:
            sock.sendall(L.build_room_event_varint(command, data_pb))
            return True
        except Exception as exc:
            print(f"[!] kirim {label} gagal: {exc}")
            return False

    def tunggu_respons(seconds, label):
        """Tunggu & catat semua RoomEvent selama `seconds` detik."""
        print(f"[*] tunggu respons {label} ({seconds} dtk)...")
        frames = fr.drain(seconds)
        for f in frames:
            _catat(f)
        return frames

    # 1) GetUserData (Count) — SEBELUM
    if not kirim(EV_COUNT, build_cr_request(EV_COUNT), "GetUserData"):
        return hasil
    tunggu_respons(4, "Count-sebelum")

    # 2) JoinGame
    if not kirim(EV_JOIN, build_cr_request(EV_JOIN), "JoinGame"):
        return hasil
    frames = tunggu_respons(6, "JoinGame")
    # GameId HANYA dari event 'crazy-run-joined' (field 2 = GameId).
    # JANGAN dari event lain: data biner tbulv bisa ter-decode sebagai
    # string panjang dan tertangkap sebagai GameId palsu (run 42402122640:
    # GameId sampah '08cab5e1...' -> Finish tidak dihitung, RunCount
    # tetap 5 padahal energi berkurang).
    for f in frames:
        name, data = decode_room_event(f)
        if name != "crazy-run-joined" or not data:
            continue
        for fno, wt, v in parse_protobuf_fields(data):
            if fno == 2 and wt == "string/bytes" and isinstance(v, str):
                hasil["game_id"] = v
                print(f"[+] GameId terdeteksi: {v}")
                break
        if hasil["game_id"]:
            break

    # 3) Tunggu Start (server mulai game setelah lobby).
    print(f"[*] tunggu Start ({wait_start} dtk)...")
    tunggu_respons(wait_start, "Start")

    # 4) GameEnd (finish) — butuh GameId; kalau tidak ada, pakai "".
    gid = hasil["game_id"] or ""
    if not gid:
        print("[!] GameId tidak ditemukan di respons Join — "
              "tetap coba Finish dengan GameId kosong.")
    if not kirim(EV_FINISH, build_cr_end(gid, run_time, False),
                 f"GameEnd(time={run_time})"):
        return hasil
    tunggu_respons(6, "Finish/PlayerResult")

    # 5) GetUserData (Count) — SESUDAH
    if not kirim(EV_COUNT, build_cr_request(EV_COUNT), "GetUserData"):
        return hasil
    tunggu_respons(4, "Count-sesudah")

    # 6) LeaveGame
    kirim(EV_LEAVE, build_cr_request(EV_LEAVE), "LeaveGame")
    tunggu_respons(2, "Leave")
    try:
        sock.close()
    except Exception:
        pass
    return hasil


async def main():
    global VERBOSE
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("CRAZYRUN_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("CRAZYRUN_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("CRAZYRUN_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--command",
                    default=os.getenv("CRAZYRUN_COMMAND", "crazy-run"))
    ap.add_argument("--time", type=float,
                    default=float(os.getenv("CRAZYRUN_TIME", "12")))
    ap.add_argument("--wait-start", type=float,
                    default=float(os.getenv("CRAZYRUN_WAIT_START", "10")))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi "
              "(input workflow / env CRAZYRUN_EMAIL & CRAZYRUN_PASSWORD).")
        return 1
    print(f"[*] Diag Crazy Run — bot {args.email} | command '{args.command}' "
          f"| finish time {args.time} dtk")
    L.QUIET = False
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await crazyrun_once(
            session, args.email, args.password, args.device_id,
            args.command, args.time, args.wait_start)
    print("=" * 56)
    print("HASIL CRAZY RUN DIAG")
    print(f"  Bot: {hasil['email']} | command: {hasil['command']}")
    print(f"  GameId: {hasil['game_id']}")
    print(f"  RunCount SEBELUM: {hasil['count_sebelum']} "
          f"(energi {hasil['energy_sebelum']})")
    print(f"  RunCount SESUDAH: {hasil['count_sesudah']} "
          f"(energi {hasil['energy_sesudah']})")
    print(f"  PlayerResult: {hasil['player_result']}")
    if (hasil['count_sebelum'] is not None
            and hasil['count_sesudah'] is not None):
        naik = hasil['count_sesudah'] - hasil['count_sebelum']
        status = "NAIK +" + str(naik) + " (BERHASIL)" if naik > 0 else "TIDAK NAIK (GAGAL)"
        print("  COUNTER: " + status)
    print(f"  RoomEvent diterima: {len(hasil['events'])}")
    for name, hx in hasil["events"]:
        print(f"    - {name}: {hx[:160]}")
    print(f"  Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 56)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
