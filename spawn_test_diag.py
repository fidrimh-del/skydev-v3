# -*- coding: utf-8 -*-
"""
spawn_test_diag.py — Tes spawn bot di town square Plaza dengan koordinat
TETAP dari Bartes (bukan smart spawn meniru pemain lain).

Latar (2026-11-12, Bartes):
- Town square BUKAN satu titik tetap di binary: client spawn di titik
  ACAK dalam cincin radius 6,0-7,5 di sekeliling Transform scene
  (_iaTransform), Y = pusat.Y + 2,0. Pusatnya ada di file scene Unity,
  bukan di libil2cpp.so.
- Bartes berdiri di tempat spawn dan membaca posisinya (CoordLog):
  X=-21.47, Y=22.19, Z=-72.63. Itu dipakai sebagai pusat acuan.
- Spawn bot = pusat + offset dalam cincin (radius 6,0-7,5),
  Y = pusat.Y + 2,0 = 24,19 — persis rumus client asli.
- Tes ini membuktikan apakah PENGIRIMAN koordinat bot (frame "aaa"
  via RoomEvent 'tbulv') benar: bila bot muncul di town square pada
  ketinggian yang benar, pengiriman koordinat BENAR; bila tetap di
  bawah tanah / tidak terlihat, ada yang salah pada pengiriman
  koordinatnya (hipotesis Bartes).
Alur: login -> alokasi destinationUserId (SkyDev) -> TCP login ->
(server auto-place; JoinedRoom dari batch 0x0d — TIDAK kirim JoinRoom
eksplisit) -> kirim posisi spawn TETAP -> kirim SendDeviceId
('Analytics/deviceId') -> tetap online 90 dtk supaya Bartes cek di
game apakah bot muncul di town square. Verdict utama = cek in-game.
"""
import argparse
import asyncio
import json
import math
import os
import random
import struct
import time

import lookup_uuid as L
import gift_diag as G

SKYDEV_UUID = "aae2efe1-1abb-48c2-8799-ea192692a49c"
BOT_UUID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"

# Pusat town square dari Bartes (CoordLog, berdiri di tempat spawn).
CENTER_X = -21.47
CENTER_Y = 22.19
CENTER_Z = -72.63
# Rumus client: Y spawn = pusat.Y + 2,0; radius cincin 6,0-7,5.
SPAWN_Y = CENTER_Y + 2.0
RING_MIN = 6.0
RING_MAX = 7.5


def parse_gsid(gsid):
    parts = (gsid or "").split("::")
    server = parts[1] if len(parts) >= 2 else ""
    room = None
    if len(parts) >= 3:
        try:
            room = int(parts[2])
        except Exception:
            room = None
    return server, room


async def allocate_to_user(session, jwt, dest_user_id):
    body = {"userToken": jwt,
            "gameServerProtocolVersion": L.GAME_PROTOCOL,
            "isPrivate": False,
            "destinationUserId": dest_user_id}
    b = json.dumps(body, separators=(",", ":")).encode("utf-8")
    try:
        r = await session.post(L.LOBBY_URL + "/lobby/allocation", data=b,
                               headers=L.HEADERS_BASE.copy(), timeout=20)
        resp = r.json()
    except Exception as exc:
        print(f"[-] alokasi destinationUserId gagal: {exc}")
        return {}
    return resp if isinstance(resp, dict) else {}


def parse_protobuf(buf):
    out = {}
    i = 0
    n = len(buf)
    while i < n:
        tag = buf[i]
        i += 1
        field = tag >> 3
        wire = tag & 7
        if wire == 0:
            v = 0
            s = 0
            while i < n:
                b = buf[i]
                i += 1
                v |= (b & 0x7F) << s
                if not (b & 0x80):
                    break
                s += 7
            out[field] = v
        elif wire == 2:
            if i >= n:
                break
            ln = buf[i]
            i += 1
            if ln & 0x80:
                out[field] = b""
                break
            chunk = buf[i:i + ln]
            i += ln
            try:
                out[field] = chunk.decode("utf-8")
            except Exception:
                out[field] = chunk
        elif wire == 5:
            if i + 4 <= n:
                out[field] = struct.unpack("<f", buf[i:i + 4])[0]
                i += 4
            else:
                break
        elif wire == 1:
            i += 8
        else:
            break
    return out


def build_spawn_aaa(x, y, z, player_id=None, simple=False):
    """Frame posisi 'aaa' (tbulv). Perbaikan 2026-11-12 (Bartes): frame
    'tbulv' ASLI dari pemain lain punya prefix `00 06 <ID-2byte>` sebelum
    `[BE32(3)]'aaa' 05` — prefix itu TIDAK ADA di frame bot sebelumnya.
    Bila player_id diberikan, sertakan prefix `00 06 <ID>` (ID = 2 byte
    big-endian dari playerId koneksi). Prefix SUDAH TERBUKTI memperbaiki
    state bot (animasi jam hilang, bot benar-benar idle, run 43221201166)
    tapi posisi TETAP tidak diterapkan. Tersangka berikut: f1 Y/X yang
    hardcode dari pemain lain (server mungkin validasi f1 terhadap
    pengirim). Bila simple=True, sub-message Y/X/Z hanya berisi f3
    (koordinat float) tanpa f1/f2 — menguji apakah server hanya membaca
    f3 dan mengabaikan f1. f3/f5 tetap -1/-1 (konstan, cocok dengan frame
    asli)."""
    f32 = lambda v: struct.pack("<f", v)
    if simple:
        sub_y = b"\x1d" + f32(y)
        sub_x = b"\x1d" + f32(x)
        sub_z = b"\x1d" + f32(z)
    else:
        sub_y = bytes.fromhex("08f980a5ab05") + b"\x1d" + f32(y)
        sub_x = bytes.fromhex("089b87987010b3f2de61") + b"\x1d" + f32(x)
        sub_z = bytes.fromhex("08a2b0b7b0fdffffffff01") + b"\x1d" + f32(z)
    sub_r = bytes.fromhex(
        "08ffffffffffffffffff0110ffffffffffffffffff01")
    sub_v = bytes.fromhex(
        "08ffffffffffffffffff0110ffffffffffffffffff01")
    pb = b""
    for field, sub in ((1, sub_y), (2, sub_x), (3, sub_r),
                       (4, sub_z), (5, sub_v)):
        pb += bytes([(field << 3) | 2, len(sub)]) + sub
    prefix = b""
    if player_id is not None:
        prefix = bytes([0x00, 0x06]) + (player_id & 0xFFFF).to_bytes(2, "big")
    return prefix + struct.pack(">I", 3) + b"aaa" + b"\x05" + pb


def find_joinedroom_in_frames(frames, rm):
    """Cari konfirmasi JoinedRoom untuk room `rm` (standalone 0x06 atau
    sub-pesan di batch 0x0d)."""
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
    for fr in frames:
        if len(fr) >= 6 and fr[0] == 0x06 and fr[1] == 1 and int.from_bytes(fr[2:6], "big") == rm:
            return True
        if fr and fr[0] == 0x0d and needle in bytes(fr):
            return True
    return False


def town_square_spawn(seed=None):
    """Titik spawn town square: pusat Bartes + offset acak dalam cincin
    radius 6,0-7,5, Y = pusat.Y + 2,0 (rumus client asli)."""
    rng = random.Random(seed)
    angle = rng.uniform(0, 2 * math.pi)
    radius = rng.uniform(RING_MIN, RING_MAX)
    x = CENTER_X + radius * math.cos(angle)
    z = CENTER_Z + radius * math.sin(angle)
    return (x, SPAWN_Y, z, f"town-square ring r={radius:.2f} a={math.degrees(angle):.0f}")


async def spawn_test_once(session, email, password, device_id,
                          dest_user_id, bot_user_id,
                          spawn_x=None, spawn_y=None, spawn_z=None,
                          device_event="Analytics/deviceId",
                          listen_s=90, simple=False):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "center": [CENTER_X, CENTER_Y, CENTER_Z],
             "spawn_pos": None, "spawn_mode": None,
             "device_event": device_event, "device_id_sent": None,
             "deviceid_sent": False, "deviceid_t_rel": None,
             "room_user_update": {"seen": False, "first_t_rel": None,
                                  "first_after": None, "uuid": None,
                                  "frame_type": None, "event": None,
                                  "hex_head": None},
             "new_events": [],
             "event_names": {}, "frame_types": {}, "rs_count": 0}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil
    body = await allocate_to_user(session, user_jwt, dest_user_id)
    if not body:
        print("[!] alokasi destinationUserId gagal/kosong.")
        return hasil
    srv, rm = parse_gsid(body.get("gameServerId", ""))
    hasil["server"] = srv
    hasil["room"] = rm
    print(f"[+] destinationUserId OK: server={srv} room={rm}")
    if not srv or rm is None:
        return hasil
    host, port = L.find_host_port(body)
    if not host or not port:
        print("[!] host/port tidak ketemu.")
        return hasil
    sock, pid = L.tcp_connect(host, port)
    if sock is None:
        print("[!] TCP connect gagal.")
        return hasil
    hasil["player_id"] = pid
    print(f"[+] Connected: playerId={pid} (2-byte ID: 0x{pid & 0xFFFF:04x}).")
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        print(f"[!] kirim login frame gagal: {exc}")
        return hasil
    fr = G.FrameReader(sock)
    print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")

    def _is_join_or_tbugv(f):
        if G._is_tbugv(f):
            return True
        if len(f) >= 6 and f[0] == 0x06 and f[1] == 1 and int.from_bytes(f[2:6], "big") == rm:
            return True
        if f and f[0] == 0x0d and needle in bytes(f):
            return True
        return False

    got, closed = fr.read_until(_is_join_or_tbugv, 3)
    if closed:
        print("[!] koneksi tertutup saat tunggu login.")
        return hasil
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
    # TIDAK kirim JoinRoom eksplisit (server auto-place).
    t0 = time.time()
    joined_here = False
    if got is not None and not G._is_tbugv(got):
        joined_here = True
    if not joined_here:
        extra = fr.drain(2)
        joined_here = find_joinedroom_in_frames(extra, rm)
    if joined_here:
        hasil["joined_room"] = rm
        print(f"[+] JoinedRoom({rm}) TERKONFIRMASI (batch 0x0d / standalone).")
    else:
        print("[!] JoinedRoom tidak ketemu di jendela login — tetap lanjut.")
    # SPAWN TETAP town square (pusat Bartes + cincin), BUKAN smart spawn.
    if spawn_x is not None and spawn_y is not None and spawn_z is not None:
        sx, sy, sz = spawn_x, spawn_y, spawn_z
        smode = "fixed-arg"
    else:
        sx, sy, sz, smode = town_square_spawn()
    hasil["spawn_pos"] = [round(sx, 2), round(sy, 2), round(sz, 2)]
    hasil["spawn_mode"] = smode
    try:
        data = build_spawn_aaa(sx, sy, sz, player_id=pid, simple=simple)
        sock.sendall(L.build_room_event_varint("tbulv", data))
        print(f"[+] Posisi spawn terkirim ({sx:.2f},{sy:.2f},{sz:.2f}) [{smode}] "
              f"dengan prefix 00 06 {pid & 0xFFFF:04x}.")
        print(f"    frame 'aaa' hex: {data.hex()}")
    except Exception as exc:
        print(f"[!] kirim spawn gagal: {exc}")
    state = {"event_names": {}, "frame_types": {}, "seen_events": set(),
             "new_events": [],
             "room_user_update": dict(hasil["room_user_update"])}
    watch = [dest_user_id, bot_user_id]

    # Heartbeat posisi: kirim ulang frame "aaa" tiap 1,0 dtk selama
    # mendengarkan. PERBAIKAN (hipotesis Bartes 2026-11-12): bot kirim
    # posisi SEKALI saja lalu tetap di bawah tanah — client asli kirim
    # posisi kontinu ~9,3x/detik; server mungkin butuh update kontinu
    # agar posisi "menempel". Frame yang dikirim ulang = data spawn.
    spawn_frame = build_spawn_aaa(sx, sy, sz, player_id=pid, simple=simple)
    hasil["heartbeat_s"] = 1.0
    hasil["heartbeat_sent"] = 0

    def _listen(seconds, label, heartbeat=None):
        end = time.time() + seconds
        last_hb = time.time()
        while time.time() < end:
            if heartbeat is not None and time.time() - last_hb >= 1.0:
                try:
                    sock.sendall(L.build_room_event_varint("tbulv", heartbeat))
                    hasil["heartbeat_sent"] += 1
                    last_hb = time.time()
                except Exception:
                    pass
            for f in fr.drain(0.5):
                raw = bytes(f)
                ftype = raw[0] if raw else None
                state["frame_types"][ftype] = state["frame_types"].get(ftype, 0) + 1
                name, d = G.decode_room_event(f)
                if name:
                    state["event_names"][name] = state["event_names"].get(name, 0) + 1
                    if name not in state["seen_events"]:
                        state["seen_events"].add(name)
                        state["new_events"].append(
                            {"t_rel": round(time.time() - t0, 1),
                             "after": label, "event": name})
                if name == "RS":
                    hasil["rs_count"] += 1
                if not state["room_user_update"]["seen"]:
                    for u in watch:
                        if u.encode("utf-8") in raw:
                            if name == "consent":
                                continue
                            state["room_user_update"] = {
                                "seen": True,
                                "first_t_rel": round(time.time() - t0, 1),
                                "first_after": label,
                                "uuid": u[:8] + "...",
                                "frame_type": ftype,
                                "event": name or "",
                                "hex_head": raw[:120].hex()}
                            print(f"[+] KANDIDAT RoomUserUpdate t+{state['room_user_update']['first_t_rel']} dtk "
                                  f"(sesudah {label}): UUID {u[:8]}... di frame type {ftype}, event '{name}'.")
                            break
    # SendDeviceId ('Analytics/deviceId').
    send_dev = device_id or DEFAULT_DEVICE_ID
    hasil["device_id_sent"] = send_dev
    try:
        sock.sendall(L.build_room_event_varint(
            device_event, send_dev.encode("utf-8")))
        hasil["deviceid_sent"] = True
        hasil["deviceid_t_rel"] = round(time.time() - t0, 1)
        print(f"[+] SendDeviceId terkirim: event '{device_event}', "
              f"deviceId='{send_dev}' (t+{hasil['deviceid_t_rel']} dtk).")
    except Exception as exc:
        print(f"[!] kirim SendDeviceId gagal: {exc}")
    _listen(3, "deviceid", heartbeat=spawn_frame)
    hasil["room_user_update"] = state["room_user_update"]
    hasil["new_events"] = state["new_events"]
    hasil["event_names"] = state["event_names"]
    hasil["frame_types"] = {str(k): v for k, v in state["frame_types"].items()}
    print(f"[*] Bot tetap online {listen_s} dtk supaya Bartes bisa cek "
          f"apakah bot muncul di town square. Mendengarkan + heartbeat "
          f"posisi tiap 1 dtk...")
    _listen(listen_s, "spawn-check", heartbeat=spawn_frame)
    hasil["room_user_update"] = state["room_user_update"]
    hasil["new_events"] = state["new_events"]
    hasil["event_names"] = state["event_names"]
    hasil["frame_types"] = {str(k): v for k, v in state["frame_types"].items()}
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. deviceid: {hasil['deviceid_sent']}, "
          f"spawn: {hasil['spawn_pos']} [{hasil['spawn_mode']}], "
          f"RoomUserUpdate: {hasil['room_user_update']['seen']} "
          f"({hasil['room_user_update']['first_after']}), "
          f"RS: {hasil['rs_count']}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("SPAWN_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("SPAWN_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("SPAWN_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("SPAWN_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("SPAWN_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--spawn-x", type=float, default=None)
    ap.add_argument("--spawn-y", type=float, default=None)
    ap.add_argument("--spawn-z", type=float, default=None)
    ap.add_argument("--device-event", default="Analytics/deviceId")
    ap.add_argument("--listen", type=float, default=90)
    ap.add_argument("--simple", action="store_true",
                    help="Sub-message Y/X/Z hanya f3 (tanpa f1/f2) — "
                    "uji apakah server hanya membaca f3")
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await spawn_test_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id,
            args.spawn_x, args.spawn_y, args.spawn_z,
            args.device_event, args.listen, args.simple)
    print("SPAWN_TEST_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
