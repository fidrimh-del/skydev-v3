# -*- coding: utf-8 -*-
"""
dance_mirror_diag.py — Bot menirukan dance Bartes dengan relay 'aaa'.

Latar (2026-11-13, TERKONFIRMASI): dance = ReactionIndex, field 7
protobuf PlayerAnimationData di user variable "aaa" (RoomEvent 'tbulv'),
tag 0x38. Verifikasi wire run 43331214520 (60 dtk, Bartes dance satu
gerakan): pid 72a9 field 7 = 43, 65 frame, t 11.6-61.2 (konsisten
sepanjang dance); pemain lain 2760/10227/42 hanya sesekali. Jadi field
7 MUNCUL saat dance, ABSEN saat diam (protobuf hilangkan nilai 0).
Cara kerja: login -> alokasi destinationUserId (SkyDev) -> TCP login ->
JoinedRoom -> kirim posisi 'C' (berdiri di samping Bartes) + 'tbulv'
idle -> loop: baca 'tbulv' masuk, kenali Bartes sebagai pid yang field
7-nya PALING SERING muncul (dia dance terus-menerus), relay SELURUH
bytes protobuf 'aaa' Bartes apa adanya sebagai 'tbulv' bot (dengan
prefix 00 06 <ID bot>). Saat field 7 Bartes hilang (berhenti dance),
bot kirim 'aaa' idle (tanpa field 7) supaya ikut berhenti. Posisi 'C'
tetap dikirim terpisah (bot tetap di samping Bartes).
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G
import tbulv_capture_diag as T

SKYDEV_UUID = "aae2efe1-1abb-48c2-8799-ea192692a49c"
BOT_UUID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"


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


def build_position_payload(x, y, z, rotation=0.0, teleported=1, sequence=0):
    return (bytes([teleported & 0xFF, sequence & 0xFF])
            + struct.pack(">f", x) + struct.pack(">f", y)
            + struct.pack(">f", z) + struct.pack(">f", rotation))


def build_tbulv_with_protobuf(protobuf_bytes, player_id):
    """Bungkus protobuf 'aaa' mentah sebagai payload 'tbulv' bot:
    [00 06 <ID 2 byte>] + [BE32(3)]"aaa"[0x05] + protobuf."""
    prefix = bytes([0x00, 0x06]) + (player_id & 0xFFFF).to_bytes(2, "big")
    return prefix + struct.pack(">I", 3) + b"aaa" + b"\x05" + bytes(protobuf_bytes)


def build_tbulv_idle(x, y, z, player_id):
    """'tbulv' idle (tanpa field 7) — bot berhenti dance."""
    import spawn_test_diag as S
    return S.build_spawn_aaa(x, y, z, player_id=player_id)


def parse_aaa(data):
    """Parse payload tbulv -> (pid, protobuf_bytes, f7).
    protobuf_bytes = isi sesudah header 12 byte (untuk relay utuh).
    f7 None = field 7 absen (tidak dance)."""
    try:
        if len(data) < 13:
            return None, None, None
        pid = bytes(data[2:4]).hex()
        pb_bytes = bytes(data[12:])
        pb = T.parse_protobuf(pb_bytes)
        return pid, pb_bytes, pb.get(7)
    except Exception:
        return None, None, None


def find_joinedroom_in_frames(frames, rm):
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
    for fr in frames:
        if len(fr) >= 6 and fr[0] == 0x06 and fr[1] == 1 and int.from_bytes(fr[2:6], "big") == rm:
            return True
        if fr and fr[0] == 0x0d and needle in bytes(fr):
            return True
    return False


async def mirror_once(session, email, password, device_id,
                      dest_user_id, bot_user_id, duration_s=60,
                      start_x=-79.99, start_y=21.96, start_z=-76.42):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "duration_s": duration_s,
             "bartes_pid": None, "bartes_f7": None,
             "relay_count": 0, "idle_sent": 0,
             "tbulv_received": 0, "c_received": 0,
             "f7_by_pid": {}}
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
    t0 = time.time()
    joined_here = False
    if got is not None and not G._is_tbugv(got):
        joined_here = True
    if not joined_here:
        extra = fr.drain(2)
        joined_here = find_joinedroom_in_frames(extra, rm)
    hasil["joined_room"] = rm if joined_here else None
    print(f"[+] JoinedRoom({rm}) = {joined_here}. MIRROR dimulai {duration_s} dtk — Bartes dance sekarang...")
    tx, ty, tz = start_x, start_y, start_z
    seq = [0]

    def send_bot_pos(x, y, z, teleported):
        seq[0] = (seq[0] + 1) & 0xFF
        try:
            sock.sendall(L.build_room_event_varint(
                "C", build_position_payload(x, y, z, teleported=teleported,
                                            sequence=seq[0])))
        except Exception:
            pass

    # tbulv idle + posisi awal (berdiri di samping Bartes).
    try:
        sock.sendall(L.build_room_event_varint(
            "tbulv", build_tbulv_idle(tx + 1.5, ty, tz, pid)))
    except Exception as exc:
        print(f"[!] kirim tbulv gagal: {exc}")
    send_bot_pos(tx + 1.5, ty, tz, teleported=1)
    print(f"[+] Bot di ({tx + 1.5:.2f},{ty:.2f},{tz:.2f}), siap mirror dance Bartes.")
    end = time.time() + duration_s
    last_hb = time.time()
    last_tbulv = time.time()
    # Kenali Bartes: pid dengan field 7 paling sering (dance terus).
    f7_count = {}
    bartes_pid = [None]
    bartes_f7 = [None]
    last_relay = [0.0]
    dancing = [False]
    while time.time() < end:
        now = time.time()
        if now - last_hb >= 1.0:
            send_bot_pos(tx + 1.5, ty, tz, teleported=0)
            last_hb = now
        if now - last_tbulv >= 5.0:
            try:
                sock.sendall(L.build_room_event_varint(
                    "tbulv", build_tbulv_idle(tx + 1.5, ty, tz, pid)))
            except Exception:
                pass
            last_tbulv = now
        for f in fr.drain(0.1):
            name, d = G.decode_room_event(f)
            if name == "tbulv" and d:
                hasil["tbulv_received"] += 1
                rpid, pb_bytes, f7 = parse_aaa(bytes(d))
                if rpid is None:
                    continue
                if f7 is not None:
                    key = f"{rpid}:{f7}"
                    hasil["f7_by_pid"][key] = hasil["f7_by_pid"].get(key, 0) + 1
                    f7_count[rpid] = f7_count.get(rpid, 0) + 1
                    # Bartes = pid dengan f7 terbanyak sejauh ini.
                    if bartes_pid[0] is None or f7_count[rpid] >= f7_count.get(bartes_pid[0], 0):
                        bartes_pid[0] = rpid
                        bartes_f7[0] = f7
                    # Relay 'aaa' Bartes apa adanya (hanya bila dari pid Bartes).
                    if rpid == bartes_pid[0] and pb_bytes:
                        try:
                            sock.sendall(L.build_room_event_varint(
                                "tbulv", build_tbulv_with_protobuf(pb_bytes, pid)))
                            hasil["relay_count"] += 1
                            last_relay[0] = now
                            if not dancing[0]:
                                print(f"[+] MIRROR MULAI: pid {rpid} f7={f7} t+{now - t0:.1f}")
                            dancing[0] = True
                        except Exception:
                            pass
            elif name == "C" and d:
                hasil["c_received"] += 1
        # Bila Bartes berhenti dance (>3 dtk tanpa relay) dan bot sedang
        # dance, kirim 'aaa' idle sekali supaya bot ikut berhenti.
        if dancing[0] and time.time() - last_relay[0] > 3.0:
            try:
                sock.sendall(L.build_room_event_varint(
                    "tbulv", build_tbulv_idle(tx + 1.5, ty, tz, pid)))
                hasil["idle_sent"] += 1
                print(f"[+] MIRROR BERHENTI (Bartes diam >3 dtk) t+{time.time() - t0:.1f}")
            except Exception:
                pass
            dancing[0] = False
    try:
        sock.close()
    except Exception:
        pass
    hasil["bartes_pid"] = bartes_pid[0]
    hasil["bartes_f7"] = bartes_f7[0]
    print(f"[+] selesai. tbulv diterima: {hasil['tbulv_received']}, "
          f"Bartes pid={hasil['bartes_pid']} f7={hasil['bartes_f7']}, "
          f"relay: {hasil['relay_count']}x, idle: {hasil['idle_sent']}x")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("MIRROR_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("MIRROR_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("MIRROR_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("MIRROR_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("MIRROR_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--start-x", type=float, default=-79.99)
    ap.add_argument("--start-y", type=float, default=21.96)
    ap.add_argument("--start-z", type=float, default=-76.42)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await mirror_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id, args.duration,
            args.start_x, args.start_y, args.start_z)
    print("MIRROR_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
