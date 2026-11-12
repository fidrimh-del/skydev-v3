# -*- coding: utf-8 -*-
"""
dance_send_diag.py — Tes jalur kirim dance LANGSUNG (tanpa relay).

Latar (2026-11-13): mirror run 43331770803 relay 45x tapi Bartes lihat
bot TETAP DIAM. Dua kemungkinan: (a) bot relay pid SALAH (75d2, pemain
asing yang dance banyak gerakan; Bartes 72a9 cuma 1 frame f7=7 selama
jendela mirror), (b) jalur kirim 'tbulv' dance-nya sendiri tidak jalan.
Diag ini mengisolasi (b): bot kirim LANGSUNG 'aaa' berisi field 7 =
ReactionIndex 43 (nomor dance Bartes yang TERKONFIRMASI di run
43331214520) + animasi idle field 1-5, tanpa membaca 'aaa' siapa pun.
Bila Bartes lihat bot dance gerakan 43 -> jalur kirim BERFUNGSI, masalah
ada di logika mirror/identifikasi pid. Bila bot tetap diam -> format
kirim 'aaa' dance-nya salah, perlu selidiki formatnya.
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


def encode_varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def build_tbulv_dance(x, y, z, player_id, reaction_index):
    """'tbulv' dengan field 7 = ReactionIndex (tag 0x38) + animasi idle
    field 1-5 (dari build_spawn_aaa)."""
    import spawn_test_diag as S
    base = S.build_spawn_aaa(x, y, z, player_id=player_id)
    # base = [00 06 <ID>] + [BE32(3)]"aaa"[0x05] + protobuf(fields 1-5).
    # Sisipkan field 7 di AKHIR protobuf: tag 0x38 + varint.
    header_len = 4 + 4 + 3 + 1  # prefix(4) + BE32(3) + 'aaa' + 0x05
    protobuf = bytes(base[header_len:])
    protobuf_dance = protobuf + b"\x38" + encode_varint(reaction_index)
    return base[:header_len] + protobuf_dance


def build_tbulv_idle(x, y, z, player_id):
    import spawn_test_diag as S
    return S.build_spawn_aaa(x, y, z, player_id=player_id)


def find_joinedroom_in_frames(frames, rm):
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
    for fr in frames:
        if len(fr) >= 6 and fr[0] == 0x06 and fr[1] == 1 and int.from_bytes(fr[2:6], "big") == rm:
            return True
        if fr and fr[0] == 0x0d and needle in bytes(fr):
            return True
    return False


async def dance_once(session, email, password, device_id,
                     dest_user_id, bot_user_id, reaction_index=43,
                     dance_s=30, idle_s=10,
                     start_x=-79.99, start_y=21.96, start_z=-76.42):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "reaction_index": reaction_index,
             "dance_s": dance_s, "idle_s": idle_s,
             "dance_sent": 0, "idle_sent": 0}
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
    joined_here = False
    if got is not None and not G._is_tbugv(got):
        joined_here = True
    if not joined_here:
        extra = fr.drain(2)
        joined_here = find_joinedroom_in_frames(extra, rm)
    hasil["joined_room"] = rm if joined_here else None
    print(f"[+] JoinedRoom({rm}) = {joined_here}. Bot dance LANGSUNG f7={reaction_index} selama {dance_s} dtk, lalu diam {idle_s} dtk.")
    tx, ty, tz = start_x + 1.5, start_y, start_z
    seq = [0]

    def send_pos(teleported):
        seq[0] = (seq[0] + 1) & 0xFF
        try:
            sock.sendall(L.build_room_event_varint(
                "C", build_position_payload(tx, ty, tz, teleported=teleported,
                                            sequence=seq[0])))
        except Exception:
            pass

    send_pos(teleported=1)
    t0 = time.time()
    # FASE DANCE: kirim 'aaa' + field 7 tiap 0.5 dtk.
    end_dance = time.time() + dance_s
    last_hb = time.time()
    while time.time() < end_dance:
        try:
            sock.sendall(L.build_room_event_varint(
                "tbulv", build_tbulv_dance(tx, ty, tz, pid, reaction_index)))
            hasil["dance_sent"] += 1
        except Exception:
            pass
        if time.time() - last_hb >= 1.0:
            send_pos(teleported=0)
            last_hb = time.time()
        for _ in fr.drain(0.1):
            pass
        await asyncio.sleep(0.4)
    # FASE DIAM: kirim 'aaa' idle (tanpa field 7) tiap 0.5 dtk.
    print(f"[+] Fase dance selesai ({hasil['dance_sent']}x). Sekarang diam {idle_s} dtk (kirim idle).")
    end_idle = time.time() + idle_s
    last_hb = time.time()
    while time.time() < end_idle:
        try:
            sock.sendall(L.build_room_event_varint(
                "tbulv", build_tbulv_idle(tx, ty, tz, pid)))
            hasil["idle_sent"] += 1
        except Exception:
            pass
        if time.time() - last_hb >= 1.0:
            send_pos(teleported=0)
            last_hb = time.time()
        for _ in fr.drain(0.1):
            pass
        await asyncio.sleep(0.4)
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. dance dikirim: {hasil['dance_sent']}x, idle: {hasil['idle_sent']}x")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("DANCE_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("DANCE_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("DANCE_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("DANCE_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("DANCE_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--reaction-index", type=int, default=43)
    ap.add_argument("--dance-s", type=float, default=30)
    ap.add_argument("--idle-s", type=float, default=10)
    ap.add_argument("--start-x", type=float, default=-79.99)
    ap.add_argument("--start-y", type=float, default=21.96)
    ap.add_argument("--start-z", type=float, default=-76.42)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await dance_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id, args.reaction_index,
            args.dance_s, args.idle_s,
            args.start_x, args.start_y, args.start_z)
    print("DANCE_SEND_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
