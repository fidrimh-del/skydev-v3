# -*- coding: utf-8 -*-
"""
dance_aaa_timeseries.py — Rekam deret waktu nilai sub-message 'aaa' di
frame 'tbulv' selama Bartes dance, untuk menemukan sinyal dance.

Latar (2026-11-12, Bartes): dua tangkapan dance (run 43229698061 +
43230187436) menunjukkan TIDAK ada RoomEvent dance/reaction terpisah.
Dance kemungkinan terselip di frame 'tbulv' key 'aaa' (5 sub-message;
tiap sub punya f1 varint, f2 varint, f3 float). Analisis 3 tangkapan
lama menyimpulkan f3 bukan koordinat dunia (f4.f3 naik monoton seperti
timer) — tapi belum pernah dibandingkan SEBELUM vs SAAT dance.
Diag ini: login -> alokasi destinationUserId (SkyDev) -> TCP login ->
JoinedRoom -> kirim posisi 'C' (berdiri di samping Bartes) -> dengar
60 dtk, rekam SEMUA frame tbulv sebagai deret waktu ringkas
(t_rel, player_id 2 byte, f1/f2/f3 tiap sub-message 1-5). Bartes dance
satu gerakan spesifik selama jendela dengar; analisis membandingkan
nilai sebelum vs saat dance untuk menemukan field yang berubah.
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
    """Payload posisi 18 byte (event 'C'): [Teleported][seq][X/Y/Z/Rot f32 BE]."""
    return (bytes([teleported & 0xFF, sequence & 0xFF])
            + struct.pack(">f", x) + struct.pack(">f", y)
            + struct.pack(">f", z) + struct.pack(">f", rotation))


def compact_tbulv(data):
    """Ambil nilai ringkas dari payload tbulv: player_id + f1/f2/f3
    tiap sub-message. Kembalikan dict atau None bila gagal parse."""
    try:
        if len(data) < 13:
            return None
        pid = bytes(data[2:4]).hex()
        pb = T.parse_protobuf(bytes(data[12:]))
        subs = {}
        for field in (1, 2, 3, 4, 5):
            sub = pb.get(field)
            if isinstance(sub, (bytes, bytearray)):
                inner = T.parse_protobuf(bytes(sub))
                subs[field] = {
                    "f1": inner.get(1),
                    "f2": inner.get(2),
                    "f3": (round(inner.get(3), 4)
                           if isinstance(inner.get(3), float) else inner.get(3)),
                }
            else:
                subs[field] = None
        return {"pid": pid, "subs": subs}
    except Exception:
        return None


def find_joinedroom_in_frames(frames, rm):
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
    for fr in frames:
        if len(fr) >= 6 and fr[0] == 0x06 and fr[1] == 1 and int.from_bytes(fr[2:6], "big") == rm:
            return True
        if fr and fr[0] == 0x0d and needle in bytes(fr):
            return True
    return False


async def capture_once(session, email, password, device_id,
                       dest_user_id, bot_user_id, listen_s=60):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "listen_s": listen_s, "tbulv_count": 0, "series": []}
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
    sock, _pid = L.tcp_connect(host, port)
    if sock is None:
        print("[!] TCP connect gagal.")
        return hasil
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
    print(f"[+] JoinedRoom({rm}) = {joined_here}. Rekam deret waktu 'aaa' {listen_s} dtk — Bartes dance sekarang...")
    px, py, pz = -79.99, 21.96, -76.42
    seq = [0]
    try:
        sock.sendall(L.build_room_event_varint(
            "C", build_position_payload(px, py, pz, teleported=1, sequence=0)))
        print(f"[+] Posisi 'C' terkirim ({px},{py},{pz}) Teleported=1.")
    except Exception as exc:
        print(f"[!] kirim posisi 'C' gagal: {exc}")
    end = time.time() + listen_s
    last_hb = time.time()
    while time.time() < end:
        if time.time() - last_hb >= 1.0:
            try:
                seq[0] = (seq[0] + 1) & 0xFF
                sock.sendall(L.build_room_event_varint(
                    "C", build_position_payload(px, py, pz, teleported=0,
                                                sequence=seq[0])))
                last_hb = time.time()
            except Exception:
                pass
        for f in fr.drain(0.5):
            name, d = G.decode_room_event(f)
            if name == "tbulv" and d:
                hasil["tbulv_count"] += 1
                c = compact_tbulv(bytes(d))
                if c:
                    c["t"] = round(time.time() - t0, 1)
                    hasil["series"].append(c)
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. tbulv diterima: {hasil['tbulv_count']}, "
          f"direkam: {len(hasil['series'])}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("AAA_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("AAA_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("AAA_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("AAA_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("AAA_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--listen", type=float, default=60)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await capture_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id, args.listen)
    print("AAA_SERIES_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
