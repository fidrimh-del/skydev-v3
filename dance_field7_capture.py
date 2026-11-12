# -*- coding: utf-8 -*-
"""
dance_field7_capture.py — Verifikasi wire field 7 (ReactionIndex) di
user variable "aaa" saat Bartes dance.

Latar (2026-11-13 pagi, Bartes): disassembly membuktikan dance =
ReactionIndex, field 7 protobuf PlayerAnimationData di user variable
"aaa" (RoomEvent 'tbulv'), tag wire 0x38 varint. Analisis lama hanya
parse sub-message 1-5 sehingga field 7 tidak pernah terlihat.
Diag ini: login -> alokasi destinationUserId (SkyDev) -> TCP login ->
JoinedRoom -> kirim posisi 'C' (berdiri di samping Bartes) -> dengar
60 dtk, rekam untuk SETIAP frame tbulv: t, player_id 2 byte, field 6
(HoldableToggledOn bool) dan field 7 (ReactionIndex varint) tingkat
atas + progres MotionAnimation (s1.f3). Bartes dance SATU gerakan
terus-menerus lalu diam. Aturan keputusan: bila tag 0x38 + nomor dance
muncul saat dance dan hilang saat diam (protobuf hilangkan nilai 0)
-> terkonfirmasi, langsung bangun tes mirror (relay bytes 'aaa').
Bila 0x38 tak pernah muncul -> field 7 bukan channel live-nya.
Target = pemain TERDEKAT dengan bot (Bartes paling dekat).
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


def compact_aaa(data):
    """Parse payload tbulv -> {pid, f6, f7, s1_f3}.
    f6 = field 6 HoldableToggledOn (bool varint, tag 0x30).
    f7 = field 7 ReactionIndex (int32 varint, tag 0x38).
    f7 None = field 7 ABSEN (protobuf hilangkan nilai 0 = tidak dance)."""
    try:
        if len(data) < 13:
            return None
        pid = bytes(data[2:4]).hex()
        pb = T.parse_protobuf(bytes(data[12:]))
        f6 = pb.get(6)
        f7 = pb.get(7)
        s1_f3 = None
        sub1 = pb.get(1)
        if isinstance(sub1, (bytes, bytearray)):
            inner = T.parse_protobuf(bytes(sub1))
            if isinstance(inner.get(3), float):
                s1_f3 = round(inner.get(3), 4)
        return {"pid": pid, "f6": f6, "f7": f7, "s1_f3": s1_f3}
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
             "listen_s": listen_s, "tbulv_count": 0,
             "f7_seen": {}, "series": []}
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
    print(f"[+] JoinedRoom({rm}) = {joined_here}. Rekam field 6-7 'aaa' {listen_s} dtk — Bartes dance SATU gerakan terus-menerus, lalu diam...")
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
                c = compact_aaa(bytes(d))
                if c:
                    c["t"] = round(time.time() - t0, 1)
                    hasil["series"].append(c)
                    if c["f7"] is not None:
                        key = f"{c['pid']}:{c['f7']}"
                        hasil["f7_seen"][key] = hasil["f7_seen"].get(key, 0) + 1
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. tbulv diterima: {hasil['tbulv_count']}, "
          f"direkam: {len(hasil['series'])}, f7 terlihat: {json.dumps(hasil['f7_seen'])}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("F7_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("F7_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("F7_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("F7_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("F7_BOT_USER_ID", BOT_UUID))
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
    print("FIELD7_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
