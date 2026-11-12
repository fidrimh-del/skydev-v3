# -*- coding: utf-8 -*-
"""
c_position_capture.py — Tangkap event posisi 'C' yang diteruskan server
(relay) untuk memahami formatnya sebelum tes follow.

Latar (2026-11-12, Bartes): tangkapan dance (run 43230187436) menunjukkan
bot menerima 450 event 'C' dalam 30 dtk — bukan cuma kiriman bot sendiri
(30x), jadi server meneruskan posisi pemain lain lewat TCP (terkompresi
Deflate, sesuai OnPlayerPositionUpdate RVA 0x74B8934). Ini MEMBUKA
kemungkinan follow: bot bisa membaca posisi live Bartes. Tapi format
relay belum dipahami: payload contoh 92 byte (jauh lebih besar dari
18 byte posisi mentah) — apakah ada ID pengirim di frame, dan apakah
Deflate-decompress menghasilkan 18 byte [Teleported][seq][X/Y/Z/Rot]?
Diag ini: login -> alokasi destinationUserId (SkyDev) -> TCP login ->
JoinedRoom -> kirim posisi 'C' (berdiri di samping Bartes) -> dengar
30 dtk, rekam SEMUA payload mentah event 'C' (hex, maks 60 sampel) +
hitungannya. Analisis lokal: coba Deflate-decompress tiap sampel,
cocokkan koordinat dengan posisi Bartes (-79.99,21.96,-76.42) untuk
mengidentifikasi aliran 'C' milik Bartes.
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G

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


def find_joinedroom_in_frames(frames, rm):
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
    for fr in frames:
        if len(fr) >= 6 and fr[0] == 0x06 and fr[1] == 1 and int.from_bytes(fr[2:6], "big") == rm:
            return True
        if fr and fr[0] == 0x0d and needle in bytes(fr):
            return True
    return False


async def capture_once(session, email, password, device_id,
                       dest_user_id, bot_user_id, listen_s=30, max_samples=60):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "listen_s": listen_s, "c_count": 0, "c_samples": [],
             "c_lens": {}}
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
    print(f"[+] JoinedRoom({rm}) = {joined_here}. Tangkap event 'C' {listen_s} dtk...")
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
            if name == "C" and d:
                hasil["c_count"] += 1
                hasil["c_lens"][str(len(d))] = hasil["c_lens"].get(str(len(d)), 0) + 1
                if len(hasil["c_samples"]) < max_samples:
                    hasil["c_samples"].append({
                        "t": round(time.time() - t0, 1),
                        "len": len(d),
                        "hex": bytes(d).hex(),
                    })
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. event 'C' diterima: {hasil['c_count']}, "
          f"sampel: {len(hasil['c_samples'])}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("CPOS_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("CPOS_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("CPOS_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("CPOS_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("CPOS_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--listen", type=float, default=30)
    ap.add_argument("--max-samples", type=int, default=60)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await capture_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id,
            args.listen, args.max_samples)
    print("CPOS_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
