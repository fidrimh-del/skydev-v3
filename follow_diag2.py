# -*- coding: utf-8 -*-
"""
follow_diag2.py — Diag tbulv (langkah 2 bot follow).

Temuan diag 1 (run 42577320695): RoomEvent 'tbulv' = 556 frame / 60 dtk
(~9.3/dtk) — frekuensi update posisi pemain (~10 Hz). Diag ini menangkap
FULL frame 'tbulv' (tanpa potong 48 byte) sebanyak N frame berurutan,
supaya bisa dianalisis offline: cari 3 float posisi (x,y,z) yang berubah
halus saat target bergerak.

Bot login -> alokasi destinationUserId -> JoinRoom -> tangkap tbulv.
Bot TIDAK bergerak / TIDAK kirim posisi. Hanya membaca.

PENTING: target (SkyDev) harus ONLINE dan BERGERAK (jalan lurus /
pindah tempat jelas) selama tes, supaya float posisi berubah dan
bisa dibedakan dari field lain.
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G


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


def parse_tbulv_floats(data):
    """Coba ekstrak semua field fixed32 (float) dari payload tbulv.
    Kembalikan list (offset, nilai_float). Hanya untuk analisis."""
    out = []
    # payload tbulv diawali struktur tertentu; scan byte demi byte cari
    # pola protobuf field fixed32 (tag 0x1d/0x25/0x2d/0x35... = wire type 5)
    # Sederhana: coba parse protobuf generik, kumpulkan fixed32.
    i = 0
    try:
        while i < len(data):
            tag = data[i]
            field = tag >> 3
            wire = tag & 7
            i += 1
            if wire == 5:  # fixed32
                if i + 4 > len(data):
                    break
                val = struct.unpack("<f", data[i:i + 4])[0]
                out.append((i - 1, field, val))
                i += 4
            elif wire == 0:  # varint
                while i < len(data) and data[i] & 0x80:
                    i += 1
                i += 1
            elif wire == 2:  # length-delimited
                ln, m = L.read_varint(data, i)
                if ln is None:
                    break
                i += m + ln
            elif wire == 1:  # fixed64
                i += 8
            else:
                break
    except Exception:
        pass
    return out


async def follow_diag2_once(session, email, password, device_id,
                            dest_user_id, n_frames):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "server": "", "room": None, "joined_room": None,
             "tbulv_count": 0, "tbulv_frames": []}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil
    print(f"[*] alokasi destinationUserId={dest_user_id} ...")
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
    got, closed = fr.read_until(
        lambda f: G._is_tbugv(f) or G._is_joinedroom(f), 3)
    if closed:
        print("[!] koneksi tertutup saat tunggu login.")
        return hasil
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
    print(f"[*] kirim JoinRoom({rm}) ...")
    try:
        sock.sendall(L.build_join_frame(rm))
    except Exception as exc:
        print(f"[!] kirim JoinRoom gagal: {exc}")
        return hasil

    def _is_target_room(f):
        return (len(f) >= 6 and f[0] == 0x06 and f[1] == 1
                and int.from_bytes(f[2:6], "big") == rm)

    joined, closed = fr.read_until(_is_target_room, 4)
    if closed:
        print("[!] koneksi tertutup saat JoinRoom.")
        return hasil
    if joined is not None:
        hasil["joined_room"] = rm
        print(f"[+] JoinedRoom({rm}) TERKONFIRMASI.")
    else:
        print("[!] TIDAK ada konfirmasi JoinedRoom — lanjut tangkap tbulv.")

    # --- tangkap N frame tbulv FULL ---
    print(f"[*] menangkap {n_frames} frame 'tbulv' FULL ...")
    print("[*] (SkyDev HARUS online + BERGERAK selama ini)")
    frames = []
    t_end = time.time() + 90
    while len(frames) < n_frames and time.time() < t_end:
        for f in fr.drain(2):
            name, data = G.decode_room_event(f)
            if name == "tbulv" and data:
                frames.append(data.hex())
                if len(frames) >= n_frames:
                    break
    try:
        sock.close()
    except Exception:
        pass
    hasil["tbulv_count"] = len(frames)
    hasil["tbulv_frames"] = frames
    print(f"[+] tertangkap {len(frames)} frame tbulv FULL.")
    # Analisis cepat: float fixed32 di tiap frame (offset yang berubah)
    for idx, hx in enumerate(frames[:5]):
        data = bytes.fromhex(hx)
        fl = parse_tbulv_floats(data)
        print(f"    tbulv#{idx} len={len(data)} floats={[(o, round(v, 3)) for _, o, v in fl]}")
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("FOLLOW_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("FOLLOW_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("FOLLOW_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--dest-user-id",
                    default=os.getenv("FOLLOW_DEST_USER_ID",
                                      "aae2efe1-1abb-48c2-8799-ea192692a49c"))
    ap.add_argument("--n-frames", type=int,
                    default=int(os.getenv("FOLLOW_N_FRAMES", "15")))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await follow_diag2_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.n_frames)
    print("FOLLOWDIAG2_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    asyncio.run(main())
