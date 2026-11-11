# -*- coding: utf-8 -*-
"""
follow_diag.py — Diag posisi pemain (langkah 1 bot follow).

Bot login -> alokasi destinationUserId (server teman) -> TCP -> JoinRoom ->
lalu TANGKAP SEMUA frame mentah selama N detik dan log tipe + panjang +
preview hex. Tujuannya: identifikasi frame apa yang membawa update posisi
pemain lain (SkyDev), dan formatnya.

Ini DIAG saja — bot TIDAK bergerak / TIDAK kirim posisi. Hanya membaca
dan mencatat. Bila frame posisi teridentifikasi, langkah berikut (follow)
baru bisa dibangun.

PENTING: target (SkyDev) harus ONLINE dan BERGERAK di game selama tes,
supaya update posisinya mengalir dan terlihat berubah di log.

Dibuat 2026-11-09 atas permintaan Bartes (tes bot teman bermain / follow).
Jangan dipakai untuk order.
"""
import argparse
import asyncio
import json
import os
import time
from collections import Counter

import lookup_uuid as L
import gift_diag as G


def parse_gsid(gsid):
    """'dc::server::room' -> (server, room_int_atau_None)."""
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
    """Alokasi ke server seorang teman (destinationUserId)."""
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


async def follow_diag_once(session, email, password, device_id,
                           dest_user_id, capture_seconds):
    """Satu percobaan diag posisi. Kembalikan dict hasil."""
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "server": "", "room": None, "joined_room": None,
             "capture_seconds": capture_seconds,
             "frame_count": 0, "by_type": {}, "room_events": {},
             "samples": []}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil

    # --- alokasi ke server teman ---
    print(f"[*] alokasi destinationUserId={dest_user_id} ...")
    body = await allocate_to_user(session, user_jwt, dest_user_id)
    if not body:
        print("[!] alokasi destinationUserId gagal/ kosong.")
        return hasil
    srv, rm = parse_gsid(body.get("gameServerId", ""))
    hasil["server"] = srv
    hasil["room"] = rm
    print(f"[+] destinationUserId OK: server={srv} room={rm}")
    if not srv or rm is None:
        print("[!] gameServerId tidak lengkap — berhenti.")
        return hasil
    host, port = L.find_host_port(body)
    if not host or not port:
        print("[!] host/port tidak ketemu di allocation.")
        return hasil
    print(f"[+] server: {host}:{port} room {rm}")

    # --- TCP + login ---
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
        print("[*] tbugv diterima — observasi 1 dtk lagi.")
        time.sleep(1)
    elif got is not None:
        print("[*] JoinedRoom diterima (auto-join).")
    else:
        print("[*] timeout jendela login (koneksi hidup) — lanjut.")

    # --- JoinRoom(room target) ---
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
        print(f"[+] JoinedRoom({rm}) TERKONFIRMASI — bot di room SkyDev.")
    else:
        print(f"[!] TIDAK ada konfirmasi JoinedRoom({rm}) dalam 4 dtk. "
              f"Lanjut tangkap frame (bot mungkin di room auto-join).")

    # --- tangkap SEMUA frame mentah ---
    print(f"[*] menangkap SEMUA frame selama {capture_seconds} dtk ...")
    print(f"[*] (SkyDev HARUS online + bergerak di game selama ini)")
    by_type = Counter()
    room_events = Counter()
    samples = {}  # tipe -> list contoh hex (maks 3 per tipe)
    t_end = time.time() + capture_seconds
    total = 0
    while time.time() < t_end:
        chunk = min(5, max(1, int(t_end - time.time())))
        for f in fr.drain(chunk):
            total += 1
            t = f[0] if f else -1
            by_type[t] += 1
            if t == 0x08:
                name, data = G.decode_room_event(f)
                if name:
                    room_events[name] += 1
            # simpan contoh hex (maks 3 per tipe, 48 byte pertama)
            lst = samples.setdefault(t, [])
            if len(lst) < 3:
                lst.append(f[:48].hex())
    try:
        sock.close()
    except Exception:
        pass

    hasil["frame_count"] = total
    hasil["by_type"] = {f"0x{k:02x}": v for k, v in by_type.items()}
    hasil["room_events"] = dict(room_events)
    hasil["samples"] = {f"0x{k:02x}": v for k, v in samples.items()}
    print(f"[+] total frame tertangkap: {total}")
    print(f"[+] per tipe: {hasil['by_type']}")
    print(f"[+] RoomEvent (0x08) per nama: {hasil['room_events']}")
    for k, v in hasil["samples"].items():
        for s in v:
            print(f"    contoh {k} ({len(bytes.fromhex(s))} B): {s}")
    return hasil


async def main():
    # Baca versi game/protokol dari dashboard panel (app_settings).
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("FOLLOW_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("FOLLOW_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("FOLLOW_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--dest-user-id",
                    default=os.getenv("FOLLOW_DEST_USER_ID",
                                      "aae2efe1-1abb-48c2-8799-ea192692a49c"))
    ap.add_argument("--capture-seconds", type=int,
                    default=int(os.getenv("FOLLOW_CAPTURE_SECONDS", "60")))
    args = ap.parse_args()

    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await follow_diag_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.capture_seconds)

    # Satu baris JSON ringkas untuk parsing dari log.
    print("FOLLOWDIAG_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    asyncio.run(main())
