# -*- coding: utf-8 -*-
"""
follow_diag.py — Bot mengikuti Bartes secara live via event posisi 'C'.

MENGGANTIKAN diag tangkap-frame 2026-11-09 (yang hanya membaca).
Latar (2026-11-12, Bartes): follow sempat DIBLOKIR karena posisi pemain
lain dianggap hanya lewat UDP (bot TCP-only). Bukti baru membatalkan
asumsi itu: tangkapan dance (run 43230187436) menunjukkan bot menerima
450 event 'C' dalam 30 dtk via TCP; tangkapan 'C' (run 43232385738) +
dekompresi lokal MEMBUKTIKAN posisi Bartes (-79.99,21.96,-76.42) ada di
aliran 'C' sebagai record 18 byte [Teleported][seq][X/Y/Z/Rot f32 BE]
(batch = N record 18 byte, panjang decompressed 18n+1; seq Bartes
terlihat naik 0,1,2 di sampel). Jadi bot BISA membaca posisi live
Bartes lewat TCP.
Cara kerja: login -> alokasi destinationUserId (SkyDev) -> TCP login ->
JoinedRoom -> kirim 'tbulv' sekali (supaya bot tetap terlihat) + posisi
'C' awal di posisi Bartes -> loop: baca frame, dekompresi event 'C'
(raw Deflate), cari posisi yang PALING DEKAT dengan posisi terakhir
Bartes (dalam radius 15 unit), update target, kirim posisi 'C' bot ke
target + offset 1.5 unit di samping Bartes (bukan di dalamnya). Kirim
'tbulv' tiap 5 dtk agar bot tetap aktif/terlihat (Bartes: bot tidak
sampai 60 dtk online — kemungkinan karena diag sebelumnya tidak kirim
'tbulv' berkala). Bartes jalan pelan selama tes; bot mengikutinya.
Durasi default 30 dtk (bukan 60) supaya bot tidak keburu hilang.
"""
import argparse
import asyncio
import json
import math
import os
import struct
import time
import zlib

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


def build_tbulv_idle():
    """Frame 'tbulv' sekali (state idle) — sama seperti spawn_test_diag.py:
    prefix 00 06 <ID bot 2 byte> + 'aaa' 5 sub-message (f3 saja)."""
    import spawn_test_diag as S
    return S.build_spawn_aaa()


def parse_c_batch(data):
    """Dekompresi payload event 'C' (raw Deflate) -> list posisi
    (x, y, z, rot, teleported, seq). Batch = N record 18 byte
    [Teleported][seq][X/Y/Z/Rot f32 BE] (+1 byte ekstra)."""
    try:
        dec = zlib.decompress(bytes(data), -15)
    except Exception:
        return []
    out = []
    # Scan record 18 byte dari offset 0 (dan offset 1 bila ada prefix).
    for base in (0, 1):
        i = base
        while i + 18 <= len(dec):
            tele, seq = dec[i], dec[i + 1]
            try:
                x, y, z, rot = struct.unpack(">ffff", dec[i + 2:i + 18])
            except Exception:
                break
            if math.isfinite(x) and math.isfinite(y) and math.isfinite(z):
                out.append((x, y, z, rot, tele, seq))
            i += 18
        if out:
            break
    return out


def find_joinedroom_in_frames(frames, rm):
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
    for fr in frames:
        if len(fr) >= 6 and fr[0] == 0x06 and fr[1] == 1 and int.from_bytes(fr[2:6], "big") == rm:
            return True
        if fr and fr[0] == 0x0d and needle in bytes(fr):
            return True
    return False


async def follow_once(session, email, password, device_id,
                      dest_user_id, bot_user_id, duration_s=30,
                      start_x=-79.99, start_y=21.96, start_z=-76.42):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "duration_s": duration_s,
             "target_start": [start_x, start_y, start_z],
             "target_seen": 0, "target_updates": [],
             "bot_sends": 0, "c_received": 0}
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
    print(f"[+] JoinedRoom({rm}) = {joined_here}. FOLLOW dimulai {duration_s} dtk — Bartes jalan pelan...")
    # Posisi target awal = posisi Bartes yang diketahui.
    tx, ty, tz = start_x, start_y, start_z
    seq = [0]

    def send_bot_pos(x, y, z, teleported):
        seq[0] = (seq[0] + 1) & 0xFF
        try:
            sock.sendall(L.build_room_event_varint(
                "C", build_position_payload(x, y, z, teleported=teleported,
                                            sequence=seq[0])))
            hasil["bot_sends"] += 1
        except Exception:
            pass

    # tbulv sekali + posisi awal (berdiri di samping Bartes).
    try:
        sock.sendall(L.build_room_event_varint("tbulv", build_tbulv_idle()))
    except Exception as exc:
        print(f"[!] kirim tbulv gagal: {exc}")
    send_bot_pos(tx + 1.5, ty, tz, teleported=1)
    print(f"[+] Bot di ({tx + 1.5:.2f},{ty:.2f},{tz:.2f}), target Bartes ({tx:.2f},{ty:.2f},{tz:.2f})")
    end = time.time() + duration_s
    last_hb = time.time()
    last_tbulv = time.time()
    last_target_seen = time.time()
    while time.time() < end:
        now = time.time()
        # Heartbeat posisi bot tiap 1 dtk (ke posisi target terakhir).
        if now - last_hb >= 1.0:
            send_bot_pos(tx + 1.5, ty, tz, teleported=0)
            last_hb = now
        # tbulv tiap 5 dtk agar bot tetap aktif/terlihat.
        if now - last_tbulv >= 5.0:
            try:
                sock.sendall(L.build_room_event_varint("tbulv", build_tbulv_idle()))
            except Exception:
                pass
            last_tbulv = now
        for f in fr.drain(0.2):
            name, d = G.decode_room_event(f)
            if name == "C" and d:
                hasil["c_received"] += 1
                positions = parse_c_batch(d)
                # Cari posisi terdekat dengan target terakhir (radius 15).
                best = None
                best_dist = 15.0
                for (x, y, z, rot, tele, s) in positions:
                    dist = math.sqrt((x - tx) ** 2 + (y - ty) ** 2 + (z - tz) ** 2)
                    if dist < best_dist:
                        best_dist = dist
                        best = (x, y, z)
                if best is not None:
                    tx, ty, tz = best
                    hasil["target_seen"] += 1
                    last_target_seen = now
                    if len(hasil["target_updates"]) < 40:
                        hasil["target_updates"].append({
                            "t": round(now - t0, 1),
                            "x": round(tx, 2), "y": round(ty, 2),
                            "z": round(tz, 2),
                        })
                    # Langsung pindah bot ke samping target baru.
                    send_bot_pos(tx + 1.5, ty, tz, teleported=0)
    try:
        sock.close()
    except Exception:
        pass
    hasil["target_final"] = [round(tx, 2), round(ty, 2), round(tz, 2)]
    print(f"[+] selesai. 'C' diterima: {hasil['c_received']}, target terlihat: "
          f"{hasil['target_seen']}x, bot kirim posisi: {hasil['bot_sends']}x, "
          f"target akhir: {hasil['target_final']}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("FOLLOW_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("FOLLOW_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("FOLLOW_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("FOLLOW_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("FOLLOW_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--start-x", type=float, default=-79.99)
    ap.add_argument("--start-y", type=float, default=21.96)
    ap.add_argument("--start-z", type=float, default=-76.42)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await follow_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id, args.duration,
            args.start_x, args.start_y, args.start_z)
    print("FOLLOW_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
