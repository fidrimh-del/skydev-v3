# -*- coding: utf-8 -*-
"""
plaza_first_school_diag.py — Tes hipotesis Bartes 2026-11-13:
bot harus ke map utama (Plaza) DULU, baru join teman ke Sekolah.

Latar: bot selama ini join Sekolah LANGSUNG via alokasi
destinationUserId — dan wujudnya TIDAK PERNAH terlihat di Sekolah
(ESP hanya lihat titiknya, kotak hijau kosong), padahal di Plaza
wujudnya TERBUKTI muncul. Pemain asli selalu lewat map utama dulu.
Dugaan: server baru daftarkan bot sebagai pemain penuh (RoomUserUpdate)
lewat jalur map utama itu.

Alur (SATU variabel yang diubah = urutan Plaza dulu; sisanya sama
dengan tes Sekolah sebelumnya):
  Fase 1 — Plaza (alokasi SENDIRI, tanpa destinationUserId, dari
    L.login_and_allocate): TCP login -> SendDeviceId
    ('Analytics/deviceId') -> 'tbulv' idle -> posisi 'C' di koordinat
    Bartes kemarin (-79.99, 21.96, -76.42) + heartbeat 'C' tiap 1 dtk
    + 'tbulv' tiap 5 dtk selama plaza_s (default 30 dtk).
  Fase 2 — Sekolah (alokasi destinationUserId = Bartes): putus TCP
    Plaza, alokasi ulang ke Bartes, TCP login ke server activity ->
    SendDeviceId -> 'tbulv' idle -> dengar 'C' 10 dtk, spawn di posisi
    pemain paling sering terlihat (+1.5 unit X); bila TIDAK ADA 'C'
    sama sekali, fallback ke lantai Sekolah (5.80, 2.01, 830.87) ->
    heartbeat 'C' tiap 1 dtk + 'tbulv' tiap 5 dtk selama school_s
    (default 60 dtk). Bartes cek di game apakah wujud bot muncul.
TIDAK kirim JoinRoom eksplisit (server auto-place; JoinedRoom dari
batch 0x0d). TIDAK kirim invite/consent — tes ini HANYA soal wujud.
Verdict utama = cek in-game Bartes (ESP + mata).
"""
import argparse
import asyncio
import json
import math
import os
import struct
import time
import zlib
from collections import Counter

import lookup_uuid as L
import gift_diag as G
import spawn_test_diag as S

SKYDEV_UUID = "aae2efe1-1abb-48c2-8799-ea192692a49c"
BOT_UUID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"

# Koordinat Bartes kemarin di Plaza (jangkar yang dipakai semua tes).
PLAZA_X = -79.99
PLAZA_Y = 21.96
PLAZA_Z = -76.42
# Lantai Sekolah dari tes smart spawn sebelumnya (fallback bila 'C' kosong).
SCHOOL_FALLBACK = (5.80, 2.01, 830.87)


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
    """Payload posisi 'C' 18 byte (TERBUKTI, disassembly 2026-11-12):
    [Teleported 1B][sequence 1B][X/Y/Z/Rot f32 BE]."""
    return (bytes([teleported & 0xFF, sequence & 0xFF])
            + struct.pack(">f", x) + struct.pack(">f", y)
            + struct.pack(">f", z) + struct.pack(">f", rotation))


def parse_c_batch(data):
    """Dekompresi payload event 'C' (raw Deflate) -> list posisi
    (x, y, z, rot, teleported, seq). Batch = N record 18 byte."""
    try:
        dec = zlib.decompress(bytes(data), -15)
    except Exception:
        return []
    out = []
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


def wait_login_window(fr, rm):
    """Tunggu tbugv/JoinedRoom (maks 3 dtk). Kembalikan (joined, got)."""
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
        return False, got
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
    joined = False
    if got is not None and not G._is_tbugv(got):
        joined = True
    if not joined:
        joined = find_joinedroom_in_frames(fr.drain(2), rm)
    return joined, got


def track_room_user_update(state, raw, name, watch, t0, label):
    """Heuristik RoomUserUpdate (sama seperti spawn_test_diag):
    UUID target/bot muncul di frame (bukan consent)."""
    if state["seen"]:
        return
    for u in watch:
        if u.encode("utf-8") in raw:
            if name == "consent":
                continue
            state.update({"seen": True,
                          "first_t_rel": round(time.time() - t0, 1),
                          "first_after": label,
                          "uuid": u[:8] + "...",
                          "frame_type": raw[0] if raw else None,
                          "event": name or "",
                          "hex_head": raw[:120].hex()})
            print(f"[+] KANDIDAT RoomUserUpdate t+{state['first_t_rel']} dtk "
                  f"(sesudah {label}): UUID {u[:8]}... event '{name}'.")
            break


def phase_plaza(sock, pid, device_id, plaza_pos, plaza_s, watch, t0):
    """Fase 1: bot sudah TCP-login di server Plaza-nya sendiri.
    Kirim SendDeviceId + tbulv + posisi 'C', heartbeat selama plaza_s."""
    hasil = {"server_room": None, "joined_room": None,
             "pos": [round(plaza_pos[0], 2), round(plaza_pos[1], 2),
                     round(plaza_pos[2], 2)],
             "heartbeat_sent": 0, "tbulv_sent": 0,
             "room_user_update": {"seen": False, "first_t_rel": None,
                                  "first_after": None, "uuid": None,
                                  "frame_type": None, "event": None,
                                  "hex_head": None},
             "event_names": {}, "c_received": 0}
    sx, sy, sz = plaza_pos
    seq = [0]

    def send_c(x, y, z, teleported):
        seq[0] = (seq[0] + 1) & 0xFF
        try:
            sock.sendall(L.build_room_event_varint(
                "C", build_position_payload(x, y, z, rotation=0.0,
                                            teleported=teleported,
                                            sequence=seq[0])))
            hasil["heartbeat_sent"] += 1
        except Exception:
            pass

    def send_tbulv():
        try:
            sock.sendall(L.build_room_event_varint(
                "tbulv", S.build_spawn_aaa(sx, sy, sz, player_id=pid)))
            hasil["tbulv_sent"] += 1
        except Exception:
            pass

    fr = G.FrameReader(sock)
    send_tbulv()
    send_c(sx, sy, sz, teleported=1)
    print(f"[+] Plaza: posisi 'C' terkirim ({sx:.2f},{sy:.2f},{sz:.2f}) Teleported=1.")
    try:
        sock.sendall(L.build_room_event_varint(
            "Analytics/deviceId", (device_id or DEFAULT_DEVICE_ID).encode("utf-8")))
        print(f"[+] Plaza: SendDeviceId terkirim.")
    except Exception as exc:
        print(f"[!] Plaza: kirim SendDeviceId gagal: {exc}")
    end = time.time() + plaza_s
    last_hb = time.time()
    last_tb = time.time()
    while time.time() < end:
        now = time.time()
        if now - last_hb >= 1.0:
            send_c(sx, sy, sz, teleported=0)
            last_hb = now
        if now - last_tb >= 5.0:
            send_tbulv()
            last_tb = now
        for f in fr.drain(0.5):
            raw = bytes(f)
            name, d = G.decode_room_event(f)
            if name:
                hasil["event_names"][name] = hasil["event_names"].get(name, 0) + 1
                if name == "C":
                    hasil["c_received"] += 1
            track_room_user_update(hasil["room_user_update"], raw, name,
                                   watch, t0, "plaza")
    print(f"[+] Plaza selesai: heartbeat 'C' {hasil['heartbeat_sent']}x, "
          f"tbulv {hasil['tbulv_sent']}x, RoomUserUpdate: "
          f"{hasil['room_user_update']['seen']}")
    return hasil


def phase_school(sock, pid, device_id, dest_user_id, bot_user_id,
                 fallback_pos, school_s, watch, t0):
    """Fase 2: bot sudah TCP-login di server Sekolah (alokasi ke Bartes).
    Dengar 'C' 10 dtk -> spawn di posisi pemain paling sering terlihat
    (+1.5 X); bila 'C' kosong, fallback lantai Sekolah. Heartbeat."""
    hasil = {"server_room": None, "joined_room": None,
             "c_received": 0, "c_positions_seen": 0,
             "most_observed_pos": None, "most_observed_count": 0,
             "spawn_pos": None, "spawn_mode": None,
             "heartbeat_sent": 0, "tbulv_sent": 0,
             "room_user_update": {"seen": False, "first_t_rel": None,
                                  "first_after": None, "uuid": None,
                                  "frame_type": None, "event": None,
                                  "hex_head": None},
             "event_names": {}}
    fr = G.FrameReader(sock)
    # Dengar 'C' 10 dtk dulu untuk cari posisi pemain (Bartes).
    pos_counter = Counter()
    end_listen = time.time() + 10
    while time.time() < end_listen:
        for f in fr.drain(0.5):
            raw = bytes(f)
            name, d = G.decode_room_event(f)
            if name:
                hasil["event_names"][name] = hasil["event_names"].get(name, 0) + 1
                if name == "C" and d:
                    hasil["c_received"] += 1
                    for (x, y, z, rot, tele, s) in parse_c_batch(d):
                        hasil["c_positions_seen"] += 1
                        pos_counter[(round(x, 1), round(y, 1), round(z, 1))] += 1
            track_room_user_update(hasil["room_user_update"], raw, name,
                                   watch, t0, "school-listen")
    if pos_counter:
        (mx, my, mz), cnt = pos_counter.most_common(1)[0]
        hasil["most_observed_pos"] = [mx, my, mz]
        hasil["most_observed_count"] = cnt
        sx, sy, sz = mx + 1.5, my, mz
        smode = "most-observed-C"
    else:
        sx, sy, sz = fallback_pos
        smode = "fallback-floor"
    hasil["spawn_pos"] = [round(sx, 2), round(sy, 2), round(sz, 2)]
    hasil["spawn_mode"] = smode
    print(f"[+] Sekolah: 'C' diterima {hasil['c_received']}x, posisi terlihat "
          f"{hasil['c_positions_seen']}x, paling sering: "
          f"{hasil['most_observed_pos']} ({hasil['most_observed_count']}x).")
    print(f"[+] Sekolah: bot spawn di ({sx:.2f},{sy:.2f},{sz:.2f}) [{smode}].")
    seq = [0]

    def send_c(x, y, z, teleported):
        seq[0] = (seq[0] + 1) & 0xFF
        try:
            sock.sendall(L.build_room_event_varint(
                "C", build_position_payload(x, y, z, rotation=0.0,
                                            teleported=teleported,
                                            sequence=seq[0])))
            hasil["heartbeat_sent"] += 1
        except Exception:
            pass

    def send_tbulv():
        try:
            sock.sendall(L.build_room_event_varint(
                "tbulv", S.build_spawn_aaa(sx, sy, sz, player_id=pid)))
            hasil["tbulv_sent"] += 1
        except Exception:
            pass

    send_tbulv()
    send_c(sx, sy, sz, teleported=1)
    try:
        sock.sendall(L.build_room_event_varint(
            "Analytics/deviceId", (device_id or DEFAULT_DEVICE_ID).encode("utf-8")))
        print(f"[+] Sekolah: SendDeviceId terkirim.")
    except Exception as exc:
        print(f"[!] Sekolah: kirim SendDeviceId gagal: {exc}")
    end = time.time() + school_s
    last_hb = time.time()
    last_tb = time.time()
    while time.time() < end:
        now = time.time()
        if now - last_hb >= 1.0:
            send_c(sx, sy, sz, teleported=0)
            last_hb = now
        if now - last_tb >= 5.0:
            send_tbulv()
            last_tb = now
        for f in fr.drain(0.5):
            raw = bytes(f)
            name, d = G.decode_room_event(f)
            if name:
                hasil["event_names"][name] = hasil["event_names"].get(name, 0) + 1
                if name == "C" and d:
                    hasil["c_received"] += 1
                    hasil["c_positions_seen"] += len(parse_c_batch(d))
            track_room_user_update(hasil["room_user_update"], raw, name,
                                   watch, t0, "school")
    print(f"[+] Sekolah selesai: heartbeat 'C' {hasil['heartbeat_sent']}x, "
          f"tbulv {hasil['tbulv_sent']}x, RoomUserUpdate: "
          f"{hasil['room_user_update']['seen']}, spawn: {hasil['spawn_pos']} "
          f"[{hasil['spawn_mode']}]")
    return hasil


async def plaza_first_once(session, email, password, device_id,
                           dest_user_id, bot_user_id,
                           plaza_pos, plaza_s, school_s, fallback_pos):
    t0 = time.time()
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "plaza": {}, "school": {}}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi (Plaza sendiri) gagal.")
        return hasil
    # ---- FASE 1: Plaza (alokasi sendiri, tanpa destinationUserId) ----
    srv, rm = parse_gsid(alloc.get("gameServerId", ""))
    print(f"[+] Fase 1 Plaza: server={srv} room={rm}")
    if not srv or rm is None:
        print("[!] Fase 1: server/room Plaza tidak ketemu.")
        return hasil
    host, port = L.find_host_port(alloc)
    if not host or not port:
        print("[!] Fase 1: host/port Plaza tidak ketemu.")
        return hasil
    sock, pid = L.tcp_connect(host, port)
    if sock is None:
        print("[!] Fase 1: TCP connect Plaza gagal.")
        return hasil
    print(f"[+] Fase 1 Connected Plaza: playerId={pid}.")
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        print(f"[!] Fase 1: kirim login frame gagal: {exc}")
        return hasil
    fr = G.FrameReader(sock)
    joined, _ = wait_login_window(fr, rm)
    print(f"[+] Fase 1 JoinedRoom({rm}) = {joined}.")
    ph = phase_plaza(sock, pid, device_id, plaza_pos, plaza_s,
                     [dest_user_id, bot_user_id], t0)
    ph["server_room"] = f"{srv}::{rm}"
    ph["joined_room"] = rm if joined else None
    hasil["plaza"] = ph
    try:
        sock.close()
    except Exception:
        pass
    print("[*] Fase 1 selesai — putus Plaza, alokasi ulang ke Bartes (Sekolah)...")
    # ---- FASE 2: Sekolah (alokasi destinationUserId = Bartes) ----
    body = await allocate_to_user(session, user_jwt, dest_user_id)
    if not body:
        print("[!] Fase 2: alokasi destinationUserId gagal/kosong.")
        return hasil
    srv2, rm2 = parse_gsid(body.get("gameServerId", ""))
    print(f"[+] Fase 2 Sekolah: server={srv2} room={rm2}")
    if not srv2 or rm2 is None:
        print("[!] Fase 2: server/room Sekolah tidak ketemu.")
        return hasil
    host2, port2 = L.find_host_port(body)
    if not host2 or not port2:
        print("[!] Fase 2: host/port Sekolah tidak ketemu.")
        return hasil
    sock2, pid2 = L.tcp_connect(host2, port2)
    if sock2 is None:
        print("[!] Fase 2: TCP connect Sekolah gagal.")
        return hasil
    print(f"[+] Fase 2 Connected Sekolah: playerId={pid2}.")
    try:
        sock2.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        print(f"[!] Fase 2: kirim login frame gagal: {exc}")
        return hasil
    fr2 = G.FrameReader(sock2)
    joined2, _ = wait_login_window(fr2, rm2)
    print(f"[+] Fase 2 JoinedRoom({rm2}) = {joined2}.")
    sh = phase_school(sock2, pid2, device_id, dest_user_id, bot_user_id,
                      fallback_pos, school_s, [dest_user_id, bot_user_id], t0)
    sh["server_room"] = f"{srv2}::{rm2}"
    sh["joined_room"] = rm2 if joined2 else None
    hasil["school"] = sh
    try:
        sock2.close()
    except Exception:
        pass
    print(f"[+] selesai. Plaza RoomUserUpdate: "
          f"{hasil['plaza'].get('room_user_update', {}).get('seen')}, "
          f"Sekolah RoomUserUpdate: "
          f"{hasil['school'].get('room_user_update', {}).get('seen')}, "
          f"Sekolah spawn: {hasil['school'].get('spawn_pos')} "
          f"[{hasil['school'].get('spawn_mode')}]")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("PLAZA_FIRST_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("PLAZA_FIRST_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("PLAZA_FIRST_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("PLAZA_FIRST_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("PLAZA_FIRST_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--plaza-x", type=float, default=PLAZA_X)
    ap.add_argument("--plaza-y", type=float, default=PLAZA_Y)
    ap.add_argument("--plaza-z", type=float, default=PLAZA_Z)
    ap.add_argument("--plaza-s", type=float, default=30)
    ap.add_argument("--school-s", type=float, default=60)
    ap.add_argument("--school-fallback-x", type=float, default=SCHOOL_FALLBACK[0])
    ap.add_argument("--school-fallback-y", type=float, default=SCHOOL_FALLBACK[1])
    ap.add_argument("--school-fallback-z", type=float, default=SCHOOL_FALLBACK[2])
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await plaza_first_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id,
            (args.plaza_x, args.plaza_y, args.plaza_z),
            args.plaza_s, args.school_s,
            (args.school_fallback_x, args.school_fallback_y,
             args.school_fallback_z))
    print("PLAZA_FIRST_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
