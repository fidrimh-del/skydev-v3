# -*- coding: utf-8 -*-
"""
activity_join_diag.py — Tes Tahap B (H1): bot join activity lewat
alokasi ACTIVITY, bukan alokasi destinationUserId.

Latar (analisis Tahap A, ~/workspace/transition_message_analysis.md):
- TIDAK ADA pesan transisi Plaza->activity. Client asli buang koneksi
  Plaza -> HTTP allocation baru -> TCP baru -> login ulang.
- Pembedanya = field alokasi. Bot selama ini pakai
  POST /lobby/allocation + destinationUserId -> server anggap bot
  "ikut user" (OnlineJoiningFriend=1). Client asli pakai
  POST /lobby/activity/allocation + AllocateActivityRequest
  (userToken, activityId, gameServerProtocolVersion, datacenterId)
  -> server anggap "pemain activity" (JoiningActivity=3 /
  InitialConnectionToActivity=6).
- Script ini mencoba endpoint activity dulu
  (/lobby/activity/allocation + datacenterId "blr1"); bila tak
  kembalikan host, fallback ke /lobby/allocation + activityId
  (bentuk yang dipakai activity_standby.py yang terbukti).
- Sesudah login: kirim init activity (init_pos, init_day_night,
  preset, fetch_events, activity/get — persis urutan game, terbukti
  di activity_standby.py) + SendDeviceId + 'tbulv' idle, lalu dengar
  'C' 10 dtk -> spawn di posisi pemain paling sering terlihat
  (+1.5 X); bila 'C' kosong, fallback (0, 2, 0). Heartbeat 'C' 1 dtk
  + 'tbulv' 5 dtk selama listen_s (default 60).
- Verdict utama = mata Bartes di dalam world (apakah bot terlihat)
  + log ('C' diterima > 0 berarti relay activity jalan; RoomUserUpdate
  untuk bot berarti server mendaftarkannya).
TIDAK kirim JoinRoom eksplisit (server auto-place). TIDAK kirim
invite/consent — tes ini HANYA soal wujud/registrasi di activity.
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

UGC_BARTES = "3e7cec3f-1ae9-4ba2-a19b-a6339e0f9b23"
BOT_UUID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"
FALLBACK_POS = (0.0, 2.0, 0.0)


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


async def allocate_activity(session, jwt, activity_id):
    """Alokasi ACTIVITY. Coba endpoint activity dulu, fallback ke
    /lobby/allocation + activityId (bentuk activity_standby.py).
    Kembalikan (body, endpoint_yang_berhasil) atau ({}, '')."""
    attempts = [
        ("/lobby/activity/allocation",
         {"userToken": jwt, "activityId": activity_id,
          "gameServerProtocolVersion": L.GAME_PROTOCOL,
          "datacenterId": "blr1"}),
        ("/lobby/allocation",
         {"userToken": jwt, "gameServerProtocolVersion": L.GAME_PROTOCOL,
          "isPrivate": False, "activityId": activity_id}),
        ("/lobby/allocation",
         {"userToken": jwt, "gameServerProtocolVersion": L.GAME_PROTOCOL,
          "isPrivate": False, "activityId": activity_id,
          "datacenterId": "blr1"}),
    ]
    for endpoint, body in attempts:
        b = json.dumps(body, separators=(",", ":")).encode("utf-8")
        try:
            r = await session.post(L.LOBBY_URL + endpoint, data=b,
                                   headers=L.HEADERS_BASE.copy(), timeout=20)
            resp = r.json()
        except Exception as exc:
            print(f"[-] alokasi {endpoint} gagal: {exc}")
            continue
        if not isinstance(resp, dict):
            print(f"[-] alokasi {endpoint}: balasan bukan dict.")
            continue
        host, port = L.find_host_port(resp)
        print(f"[*] alokasi {endpoint}: host={host} port={port} "
              f"kunci={list(resp.keys())[:10]}")
        if host and port:
            return resp, endpoint
        print(f"[-] alokasi {endpoint}: tanpa host/port, coba berikut.")
    return {}, ""


def build_position_payload(x, y, z, rotation=0.0, teleported=1, sequence=0):
    """Payload posisi 'C' 18 byte (TERBUKTI): [Teleported][seq][X/Y/Z/Rot]."""
    return (bytes([teleported & 0xFF, sequence & 0xFF])
            + struct.pack(">f", x) + struct.pack(">f", y)
            + struct.pack(">f", z) + struct.pack(">f", rotation))


def parse_c_batch(data):
    """Dekompresi payload event 'C' (raw Deflate) -> list posisi."""
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


async def activity_join_once(session, email, password, device_id,
                             activity_id, bot_user_id,
                             listen_s=60, fallback_pos=FALLBACK_POS):
    t0 = time.time()
    hasil = {"email": email, "activity_id": activity_id,
             "bot_user_id": bot_user_id,
             "alloc_endpoint": "", "alloc_keys": [],
             "server": "", "room": None, "joined_room": None,
             "c_received": 0, "c_positions_seen": 0,
             "most_observed_pos": None, "most_observed_count": 0,
             "spawn_pos": None, "spawn_mode": None,
             "heartbeat_sent": 0, "tbulv_sent": 0,
             "room_user_update": {"seen": False, "first_t_rel": None,
                                  "first_after": None, "uuid": None,
                                  "frame_type": None, "event": None,
                                  "hex_head": None},
             "event_names": {}}
    user_jwt, _alloc = await L.login_and_allocate(email, password, device_id,
                                                  session)
    if not user_jwt:
        print("[!] login gagal.")
        return hasil
    body, endpoint = await allocate_activity(session, user_jwt, activity_id)
    if not body:
        print("[!] alokasi activity gagal di semua percobaan.")
        return hasil
    hasil["alloc_endpoint"] = endpoint
    hasil["alloc_keys"] = list(body.keys())
    srv, rm = parse_gsid(body.get("gameServerId", ""))
    hasil["server"] = srv
    hasil["room"] = rm
    print(f"[+] alokasi activity OK via {endpoint}: server={srv} room={rm}")
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
    print(f"[+] Connected: playerId={pid}.")
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        print(f"[!] kirim login frame gagal: {exc}")
        return hasil
    fr = G.FrameReader(sock)
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
    joined = False
    if got is not None and not G._is_tbugv(got):
        joined = True
    if not joined:
        joined = find_joinedroom_in_frames(fr.drain(2), rm)
    hasil["joined_room"] = rm if joined else None
    print(f"[+] JoinedRoom({rm}) = {joined}.")
    watch = [bot_user_id, activity_id]

    def track_ruu(raw, name):
        st = hasil["room_user_update"]
        if st["seen"]:
            return
        for u in watch:
            if u.encode("utf-8") in raw:
                if name == "consent":
                    continue
                st.update({"seen": True,
                           "first_t_rel": round(time.time() - t0, 1),
                           "first_after": "activity",
                           "uuid": u[:8] + "...",
                           "frame_type": raw[0] if raw else None,
                           "event": name or "",
                           "hex_head": raw[:120].hex()})
                print(f"[+] KANDIDAT RoomUserUpdate t+{st['first_t_rel']} dtk: "
                      f"UUID {u[:8]}... event '{name}'.")
                break

    # Init activity (persis urutan game, terbukti di activity_standby.py).
    try:
        for name in ["init_pos", "init_day_night", "preset", "fetch_events"]:
            sock.sendall(L.build_room_event_varint(name, b""))
        time.sleep(0.3)
        sock.sendall(L.build_room_event_varint("activity/get", b""))
        print("[+] Init activity terkirim (init_pos, init_day_night, "
              "preset, fetch_events, activity/get).")
    except Exception as exc:
        print(f"[!] kirim init activity gagal: {exc}")
    # Dengar 'C' 10 dtk untuk cari posisi pemain (Bartes).
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
            track_ruu(raw, name)
    if pos_counter:
        (mx, my, mz), cnt = pos_counter.most_common(1)[0]
        hasil["most_observed_pos"] = [mx, my, mz]
        hasil["most_observed_count"] = cnt
        sx, sy, sz = mx + 1.5, my, mz
        smode = "most-observed-C"
    else:
        sx, sy, sz = fallback_pos
        smode = "fallback"
    hasil["spawn_pos"] = [round(sx, 2), round(sy, 2), round(sz, 2)]
    hasil["spawn_mode"] = smode
    print(f"[+] 'C' diterima {hasil['c_received']}x, posisi terlihat "
          f"{hasil['c_positions_seen']}x, paling sering: "
          f"{hasil['most_observed_pos']} ({hasil['most_observed_count']}x).")
    print(f"[+] Bot spawn di ({sx:.2f},{sy:.2f},{sz:.2f}) [{smode}].")
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
        print("[+] SendDeviceId terkirim.")
    except Exception as exc:
        print(f"[!] kirim SendDeviceId gagal: {exc}")
    end = time.time() + listen_s
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
            track_ruu(raw, name)
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. endpoint={hasil['alloc_endpoint']}, "
          f"'C' diterima: {hasil['c_received']}, posisi terlihat: "
          f"{hasil['c_positions_seen']}, heartbeat 'C': "
          f"{hasil['heartbeat_sent']}x, RoomUserUpdate: "
          f"{hasil['room_user_update']['seen']}, spawn: {hasil['spawn_pos']} "
          f"[{hasil['spawn_mode']}]")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("ACTIVITY_JOIN_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("ACTIVITY_JOIN_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("ACTIVITY_JOIN_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--activity-id",
                    default=os.getenv("ACTIVITY_JOIN_ACTIVITY_ID", UGC_BARTES))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("ACTIVITY_JOIN_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--listen", type=float, default=60)
    ap.add_argument("--fallback-x", type=float, default=FALLBACK_POS[0])
    ap.add_argument("--fallback-y", type=float, default=FALLBACK_POS[1])
    ap.add_argument("--fallback-z", type=float, default=FALLBACK_POS[2])
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await activity_join_once(
            session, args.email, args.password, args.device_id,
            args.activity_id, args.bot_user_id, args.listen,
            (args.fallback_x, args.fallback_y, args.fallback_z))
    print("ACTIVITY_JOIN_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
