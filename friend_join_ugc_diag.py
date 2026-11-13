# -*- coding: utf-8 -*-
"""
friend_join_ugc_diag.py — Tes join TEMAN ke world UGC (ide Bartes 2026-11-13).

Latar: Tahap B (run 43354159189) — bot join UGC via alokasi ACTIVITY
tapi Bartes tidak melihat bot join. Alokasi activity TIDAK menjamin
bot satu server/instance dengan Bartes, walau world-nya sama.
Tes ini: bot join lewat alokasi destinationUserId (POST
/lobby/allocation + destinationUserId = UUID Bartes/SkyDev, pemilik
world) — cara yang dipakai semua tes Sekolah/Plaza yang TERBUKTI
menaruh bot di server/instance target — DITAMBAH init activity
(init_pos, init_day_night, preset, fetch_events, activity/get),
SendDeviceId, 'tbulv', lalu dengar 'C' 10 dtk -> spawn di posisi
pemain paling sering terlihat (+1.5 X) atau fallback (0, 2, 0),
heartbeat 'C' 1 dtk + 'tbulv' 5 dtk selama listen_s (default 60).
- Bila Bartes MELIHAT bot -> kombinasi "instance sama + init activity"
  bekerja; alokasi activity murni yang salah.
- Bila Bartes tetap TIDAK melihat -> instance bukan masalahnya;
  registrasi di activity memang gagal dari sisi server.
TIDAK kirim JoinRoom eksplisit (server auto-place). TIDAK kirim
invite/consent — tes ini HANYA soal wujud/registrasi di activity.
"""
import argparse
import asyncio
import json
import os
import time
from collections import Counter

import lookup_uuid as L
import gift_diag as G
import spawn_test_diag as S
from activity_join_diag import (parse_c_batch, build_position_payload,
                                parse_gsid, find_joinedroom_in_frames)

BARTES_UUID = "aae2efe1-1abb-48c2-8799-ea192692a49c"  # SkyDev#431
BOT_UUID = "71056756-53c7-478e-9356-82c87fb2dd63"     # pkxdshop test bot
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"
FALLBACK_POS = (0.0, 2.0, 0.0)


async def allocate_to_user(session, jwt, dest_user_id):
    """Alokasi ke server seorang teman (AllocateToUserIdRequest, TERBUKTI
    di gift_join_room_diag.py): POST /lobby/allocation dengan
    destinationUserId = UUID teman. Kembalikan dict body."""
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


async def friend_join_once(session, email, password, device_id,
                           dest_user_id, bot_user_id,
                           listen_s=60, fallback_pos=FALLBACK_POS):
    t0 = time.time()
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id, "alloc_keys": [],
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
    body = await allocate_to_user(session, user_jwt, dest_user_id)
    if not body:
        print("[!] alokasi destinationUserId gagal.")
        return hasil
    hasil["alloc_keys"] = list(body.keys())
    srv, rm = parse_gsid(body.get("gameServerId", ""))
    hasil["server"] = srv
    hasil["room"] = rm
    print(f"[+] alokasi teman OK: server={srv} room={rm}")
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
    watch = [bot_user_id, dest_user_id]

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
                           "first_after": "friend-join",
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
    print(f"[+] selesai. server={hasil['server']} room={hasil['room']}, "
          f"'C' diterima: {hasil['c_received']}, posisi terlihat: "
          f"{hasil['c_positions_seen']}, heartbeat 'C': "
          f"{hasil['heartbeat_sent']}x, RoomUserUpdate: "
          f"{hasil['room_user_update']['seen']}, spawn: {hasil['spawn_pos']} "
          f"[{hasil['spawn_mode']}]")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("FRIEND_JOIN_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("FRIEND_JOIN_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("FRIEND_JOIN_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("FRIEND_JOIN_DEST_USER_ID", BARTES_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("FRIEND_JOIN_BOT_USER_ID", BOT_UUID))
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
        hasil = await friend_join_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id, args.listen,
            (args.fallback_x, args.fallback_y, args.fallback_z))
    print("FRIEND_JOIN_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
