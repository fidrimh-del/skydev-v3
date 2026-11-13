# -*- coding: utf-8 -*-
"""
friend_observer_diag.py — Tes H2 (observer via friend-join): apakah relay
'C' activity sampai ke client TCP yang ADA di instance yang sama?

Latar: Tes friend-join UGC (run 43355274811) — bot ADA di instance
Bartes (titiknya terlihat di posisi 0) tapi tanpa wujud; UGC dan
Sekolah sistemnya sama. Instance mismatch GUGUR. Yang belum dijawab:
apakah server activity me-relay posisi pemain asli ('C') ke client
TCP sama sekali, atau relay itu hanya lewat UDP.
Tes ini: SATU bot observer join via destinationUserId (UUID
Bartes/SkyDev — dijamin satu instance) + init activity + SendDeviceId
+ 'tbulv' idle, lalu HANYA MENDENGARKAN selama listen_s (default 60)
tanpa kirim 'C' (observer murni — tidak mengotori ruangan).
Bartes HARUS di dalam world dan BERGERAK (jalan) selama tes, supaya
'C'-nya ada untuk di-relay.
- Bila 'C' diterima > 0 -> relay activity SAMPAI ke TCP (H2 GUGUR);
  masalahnya spesifik pada pengumuman/registrasi bot target.
- Bila 'C' diterima = 0 padahal Bartes bergerak di instance yang sama
  -> H2 NAIK: relay activity tidak sampai ke client TCP (kemungkinan
  UDP-only) — bot TCP tidak akan pernah menerima posisi di activity.
TIDAK kirim JoinRoom eksplisit. TIDAK kirim invite/consent.
"""
import argparse
import asyncio
import json
import os
import time

import lookup_uuid as L
import gift_diag as G
import spawn_test_diag as S
from activity_join_diag import parse_c_batch, parse_gsid
from friend_join_ugc_diag import allocate_to_user

BARTES_UUID = "aae2efe1-1abb-48c2-8799-ea192692a49c"  # SkyDev#431
BOT_UUID = "71056756-53c7-478e-9356-82c87fb2dd63"     # pkxdshop test bot
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"


async def friend_observer_once(session, email, password, device_id,
                               dest_user_id, bot_user_id, listen_s=60):
    t0 = time.time()
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id, "alloc_keys": [],
             "server": "", "room": None, "joined_room": None,
             "c_received": 0, "c_positions_seen": 0,
             "most_observed_pos": None, "most_observed_count": 0,
             "tbulv_sent": 0,
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
        from activity_join_diag import find_joinedroom_in_frames
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
                           "first_after": "friend-observer",
                           "uuid": u[:8] + "...",
                           "frame_type": raw[0] if raw else None,
                           "event": name or "",
                           "hex_head": raw[:120].hex()})
                print(f"[+] KANDIDAT RoomUserUpdate t+{st['first_t_rel']} dtk: "
                      f"UUID {u[:8]}... event '{name}'.")
                break

    try:
        for name in ["init_pos", "init_day_night", "preset", "fetch_events"]:
            sock.sendall(L.build_room_event_varint(name, b""))
        time.sleep(0.3)
        sock.sendall(L.build_room_event_varint("activity/get", b""))
        print("[+] Init activity terkirim.")
    except Exception as exc:
        print(f"[!] kirim init activity gagal: {exc}")
    try:
        sock.sendall(L.build_room_event_varint(
            "Analytics/deviceId", (device_id or DEFAULT_DEVICE_ID).encode("utf-8")))
        sock.sendall(L.build_room_event_varint(
            "tbulv", S.build_spawn_aaa(0.0, 2.0, 0.0, player_id=pid)))
        hasil["tbulv_sent"] += 1
        print("[+] SendDeviceId + tbulv idle terkirim. Mendengarkan ...")
    except Exception as exc:
        print(f"[!] kirim init gagal: {exc}")
    from collections import Counter
    pos_counter = Counter()
    end = time.time() + listen_s
    last_tb = time.time()
    while time.time() < end:
        if time.time() - last_tb >= 5.0:
            try:
                sock.sendall(L.build_room_event_varint(
                    "tbulv", S.build_spawn_aaa(0.0, 2.0, 0.0, player_id=pid)))
                hasil["tbulv_sent"] += 1
            except Exception:
                pass
            last_tb = time.time()
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
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. server={hasil['server']} room={hasil['room']}, "
          f"'C' diterima: {hasil['c_received']}, posisi terlihat: "
          f"{hasil['c_positions_seen']}, paling sering: "
          f"{hasil['most_observed_pos']} ({hasil['most_observed_count']}x), "
          f"RoomUserUpdate: {hasil['room_user_update']['seen']}.")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("FRIEND_OBS_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("FRIEND_OBS_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("FRIEND_OBS_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("FRIEND_OBS_DEST_USER_ID", BARTES_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("FRIEND_OBS_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--listen", type=float, default=60)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await friend_observer_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id, args.listen)
    print("FRIEND_OBS_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
