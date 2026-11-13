# -*- coding: utf-8 -*-
"""
activity_observer_diag.py — Tes H2 (observer 2-bot): apakah relay 'C' /
RoomUserUpdate activity sampai ke client TCP, dan apakah bot target
diumumkan ke siapa pun di instance yang sama?

Latar: Tahap B (run 43354159189) — bot join UGC via alokasi activity
(endpoint benar, JoinedRoom OK) tapi 'C' diterima tetap 0 dan Bartes
tidak melihat bot join sama sekali. Dua penjelasan tersisa:
(a) bot masuk INSTANCE berbeda dari Bartes (alokasi activity tanpa
destinationUserId), atau (b) server activity tidak mendaftarkan /
me-relay bot TCP sama sekali.
Tes ini memakai DUA bot (diambil dari panel, unnamed, via RPC fresh
— tidak menulis histori): OBSERVER masuk dulu, TARGET masuk sesudahnya.
Keduanya via alokasi activity ke activityId yang sama.
- Bila observer & target dapat server+room YANG SAMA -> satu instance.
  Observer hitung 'C' dari siapa pun (termasuk Bartes bila ia bergerak
  di world) + RoomUserUpdate/'C' untuk target.
- Bila server+room BERBEDA -> alokasi tidak menggabungkan instance
  (confound Tahap B terkonfirmasi) -> tes berikut harus pakai
  destinationUserId agar satu instance.
- Bila 'C' dari pemain asli > 0 -> relay sampai ke TCP (H2 GUGUR).
- Bila 'C' dari pemain asli = 0 DAN observer+target satu instance
  -> H2 NAIK: relay activity tak sampai ke client TCP.
- Bila observer menerima RoomUserUpdate untuk target -> target
  diumumkan; bila tidak (satu instance) -> target tidak terdaftar.
Bartes HARUS di dalam world dan BERGERAK (jalan) selama tes, supaya
'C'-nya ada untuk di-relay.
TIDAK kirim JoinRoom eksplisit, TIDAK kirim invite/consent.
"""
import asyncio
import base64
import concurrent.futures
import json
import os
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
import zlib
from collections import Counter

import lookup_uuid as L
import gift_diag as G
import spawn_test_diag as S
from activity_join_diag import (allocate_activity, parse_c_batch,
                                build_position_payload, parse_gsid,
                                find_joinedroom_in_frames)

UGC_BARTES = "3e7cec3f-1ae9-4ba2-a19b-a6339e0f9b23"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"
FALLBACK_POS = (0.0, 2.0, 0.0)


def _supabase():
    url = (os.getenv("PANEL_SUPABASE_URL")
           or os.getenv("SUPABASE_URL", "")).rstrip("/")
    key = (os.getenv("PANEL_SUPABASE_SERVICE_KEY")
           or os.getenv("SUPABASE_SERVICE_KEY", ""))
    return url, key


def get_bots(limit):
    """Ambil bot unnamed dari panel (tanpa menulis histori).
    Coba RPC get_fresh_bots dulu; bila 404 (RPC tidak ada di proyek
    yang dituju SUPABASE_URL), fallback ke query langsung tabel
    accounts (2 akun terbaru — paling mungkin fresh)."""
    url, key = _supabase()
    if not url or not key:
        print("[-] SUPABASE_URL / SUPABASE_SERVICE_KEY tidak ada di env.")
        return []
    headers = {"apikey": key, "Authorization": "Bearer " + key,
               "Content-Type": "application/json"}
    req = urllib.request.Request(
        url + "/rest/v1/rpc/get_fresh_bots",
        data=json.dumps({"p_limit": limit}).encode("utf-8"),
        headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            rows = json.loads(r.read().decode("utf-8"))
        bots = [b for b in rows
                if b.get("email") and b.get("password")
                and b.get("device_id")]
        print(f"[*] {len(bots)} bot unnamed didapat (RPC get_fresh_bots).")
        if bots:
            return bots
    except urllib.error.HTTPError as e:
        print(f"[-] RPC get_fresh_bots HTTP {e.code} -> fallback tabel accounts.")
    except Exception as e:
        print(f"[-] RPC get_fresh_bots gagal ({e}) -> fallback tabel accounts.")
    req2 = urllib.request.Request(
        url + "/rest/v1/accounts?select=email,password,device_id"
        f"&order=id.desc&limit={limit}",
        headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req2, timeout=20) as r:
            rows = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[-] query accounts gagal: {e}")
        return []
    bots = [b for b in rows
            if b.get("email") and b.get("password") and b.get("device_id")]
    print(f"[*] {len(bots)} bot unnamed didapat (tabel accounts, terbaru).")
    return bots


def jwt_sub(jwt):
    try:
        parts = jwt.split(".")
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        return json.loads(base64.urlsafe_b64decode(payload)).get("sub", "")
    except Exception:
        return ""


async def login_and_alloc(session, bot, activity_id):
    jwt, _ = await L.login_and_allocate(bot["email"], bot["password"],
                                        bot["device_id"], session)
    if not jwt:
        return None
    body, endpoint = await allocate_activity(session, jwt, activity_id)
    if not body:
        return None
    srv, rm = parse_gsid(body.get("gameServerId", ""))
    host, port = L.find_host_port(body)
    return {"bot": bot, "jwt": jwt, "uuid": jwt_sub(jwt),
            "endpoint": endpoint, "server": srv, "room": rm,
            "host": host, "port": port}


def tcp_login(info):
    """TCP connect + login frame + tunggu JoinedRoom (max ~5 dtk).
    Kembalikan (sock, pid, FrameReader, joined_bool)."""
    sock, pid = L.tcp_connect(info["host"], info["port"])
    if sock is None:
        return None, None, None, False
    try:
        sock.sendall(L.build_login_frame(info["jwt"]))
    except Exception:
        return None, None, None, False
    fr = G.FrameReader(sock)
    rm = info["room"]
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
        return sock, pid, fr, False
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
    joined = False
    if got is not None and not G._is_tbugv(got):
        joined = True
    if not joined:
        joined = find_joinedroom_in_frames(fr.drain(2), rm)
    return sock, pid, fr, joined


def send_init_activity(sock):
    try:
        for name in ["init_pos", "init_day_night", "preset", "fetch_events"]:
            sock.sendall(L.build_room_event_varint(name, b""))
        time.sleep(0.3)
        sock.sendall(L.build_room_event_varint("activity/get", b""))
    except Exception as exc:
        print(f"[!] kirim init activity gagal: {exc}")


def observer_thread(info, target_uuid, activity_id, listen_s, joined_event):
    """Observer: join, kirim init + SendDeviceId + tbulv idle, dengar
    60 dtk. Hitung 'C' dari siapa pun + RoomUserUpdate untuk target."""
    tag = "OBSERVER"
    t0 = time.time()
    res = {"tag": tag, "uuid": info["uuid"][:8] + "...",
           "server": info["server"], "room": info["room"],
           "joined_room": None, "c_received": 0, "c_positions_seen": 0,
           "c_from_target": 0, "room_user_update_target": False,
           "room_user_update_first_t_rel": None, "event_names": {}}
    sock, pid, fr, joined = tcp_login(info)
    if sock is None:
        print(f"[-] {tag}: TCP/login gagal.")
        return res
    res["joined_room"] = info["room"] if joined else None
    print(f"[+] {tag}: login OK (playerId={pid}), JoinedRoom={joined}.")
    send_init_activity(sock)
    try:
        sock.sendall(L.build_room_event_varint(
            "Analytics/deviceId",
            (info["bot"]["device_id"] or DEFAULT_DEVICE_ID).encode("utf-8")))
        sock.sendall(L.build_room_event_varint(
            "tbulv", S.build_spawn_aaa(0.0, 2.0, 0.0, player_id=pid)))
    except Exception:
        pass
    joined_event.set()
    watch = [u for u in (target_uuid, activity_id) if u]
    end = time.time() + listen_s
    while time.time() < end:
        for f in fr.drain(0.5):
            raw = bytes(f)
            name, d = G.decode_room_event(f)
            if name:
                res["event_names"][name] = res["event_names"].get(name, 0) + 1
                if name == "C" and d:
                    res["c_received"] += 1
                    poss = parse_c_batch(d)
                    res["c_positions_seen"] += len(poss)
            if not res["room_user_update_target"]:
                for u in watch:
                    if u.encode("utf-8") in raw and name != "consent":
                        # 'get-activity' berisi activityId -> bukan bukti.
                        if u == activity_id and name == "get-activity":
                            continue
                        res["room_user_update_target"] = True
                        res["room_user_update_first_t_rel"] = round(time.time() - t0, 1)
                        print(f"[+] {tag}: kandidat RoomUserUpdate untuk "
                              f"target t+{res['room_user_update_first_t_rel']} dtk "
                              f"(event '{name}').")
                        break
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] {tag} selesai: 'C' diterima {res['c_received']}x, posisi "
          f"{res['c_positions_seen']}x, RoomUserUpdate-target: "
          f"{res['room_user_update_target']}.")
    return res


def target_thread(info, activity_id, listen_s, start_event):
    """Target: tunggu observer join, lalu join + init + dengar 'C' 10 dtk
    -> spawn -> heartbeat 'C' 1 dtk + tbulv 5 dtk selama listen_s."""
    tag = "TARGET"
    t0 = time.time()
    res = {"tag": tag, "uuid": info["uuid"][:8] + "...",
           "server": info["server"], "room": info["room"],
           "joined_room": None, "c_received": 0, "c_positions_seen": 0,
           "spawn_pos": None, "spawn_mode": None, "heartbeat_sent": 0,
           "tbulv_sent": 0, "event_names": {}}
    start_event.wait(timeout=30)
    sock, pid, fr, joined = tcp_login(info)
    if sock is None:
        print(f"[-] {tag}: TCP/login gagal.")
        return res
    res["joined_room"] = info["room"] if joined else None
    print(f"[+] {tag}: login OK (playerId={pid}), JoinedRoom={joined}.")
    send_init_activity(sock)
    try:
        sock.sendall(L.build_room_event_varint(
            "Analytics/deviceId",
            (info["bot"]["device_id"] or DEFAULT_DEVICE_ID).encode("utf-8")))
    except Exception:
        pass
    pos_counter = Counter()
    end_listen = time.time() + 10
    while time.time() < end_listen:
        for f in fr.drain(0.5):
            name, d = G.decode_room_event(f)
            if name:
                res["event_names"][name] = res["event_names"].get(name, 0) + 1
                if name == "C" and d:
                    res["c_received"] += 1
                    for (x, y, z, rot, tele, s) in parse_c_batch(d):
                        res["c_positions_seen"] += 1
                        pos_counter[(round(x, 1), round(y, 1), round(z, 1))] += 1
    if pos_counter:
        (mx, my, mz), _cnt = pos_counter.most_common(1)[0]
        sx, sy, sz = mx + 1.5, my, mz
        smode = "most-observed-C"
    else:
        sx, sy, sz = FALLBACK_POS
        smode = "fallback"
    res["spawn_pos"] = [round(sx, 2), round(sy, 2), round(sz, 2)]
    res["spawn_mode"] = smode
    print(f"[+] {tag}: 'C' diterima {res['c_received']}x, spawn di "
          f"({sx:.2f},{sy:.2f},{sz:.2f}) [{smode}].")
    seq = [0]

    def send_c(x, y, z, teleported):
        seq[0] = (seq[0] + 1) & 0xFF
        try:
            sock.sendall(L.build_room_event_varint(
                "C", build_position_payload(x, y, z, rotation=0.0,
                                            teleported=teleported,
                                            sequence=seq[0])))
            res["heartbeat_sent"] += 1
        except Exception:
            pass

    def send_tbulv():
        try:
            sock.sendall(L.build_room_event_varint(
                "tbulv", S.build_spawn_aaa(sx, sy, sz, player_id=pid)))
            res["tbulv_sent"] += 1
        except Exception:
            pass

    send_tbulv()
    send_c(sx, sy, sz, teleported=1)
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
            name, d = G.decode_room_event(f)
            if name:
                res["event_names"][name] = res["event_names"].get(name, 0) + 1
                if name == "C" and d:
                    res["c_received"] += 1
                    res["c_positions_seen"] += len(parse_c_batch(d))
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] {tag} selesai: 'C' diterima {res['c_received']}x, "
          f"heartbeat 'C' {res['heartbeat_sent']}x, spawn "
          f"{res['spawn_pos']} [{res['spawn_mode']}].")
    return res


async def main_async(activity_id, listen_s):
    L.apply_panel_versions()
    bots = get_bots(2)
    if len(bots) < 2:
        print("[!] butuh 2 bot, hanya dapat "
              f"{len(bots)} — berhenti.")
        return 1
    infos = []
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        for i, bot in enumerate(bots[:2], 1):
            print(f"[*] --- Login + alokasi Bot {i}/2 ---")
            info = await login_and_alloc(session, bot, activity_id)
            if not info or not info["host"] or not info["port"]:
                print(f"[-] Bot {i}/2: alokasi activity gagal.")
                continue
            print(f"[+] Bot {i}/2: server={info['server']} "
                  f"room={info['room']} via {info['endpoint']}.")
            infos.append(info)
    if len(infos) < 2:
        print("[!] kurang dari 2 bot dapat server — berhenti.")
        return 1
    obs_info, tgt_info = infos[0], infos[1]
    same = (obs_info["server"] == tgt_info["server"]
            and obs_info["room"] == tgt_info["room"])
    print(f"[*] Observer & target {'SATU' if same else 'BEDA'} instance: "
          f"{obs_info['server']} room {obs_info['room']} vs "
          f"{tgt_info['server']} room {tgt_info['room']}.")
    joined_event = threading.Event()
    hasil = {"activity_id": activity_id, "listen_s": listen_s,
             "same_instance": same,
             "observer_server": obs_info["server"],
             "observer_room": obs_info["room"],
             "target_server": tgt_info["server"],
             "target_room": tgt_info["room"]}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f_obs = ex.submit(observer_thread, obs_info, tgt_info["uuid"],
                          activity_id, listen_s + 20, joined_event)
        f_tgt = ex.submit(target_thread, tgt_info, activity_id, listen_s,
                          joined_event)
        hasil["observer"] = f_obs.result()
        hasil["target"] = f_tgt.result()
    print("OBSERVER_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--activity-id",
                    default=os.getenv("OBSERVER_ACTIVITY_ID", UGC_BARTES))
    ap.add_argument("--listen", type=float, default=60)
    args = ap.parse_args()
    return asyncio.run(main_async(args.activity_id, args.listen))


if __name__ == "__main__":
    sys.exit(main())
