"""
activity_joinleave.py — Kirim bot join-leave activity/world UGC PK XD.
Revisi: Dukungan mode Matrix paralel (partisi), pacing koneksi TCP, 
dan pembatasan worker pool agar tidak terkena drop server / rate-limit IP.
"""

import asyncio
import concurrent.futures
import json
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import lookup_uuid as L

L.QUIET = False


def _supabase():
    url = (os.getenv("PANEL_SUPABASE_URL")
           or os.getenv("SUPABASE_URL", "")).rstrip("/")
    key = (os.getenv("PANEL_SUPABASE_SERVICE_KEY")
           or os.getenv("SUPABASE_SERVICE_KEY", ""))
    return url, key


def get_bots(bot_type, limit, partisi=None, jumlah_partisi=None):
    """Ambil bot dari panel lewat RPC fresh.
    Mendukung RPC biasa dan RPC _part untuk mode Matrix."""
    url, key = _supabase()
    if not url or not key:
        print("[-] SUPABASE_URL / SUPABASE_SERVICE_KEY tidak ada di env.")
        return [], bot_type

    order = [bot_type] + (["accounts"] if bot_type == "named_bots" else ["named_bots"])
    for tipe in order:
        if partisi is not None and jumlah_partisi is not None:
            rpc = "get_fresh_named_bots_part" if tipe == "named_bots" else "get_fresh_bots_part"
            payload = {
                "p_target_uuid": "00000000-0000-0000-0000-000000000000",
                "p_limit": limit,
                "p_partisi": partisi,
                "p_jumlah_partisi": jumlah_partisi
            }
        else:
            rpc = "get_fresh_named_bots" if tipe == "named_bots" else "get_fresh_bots"
            payload = {
                "p_target_uuid": "00000000-0000-0000-0000-000000000000",
                "p_limit": limit
            }

        req = urllib.request.Request(
            url + f"/rest/v1/rpc/{rpc}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"apikey": key, "Authorization": "Bearer " + key,
                     "Content-Type": "application/json"},
            method="POST")
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                rows = json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            # Fallback jika RPC _part belum tersedia di database
            if partisi is not None and e.code == 404:
                print(f"[-] RPC {rpc} 404 -> coba RPC tanpa partisi.")
                return get_bots(bot_type, limit, None, None)
            print(f"[-] RPC {rpc} HTTP {e.code} -> coba tipe lain.")
            continue
        except Exception as e:
            print(f"[-] RPC {rpc} gagal ({e}) -> coba tipe lain.")
            continue

        bots = [b for b in rows if b.get("email") and b.get("password") and b.get("device_id")]
        print(f"[*] {len(bots)} bot didapat (RPC {rpc}, limit {limit}).")
        if bots:
            return bots, tipe
    return [], bot_type


async def login_for_activity(session, bot, activity_id):
    """Login + alokasi ACTIVITY."""
    jwt, _body = await L.login_and_allocate(
        bot["email"], bot["password"], bot["device_id"], session)
    if not jwt:
        return None, None, None, None

    for attempt, extra in enumerate([
            {"userToken": jwt,
             "gameServerProtocolVersion": L.GAME_PROTOCOL,
             "isPrivate": False, "activityId": activity_id,
             "datacenterId": "blr1"},
            {"userToken": jwt,
             "gameServerProtocolVersion": L.GAME_PROTOCOL,
             "isPrivate": False, "activityId": activity_id},
    ], 1):
        b = json.dumps(extra, separators=(",", ":")).encode("utf-8")
        try:
            r = await session.post(L.LOBBY_URL + "/lobby/activity/allocation",
                                   data=b, headers=L.HEADERS_BASE.copy(),
                                   timeout=20)
            body = r.json()
        except Exception as e:
            print(f"[-] Alokasi activity percobaan {attempt} gagal: {e}")
            continue
        host, port = L.find_host_port(body)
        gsid = body.get("gameServerId", "") if isinstance(body, dict) else ""
        print(f"[*] Alokasi activity percobaan {attempt}: "
              f"host={host} port={port} "
              f"gsid={'ada' if gsid else 'KOSONG'} "
              f"type={body.get('gameServerType', '?') if isinstance(body, dict) else '?'} "
              f"dc={body.get('gameServerDatacenterId', '?') if isinstance(body, dict) else '?'}")
        if host and port:
            return jwt, host, port, gsid
    return jwt, None, None, None


def joinleave_thread(idx, total, bot, jwt, host, port, gsid, tahan_detik=0):
    """Satu bot: TCP + login + init + activity/get + TAHAN + KELUAR."""
    tag = f"Bot {idx}/{total}"
    
    # Stagger koneksi soket TCP agar tidak memicu SYN flood filter
    time.sleep(random.uniform(0.5, 1.5))
    
    t0 = time.time()
    res = {"tag": tag, "masuk": False, "online_detik": 0,
           "server": f"{host}:{port}", "room": None, "putus": None,
           "pongs": 0}
    s, pid = L.tcp_connect(host, port)
    if s is None:
        print(f"[-] {tag}: TCP/Connected gagal.")
        return res
    try:
        s.sendall(L.build_login_frame(jwt))
    except Exception as e:
        print(f"[-] {tag}: gagal kirim login frame: {e}")
        s.close()
        return res
    frames, closed, pongs = L.read_frames(s, 5)
    res["pongs"] += pongs
    try:
        _tbugv, room = L.scan_login_window(frames)
    except Exception:
        room = None

    if room is None:
        print(f"[*] {tag}: tidak auto-join (room=None) — lanjut kirim init + activity/get ...")
    res["room"] = room
    print(f"[+] {tag}: login OK (playerId={pid}, room={room}). Kirim init + activity/get ...")
    try:
        for name in ["init_pos", "init_day_night", "preset", "fetch_events"]:
            s.sendall(L.build_room_event_varint(name, b""))
        time.sleep(0.3)
        s.sendall(L.build_room_event_varint("activity/get", b""))
    except Exception as e:
        print(f"[-] {tag}: gagal kirim init/activity-get: {e}")
        s.close()
        return res
    res["masuk"] = True
    if tahan_detik > 0:
        print(f"[+] {tag}: MASUK activity, TAHAN {tahan_detik} detik lalu KELUAR ...")
        deadline = time.time() + tahan_detik
        while time.time() < deadline:
            _fr, closed, pongs = L.read_frames(s, 5)
            res["pongs"] += pongs
            if closed:
                res["putus"] = round(time.time() - t0)
                print(f"[!] {tag}: koneksi DITUTUP server di detik {res['putus']} (dari tahan {tahan_detik}).")
                break
    else:
        print(f"[+] {tag}: MASUK activity, KELUAR langsung (tanpa tahan) ...")
    res["online_detik"] = round(time.time() - t0)
    try:
        s.close()
    except Exception:
        pass
    return res


async def main_async(activity_id, jumlah, bot_type, tahan_detik, partisi=None, jumlah_partisi=None):
    bots, dipakai = get_bots(bot_type, jumlah, partisi, jumlah_partisi)
    if not bots:
        print("[!] Tidak ada bot — berhenti.")
        return 1
    bots = bots[:jumlah]
    print(f"[*] Uji join-leave activity {activity_id}: {len(bots)} bot (tipe {dipakai})")
    if partisi is not None:
        print(f"[*] Mode matrix: partisi {partisi}/{jumlah_partisi}")

    t0 = time.time()
    sem = asyncio.Semaphore(10)

    async def login_satu(i, bot, session):
        async with sem:
            print(f"[*] --- Login + alokasi Bot {i}/{len(bots)} ---")
            jwt, host, port, gsid = await login_for_activity(session, bot, activity_id)
            if not host or not port:
                print(f"[-] Bot {i}/{len(bots)}: alokasi activity gagal.")
            return (i, bot, jwt, host, port, gsid)

    async with L.AsyncSession(impersonate="chrome110", verify=False) as session:
        hasil_login = await asyncio.gather(*(login_satu(i, bot, session) for i, bot in enumerate(bots, 1)))
        siap = [(i, bot, jwt, host, port, gsid)
                for (i, bot, jwt, host, port, gsid) in hasil_login
                if host and port]

    if not siap:
        print("[!] TIDAK ADA bot yang dapat server activity.")
        return 1

    print(f"[*] {len(siap)} bot dapat server, mulai join-leave bertahap...")
    hasil = []
    
    # Batasi konkurensi koneksi TCP maksimal 10 thread simultan
    max_tcp_workers = min(10, len(siap))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_tcp_workers) as ex:
        futs = [ex.submit(joinleave_thread, i, len(bots), bot, jwt, host, port, gsid, tahan_detik)
                for (i, bot, jwt, host, port, gsid) in siap]
        for f in concurrent.futures.as_completed(futs):
            hasil.append(f.result())

    masuk = sum(1 for h in hasil if h["masuk"])
    print("=" * 56)
    print(f"HASIL JOIN-LEAVE ACTIVITY: {activity_id}")
    print(f"  Bot diminta: {len(bots)} | dapat server: {len(siap)} | "
          f"masuk: {masuk} | keluar: {masuk} | tahan: {tahan_detik} dtk")
    for h in sorted(hasil, key=lambda x: x["tag"]):
        print(f"  {h['tag']}: server {h['server']} room={h['room']} "
              f"online {h['online_detik']} dtk pongs={h['pongs']} "
              f"{'(putus detik ' + str(h['putus']) + ')' if h['putus'] else ''}")
    print(f"  Waktu total: {round(time.time() - t0)} detik")
    print("=" * 56)
    return 0 if masuk > 0 else 1


def main():
    L.apply_panel_versions()
    activity_id = sys.argv[1] if len(sys.argv) > 1 else os.getenv("ACTIVITY_ID", "")
    if not activity_id:
        print("Pakai: activity_joinleave.py <activity_id> [jumlah=5] [bot_type=named_bots] [tahan_detik=0] [partisi] [jumlah_partisi]")
        return 2
    jumlah = int(sys.argv[2] if len(sys.argv) > 2 else os.getenv("JUMLAH", "5"))
    bot_type = sys.argv[3] if len(sys.argv) > 3 else os.getenv("BOT_TYPE", "named_bots")
    tahan = int(sys.argv[4] if len(sys.argv) > 4 else os.getenv("TAHAN_DETIK", "0"))
    
    partisi = None
    jumlah_partisi = None
    if len(sys.argv) > 6:
        try:
            partisi = int(sys.argv[5])
            jumlah_partisi = int(sys.argv[6])
        except ValueError:
            pass

    return asyncio.run(main_async(activity_id, jumlah, bot_type, tahan, partisi, jumlah_partisi))


if __name__ == "__main__":
    sys.exit(main())
