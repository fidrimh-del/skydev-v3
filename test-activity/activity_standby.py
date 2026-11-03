"""activity_standby.py — UJI COBA (repo skydev-test SAJA, bukan skydev-v3).

Bot online & muncul di world UGC/builder (activity) PK XD, lalu TAHAN
koneksi selama durasi tertentu supaya terlihat di dalam world.

Alur (terbukti dari netlog UGC 2026-11-03 + dump.cs):
  1. Login akun bot (mesin login_and_allocate dari lookup_uuid.py,
     file yang sama persis dengan skydev-v3 — tidak diubah).
  2. Alokasi ACTIVITY (bukan rumah): POST {LOBBY_URL}/lobby/allocation
     dengan body + activityId (AllocateActivityRequest di dump:
     userToken, activityId, gameServerProtocolVersion, datacenterId).
     Balasan berisi gameServerAddress/gameServerPort/gameServerId.
  3. TCP connect ke server activity itu, kirim login frame.
  4. Kirim event init (init_pos, init_day_night, preset, fetch_events)
     lalu activity/get — persis urutan game (activity/get terakhir).
  5. TAHAN: baca frame terus (auto-pong menjawab ping server 0x01
     dengan 0x02, itulah keepalive-nya) sampai durasi habis atau
     koneksi putus.

ATURAN UJI COBA:
  - TIDAK menulis histori apa pun (follow/like/history) — bot TIDAK
    terpakai dan tetap fresh untuk job lain.
  - Kredensial TIDAK PERNAH dicetak ke log (hanya "Bot i/N").
  - Satu bot gagal tidak menggagalkan bot lain.

CARA PAKAI:
  python -u activity_standby.py <activity_id> [jumlah=5] [durasi_menit=60] [bot_type=accounts]
  atau lewat env: ACTIVITY_ID, JUMLAH, DURASI_MENIT, BOT_TYPE.
  Env Supabase panel: SUPABASE_URL / SUPABASE_SERVICE_KEY
  (atau PANEL_SUPABASE_URL / PANEL_SUPABASE_SERVICE_KEY).
  Env opsional: FSTR_APP_KEY / FSTR_APP_SECRET (fallback di lookup_uuid).
"""

import asyncio
import concurrent.futures
import json
import os
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


def get_bots(bot_type, limit):
    """Ambil bot dari panel lewat RPC fresh (tanpa menulis apa pun).
    Kembalikan (bots, tipe_yang_dipakai). Fallback sekali ke tipe lain
    bila RPC tipe yang diminta kosong/gagal — dicetak jelas di log."""
    url, key = _supabase()
    if not url or not key:
        print("[-] SUPABASE_URL / SUPABASE_SERVICE_KEY tidak ada di env.")
        return [], bot_type
    order = [bot_type] + (["accounts"] if bot_type == "named_bots"
                          else ["named_bots"])
    for tipe in order:
        rpc = ("get_fresh_named_bots" if tipe == "named_bots"
               else "get_fresh_bots")
        # RPC follow wajib p_target_uuid (tanpa itu PostgREST 404:
        # tidak ada overload yang cocok). UUID nol = tidak ada bot
        # yang punya histori terhadapnya, jadi tanpa filter — tepat
        # untuk standby yang tidak menulis histori apa pun.
        req = urllib.request.Request(
            url + f"/rest/v1/rpc/{rpc}",
            data=json.dumps({"p_target_uuid": "00000000-0000-0000-0000-000000000000",
                      "p_limit": limit}).encode("utf-8"),
            headers={"apikey": key, "Authorization": "Bearer " + key,
                     "Content-Type": "application/json"},
            method="POST")
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                rows = json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            print(f"[-] RPC {rpc} HTTP {e.code} -> coba tipe lain.")
            continue
        except Exception as e:
            print(f"[-] RPC {rpc} gagal ({e}) -> coba tipe lain.")
            continue
        bots = [b for b in rows
                if b.get("email") and b.get("password")
                and b.get("device_id")]
        print(f"[*] {len(bots)} bot didapat (RPC {rpc}, limit {limit}).")
        if bots:
            return bots, tipe
    return [], bot_type


async def login_for_activity(session, bot, activity_id):
    """Login (pakai mesin L yang terbukti) + alokasi ACTIVITY.
    Kembalikan (jwt, host, port, gsid) atau (None, None, None, None)."""
    jwt, _body = await L.login_and_allocate(
        bot["email"], bot["password"], bot["device_id"], session)
    if not jwt:
        return None, None, None, None
    # Pelajaran run 41580669027: percobaan TANPA datacenterId berhasil
    # (201) tapi lobby memberi host lain (167.172.239.25:20102,
    # 45.55.68.126:20466) — BUKAN instance blr1 world Bartes
    # (168.144.153.155:20442) — sehingga bot tidak terlihat walau sempat
    # "MASUK". Client game selalu mengirim datacenterId (field
    # AllocateActivityRequest), dan world Bartes ada di blr1
    # (hostname gs digitalocean-blr1-02). Maka blr1 WAJIB di percobaan
    # pertama; tanpa-datacenterId hanya fallback bila blr1 tanpa host.
    # TEMUAN PROBE 2026-11-03: /lobby/allocation adalah allocator GENERIC
    # (selalu gameServerType DEFAULT, datacenterId diabaikan, geo-default
    # nyc3) — BUKAN alokasi activity. Endpoint activity yang benar adalah
    # /lobby/activity/allocation (terbukti mengembalikan gameServerType
    # ACTIVITY, pod activity-tier-0, room stabil per activity). Inilah
    # penyebab run 2 & 3 tidak terlihat: bot tidak pernah sampai ke
    # instance activity mana pun. datacenterId tetap dikirim (diabaikan
    # lobby, tidak merugikan); routing region ditentukan lobby dari sisi
    # requester.
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


def standby_thread(idx, total, bot, jwt, host, port, gsid, durasi_detik):
    """Satu bot: TCP + login + init + activity/get + tahan koneksi.
    Berjalan di thread sendiri (socket blocking). Kembalikan dict hasil."""
    tag = f"Bot {idx}/{total}"
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
    # Pelajaran run 41580669027: client game TIDAK PERNAH mengirim
    # frame join-room untuk activity (log v15/v17: login -> init ->
    # activity/get, tanpa room frame). tcp_join_only di sini selalu
    # gagal (room 0-3) dan 3 koneksi ditutup server (Broken pipe) saat
    # kirim init sesudahnya. Maka JANGAN join-room manual: langsung
    # kirim init + activity/get seperti client game; room hanya info
    # bila server memberikannya di jendela login.
    if room is None:
        print(f"[*] {tag}: tidak auto-join (room=None) — lanjut kirim "
              f"init + activity/get seperti client game ...")
    res["room"] = room
    print(f"[+] {tag}: login OK (playerId={pid}, room={room}). "
          f"Kirim init + activity/get ...")
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
    print(f"[+] {tag}: MASUK activity, tahan koneksi "
          f"{durasi_detik // 60} menit ...")
    deadline = time.time() + durasi_detik
    while time.time() < deadline:
        _fr, closed, pongs = L.read_frames(s, 5)
        res["pongs"] += pongs
        if closed:
            res["putus"] = round(time.time() - t0)
            print(f"[!] {tag}: koneksi DITUTUP server di detik "
                  f"{res['putus']} (dari {durasi_detik}).")
            break
    res["online_detik"] = round(time.time() - t0)
    try:
        s.close()
    except Exception:
        pass
    return res


async def main_async(activity_id, jumlah, durasi_menit, bot_type):
    bots, dipakai = get_bots(bot_type, jumlah)
    if not bots:
        print("[!] Tidak ada bot — berhenti.")
        return 1
    bots = bots[:jumlah]
    print(f"[*] Uji standby activity {activity_id}: {len(bots)} bot "
          f"(tipe {dipakai}), durasi {durasi_menit} menit.")
    t0 = time.time()
    siap = []
    # PERCEPATAN JOIN (permintaan Bartes 2026-11-03: 100 bot jangan lama):
    # fase login+alokasi DULU berurutan (~1,2 detik/bot -> 100 bot = 2 menit).
    # Kini paralel dengan Semaphore 20 (angka yang sudah terbukti aman di
    # like-profile paralel 20): 100 bot ~6-10 detik. gather mempertahankan
    # urutan input, jadi `siap` tetap terurut nomor bot. Satu sesi
    # AsyncSession dipakai bersama (curl_cffi AsyncSession dirancang untuk
    # request bersamaan; pola yang sama dipakai alur register produksi).
    sem = asyncio.Semaphore(20)

    async def login_satu(i, bot):
        async with sem:
            print(f"[*] --- Login + alokasi Bot {i}/{len(bots)} ---")
            jwt, host, port, gsid = await login_for_activity(
                session, bot, activity_id)
            if not host or not port:
                print(f"[-] Bot {i}/{len(bots)}: alokasi activity gagal.")
            return (i, bot, jwt, host, port, gsid)

    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil_login = await asyncio.gather(
            *(login_satu(i, bot) for i, bot in enumerate(bots, 1)))
        siap = [(i, bot, jwt, host, port, gsid)
                for (i, bot, jwt, host, port, gsid) in hasil_login
                if host and port]
    if not siap:
        print("[!] TIDAK ADA bot yang dapat server activity.")
        return 1
    print(f"[*] {len(siap)} bot dapat server, mulai standby paralel ...")
    durasi_detik = durasi_menit * 60
    hasil = []
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(siap)) as ex:
        futs = [ex.submit(standby_thread, i, len(bots), bot, jwt,
                          host, port, gsid, durasi_detik)
                for (i, bot, jwt, host, port, gsid) in siap]
        for f in concurrent.futures.as_completed(futs):
            hasil.append(f.result())
    masuk = sum(1 for h in hasil if h["masuk"])
    penuh = sum(1 for h in hasil
                if h["masuk"] and h["online_detik"] >= durasi_detik - 30)
    print("=" * 56)
    print(f"HASIL STANDBY ACTIVITY: {activity_id}")
    print(f"  Bot diminta: {len(bots)} | dapat server: {len(siap)} | "
          f"masuk: {masuk} | online penuh: {penuh}")
    for h in sorted(hasil, key=lambda x: x["tag"]):
        print(f"  {h['tag']}: server {h['server']} room={h['room']} "
              f"online {h['online_detik']} dtk pongs={h['pongs']} "
              f"{'(putus detik ' + str(h['putus']) + ')' if h['putus'] else ''}")
    print(f"  Waktu total: {round(time.time() - t0)} detik")
    print("=" * 56)
    return 0 if penuh > 0 else 1


def main():
    activity_id = (sys.argv[1] if len(sys.argv) > 1
                   else os.getenv("ACTIVITY_ID", ""))
    if not activity_id:
        print("Pakai: activity_standby.py <activity_id> "
              "[jumlah=5] [durasi_menit=60] [bot_type=accounts]")
        return 2
    jumlah = int(sys.argv[2] if len(sys.argv) > 2
                 else os.getenv("JUMLAH", "5"))
    durasi = int(sys.argv[3] if len(sys.argv) > 3
                 else os.getenv("DURASI_MENIT", "60"))
    bot_type = (sys.argv[4] if len(sys.argv) > 4
                else os.getenv("BOT_TYPE", "accounts"))
    return asyncio.run(main_async(activity_id, jumlah, durasi, bot_type))


if __name__ == "__main__":
    sys.exit(main())
