"""SKYDEV Like Rumah DIAGNOSA (file terpisah) — BUKAN produksi.

Dibuat 2026-11-04 atas permintaan Bartes, meniru pola
like_profile_diag.py: diagnosis di file TERPISAH supaya
like_house_cloud.py / house_like_probe.py TIDAK disentuh.

Yang diukur (masalah house = fail-noanswer ~10-18%, BUKAN penolakan):
  1. ANATOMI NOANSWER — apakah noanswer berkorelasi dengan metode
     JOIN (auto-join dari jendela login vs join eksplisit), server,
     atau room? Produksi house SUDAH menunggu jendela login (5 dtk)
     + JOIN sebelum Create, jadi 0 answered-False sudah diduga;
     yang dicari adalah pola di balik diamnya IsThere.
  2. LAMBAT vs TIDAK PERNAH — untuk bot yang tidak True di urutan
     normal (settle 2 dtk + IsThere 8 dtk + jeda 3 dtk + IsThere 8 dtk),
     diag mengirim IsThere TERTUNDA sekali lagi (baca 10 dtk sambil
     pong, lalu query 2x + jendela 15 dtk):
       - delayed True  -> like mendarat, verifikasi normal hanya
                          lambat/hilang (artefak timing).
       - delayed False -> like benar-benar tidak ada (gagal beneran,
                          terkonfirmasi dua kali).
       - delayed None  -> sesi/room mati untuk query (tidak bisa
                          menjawab sama sekali).
  3. KECEPATAN — durasi tiap fase per bot + waktu total run.

Alur per bot MIRROR PERSIS produksi (house_like_probe.like_house_once):
login_and_allocate -> tcp_connect -> kirim login frame ->
tunggu jendela login 5 dtk (predikat scan_login_window, room-gated)
-> bila tidak auto-join, tcp_join_only -> Create SEKALI (LikeType 0)
-> settle 2 dtk -> IsThere-sesudah (query 2x, jendela 8 dtk)
-> bila belum True: jeda 3 dtk -> IsThere lagi (8 dtk)
-> [DIAG SAJA] IsThere tertunda (baca 10 dtk + query 2x + 15 dtk)
-> history_put_house HANYA bila IsThere menjawab True (normal atau
   tertunda — keduanya berarti like benar-benar ada di server).

Kelas hasil per bot:
  success          : IsThere normal True.
  success-delayed  : normal tidak True, IsThere tertunda True.
  fail-answered    : normal False DAN tertunda False.
  fail-noanswer    : normal diam DAN tertunda diam.
  fail-mixed       : kombinasi lain (normal False + tertunda diam,
                     atau normal diam + tertunda False) — detail
                     pasangan (normal, delayed) dicetak per bot.
  tcp-fail / join-fail / alloc-fail / alloc-nohost /
  login-send-fail / create-send-fail / exception : seperti namanya.

Satu ronde, tepat <jumlah> bot (fetch H.get_house_bots, BOT_TYPE env),
tanpa ekspansi ronde dan tanpa retry ronde. Exit 0 selalu (diag:
0 terkirim adalah hasil yang sah, bukan failure job).
Kredensial tidak pernah dicetak.
"""

import asyncio
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import house_like_probe as H  # noqa: E402
import like_profile_probe as P  # noqa: E402
import lookup_uuid as L  # noqa: E402

LIKE_TYPE_HOUSE = getattr(H, "LIKE_TYPE_HOUSE", 0)


def _fmt_stats(values):
    if not values:
        return "-"
    return (f"rata {sum(values) / len(values):.1f}d | "
            f"min {min(values):.1f}d | maks {max(values):.1f}d")


async def _run_bot(bot, idx, target_uuid):
    r = {"idx": idx, "outcome": None, "detail": "",
         "login_alloc": None, "tcp": None, "wait_login": None,
         "tbugv_seen": False, "join": None, "join_time": None,
         "normal": None, "normal_lat": None,
         "delayed": None, "delayed_lat": None,
         "server": None, "total": None}
    t_start = time.time()
    try:
        session = L.AsyncSession(
            impersonate=random.choice(("chrome110", "chrome116", "safari15_5")))
        t0 = time.time()
        user_jwt, alloc = await L.login_and_allocate(
            bot["email"], bot["password"], bot["device_id"], session)
        r["login_alloc"] = time.time() - t0
        if not alloc:
            r["outcome"] = "alloc-fail"
            r["detail"] = "login_and_allocate tanpa alloc"
            return r
        host, port = L.find_host_port(alloc)
        gsid = alloc.get("gameServerId", "")
        suffix = gsid.rsplit("::", 1)[1] if "::" in gsid else "?"
        r["server"] = f"{host}:{port} (suffix {suffix})"
        if not host or not port:
            r["outcome"] = "alloc-nohost"
            r["detail"] = "host/port tidak ketemu di allocation"
            return r
        t0 = time.time()
        s, pid = L.tcp_connect(host, port)
        r["tcp"] = time.time() - t0
        if s is None:
            r["outcome"] = "tcp-fail"
            r["detail"] = "TCP/Connected gagal"
            return r
        reader = P.FrameReader(s)
        try:
            s.sendall(L.build_login_frame(user_jwt))
        except Exception as e:
            r["outcome"] = "login-send-fail"
            r["detail"] = f"gagal kirim event login: {e}"
            s.close()
            return r
        # ---- Mirror produksi: tunggu jendela login (room-gated, 5 dtk).
        jstate = {"room": None, "tbugv": False}

        def _login_pred(fr):
            t, room = L.scan_login_window([fr])
            if t:
                jstate["tbugv"] = True
            if room is not None:
                jstate["room"] = room
            return jstate["room"] is not None

        t0 = time.time()
        fr, closed = reader.read_until(_login_pred, 5.0)
        r["wait_login"] = time.time() - t0
        r["tbugv_seen"] = jstate["tbugv"]
        if closed:
            r["outcome"] = "exception"
            r["detail"] = "koneksi ditutup saat menunggu balasan login"
            s.close()
            return r
        room = jstate["room"]
        t0 = time.time()
        if room is None:
            room = L.tcp_join_only(s, gsid)
            r["join"] = f"explicit:{room}" if room is not None else "fail"
        else:
            r["join"] = f"auto:{room}"
        r["join_time"] = time.time() - t0
        if room is None:
            r["outcome"] = "join-fail"
            r["detail"] = "join gagal di instance ini (auto + eksplisit)"
            s.close()
            return r

        def do_is_there(seconds=8):
            frame = L.build_room_event_varint(
                "like", P.build_like_protobuf(2, target_uuid, LIKE_TYPE_HOUSE))
            try:
                s.sendall(frame)
                s.sendall(frame)  # query: aman dikirim 2x
            except Exception:
                return None, None
            t_q = time.time()
            fr2, _c = reader.read_until(
                lambda f: P.decode_is_there_result(f) is not None, seconds)
            if fr2 is None:
                return None, None
            dec = P.decode_is_there_result(fr2)
            return dec["exists"], time.time() - t_q

        # ---- Create SEKALI (LikeType 0), mirror produksi.
        try:
            s.sendall(L.build_room_event_varint(
                "like", P.build_like_protobuf(0, target_uuid, LIKE_TYPE_HOUSE)))
        except Exception:
            r["outcome"] = "create-send-fail"
            r["detail"] = "gagal kirim Create"
            s.close()
            return r
        time.sleep(2)
        normal, lat = do_is_there()
        if not (normal is True):
            time.sleep(3)
            normal2, lat2 = do_is_there()
            if normal2 is not None:
                normal, lat = normal2, lat2
        r["normal"] = normal
        r["normal_lat"] = lat
        # ---- DIAG SAJA: IsThere tertunda (bila normal belum True).
        delayed = "skip"
        delayed_lat = None
        if normal is not True:
            reader.read_until(lambda f: False, 10.0)  # baca + pong 10 dtk
            delayed, delayed_lat = do_is_there(15)
        r["delayed"] = delayed
        r["delayed_lat"] = delayed_lat
        s.close()
        # ---- Klasifikasi + histori (hanya bila True terbukti).
        if normal is True:
            r["outcome"] = "success"
            H.history_put_house(bot["email"], target_uuid)
        elif delayed is True:
            r["outcome"] = "success-delayed"
            H.history_put_house(bot["email"], target_uuid)
        elif normal is False and delayed is False:
            r["outcome"] = "fail-answered"
        elif normal is None and delayed is None:
            r["outcome"] = "fail-noanswer"
        else:
            r["outcome"] = "fail-mixed"
            r["detail"] = f"normal={normal} delayed={delayed}"
    except Exception as e:
        r["outcome"] = "exception"
        r["detail"] = f"{type(e).__name__}: {e}"
    r["total"] = time.time() - t_start
    return r


def _worker(args):
    bot, idx, target_uuid = args
    return asyncio.run(_run_bot(bot, idx, target_uuid))


def main():
    # Versi game/protokol dari panel (tabel app_settings); fallback
    # bawaan bila tak tersedia — job tetap jalan.
    L.apply_panel_versions()
    target_uuid = sys.argv[1]
    jumlah = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    max_paralel = int(sys.argv[3]) if len(sys.argv) > 3 else 50
    bot_type = os.getenv("BOT_TYPE", "accounts")
    print(f"DIAG RUMAH: target={target_uuid} jumlah={jumlah} "
          f"paralel={max_paralel} bot_type={bot_type}")
    bots = H.get_house_bots(target_uuid, limit=jumlah)
    print(f"DIAG RUMAH: didapat {len(bots)} bot (diminta {jumlah})")
    t_run = time.time()
    results = []
    if bots:
        with ThreadPoolExecutor(max_workers=max_paralel) as ex:
            results = list(ex.map(
                _worker, [(b, i + 1, target_uuid) for i, b in enumerate(bots)]))
    for r in results:
        print(f"DIAG-RUMAH Bot {r['idx']}: {r['outcome']} | "
              f"join={r['join']} | wait={r['wait_login'] if r['wait_login'] is None else round(r['wait_login'], 1)}d | "
              f"tbugv={'y' if r['tbugv_seen'] else 'n'} | "
              f"normal={r['normal']} | delayed={r['delayed']} | "
              f"server={r['server']} | total={r['total'] if r['total'] is None else round(r['total'], 1)}d"
              + (f" | {r['detail']}" if r['detail'] else ""))
    counts = {}
    for r in results:
        counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
    print("=" * 56)
    print("HASIL DIAG RUMAH")
    print(f"  target: {target_uuid} | bot: {bot_type} | paralel: {max_paralel}")
    print(f"  diminta: {jumlah} | didapat server: {len(bots)}")
    print("  hasil: " + " | ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    # Tabel normal x delayed (hanya bot yang mencapai Create).
    tab = {}
    for r in results:
        if r["normal"] is not None or r["delayed"] != "skip":
            key = (str(r["normal"]), str(r["delayed"]))
            tab[key] = tab.get(key, 0) + 1
    print("  normal x delayed (True/False/None): " +
          ", ".join(f"{k}={v}" for k, v in sorted(tab.items())))
    # Tabel metode JOIN x hasil.
    jtab = {}
    for r in results:
        if r["join"]:
            method = r["join"].split(":")[0]
            key = (method, r["outcome"])
            jtab[key] = jtab.get(key, 0) + 1
    print("  join x hasil: " +
          ", ".join(f"{k[0]}/{k[1]}={v}" for k, v in sorted(jtab.items())))
    print(f"  waktu login+alokasi (d): {_fmt_stats([r['login_alloc'] for r in results if r['login_alloc'] is not None])}")
    print(f"  waktu TCP connect (d): {_fmt_stats([r['tcp'] for r in results if r['tcp'] is not None])}")
    print(f"  waktu tunggu login-ACK (d): {_fmt_stats([r['wait_login'] for r in results if r['wait_login'] is not None])}")
    print(f"  waktu JOIN (d): {_fmt_stats([r['join_time'] for r in results if r['join_time'] is not None])}")
    print(f"  waktu total per bot (d): {_fmt_stats([r['total'] for r in results if r['total'] is not None])}")
    servers = {}
    for r in results:
        if r["server"]:
            servers[r["server"]] = servers.get(r["server"], 0) + 1
    print(f"  server tujuan: {servers}")
    print(f"  waktu total run: {time.time() - t_run:.0f} detik")
    print("=" * 56)
    print("DIAG-RUMAH SELESAI (exit 0 — hasil di atas yang dianalisis).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
