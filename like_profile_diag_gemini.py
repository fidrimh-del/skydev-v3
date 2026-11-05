"""like_profile_diag_gemini.py — DIAGNOSA like profil, VARIAN SARAN GEMINI
(FILE TERPISAH).

Dibuat 2026-11-04 atas permintaan Bartes: uji saran Gemini (analisis
kode 2026-11-04) terhadap like profil, di file TERPISAH supaya file
produksi (like_profile_cloud.py, like_profile_probe.py) dan file
diagnosa lama (like_profile_diag.py) TIDAK disentuh. File ini hanya
MENGIMPOR builder/decoder/FrameReader/RPC yang terbukti dari mereka
(read-only) dan menjalankan alur per-botnya sendiri.

Saran Gemini yang diuji (4 klaim, 2 sudah tertutup SEBELUM file ini
dibuat — lihat catatan di bawah — jadi file ini menguji 2 sisanya):

  1. WAIT-LOGIN (BELUM DIUJI): produksi kirim login frame lalu
     LANGSUNG Create tanpa menunggu balasan login. Varian ini, bila
     GEM_WAIT_LOGIN=1, menunggu balasan login dulu: event 'tbugv'
     atau JoinedRoom (0x06 success=1), maks 3 detik, via FrameReader
     yang sama (satu buffer, tidak dicampur reader lain). Bila
     tbugv tiba, dicek 1 detik lagi apakah ada auto-join room.
     Bila tidak ada ACK dalam 3 detik, Create TETAP dikirim (supaya
     tiap bot tetap terukur) tapi ack='none' dicatat — korelasi
     ack vs sukses dibaca dari ringkasan, bukan dari asumsi.
  2. NONBLOCK SETTLE (SUDAH ADA DI DIAG LAMA): diag lama sudah
     mendengarkan balasan Create selama settle 2 detik (bukan sleep
     buta) dan tetap gagal 0% — jadi saran ini TERTUTUP sebagai
     perbaikan tunggal. Varian ini MELENGKAPINYA bila
     GEM_NONBLOCK=1: sisa jendela settle + jeda retry 3 detik juga
     diisi loop baca (auto-pong, buang frame ambient), bukan
     time.sleep — persis saran Gemini "jangan blokir heartbeat".
  3. PARALEL (SUDAH DIGUGURKAN): matriks 2x2 (paralel 20 vs 50,
     accounts vs named) + login_alloc_errors=0 membuktikan paralel
     bukan variabel. TIDAK diuji ulang di sini; paralel tetap input
     workflow (default 50, sama dengan baseline) supaya laju
     (rate) tidak menjadi variabel pengganggu.
  4. JOIN SEBELUM CREATE (BELUM DIUJI): bila GEM_JOIN=1 dan sesi
     belum auto-join, varian ini mengirim JoinRoom gaya jalur house
     (kandidat: suffix gsid, lalu room 0-3, masing-masing ditunggu
     maks 3 detik, kriteria sukses = frame 0x06 success=1 — logika
     yang sama dengan lookup_uuid.tcp_join_only, tapi ditulis
     ulang di atas FrameReader yang sama supaya buffer tidak
     tercampur). Bila join gagal semua, Create TETAP dikirim dan
     join=None dicatat — bot join-gagal menjadi kelompok kontrol
     internal: bila bot join-sukses lolos dan join-gagal gagal,
     JOIN adalah mekanismenya; bila keduanya gagal, JOIN bukan
     jawabannya. CATATAN: join ini adalah join room pada instance
     hasil alokasi generik (persis seperti jalur house) — alokasi
     generik tidak mengenal "house target", jadi ini BUKAN join
     ke house Skydev secara spesifik.

Perilaku kawat yang SAMA dengan produksi/diag: Create SEKALI per bot
(event "like", LikeType 1 = profil), IsThere-sesudah dikirim 2x lalu
diulang 1x bila belum True, history_put HANYA bila IsThere = True
(bot gagal TIDAK tercatat, stok tidak terbuang oleh tes ini).

Jalankan: python -u like_profile_diag_gemini.py <target> <jumlah> <paralel>
  env: SUPABASE_URL, SUPABASE_SERVICE_KEY (panel), BOT_TYPE
  (accounts | named_bots, default accounts), GEM_WAIT_LOGIN (0/1,
  default 1), GEM_JOIN (0/1, default 1), GEM_NONBLOCK (0/1, default 1).
Keluar: selalu exit 0 bila run selesai (diagnosa, bukan delivery).
Kredensial TIDAK PERNAH dicetak (hanya "DIAG-GEMINI Bot N").
"""

import asyncio
import os
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lookup_uuid as L
import like_profile_probe as P


def _fmt_stats(values):
    if not values:
        return "-"
    return (f"rata {statistics.mean(values):.1f}d | "
            f"min {min(values):.1f}d | maks {max(values):.1f}d")


def _is_tbugv(frame):
    """Event 'tbugv': payload 0x05 + varint-len + nama 'tbugv'."""
    if len(frame) >= 2 and frame[0] == 0x05:
        try:
            ln = frame[1]
            if ln < 0x80 and frame[2:2 + ln] == b"tbugv":
                return True
        except Exception:
            pass
    return False


def _is_joinedroom(frame):
    """JoinedRoom sukses: opcode 0x06, success byte 1, roomId 4 byte."""
    return len(frame) >= 6 and frame[0] == 0x06 and frame[1] == 1


def _room_id(frame):
    try:
        return int.from_bytes(frame[2:6], "big")
    except Exception:
        return None


def _join_candidates(gsid):
    """Kandidat room persis tcp_join_only: suffix gsid, lalu 0-3."""
    cands = []
    suffix = None
    if "::" in gsid:
        try:
            suffix = int(gsid.rsplit("::", 1)[1])
        except Exception:
            suffix = None
    if suffix is not None:
        cands.append(suffix)
    for r in range(0, 4):
        if r not in cands:
            cands.append(r)
    return cands


async def gemini_once(bot, idx, target_uuid, wait_login, do_join, nonblock):
    """Satu percobaan like profil varian Gemini, instrumentasi penuh."""
    r = {"idx": idx, "outcome": "unknown", "login_alloc": None,
         "tcp": None, "wait_login": None, "join_time": None,
         "isthere_lat": None, "total": None,
         "create_result": None, "isthere": None, "server": None,
         "ack": "skip", "join": "skip", "detail": ""}
    t_start = time.time()
    try:
        async with L.AsyncSession(
                impersonate=random.choice(
                    ("chrome110", "chrome116", "safari15_5")),
                verify=False) as session:
            t0 = time.time()
            user_jwt, alloc = await L.login_and_allocate(
                bot["email"], bot["password"], bot["device_id"], session)
            r["login_alloc"] = time.time() - t0
            if not alloc:
                r["outcome"] = "alloc-fail"
                r["detail"] = "login/alokasi tidak mengembalikan alloc"
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
            # ---- SARAN 1: tunggu balasan login (tbugv / JoinedRoom).
            auto_room = None
            if wait_login:
                t0 = time.time()
                fr, _c = reader.read_until(
                    lambda f: _is_tbugv(f) or _is_joinedroom(f), 3.0)
                r["wait_login"] = time.time() - t0
                if fr is not None and _is_joinedroom(fr):
                    auto_room = _room_id(fr)
                    r["ack"] = f"room:{auto_room}"
                elif fr is not None and _is_tbugv(fr):
                    r["ack"] = "tbugv"
                    # Tbugv tiba: cek 1 detik apakah auto-join menyusul.
                    fr2, _c2 = reader.read_until(_is_joinedroom, 1.0)
                    if fr2 is not None:
                        auto_room = _room_id(fr2)
                        r["ack"] = f"tbugv+room:{auto_room}"
                else:
                    r["ack"] = "none"
            else:
                r["ack"] = "skip"
            # ---- SARAN 4: JoinRoom gaya jalur house sebelum Create.
            if do_join:
                t0 = time.time()
                if auto_room is not None:
                    r["join"] = f"auto:{auto_room}"
                else:
                    joined = None
                    for rid in _join_candidates(gsid):
                        try:
                            s.sendall(L.build_join_frame(rid))
                        except Exception:
                            break
                        fr3, closed = reader.read_until(_is_joinedroom, 3.0)
                        if fr3 is not None:
                            joined = _room_id(fr3)
                            break
                        if closed:
                            break
                    r["join"] = (f"room:{joined}" if joined is not None
                                 else "none")
                r["join_time"] = time.time() - t0
            else:
                r["join"] = "skip"
            # ---- Create SEKALI (identik produksi).
            try:
                s.sendall(L.build_room_event_varint(
                    "like", P.build_like_protobuf(0, target_uuid)))
            except Exception as e:
                r["outcome"] = "create-send-fail"
                r["detail"] = f"gagal kirim Create: {e}"
                s.close()
                return r
            t_create = time.time()
            # ---- Settle 2 detik: dengarkan balasan Create (diag lama
            # sudah begini). Bila NONBLOCK, sisa jendela diisi loop
            # baca (auto-pong), bukan sleep — saran Gemini #2 penuh.
            fr, _closed = reader.read_until(
                lambda f: P.decode_create_like_result(f) is not None, 2.0)
            if fr is not None:
                r["create_result"] = P.decode_create_like_result(fr)
            sisa = 2.0 - (time.time() - t_create)
            if sisa > 0:
                if nonblock:
                    reader.read_until(lambda f: False, sisa)
                else:
                    time.sleep(sisa)

            def do_is_there():
                frame = L.build_room_event_varint(
                    "like", P.build_like_protobuf(2, target_uuid))
                try:
                    s.sendall(frame)
                    s.sendall(frame)  # query: aman dikirim 2x
                except Exception:
                    return None, None
                t_q = time.time()
                fr2, _c = reader.read_until(
                    lambda f: P.decode_is_there_result(f) is not None, 8)
                if fr2 is None:
                    return None, None
                return P.decode_is_there_result(fr2), time.time() - t_q

            after, lat = do_is_there()
            answered = after is not None
            if not (after and after["exists"] is True):
                if nonblock:
                    reader.read_until(lambda f: False, 3.0)
                else:
                    time.sleep(3)
                after2, lat2 = do_is_there()
                if after2 is not None:
                    after, lat = after2, lat2
                    answered = True
            r["isthere"] = after
            if lat is not None:
                r["isthere_lat"] = lat
            s.close()
            if after and after["exists"] is True:
                P.history_put(bot["email"], target_uuid)
                r["outcome"] = "success"
            elif answered:
                r["outcome"] = "fail-answered"
                r["detail"] = "IsThere menjawab exists=False"
            else:
                r["outcome"] = "fail-noanswer"
                r["detail"] = "IsThere tidak menjawab (8d x 2)"
    except Exception as e:
        r["outcome"] = "exception"
        r["detail"] = f"{type(e).__name__}: {e}"
    finally:
        r["total"] = time.time() - t_start
    return r


def _run_bot(args):
    bot, idx, target, wl, jn, nb = args
    return asyncio.run(gemini_once(bot, idx, target, wl, jn, nb))


def main():
    # Versi game/protokol dari panel (tabel app_settings); fallback
    # bawaan bila tak tersedia — job tetap jalan.
    L.apply_panel_versions()
    target = sys.argv[1] if len(sys.argv) > 1 else (
        "aae2efe1-1abb-48c2-8799-ea192692a49c")
    jumlah = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    max_paralel = int(sys.argv[3]) if len(sys.argv) > 3 else 50
    bot_type = os.getenv("BOT_TYPE", "accounts")
    wait_login = os.getenv("GEM_WAIT_LOGIN", "1") != "0"
    do_join = os.getenv("GEM_JOIN", "1") != "0"
    nonblock = os.getenv("GEM_NONBLOCK", "1") != "0"
    print(f"[*] DIAG-GEMINI like profil | target {target} | "
          f"jumlah {jumlah} | paralel {max_paralel} | bot {bot_type} | "
          f"wait_login={int(wait_login)} join={int(do_join)} "
          f"nonblock={int(nonblock)}")
    t_run = time.time()
    bots = P.get_profile_bots(target, limit=jumlah)
    print(f"[*] DIAG-GEMINI fetch: didapat {len(bots)} bot "
          f"(diminta {jumlah})"
          + (" | FETCH KURANG DARI DIMINTA" if len(bots) < jumlah else ""))
    if not bots:
        print("[!] DIAG-GEMINI: tidak ada bot — berhenti.")
        print("=" * 56)
        print("HASIL DIAG GEMINI: fetch 0 bot, tidak ada percobaan.")
        print("=" * 56)
        return 0
    results = []
    with ThreadPoolExecutor(max_workers=max_paralel) as ex:
        futs = [ex.submit(_run_bot, (b, i + 1, target,
                                     wait_login, do_join, nonblock))
                for i, b in enumerate(bots)]
        done = 0
        for f in futs:
            res = f.result()
            results.append(res)
            done += 1
            cr = res["create_result"]
            cr_txt = ("CREATE-RESULT "
                      f"liked={cr['liked_user']} err={cr['error']!r} "
                      f"type={cr['like_type']}" if cr else "tanpa balasan Create")
            it = res["isthere"]
            it_txt = (f"exists={it['exists']} err={it['error']!r}"
                      if it else "tanpa jawaban IsThere")
            print(f"DIAG-GEMINI Bot {res['idx']}: {res['outcome']} "
                  f"({res['total']:.0f}d) ack={res['ack']} "
                  f"join={res['join']} login={res['login_alloc']} "
                  f"tcp={res['tcp']} isthere_lat={res['isthere_lat']} | "
                  f"{cr_txt} | {it_txt} | {res['server']}"
                  + (f" | {res['detail']}" if res["detail"] else ""))
            if done % 10 == 0 or done == len(bots):
                print(f"[*] DIAG-GEMINI progres: {done}/{len(bots)} bot selesai.")
    # Ringkasan.
    def hitung(outcome):
        return sum(1 for r in results if r["outcome"] == outcome)
    sukses = hitung("success")
    fa = hitung("fail-answered")
    fn = hitung("fail-noanswer")
    tcpf = hitung("tcp-fail")
    lain = len(results) - sukses - fa - fn - tcpf
    cr_list = [r["create_result"] for r in results if r["create_result"]]
    cr_err = sorted({c["error"] for c in cr_list if c.get("error")})
    it_err = sorted({r["isthere"]["error"] for r in results
                     if r["isthere"] and r["isthere"].get("error")})
    servers = {}
    for r in results:
        servers[r["server"]] = servers.get(r["server"], 0) + 1
    # Breakdown kunci: ack x outcome, join x outcome.
    def breakdown(key):
        table = {}
        for r in results:
            k = r[key]
            table.setdefault(k, {"success": 0, "fail": 0, "other": 0})
            if r["outcome"] == "success":
                table[k]["success"] += 1
            elif r["outcome"] in ("fail-answered", "fail-noanswer"):
                table[k]["fail"] += 1
            else:
                table[k]["other"] += 1
        return table
    ack_tab = breakdown("ack")
    join_tab = breakdown("join")
    print("=" * 56)
    print("HASIL DIAG GEMINI")
    print(f"  target: {target} | bot: {bot_type} | paralel: {max_paralel} | "
          f"wait_login={int(wait_login)} join={int(do_join)} "
          f"nonblock={int(nonblock)}")
    print(f"  diminta: {jumlah} | didapat server: {len(bots)}")
    print(f"  sukses: {sukses} | fail-answered (IsThere False): {fa} | "
          f"fail-noanswer: {fn} | tcp-fail: {tcpf} | lain: {lain}")
    print(f"  balasan Create tertangkap: {len(cr_list)} dari {len(results)}"
          + (f" | error Create: {cr_err}" if cr_err
             else " | (tidak ada balasan Create — sesuai observasi v1)"))
    print(f"  error IsThere terlihat: {it_err if it_err else 'tidak ada'}")
    print(f"  breakdown ack (tbugv/room/none/skip) x hasil: {ack_tab}")
    print(f"  breakdown join (room/none/skip) x hasil: {join_tab}")
    print(f"  waktu login+alokasi (d): "
          f"{_fmt_stats([r['login_alloc'] for r in results if r['login_alloc'] is not None])}")
    print(f"  waktu TCP connect (d): "
          f"{_fmt_stats([r['tcp'] for r in results if r['tcp'] is not None])}")
    print(f"  waktu tunggu login-ACK (d): "
          f"{_fmt_stats([r['wait_login'] for r in results if r['wait_login'] is not None])}")
    print(f"  waktu join (d): "
          f"{_fmt_stats([r['join_time'] for r in results if r['join_time'] is not None])}")
    print(f"  latensi jawab IsThere (d): "
          f"{_fmt_stats([r['isthere_lat'] for r in results if r['isthere_lat'] is not None])}")
    print(f"  waktu total per bot (d): "
          f"{_fmt_stats([r['total'] for r in results if r['total'] is not None])}")
    print(f"  server tujuan: {servers}")
    print(f"  waktu total run: {time.time() - t_run:.0f} detik")
    print("=" * 56)
    print("DIAG-GEMINI SELESAI (exit 0 — hasil di atas yang dianalisis).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
