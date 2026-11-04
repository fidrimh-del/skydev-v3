"""like_profile_diag.py — DIAGNOSA like profil (FILE TERPISAH).

Dibuat 2026-11-04 atas permintaan Bartes: diagnosis like profil di
file TERPISAH supaya file produksi (like_profile_cloud.py dan
like_profile_probe.py) TIDAK disentuh dan order dadakan tidak
terganggu. File ini TIDAK mengubah kedua file itu; ia hanya MENGIMPOR
builder/decoder/FrameReader/RPC yang terbukti dari mereka (read-only)
dan menjalankan alur per-botnya sendiri dengan instrumentasi.

Yang diukur (2 dimensi yang diminta Bartes):
  1. LOGIKA: langkah mana yang gagal per bot — login/alokasi, TCP,
     kirim login, Create, IsThere-sesudah — PLUS satu hal yang TIDAK
     pernah dicatat produksi: balasan Create (create_like_result).
     Produksi v1 menyimpulkan "Create tidak pernah dibalas" dari 4
     run; diagnosa ini MENDENGARKAN balasan Create selama jendela
     settle 2 detik dan mencatatnya bila tiba (termasuk error-nya).
  2. KECEPATAN: durasi tiap fase per bot (login+alokasi, TCP, jawab
     IsThere) + waktu total per bot + waktu total run.

Perilaku kawat (wire) IDENTIK dengan produksi: Create SEKALI per bot
(event "like", LikeType 1 = profil), IsThere-sesudah dikirim 2x lalu
diulang 1x sesudah jeda 3 detik bila belum True, TANPA JOIN, dan
history_put HANYA bila IsThere-sesudah = True (bot gagal TIDAK
tercatat di histori, jadi stok tidak terbuang oleh tes ini).

Jalankan: python -u like_profile_diag.py <target_uuid> <jumlah> <max_paralel>
  env: SUPABASE_URL, SUPABASE_SERVICE_KEY (panel), BOT_TYPE
  (accounts | named_bots, default accounts).
Keluar: selalu exit 0 bila run selesai (diagnosa, bukan delivery —
0 terkirim tetap hasil yang sah untuk dianalisis, bukan failure).
Kredensial TIDAK PERNAH dicetak (hanya "DIAG Bot N").
"""

import asyncio
import json
import os
import random
import statistics
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lookup_uuid as L
import like_profile_probe as P


def _fmt_stats(values):
    if not values:
        return "-"
    return (f"rata {statistics.mean(values):.1f}d | "
            f"min {min(values):.1f}d | maks {max(values):.1f}d")


async def diag_once(bot, idx, target_uuid):
    """Satu percobaan like profil dengan instrumentasi penuh.
    Kembalikan dict hasil (tidak pernah raise ke pemanggil)."""
    r = {"idx": idx, "outcome": "unknown", "login_alloc": None,
         "tcp": None, "isthere_lat": None, "total": None,
         "create_result": None, "isthere": None, "server": None,
         "detail": ""}
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
            # Create SEKALI (identik produksi).
            try:
                s.sendall(L.build_room_event_varint(
                    "like", P.build_like_protobuf(0, target_uuid)))
            except Exception as e:
                r["outcome"] = "create-send-fail"
                r["detail"] = f"gagal kirim Create: {e}"
                s.close()
                return r
            t_create = time.time()
            # DENGARKAN balasan Create selama jendela settle 2 detik
            # (produksi hanya sleep buta; diagnosa ini mendengarkan).
            # Bila balasan tiba lebih awal, sisa jendela tetap ditunggu
            # supaya timing IsThere sebanding dengan produksi.
            fr, _closed = reader.read_until(
                lambda f: P.decode_create_like_result(f) is not None, 2.0)
            if fr is not None:
                r["create_result"] = P.decode_create_like_result(fr)
            sisa = 2.0 - (time.time() - t_create)
            if sisa > 0:
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


def fetch_new_bots(bot_type, created_after, limit):
    """Ambil bot yang dibuat SETELAH created_after (timestamp ISO),
    langsung dari tabel accounts/named_bots via PostgREST (bukan RPC
    acak), supaya tes like memakai persis bot baru hasil run
    pembuatan. Kembalikan list triple (email, password, device_id)."""
    base = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not base or not key:
        print("[!] DIAG: SUPABASE_URL/KEY kosong — mode created_after batal.")
        return []
    table = "named_bots" if bot_type == "named_bots" else "accounts"
    params = {
        "select": "email,password,device_id",
        "status": "eq.SUCCESS",
        "created_at": f"gte.{created_after}",
        "order": "created_at.asc",
        "limit": str(limit),
    }
    url = f"{base}/rest/v1/{table}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url)
    req.add_header("apikey", key)
    req.add_header("Authorization", f"Bearer {key}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            rows = json.loads(resp.read().decode())
    except Exception as e:
        print(f"[!] DIAG: fetch created_after gagal: {type(e).__name__}: {e}")
        return []
    return [r for r in rows
            if r.get("email") and r.get("password") and r.get("device_id")]


def _run_bot(args):
    bot, idx, target = args
    return asyncio.run(diag_once(bot, idx, target))


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else (
        "aae2efe1-1abb-48c2-8799-ea192692a49c")
    jumlah = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    max_paralel = int(sys.argv[3]) if len(sys.argv) > 3 else 50
    bot_type = os.getenv("BOT_TYPE", "accounts")
    created_after = ((sys.argv[4] if len(sys.argv) > 4 else "")
                     or os.getenv("DIAG_CREATED_AFTER", ""))
    print(f"[*] DIAG like profil | target {target} | "
          f"jumlah {jumlah} | paralel {max_paralel} | bot {bot_type}"
          + (f" | mode created_after>={created_after}" if created_after else ""))
    t_run = time.time()
    if created_after:
        bots = fetch_new_bots(bot_type, created_after, jumlah)
    else:
        bots = P.get_profile_bots(target, limit=jumlah)
    print(f"[*] DIAG fetch: didapat {len(bots)} bot "
          f"(diminta {jumlah})"
          + (" | FETCH KURANG DARI DIMINTA" if len(bots) < jumlah else ""))
    if not bots:
        print("[!] DIAG: tidak ada bot — berhenti.")
        print("=" * 56)
        print("HASIL DIAG: fetch 0 bot, tidak ada percobaan.")
        print("=" * 56)
        return 0
    results = []
    with ThreadPoolExecutor(max_workers=max_paralel) as ex:
        futs = [ex.submit(_run_bot, (b, i + 1, target))
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
            print(f"DIAG Bot {res['idx']}: {res['outcome']} "
                  f"({res['total']:.0f}d) login={res['login_alloc']} "
                  f"tcp={res['tcp']} isthere_lat={res['isthere_lat']} | "
                  f"{cr_txt} | {it_txt} | {res['server']}"
                  + (f" | {res['detail']}" if res["detail"] else ""))
            if done % 10 == 0 or done == len(bots):
                print(f"[*] DIAG progres: {done}/{len(bots)} bot selesai.")
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
    print("=" * 56)
    print("HASIL DIAG")
    print(f"  target: {target} | bot: {bot_type} | paralel: {max_paralel}"
          + (f" | created_after>={created_after}" if created_after else ""))
    print(f"  diminta: {jumlah} | didapat server: {len(bots)}")
    print(f"  sukses: {sukses} | fail-answered (IsThere False): {fa} | "
          f"fail-noanswer: {fn} | tcp-fail: {tcpf} | lain: {lain}")
    print(f"  balasan Create tertangkap: {len(cr_list)} dari {len(results)}"
          + (f" | error Create: {cr_err}" if cr_err
             else " | (tidak ada balasan Create — sesuai observasi v1)"))
    print(f"  error IsThere terlihat: {it_err if it_err else 'tidak ada'}")
    print(f"  waktu login+alokasi (d): "
          f"{_fmt_stats([r['login_alloc'] for r in results if r['login_alloc'] is not None])}")
    print(f"  waktu TCP connect (d): "
          f"{_fmt_stats([r['tcp'] for r in results if r['tcp'] is not None])}")
    print(f"  latensi jawab IsThere (d): "
          f"{_fmt_stats([r['isthere_lat'] for r in results if r['isthere_lat'] is not None])}")
    print(f"  waktu total per bot (d): "
          f"{_fmt_stats([r['total'] for r in results if r['total'] is not None])}")
    print(f"  server tujuan: {servers}")
    print(f"  waktu total run: {time.time() - t_run:.0f} detik")
    print("=" * 56)
    print("DIAG SELESAI (exit 0 — hasil di atas yang dianalisis).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
