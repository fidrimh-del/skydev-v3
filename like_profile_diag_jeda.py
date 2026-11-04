"""like_profile_diag_jeda.py — DIAGNOSA frame jendela login like profil
(FILE TERPISAH).

VARIAN JEDA (2026-11-04, atas permintaan Bartes: '1 run saja tapi kita gunakan jeda'): turunan diag-login untuk menguji apakah SATU run dengan jeda internal bisa melewati gate alokasi ~120 — atau gate itu per-runner sehingga grup 2 tetap menembok 429. Bot dibagi grup 100; antar grup sleep jeda_detik (argumen ke-4, default 30; tes berikutnya 10 bila 30 berhasil). SEMUA grup jalan di runner yang SAMA (satu job, satu IP) — inilah variabel yang diuji. Selain batching + sleep, alur per-bot PERSIS diag-login (wait-login + rekam frame + Create sekali + IsThere + history_put bila True). Produksi tidak disentuh.

Dibuat 2026-11-04 atas permintaan Bartes: "coba dulu tapi jangan di
produksi" — melihat RESPON SERVER saat login berhasil/gagal, di file
TERPISAH supaya file produksi (like_profile_cloud.py,
like_profile_probe.py) TIDAK disentuh. File ini hanya MENGIMPOR
builder/decoder/FrameReader/RPC yang terbukti dari mereka (read-only)
dan menjalankan alur per-botnya sendiri.

Yang direkam (BARU dibanding diag-gemini): SELURUH frame yang DITERIMA
selama jendela login (wait-login produksi: maks 3 detik sampai event
'tbugv' atau JoinedRoom, plus 1 detik observasi auto-join bila tbugv
tiba). Perekaman memakai salinan persis loop FrameReader.read_until
(buffer, auto-pong, timeout sama) — hanya ditambah pencatatan, tidak
mengubah perilaku kawat. Alur selebihnya PERSIS produksi: Create
SEKALI, settle sleep 2 detik, IsThere-sesudah 2x + ulang 1x bila
belum True, history_put HANYA bila IsThere = True (bot gagal TIDAK
tercatat, stok tidak terbuang oleh tes ini).

Yang dibedakan (BARU dibanding log cloud): bot yang koneksinya
TERTUTUP saat wait-login mendapat outcome sendiri "wait-closed"
(produksi menggolongkannya fail-noanswer tanpa jejak tahap).
Jadi ringkasan menjawab: saat runtuh, bot mati di tahap apa —
TCP connect, kirim login, wait-login (frame terakhir apa sebelum
tutup), atau IsThere — bukan sekadar "noanswer".

KEAMANAN: frame yang DIKIRIM (login frame berisi JWT) TIDAK PERNAH
dicetak. Frame yang diterima tidak mengandung token (token hanya
di lapis HTTP). Email/password/device_id bot TIDAK PERNAH dicetak
(hanya "DIAG-JEDA Bot N"). Host/port/roomId/playerId adalah ID
game (sama seperti yang sudah dicetak diag-gemini).

Jalankan: python -u like_profile_diag_jeda.py <target> <jumlah> <paralel> <jeda_detik>
  env: SUPABASE_URL, SUPABASE_SERVICE_KEY (panel), BOT_TYPE
  (accounts | named_bots, default accounts).
Keluar: selalu exit 0 bila run selesai (diagnosa, bukan delivery).
"""

import asyncio
import os
import random
import socket
import statistics
import struct
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


def _event_name(frame):
    """Nama event 0x05 (varint-len satu byte, seperti _is_tbugv)."""
    try:
        if len(frame) >= 2 and frame[1] < 0x80:
            ln = frame[1]
            raw = frame[2:2 + ln]
            if len(raw) == ln:
                return raw.decode("ascii", errors="replace")
    except Exception:
        pass
    return "?"


def _summarize_frame(frame):
    """Ringkasan SATU frame diterima (tanpa token, tanpa payload penuh)."""
    if not frame:
        return "empty"
    op = frame[0]
    if op == 0x01:
        return "ping"
    if op == 0x03:
        pid = frame[1:5].hex() if len(frame) >= 5 else "?"
        return f"connected pid={pid}"
    if op == 0x05:
        return f"event:{_event_name(frame)} len={len(frame)}"
    if op == 0x06:
        ok = frame[1] if len(frame) >= 2 else "?"
        rid = _room_id(frame)
        return f"joinedroom ok={ok} room={rid}"
    if op == 0x08:
        return f"roomevent len={len(frame)}"
    head = frame[:16].hex()
    return f"op=0x{op:02x} len={len(frame)} head={head}"


class RecordingReader(P.FrameReader):
    """FrameReader produksi + pencatat frame diterima.

    read_until_recorded() adalah salinan persis loop
    FrameReader.read_until (buffer, auto-pong, batas 262144,
    timeout socket 0.5d) — hanya ditambah: setiap frame yang
    dikeluarkan dari buffer dicatat (detik relatif + ringkasan)
    SEBELUM predikat dicek. Perilaku kawat tidak berubah.
    """

    def __init__(self, sock):
        super().__init__(sock)
        self.record = []

    def read_until_recorded(self, pred, seconds):
        """Kembalikan (frame_cocok_atau_None, koneksi_tertutup).

        self.record terisi [(detik_relatif, ringkasan), ...] untuk
        semua frame yang diterima selama jendela ini (termasuk ping).
        """
        t_start = time.time()
        deadline = t_start + seconds
        self.s.settimeout(0.5)
        while time.time() < deadline:
            while len(self.buf) >= 4:
                ln = struct.unpack(">I", self.buf[:4])[0]
                if ln > 262144:
                    return None, False
                if len(self.buf) - 4 < ln:
                    break
                fr = bytes(self.buf[4:4 + ln])
                del self.buf[:4 + ln]
                if fr == b"\x01":
                    self.record.append(
                        (round(time.time() - t_start, 2), "ping"))
                    try:
                        self.s.sendall(struct.pack(">I", 1) + b"\x02")
                    except Exception:
                        pass
                    continue
                self.record.append(
                    (round(time.time() - t_start, 2),
                     _summarize_frame(fr)))
                if pred(fr):
                    return fr, False
            try:
                chunk = self.s.recv(65536)
            except socket.timeout:
                continue
            except Exception:
                return None, True
            if chunk == b"":
                return None, True
            self.buf += chunk
        return None, False


async def login_once(bot, idx, target_uuid):
    """Satu percobaan like profil: PERSIS produksi + rekam frame login.

    Outcome: success / fail-answered / fail-noanswer / wait-closed /
    tcp-fail / alloc-fail / alloc-nohost / login-send-fail /
    create-send-fail / exception. wait-closed = koneksi tertutup
    saat wait-login (produksi: fail-noanswer tanpa jejak tahap).
    """
    r = {"idx": idx, "outcome": "unknown", "login_alloc": None,
         "tcp": None, "wait_login": None, "wait_end": "skip",
         "ack": "skip", "frames": [], "isthere": None,
         "isthere_lat": None, "total": None, "server": None,
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
            reader = RecordingReader(s)
            try:
                # JANGAN PERNAH log frame kirim (berisi JWT).
                s.sendall(L.build_login_frame(user_jwt))
            except Exception as e:
                r["outcome"] = "login-send-fail"
                r["detail"] = f"gagal kirim event login: {e}"
                s.close()
                return r
            # ---- WAIT-LOGIN produksi (rekam frame) ----
            t0 = time.time()
            fr, closed = reader.read_until_recorded(
                lambda f: _is_tbugv(f) or _is_joinedroom(f), 3.0)
            r["wait_login"] = time.time() - t0
            r["frames"] = list(reader.record)
            if closed:
                r["outcome"] = "wait-closed"
                r["wait_end"] = "closed"
                r["detail"] = ("koneksi tertutup saat wait-login "
                               f"(frame tercatat: {len(r['frames'])})")
                s.close()
                return r
            if fr is not None and _is_joinedroom(fr):
                r["ack"] = f"room:{_room_id(fr)}"
                r["wait_end"] = "matched"
            elif fr is not None and _is_tbugv(fr):
                r["ack"] = "tbugv"
                r["wait_end"] = "matched"
                # Produksi: amati 1 detik lagi (hasilnya dibuang);
                # diag: catat apakah auto-join menyusul.
                fr2, closed2 = reader.read_until_recorded(
                    _is_joinedroom, 1.0)
                r["frames"] = list(reader.record)
                if fr2 is not None:
                    r["ack"] = f"tbugv+room:{_room_id(fr2)}"
                elif closed2:
                    r["wait_end"] = "matched-then-closed"
            else:
                r["ack"] = "none"
                r["wait_end"] = "timeout"
            # ---- Create SEKALI (identik produksi) ----
            try:
                s.sendall(L.build_room_event_varint(
                    "like", P.build_like_protobuf(0, target_uuid)))
            except Exception as e:
                r["outcome"] = "create-send-fail"
                r["detail"] = f"gagal kirim Create: {e}"
                s.close()
                return r
            # Settle 2 detik (produksi: sleep, bukan listen).
            time.sleep(2)

            def do_is_there():
                frame = L.build_room_event_varint(
                    "like", P.build_like_protobuf(2, target_uuid))
                try:
                    s.sendall(frame)
                    s.sendall(frame)  # query: aman dikirim 2x
                except Exception:
                    return None, None
                t_q = time.time()
                fr2, _c = reader.read_until_recorded(
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


def _run_bot(args):
    bot, idx, target = args
    return asyncio.run(login_once(bot, idx, target))


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else (
        "aae2efe1-1abb-48c2-8799-ea192692a49c")
    jumlah = int(sys.argv[2]) if len(sys.argv) > 2 else 200
    max_paralel = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    jeda_detik = int(sys.argv[4]) if len(sys.argv) > 4 else 30
    bot_type = os.getenv("BOT_TYPE", "accounts")
    print(f"[*] DIAG-JEDA like profil | target {target} | "
          f"jumlah {jumlah} | paralel {max_paralel} | bot {bot_type} | "
          f"jeda {jeda_detik} detik per 100 bot, satu runner | "
          f"rekam frame jendela login (produksi tidak disentuh)")
    t_run = time.time()
    bots = P.get_profile_bots(target, limit=jumlah)
    print(f"[*] DIAG-JEDA fetch: didapat {len(bots)} bot "
          f"(diminta {jumlah})"
          + (" | FETCH KURANG DARI DIMINTA" if len(bots) < jumlah else ""))
    if not bots:
        print("[!] DIAG-JEDA: tidak ada bot — berhenti.")
        print("=" * 56)
        print("HASIL DIAG JEDA: fetch 0 bot, tidak ada percobaan.")
        print("=" * 56)
        return 0
    # DIAG-JEDA: bot dibagi grup 100, jeda jeda_detik antar grup,
    # SEMUA grup jalan di runner yang SAMA (satu job, satu IP) —
    # inilah yang diuji: apakah jeda saja cukup melewati gate ~120,
    # atau gate itu per-runner (maka grup 2 tetap menembok 429).
    ukuran_grup = 100
    grups = [bots[i:i + ukuran_grup]
             for i in range(0, len(bots), ukuran_grup)]
    print(f"[*] DIAG-JEDA: {len(bots)} bot dibagi {len(grups)} grup "
          f"(maks {ukuran_grup} bot/grup), jeda {jeda_detik} detik "
          f"antar grup, runner tunggal.")
    results = []
    done_total = 0
    for gi, grup in enumerate(grups, 1):
        awal = (gi - 1) * ukuran_grup + 1
        akhir = awal + len(grup) - 1
        print(f"[*] DIAG-JEDA grup {gi}/{len(grups)} mulai "
              f"(bot {awal}-{akhir}).")
        with ThreadPoolExecutor(max_workers=max_paralel) as ex:
            futs = [ex.submit(_run_bot, (b, awal + k, target))
                    for k, b in enumerate(grup)]
            for f in futs:
                res = f.result()
                results.append(res)
                done_total += 1
                fr_txt = "; ".join(f"{t}s {s}" for t, s in res["frames"][:12])
                if len(res["frames"]) > 12:
                    fr_txt += f"; ...(+{len(res['frames']) - 12})"
                it = res["isthere"]
                it_txt = (f"exists={it['exists']} err={it['error']!r}"
                          if it else "tanpa jawaban IsThere")
                print(f"DIAG-JEDA Bot {res['idx']}: {res['outcome']} "
                      f"({res['total']:.0f}d) ack={res['ack']} "
                      f"wait_end={res['wait_end']} wait={res['wait_login']} "
                      f"login={res['login_alloc']} "
                      f"tcp={res['tcp']} | frames=[{fr_txt}] | "
                      f"{it_txt} | {res['server']}"
                      + (f" | {res['detail']}" if res["detail"] else ""))
                if done_total % 10 == 0 or done_total == len(bots):
                    print(f"[*] DIAG-JEDA progres: {done_total}/{len(bots)} "
                          f"bot selesai.")
        print(f"[*] DIAG-JEDA grup {gi}/{len(grups)} selesai.")
        if gi < len(grups):
            print(f"[*] DIAG-JEDA jeda {jeda_detik} detik sebelum grup "
                  f"{gi + 1}/{len(grups)}...")
            time.sleep(jeda_detik)
    # Ringkasan.
    def hitung(outcome):
        return sum(1 for r in results if r["outcome"] == outcome)

    sukses = hitung("success")
    fa = hitung("fail-answered")
    fn = hitung("fail-noanswer")
    wc = hitung("wait-closed")
    tcpf = hitung("tcp-fail")
    lain = len(results) - sukses - fa - fn - wc - tcpf
    it_err = sorted({r["isthere"]["error"] for r in results
                     if r["isthere"] and r["isthere"].get("error")})
    servers = {}
    for r in results:
        servers[r["server"]] = servers.get(r["server"], 0) + 1

    def breakdown(key):
        table = {}
        for r in results:
            k = r[key]
            table.setdefault(k, {"success": 0, "fail": 0, "other": 0})
            if r["outcome"] == "success":
                table[k]["success"] += 1
            elif r["outcome"] in ("fail-answered", "fail-noanswer",
                                  "wait-closed"):
                table[k]["fail"] += 1
            else:
                table[k]["other"] += 1
        return table

    wait_tab = breakdown("wait_end")
    ack_tab = breakdown("ack")
    # Analisis wait-closed: frame terakhir sebelum tutup + jumlah frame.
    wc_last = {}
    wc_nframes = []
    wc_close_t = []
    for r in results:
        if r["outcome"] == "wait-closed":
            last = r["frames"][-1][1] if r["frames"] else "(tanpa frame)"
            wc_last[last] = wc_last.get(last, 0) + 1
            wc_nframes.append(len(r["frames"]))
            wc_close_t.append(r["wait_login"])
    print("=" * 56)
    print("HASIL DIAG JEDA")
    print(f"  target: {target} | bot: {bot_type} | paralel: {max_paralel}")
    print(f"  diminta: {jumlah} | didapat server: {len(bots)}")
    print(f"  sukses: {sukses} | fail-answered (IsThere False): {fa} | "
          f"fail-noanswer (IsThere diam): {fn} | "
          f"wait-closed (tutup saat wait-login): {wc} | "
          f"tcp-fail: {tcpf} | lain: {lain}")
    print(f"  error IsThere terlihat: {it_err if it_err else 'tidak ada'}")
    print(f"  breakdown wait_end (matched/timeout/closed) x hasil: {wait_tab}")
    print(f"  breakdown ack (tbugv/room/none) x hasil: {ack_tab}")
    if wc:
        print(f"  wait-closed: frame terakhir sebelum tutup: {wc_last}")
        print(f"  wait-closed: jumlah frame tercatat: "
              f"{_fmt_stats(wc_nframes)}")
        print(f"  wait-closed: waktu sampai tutup (d): "
              f"{_fmt_stats(wc_close_t)}")
    print(f"  waktu login+alokasi (d): "
          f"{_fmt_stats([r['login_alloc'] for r in results if r['login_alloc'] is not None])}")
    print(f"  waktu TCP connect (d): "
          f"{_fmt_stats([r['tcp'] for r in results if r['tcp'] is not None])}")
    print(f"  waktu tunggu login-ACK (d): "
          f"{_fmt_stats([r['wait_login'] for r in results if r['wait_login'] is not None])}")
    print(f"  latensi jawab IsThere (d): "
          f"{_fmt_stats([r['isthere_lat'] for r in results if r['isthere_lat'] is not None])}")
    print(f"  waktu total per bot (d): "
          f"{_fmt_stats([r['total'] for r in results if r['total'] is not None])}")
    print(f"  server tujuan: {servers}")
    print(f"  waktu total run: {time.time() - t_run:.0f} detik")
    print("  per grup (100 bot/grup, runner tunggal):")
    for gi, grup in enumerate(grups, 1):
        awal = (gi - 1) * ukuran_grup + 1
        akhir = awal + len(grup) - 1
        gres = [r for r in results if awal <= r["idx"] <= akhir]

        def ghitung(outcome, gres=gres):
            return sum(1 for r in gres if r["outcome"] == outcome)

        terhitung = (ghitung("success") + ghitung("fail-answered")
                     + ghitung("fail-noanswer") + ghitung("wait-closed")
                     + ghitung("tcp-fail") + ghitung("alloc-fail")
                     + ghitung("alloc-nohost"))
        print(f"    grup {gi} (bot {awal}-{akhir}): sukses={ghitung('success')} "
              f"fail-answered={ghitung('fail-answered')} "
              f"fail-noanswer={ghitung('fail-noanswer')} "
              f"wait-closed={ghitung('wait-closed')} "
              f"tcp-fail={ghitung('tcp-fail')} "
              f"alloc-fail={ghitung('alloc-fail')} "
              f"alloc-nohost={ghitung('alloc-nohost')} "
              f"lain={len(gres) - terhitung}")
    print("=" * 56)
    print("DIAG-JEDA SELESAI (exit 0 — hasil di atas yang dianalisis).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
