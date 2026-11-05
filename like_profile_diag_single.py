"""like_profile_diag_single.py — DIAGNOSA single-bot like profil (FILE TERPISAH).

Dibuat 2026-11-04 atas permintaan Bartes: 1 bot saja, TANPA paralel,
dengan log langkah-demi-langkah yang lengkap, untuk melihat dengan
mata sendiri apa yang terjadi pada satu percobaan like profil ke
target yang sedang tertutup (aae2efe1) — apakah ini cooldown server
(penolakan diam-diam) atau ada langkah yang gagal.

File TERPISAH: tidak mengubah like_profile_cloud.py,
like_profile_probe.py, maupun like_profile_diag.py; hanya mengimpor
builder/decoder/FrameReader/RPC mereka (read-only).

Alur kawat IDENTIK dengan diagnosa 50/60 bot: login+alokasi ->
TCP -> login frame -> Create SEKALI (LikeType 1) -> dengarkan
balasan Create 2 detik -> IsThere-sesudah (dikirim 2x, diulang 1x
sesudah 3 detik bila belum True) -> history_put HANYA bila True.
Tanpa JOIN (like profil terbukti tidak butuh JOIN).

Catatan jujur: satu sampel tidak bisa mengukur rate (1 bot gagal =
wajar pada rate 10%; 1 bot sukses = wajar juga). Nilai tes ini
adalah TRACE-nya: langkah mana yang menjawab, berapa lama, dan apa
isi jawabannya — tanpa interleaving log paralel.
Keluar: selalu exit 0 (diagnosa).
Kredensial TIDAK PERNAH dicetak.
"""

import asyncio
import json
import os
import random
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lookup_uuid as L
import like_profile_probe as P

T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:6.1f}d] {msg}", flush=True)


def fetch_one_rpc(target):
    bots = P.get_profile_bots(target, limit=1)
    return bots[0] if bots else None


def fetch_one_new(bot_type, created_after):
    base = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not base or not key:
        return None
    table = "named_bots" if bot_type == "named_bots" else "accounts"
    params = {"select": "email,password,device_id", "status": "eq.SUCCESS",
              "created_at": f"gte.{created_after}", "order": "created_at.asc",
              "limit": "1"}
    url = f"{base}/rest/v1/{table}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url)
    req.add_header("apikey", key)
    req.add_header("Authorization", f"Bearer {key}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            rows = json.loads(resp.read().decode())
    except Exception as e:
        log(f"FETCH created_after gagal: {type(e).__name__}: {e}")
        return None
    rows = [r for r in rows
            if r.get("email") and r.get("password") and r.get("device_id")]
    return rows[0] if rows else None


async def single(bot, target):
    log("MULAI single-bot | target " + target + " | TANPA paralel (1 worker)")
    try:
        async with L.AsyncSession(
                impersonate=random.choice(
                    ("chrome110", "chrome116", "safari15_5")),
                verify=False) as session:
            t0 = time.time()
            log("LANGKAH 1: login+alokasi (HTTP: sessions -> users/sessions -> lobby/allocation)...")
            user_jwt, alloc = await L.login_and_allocate(
                bot["email"], bot["password"], bot["device_id"], session)
            log(f"  -> login+alokasi selesai {time.time() - t0:.1f}d | alloc={'YA' if alloc else 'TIDAK'}")
            if not alloc:
                log("HASIL: alloc-fail (login/alokasi tidak mengembalikan alloc)")
                return "alloc-fail"
            host, port = L.find_host_port(alloc)
            gsid = alloc.get("gameServerId", "")
            suffix = gsid.rsplit("::", 1)[1] if "::" in gsid else "?"
            log(f"  -> server {host}:{port} (suffix {suffix}) | gameServerType={alloc.get('gameServerType', '?')}")
            if not host or not port:
                log("HASIL: alloc-nohost")
                return "alloc-nohost"
            t0 = time.time()
            log("LANGKAH 2: TCP connect (tunggu frame Connected, maks 6d)...")
            s, pid = L.tcp_connect(host, port)
            log(f"  -> TCP connect {time.time() - t0:.2f}d | {'OK' if s is not None else 'GAGAL'}")
            if s is None:
                log("HASIL: tcp-fail (TCP/Connected gagal)")
                return "tcp-fail"
            reader = P.FrameReader(s)
            try:
                s.sendall(L.build_login_frame(user_jwt))
                log("LANGKAH 3: login frame terkirim OK")
            except Exception as e:
                log(f"LANGKAH 3: login frame GAGAL: {e}")
                s.close()
                log("HASIL: login-send-fail")
                return "login-send-fail"
            try:
                s.sendall(L.build_room_event_varint(
                    "like", P.build_like_protobuf(0, target)))
                log("LANGKAH 4: Create (event like, LikeType 1) terkirim SEKALI")
            except Exception as e:
                log(f"LANGKAH 4: Create GAGAL dikirim: {e}")
                s.close()
                log("HASIL: create-send-fail")
                return "create-send-fail"
            t_create = time.time()
            log("LANGKAH 5: dengarkan balasan Create selama jendela settle 2d...")
            fr, _closed = reader.read_until(
                lambda f: P.decode_create_like_result(f) is not None, 2.0)
            if fr is not None:
                cr = P.decode_create_like_result(fr)
                log(f"  -> balasan Create TERTANGKAP: liked={cr['liked_user']} err={cr['error']!r} type={cr['like_type']}")
            else:
                log("  -> tidak ada balasan Create (server tidak pernah menjawab Create)")
            sisa = 2.0 - (time.time() - t_create)
            if sisa > 0:
                time.sleep(sisa)
            log(f"  -> jendela settle 2d selesai (total sejak Create {time.time() - t_create:.1f}d)")

            def do_is_there(label):
                frame = L.build_room_event_varint(
                    "like", P.build_like_protobuf(2, target))
                try:
                    s.sendall(frame)
                    s.sendall(frame)
                except Exception as e:
                    log(f"  {label}: GAGAL kirim query IsThere: {e}")
                    return None
                t_q = time.time()
                fr2, _c = reader.read_until(
                    lambda f: P.decode_is_there_result(f) is not None, 8)
                if fr2 is None:
                    log(f"  {label}: IsThere TIDAK MENJAWAB (8d)")
                    return None
                res = P.decode_is_there_result(fr2)
                log(f"  {label}: IsThere MENJAWAB exists={res['exists']} err={res['error']!r} lat={time.time() - t_q:.3f}d")
                return res

            log("LANGKAH 6: IsThere-sesudah #1 (query dikirim 2x)...")
            after = do_is_there("IsThere #1")
            if not (after and after["exists"] is True):
                log("  -> belum True, jeda 3d lalu IsThere #2 (ulangan)...")
                time.sleep(3)
                log("LANGKAH 7: IsThere-sesudah #2 (ulangan)...")
                after2 = do_is_there("IsThere #2")
                if after2 is not None:
                    after = after2
            s.close()
            if after and after["exists"] is True:
                P.history_put(bot["email"], target)
                log("LANGKAH 8: history_put DIPANGGIL (sukses tercatat di profile_like_history)")
                log("HASIL: success (like NYATA, IsThere True)")
                return "success"
            elif after is not None:
                log("LANGKAH 8: history_put TIDAK dipanggil (gagal tidak tercatat, bot tetap segar)")
                log("HASIL: fail-answered (semua langkah OK, server menjawab exists=False = penolakan diam-diam)")
                return "fail-answered"
            else:
                log("LANGKAH 8: history_put TIDAK dipanggil")
                log("HASIL: fail-noanswer (IsThere tidak menjawab sama sekali)")
                return "fail-noanswer"
    except Exception as e:
        log(f"HASIL: exception {type(e).__name__}: {e}")
        return "exception"


def main():
    # Versi game/protokol dari panel (tabel app_settings); fallback
    # bawaan bila tak tersedia — job tetap jalan.
    L.apply_panel_versions()
    target = sys.argv[1] if len(sys.argv) > 1 else (
        "aae2efe1-1abb-48c2-8799-ea192692a49c")
    bot_type = os.getenv("BOT_TYPE", "accounts")
    created_after = os.getenv("DIAG_CREATED_AFTER", "")
    log(f"DIAG SINGLE | target {target} | bot {bot_type} | paralel: 1 (TIDAK ADA)"
        + (f" | created_after>={created_after}" if created_after else " | mode RPC stok segar"))
    if created_after:
        bot = fetch_one_new(bot_type, created_after)
    else:
        bot = fetch_one_rpc(target)
    if not bot:
        log("FETCH: tidak ada bot segar untuk target ini — berhenti.")
        print("=" * 56)
        print("HASIL DIAG SINGLE: fetch 0 bot.")
        print("=" * 56)
        return 0
    log("FETCH: 1 bot segar didapat (kredensial tidak dicetak)")
    outcome = asyncio.run(single(bot, target))
    print("=" * 56)
    print(f"HASIL DIAG SINGLE: {outcome}")
    print("=" * 56)
    print("DIAG SINGLE SELESAI (exit 0 — hasil di atas yang dianalisis).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
