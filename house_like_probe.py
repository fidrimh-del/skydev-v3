"""house_like_probe.py — Uji coba like RUMAH PK XD via TCP (sekali jalan).

Sama persis dengan like profil (like_profile_probe.py v2), HANYA
LikeType yang berubah: 1 (Player) -> 0 (House). Target = UUID PEMILIK
rumah. TIDAK ADA house id terpisah di protokol: CreateLikeRequest
hanya punya field EventId / LikedUser (string) / LikeType — jadi
"rumah si A" diidentifikasi oleh UUID si A (model dump: HomeOwnerId /
OwnerUserId / CurrentInternalHouseOwnerUserId; tiap pemain punya
satu rumah).

PERBEDAAN PENTING vs probe profil (jangan dilanggar):
  - TIDAK memakai tabel profile_like_history sama sekali. Histori
    itu mencatat like PROFIL. Like rumah adalah counter terpisah
    (LikesCount per LikeType), jadi bot yang sudah like profil
    MASIH BOLEH like rumah orang yang sama. Menulis like rumah
    ke histori profil akan MERACUNI filter job profil (bot itu
    dilewati padahal belum pernah like profil).
  - Sebaliknya, like rumah punya histori SENDIRI: tabel
    house_like_history (DDL: house-like-history.sql, dijalankan
    sekali di SQL Editor Supabase PANEL). Pasangan yang sudah
    tercatat dilewati TANPA sesi; histori HANYA ditulis sesudah
    IsThere-sesudah (LikeType=0) = True. Tanpa tabel, jalan normal
    dengan fallback IsThere per bot (lebih lambat untuk bot yang
    sudah pernah like, tapi tetap benar).
  - Create dikirim SEKALI per bot (aksi tulis; 2x berisiko +2).
  - Verifikasi = IsThere-sesudah dengan LikeType=0 SAJA (pola
    like-post, diadopsi dari probe profil 2026-10-29 — tanpa
    IsThere-sebelum). IsThere query boleh dikirim 2x.
  - Kredensial TIDAK PERNAH dicetak ke log (hanya "Bot i/N").
  - Mesin login/TCP dipakai ulang dari lookup_uuid.py (terbukti)
    + pembaca frame & decoder dari like_profile_probe.py v2.

CARA PAKAI: python -u house_like_probe.py <UUID pemilik rumah>
(atau env TARGET_UUID; default = DesertEagle#431).
"""

import asyncio
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import lookup_uuid as L
import like_profile_probe as P

TARGET_DEFAULT = "aae2efe1-1abb-48c2-8799-ea192692a49c"  # DesertEagle#431
LIKE_TYPE_HOUSE = 0

# Status histori RUMAH: True = tabel house_like_history ada & bisa
# dipakai; False = belum ada / error (fallback ke IsThere, tanpa
# histori). None = belum dicek. TERPISAH dari flag histori profil
# di like_profile_probe (HISTORY_OK) — jangan dicampur.
HOUSE_HISTORY_OK = None


def history_get_house(bot_email, target_uuid):
    """True = pasangan sudah tercatat pernah like RUMAH (lewati tanpa
    sesi). False = belum tercatat. None = histori tidak bisa dipakai
    (tabel belum ada / error) -> fallback ke IsThere."""
    global HOUSE_HISTORY_OK
    if HOUSE_HISTORY_OK is False:
        return None
    url, key = P._supabase()
    if not url or not key:
        return None
    q = ("/rest/v1/house_like_history?select=bot_email"
         "&bot_email=eq." + urllib.parse.quote(bot_email, safe="")
         + "&target_uuid=eq." + urllib.parse.quote(target_uuid, safe=""))
    req = urllib.request.Request(url + q, headers={
        "apikey": key, "Authorization": "Bearer " + key}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            rows = json.loads(r.read().decode("utf-8"))
        HOUSE_HISTORY_OK = True
        return len(rows) > 0
    except urllib.error.HTTPError as e:
        if e.code == 404:
            if HOUSE_HISTORY_OK is not False:
                print("[!] Tabel house_like_history BELUM ADA di DB panel "
                      "-> jalan tanpa histori (buat dari "
                      "house-like-history.sql; fallback IsThere).")
            HOUSE_HISTORY_OK = False
        else:
            print(f"[!] history_get_house HTTP {e.code} -> fallback IsThere.")
        return None
    except Exception as e:
        print(f"[!] history_get_house gagal ({e}) -> fallback IsThere.")
        return None


def history_put_house(bot_email, target_uuid, source="probe"):
    """Catat like RUMAH yang SUDAH dikonfirmasi IsThere-sesudah
    (atau sudah terbukti lewat IsThere-sebelum). Upsert (aman
    dipanggil ulang). Gagal tulis tidak membatalkan like.
    PERHATIAN: menulis ke house_like_history, BUKAN ke
    profile_like_history (counter terpisah)."""
    global HOUSE_HISTORY_OK
    if HOUSE_HISTORY_OK is False:
        return
    url, key = P._supabase()
    if not url or not key:
        return
    req = urllib.request.Request(
        url + "/rest/v1/house_like_history",
        data=json.dumps({"bot_email": bot_email,
                         "target_uuid": target_uuid,
                         "source": source}).encode("utf-8"),
        headers={"apikey": key, "Authorization": "Bearer " + key,
                 "Content-Type": "application/json",
                 "Prefer": "resolution=merge-duplicates"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15):
            pass
        HOUSE_HISTORY_OK = True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            HOUSE_HISTORY_OK = False
            print("[!] history_put_house: tabel belum ada -> like tetap "
                  "sah, hanya tidak tercatat.")
        else:
            print(f"[!] history_put_house HTTP {e.code} (like tetap sah).")
    except Exception as e:
        print(f"[!] history_put_house gagal ({e}) (like tetap sah).")


def get_house_bots(target_uuid, limit=8, partisi=None, jumlah_partisi=None):
    """Bot FRESH untuk like RUMAH, langsung dari Supabase panel
    lewat RPC BARU get_fresh_house_like_bots /
    get_fresh_named_house_like_bots (2026-11-01).

    BEDA dengan P.get_bots() yang memakai RPC lama get_fresh_bots
    (hanya memfilter follow_history): RPC baru ini memfilter
    house_like_history langsung di DB, jadi daftar yang
    dikembalikan sudah benar-benar fresh untuk target rumah ini.
    Kembalikan [] bila RPC gagal, supaya pemanggil bisa fallback
    ke jalur lama.
    Mode matrix: bila partisi/jumlah_partisi diisi, pakai RPC _part
    dengan parameter partisi (pemanggil tidak memakai fallback
    jalur lama dalam mode ini)."""
    url, key = P._supabase()
    if not url or not key:
        print("[-] SUPABASE_URL / SUPABASE_SERVICE_KEY tidak ada di env.")
        return []
    _base = ("get_fresh_named_house_like_bots"
             if os.getenv("BOT_TYPE", "accounts") == "named_bots"
             else "get_fresh_house_like_bots")
    _rpc = _base + ("_part" if partisi is not None else "")
    payload = {"p_target_uuid": target_uuid, "p_limit": limit}
    if partisi is not None:
        payload["p_partisi"] = partisi
        payload["p_jumlah_partisi"] = jumlah_partisi
    req = urllib.request.Request(
        url + f"/rest/v1/rpc/{_rpc}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"apikey": key, "Authorization": "Bearer " + key,
                 "Content-Type": "application/json"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            rows = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[-] {_rpc} HTTP {e.code}")
        return []
    except Exception as e:
        print(f"[-] {_rpc} gagal: {e}")
        return []
    bots = [b for b in rows
            if b.get("email") and b.get("password") and b.get("device_id")]
    print(f"[*] {len(bots)} bot fresh rumah didapat dari Supabase "
          f"(RPC {_rpc}, limit {limit}).")
    return bots


async def like_house_once(session, bot, target_uuid):
    """Satu percobaan penuh (tanpa cek histori — pemanggil yang mengecek
    histori dulu). POLA LIKE-POST (tanpa IsThere-sebelum — bot non-histori
    langsung Create, percaya filter histori Supabase pemanggil) TAPI
    DENGAN JOIN: terbukti 2026-10-29 pola tanpa-join (yang sah untuk
    like-profil) membuat IsThere-rumah selalu False — server butuh bot
    masuk room agar Create like rumah tercatat. IsThere-sesudah
    DIPERTAHANKAN sebagai bukti kirim (pengganti HTTP 200; Create TCP
    tidak pernah dibalas, jadi tanpa ini "terkirim" = klaim tanpa bukti).
    Alur: login -> JOIN room -> Create SEKALI (LikeType=0) -> jeda 2 dtk ->
    IsThere-sesudah (LikeType=0) (+1x ulang kalau belum True).
    Create SEKALI per sesi penuh (aturan keras terpenuhi penuh).
    Histori ditulis ke house_like_history, BUKAN profile_like_history
    (counter terpisah — menulis ke histori profil akan meracuninya).
    Kembalikan dict (dengan flag "answered": True bila ada satu pun
    jawaban IsThere di sesi ini — pemanggil memakai ini untuk memilah
    bot no-answer yang tidak ikut ronde retry) atau None kalau gagal
    di tengah jalan (login/konek/alokasi/join)."""
    user_jwt, alloc = await L.login_and_allocate(
        bot["email"], bot["password"], bot["device_id"], session)
    if not alloc:
        P.vlog("[-] Allocation gagal.")
        return None
    host, port = L.find_host_port(alloc)
    gsid = alloc.get("gameServerId", "")
    suffix = gsid.rsplit("::", 1)[1] if "::" in gsid else "?"
    P.vlog(f"[*] Server: {host}:{port} (room suffix {suffix})")
    if not host or not port:
        P.vlog("[-] Host/port tidak ketemu di allocation.")
        return None
    s, pid = L.tcp_connect(host, port)
    if s is None:
        return None
    reader = P.FrameReader(s)
    try:
        s.sendall(L.build_login_frame(user_jwt))
        P.vlog("[+] Event login terkirim.")
    except Exception as e:
        P.vlog(f"[-] Gagal kirim event login: {e}")
        s.close()
        return None

    # JOIN WAJIB untuk like rumah (tanpa ini IsThere-sesudah selalu
    # False — terbukti di run 40870370181). Diadopsi dari versi lama
    # yang terbukti (probe run 40657191412, false->true).
    _jstate = {"room": None}

    def _login_pred(fr):
        t, r = L.scan_login_window([fr])
        if r is not None:
            _jstate["room"] = r
        return _jstate["room"] is not None

    fr, closed = reader.read_until(_login_pred, 5)
    P.vlog(f"[*] Login: auto-join="
          f"{'room ' + str(_jstate['room']) if _jstate['room'] is not None else 'tidak'}")
    if closed:
        P.vlog("[!] Koneksi ditutup setelah login.")
        s.close()
        return None
    room = _jstate["room"]
    if room is None:
        room = L.tcp_join_only(s, gsid)
    if room is None:
        P.vlog("[!] Join gagal di instance ini.")
        s.close()
        return None
    P.vlog(f"[+] Bergabung room {room}.")

    def do_is_there_house():
        frame = L.build_room_event_varint(
            "like", P.build_like_protobuf(2, target_uuid, LIKE_TYPE_HOUSE))
        try:
            s.sendall(frame)
            s.sendall(frame)  # query: aman dikirim 2x
        except Exception:
            return None
        fr, closed = reader.read_until(
            lambda f: P.decode_is_there_result(f) is not None, 8)
        if fr is None:
            return None
        dec = P.decode_is_there_result(fr)
        P.vlog(f"    is_there_house: exists={dec['exists']} "
              f"liked_user={dec['liked_user']} "
              f"like_type={dec['like_type']} error={dec['error']!r}")
        return dec

    def do_create_verify():
        """Kirim Create SEKALI (LikeType=0), jeda 2 dtk,
        IsThere-sesudah (LikeType=0) (+1x ulang sesudah jeda 3 dtk).
        Kembalikan (after_dec, answered_bool)."""
        P.vlog("[*] Kirim Create like RUMAH (SEKALI, LikeType=0) ...")
        try:
            s.sendall(L.build_room_event_varint(
                "like", P.build_like_protobuf(0, target_uuid, LIKE_TYPE_HOUSE)))
        except Exception:
            return None, False
        time.sleep(2)  # beri server waktu mencatat (tanpa menunggu
        # balasan Create yang terbukti tidak pernah tiba di v1)
        P.vlog("[*] Cek IsThere RUMAH sesudah Create ...")
        after = do_is_there_house()
        answered = after is not None
        if not (after and after["exists"] is True):
            P.vlog("[*] IsThere-sesudah belum True — ulang sekali (3 dtk) ...")
            time.sleep(3)
            after = do_is_there_house()
            answered = answered or after is not None
        return after, answered

    # Pola like-post: tanpa IsThere-sebelum — langsung Create.
    after, answered = do_create_verify()
    s.close()
    if after and after["exists"] is True:
        history_put_house(bot["email"], target_uuid)
    return {"already": False, "before": None, "after": after,
            "answered": answered}


async def main():
    P.VERBOSE = True  # probe manual selalu detail
    L.QUIET = False
    # Versi game/protokol dari panel (tabel app_settings); fallback
    # bawaan bila tak tersedia — job tetap jalan.
    L.apply_panel_versions()
    target = (sys.argv[1] if len(sys.argv) > 1 else
              os.getenv("TARGET_UUID", TARGET_DEFAULT))
    print(f"[*] Target like RUMAH (UUID pemilik): {target}")
    bots = P.get_bots(target, limit=8)
    if not bots:
        print("[!] Tidak ada bot — berhenti.")
        return 1
    t0 = time.time()
    sent = 0
    skipped = 0
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        for bi, bot in enumerate(bots, 1):
            # Cek histori RUMAH dulu (tanpa sesi sama sekali kalau
            # sudah ada).
            if history_get_house(bot["email"], target) is True:
                print(f"[*] Bot {bi}/{len(bots)}: sudah like RUMAH "
                      "(histori) — lewati tanpa sesi.")
                skipped += 1
                continue
            print(f"[*] --- Bot {bi}/{len(bots)} ---")
            tb = time.time()
            for attempt in range(1, 3):
                res = await like_house_once(session, bot, target)
                if res is None:
                    print(f"[*] Instance gagal — alokasi ulang "
                          f"(bot {bi}, percobaan {attempt}).")
                    continue
                if res.get("already"):
                    skipped += 1
                    break
                after = res.get("after") or {}
                if after.get("exists") is True:
                    sent += 1
                    print(f"[+] LIKE RUMAH TERKIRIM (bot {bi}, "
                          f"{time.time() - tb:.0f} detik).")
                else:
                    print(f"[!] Like rumah belum terbukti (bot {bi}).")
                break
            if sent:
                print("=" * 48)
                print(f"HASIL LIKE RUMAH: target {target}")
                print(f"  Like rumah baru terkirim: {sent} | "
                      f"Dilewati (sudah like rumah): {skipped}")
                print(f"  Waktu total: {time.time() - t0:.0f} detik")
                print("=" * 48)
                return 0
    print("=" * 48)
    print(f"HASIL: tidak ada like rumah baru (terkirim: {sent}, "
          f"dilewati: {skipped}).")
    print(f"Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 48)
    # Bukan kegagalan mekanisme kalau semua bot memang sudah like rumah.
    return 0 if skipped > 0 and sent == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
