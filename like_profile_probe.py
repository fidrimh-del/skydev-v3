"""like_profile_probe.py v2 — Uji coba like profil PK XD via TCP (OPTIMAL).

Perubahan dari v1 (alasan: 1 like di v1 butuh ~50 detik, padahal
jawaban server biasanya tiba dalam 1-3 detik):
  1. BACAAN EARLY-EXIT (FrameReader): v1 memakai read_frames() yang
     SELALU membakar jendela penuh (12 detik x 3 = 36 detik) meski
     jawaban sudah tiba di detik pertama. v2 berhenti begitu frame
     yang dicari tiba. Sisa buffer dipertahankan antar panggilan
     dalam satu sesi, dan Pong tetap dijawab otomatis.
  2. TIDAK MENUNGGU BALASAN CREATE: create_like_result TIDAK PERNAH
     tertangkap di 4/4 run v1 dalam jendela 12 detik — menunggu itu
     murni buang waktu. v2 kirim Create sekali, jeda 2 detik (beri
     server waktu mencatat), lalu langsung verifikasi lewat
     IsThere-sesudah (itulah bukti yang terbukti akurat).
  3. HISTORI SUPABASE (ide pemilik): tabel panel profile_like_history
     (bot_email, target_uuid). Pasangan yang sudah tercatat DILEWATI
     TANPA sesi sama sekali (cek DB ~0,5 detik, bukan sesi ~15 detik).
     Histori HANYA ditulis sesudah IsThere-sesudah = True, jadi
     histori tidak pernah berbohong. Pasangan tanpa baris histori
     tetap diverifikasi sekali lewat IsThere-sebelum (pemanasan),
     lalu dicatat — sesudah itu histori menjadi otoritatif.
     Kalau tabel belum ada (404 PostgREST), v2 jalan normal tanpa
     histori (fallback IsThere) + satu baris peringatan jelas.
  4. Tanpa jeda settle 2 detik (FrameReader melewati frame ambient
     dengan sendirinya) dan tanpa menunggu tbugv (yang menentukan
     hanya auto-join/join).

YANG TIDAK BERUBAH (aturan keras, jangan dilanggar):
  - Create dikirim SEKALI saja per bot (aksi tulis; 2x berisiko +2).
  - Identitas sesi = akun bot dari Supabase panel (RPC get_fresh_bots),
    BUKAN akun probe. lookup_uuid.probe_bot TIDAK dipakai.
  - Kredensial TIDAK PERNAH dicetak ke log (hanya "Bot 1/8", dst).
  - Mesin login/TCP dipakai ulang dari lookup_uuid.py (terbukti);
    berkas itu TIDAK diubah v2 (dipakai juga poller produksi).

CATATAN JUJUR TENTANG DELAY (pisahkan dua hal ini):
  - Lag counter profil di game (9-18 menit, teramati 2026-10-28)
    adalah agregasi SISI SERVER game — TIDAK bisa dipercepat dari
    klien mana pun. Yang v2 percepat adalah waktu KERJA script
    (50 detik -> ~15 detik per like baru; ~0,5 detik per pasangan
    yang sudah ada di histori).
  - Untuk pesanan massal nanti, langkah berikutnya (BELUM dibangun,
    perlu persetujuan): worker bot paralel (mis. 5 thread) — 100 like
    dari ~80 menit berurutan menjadi ~5 menit. Bukan bagian v2 ini.

CARA PAKAI: sama dengan v1 (argv UUID / env TARGET_UUID / workflow
like-profile-probe.yml). DDL histori: profile-like-history.sql
(jalankan sekali di SQL Editor Supabase PANEL, bukan DB toko).
"""

import asyncio
import json
import os
import socket
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import lookup_uuid as L

TARGET_DEFAULT = "aae2efe1-1abb-48c2-8799-ea192692a49c"  # DesertEagle#431
LIKE_TYPE_PLAYER = 1

# Status histori: True = tabel ada & bisa dipakai; False = belum ada /
# error (fallback ke IsThere, tanpa histori). None = belum dicek.
HISTORY_OK = None


def build_like_protobuf(event_id, liked_uuid, like_type=LIKE_TYPE_PLAYER):
    """Protobuf CreateLike/IsThereLikeRequest: field 1 = EventId (varint),
    field 2 = LikedUser (string), field 3 = LikeType (varint)."""
    ub = liked_uuid.encode("utf-8")
    return (b"\x08" + L.varint(event_id)
            + b"\x12" + L.varint(len(ub)) + ub
            + b"\x18" + L.varint(like_type))


def decode_room_event_data(frame, expected_name):
    """Ambil data protobuf dari satu frame RoomEvent (0x08) kalau
    namanya cocok; None kalau bukan frame itu."""
    if len(frame) < 2 or frame[0] != 0x08:
        return None
    ln, n = L.read_varint(frame, 1)
    if ln is None:
        return None
    if frame[1 + n:1 + n + ln] != expected_name.encode("utf-8"):
        return None
    return frame[1 + n + ln:]


def decode_create_like_result(frame):
    """CreateLikeResponse: field 1 = LikedUser, field 2 = ErrorMessage,
    field 3 = LikeType. (Jarang tertangkap; bukan bukti utama.)"""
    data = decode_room_event_data(frame, "create_like_result")
    if data is None:
        return None
    out = {"liked_user": None, "error": None, "like_type": None}
    for f, wt, v in L.parse_protobuf_fields(data):
        if f == 1 and wt == 2:
            out["liked_user"] = v.decode("utf-8", "replace")
        elif f == 2 and wt == 2:
            out["error"] = v.decode("utf-8", "replace")
        elif f == 3 and wt == 0:
            out["like_type"] = v
    return out


def decode_is_there_result(frame):
    """IsThereLikeForUserResponse: field 1 = LikeExists (bool),
    field 2 = LikedUser, field 3 = ErrorMessage, field 4 = LikeType.
    CATATAN protobuf: nilai false TIDAK dikirim (default omission),
    jadi field 1 absen = belum like (bukan jawaban hilang)."""
    data = decode_room_event_data(frame, "is_there_like_result")
    if data is None:
        return None
    out = {"exists": None, "liked_user": None, "error": None, "like_type": None}
    for f, wt, v in L.parse_protobuf_fields(data):
        if f == 1 and wt == 0:
            out["exists"] = (v == 1)
        elif f == 2 and wt == 2:
            out["liked_user"] = v.decode("utf-8", "replace")
        elif f == 3 and wt == 2:
            out["error"] = v.decode("utf-8", "replace")
        elif f == 4 and wt == 0:
            out["like_type"] = v
    if out["exists"] is None:
        out["exists"] = False  # field absen = false (protobuf default)
    return out


class FrameReader:
    """Pembaca frame dengan buffer persisten + early-exit + auto-pong.
    Tidak seperti read_frames() (selalu membakar jendela penuh),
    read_until() kembali BEGITU frame yang dicari tiba."""

    def __init__(self, sock):
        self.s = sock
        self.buf = bytearray()

    def read_until(self, pred, seconds):
        """Kembalikan (frame_yang_cocok_atau_None, koneksi_tertutup).
        pred(frame) -> True berarti berhenti."""
        deadline = time.time() + seconds
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
                    try:
                        self.s.sendall(struct.pack(">I", 1) + b"\x02")
                    except Exception:
                        pass
                    continue
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


def _supabase():
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    return url.rstrip("/"), key


def get_bots(target_uuid, limit=8):
    """Akun bot dari Supabase panel lewat RPC get_fresh_bots
    (cara yang sama dengan follow_cloud.py)."""
    url, key = _supabase()
    if not url or not key:
        print("[-] SUPABASE_URL / SUPABASE_SERVICE_KEY tidak ada di env.")
        return []
    req = urllib.request.Request(
        url + "/rest/v1/rpc/get_fresh_bots",
        data=json.dumps({"p_target_uuid": target_uuid,
                         "p_limit": limit}).encode("utf-8"),
        headers={"apikey": key, "Authorization": "Bearer " + key,
                 "Content-Type": "application/json"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            rows = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[-] get_fresh_bots HTTP {e.code}")
        return []
    except Exception as e:
        print(f"[-] get_fresh_bots gagal: {e}")
        return []
    bots = [b for b in rows
            if b.get("email") and b.get("password") and b.get("device_id")]
    print(f"[*] {len(bots)} bot didapat dari Supabase.")
    return bots


def history_get(bot_email, target_uuid):
    """True = pasangan sudah tercatat pernah like (lewati tanpa sesi).
    False = belum tercatat. None = histori tidak bisa dipakai
    (tabel belum ada / error) -> fallback ke IsThere."""
    global HISTORY_OK
    if HISTORY_OK is False:
        return None
    url, key = _supabase()
    if not url or not key:
        return None
    q = ("/rest/v1/profile_like_history?select=bot_email"
         "&bot_email=eq." + urllib.parse.quote(bot_email, safe="")
         + "&target_uuid=eq." + urllib.parse.quote(target_uuid, safe=""))
    req = urllib.request.Request(url + q, headers={
        "apikey": key, "Authorization": "Bearer " + key}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            rows = json.loads(r.read().decode("utf-8"))
        HISTORY_OK = True
        return len(rows) > 0
    except urllib.error.HTTPError as e:
        if e.code == 404:
            if HISTORY_OK is not False:
                print("[!] Tabel profile_like_history BELUM ADA di DB panel "
                      "-> jalan tanpa histori (buat dari "
                      "profile-like-history.sql; fallback IsThere).")
            HISTORY_OK = False
        else:
            print(f"[!] history_get HTTP {e.code} -> fallback IsThere.")
        return None
    except Exception as e:
        print(f"[!] history_get gagal ({e}) -> fallback IsThere.")
        return None


def history_put(bot_email, target_uuid):
    """Catat like yang SUDAH dikonfirmasi IsThere-sesudah. Upsert
    (aman dipanggil ulang). Gagal tulis tidak membatalkan like."""
    global HISTORY_OK
    if HISTORY_OK is False:
        return
    url, key = _supabase()
    if not url or not key:
        return
    req = urllib.request.Request(
        url + "/rest/v1/profile_like_history",
        data=json.dumps({"bot_email": bot_email,
                         "target_uuid": target_uuid,
                         "source": "probe"}).encode("utf-8"),
        headers={"apikey": key, "Authorization": "Bearer " + key,
                 "Content-Type": "application/json",
                 "Prefer": "resolution=merge-duplicates"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15):
            pass
        HISTORY_OK = True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            HISTORY_OK = False
            print("[!] history_put: tabel belum ada -> like tetap sah, "
                  "hanya tidak tercatat.")
        else:
            print(f"[!] history_put HTTP {e.code} (like tetap sah).")
    except Exception as e:
        print(f"[!] history_put gagal ({e}) (like tetap sah).")


async def like_once(session, bot, target_uuid):
    """Satu percobaan penuh (tanpa cek histori — pemanggil yang
    mengecek histori dulu). Alur: login -> join -> IsThere-sebelum
    (pemanasan, hanya kalau belum ada histori) -> Create SEKALI ->
    jeda 2 dtk -> IsThere-sesudah (+1x ulang kalau belum True).
    Kembalikan dict atau None kalau gagal di tengah jalan."""
    user_jwt, alloc = await L.login_and_allocate(
        bot["email"], bot["password"], bot["device_id"], session)
    if not alloc:
        print("[-] Allocation gagal.")
        return None
    host, port = L.find_host_port(alloc)
    gsid = alloc.get("gameServerId", "")
    suffix = gsid.rsplit("::", 1)[1] if "::" in gsid else "?"
    print(f"[*] Server: {host}:{port} (room suffix {suffix})")
    if not host or not port:
        print("[-] Host/port tidak ketemu di allocation.")
        return None
    s, pid = L.tcp_connect(host, port)
    if s is None:
        return None
    reader = FrameReader(s)
    try:
        s.sendall(L.build_login_frame(user_jwt))
        print("[+] Event login terkirim.")
    except Exception as e:
        print(f"[-] Gagal kirim event login: {e}")
        s.close()
        return None
    state = {"room": None}

    def login_pred(fr):
        t, r = L.scan_login_window([fr])
        if r is not None:
            state["room"] = r
        return state["room"] is not None

    fr, closed = reader.read_until(login_pred, 5)
    print(f"[*] Login: auto-join="
          f"{'room ' + str(state['room']) if state['room'] is not None else 'tidak'}")
    if closed:
        print("[!] Koneksi ditutup setelah login.")
        s.close()
        return None
    room = state["room"]
    if room is None:
        room = L.tcp_join_only(s, gsid)
    if room is None:
        print("[!] Join gagal di instance ini.")
        s.close()
        return None
    print(f"[+] Bergabung room {room}.")

    def do_is_there():
        frame = L.build_room_event_varint(
            "like", build_like_protobuf(2, target_uuid))
        try:
            s.sendall(frame)
            s.sendall(frame)  # query: aman dikirim 2x
        except Exception:
            return None
        fr, closed = reader.read_until(
            lambda f: decode_is_there_result(f) is not None, 8)
        if fr is None:
            return None
        dec = decode_is_there_result(fr)
        print(f"    is_there: exists={dec['exists']} "
              f"liked_user={dec['liked_user']} error={dec['error']!r}")
        return dec

    print("[*] Cek IsThere (sudah pernah like?) ...")
    before = do_is_there()
    if before and before["exists"] is True:
        print("[!] Bot ini SUDAH pernah like target (pemanasan) — catat "
              "histori & lewati.")
        history_put(bot["email"], target_uuid)
        s.close()
        return {"already": True}
    print("[*] Kirim Create like (SEKALI) ...")
    try:
        s.sendall(L.build_room_event_varint(
            "like", build_like_protobuf(0, target_uuid)))
    except Exception:
        s.close()
        return None
    time.sleep(2)  # beri server waktu mencatat (tanpa menunggu balasan
    # Create yang terbukti tidak pernah tiba dalam 12 detik di v1)
    print("[*] Cek IsThere sesudah Create ...")
    after = do_is_there()
    if not (after and after["exists"] is True):
        print("[*] IsThere-sesudah belum True — ulang sekali (3 dtk) ...")
        time.sleep(3)
        after = do_is_there()
    s.close()
    if after and after["exists"] is True:
        history_put(bot["email"], target_uuid)
    return {"already": False, "before": before, "after": after}


async def main():
    target = (sys.argv[1] if len(sys.argv) > 1 else
              os.getenv("TARGET_UUID", TARGET_DEFAULT))
    print(f"[*] Target like profil: {target}")
    bots = get_bots(target, limit=8)
    if not bots:
        print("[!] Tidak ada bot — berhenti.")
        return 1
    t0 = time.time()
    sent = 0
    skipped = 0
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        for bi, bot in enumerate(bots, 1):
            # Cek histori DULU (tanpa sesi sama sekali kalau sudah ada).
            if history_get(bot["email"], target) is True:
                print(f"[*] Bot {bi}/{len(bots)}: sudah like (histori) "
                      "— lewati tanpa sesi.")
                skipped += 1
                continue
            print(f"[*] --- Bot {bi}/{len(bots)} ---")
            tb = time.time()
            ok = False
            for attempt in range(1, 3):
                res = await like_once(session, bot, target)
                if res is None:
                    print(f"[*] Instance gagal — alokasi ulang "
                          f"(bot {bi}, percobaan {attempt}).")
                    continue
                if res.get("already"):
                    skipped += 1
                    ok = None  # sudah like, bukan like baru
                    break
                after = res.get("after") or {}
                if after.get("exists") is True:
                    sent += 1
                    ok = True
                    print(f"[+] LIKE TERKIRIM (bot {bi}, "
                          f"{time.time() - tb:.0f} detik).")
                else:
                    print(f"[!] Like belum terbukti (bot {bi}).")
                break
            if ok:
                print("=" * 48)
                print(f"HASIL LIKE: target {target}")
                print(f"  Like baru terkirim: {sent} | "
                      f"Dilewati (sudah like): {skipped}")
                print(f"  Waktu total: {time.time() - t0:.0f} detik")
                print("=" * 48)
                return 0
    print("=" * 48)
    print(f"HASIL: tidak ada like baru (terkirim: {sent}, "
          f"dilewati: {skipped}).")
    print(f"Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 48)
    # Bukan kegagalan mekanisme kalau semua bot memang sudah like.
    return 0 if skipped > 0 and sent == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
