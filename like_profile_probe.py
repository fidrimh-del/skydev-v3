"""like_profile_probe.py — Uji coba like profil PK XD via TCP (probe).

Jalur yang dipakai (dari dump.cs, belum pernah dites live sebelum ini):
  login FSTR -> allocation -> TCP connect -> Event "login"
  (USER_SESSION JWT + 1.93.3) -> auto-join/JoinRoom ->
  RoomEvent "like" + protobuf CreateLikeRequest (EventId=0/Create,
  LikedUser=UUID target, LikeType=1/Player) -> "create_like_result".

Mesin TCP + login DIPAKAI ULANG dari lookup_uuid.py (terbukti tahap 16
+ poller produksi) lewat import — bukan salinan baru. Yang baru di sini
hanya: ambil bot dari Supabase panel (RPC get_fresh_bots, seperti
follow_cloud.py), builder/decode protobuf like, dan alur IsThere ->
Create -> IsThere.

ATURAN PENTING (jangan dilanggar):
  - Create like dikirim SEKALI saja. Tidak seperti search (query
    idempotent yang aman dikirim 2x), Create adalah aksi tulis: kirim
    2x berisiko +2, bukan +1. IsThere (query) boleh dikirim 2x.
  - Kredensial TIDAK PERNAH dicetak ke log (hanya "Bot 1/3", dst).
  - Identitas sesi = akun bot dari Supabase, BUKAN akun probe.
    (lookup_uuid.probe_bot TIDAK dipakai di sini.)

CARA PAKAI:
  python like_profile_probe.py <UUID_TARGET>
  python like_profile_probe.py            (env TARGET_UUID, default
                                           UUID DesertEagle#431)
  Di GitHub Actions: pakai like-profile-probe.yml (input target_uuid).
"""

import asyncio
import json
import os
import sys
import time
import urllib.request
import urllib.error

import lookup_uuid as L

TARGET_DEFAULT = "aae2efe1-1abb-48c2-8799-ea192692a49c"  # DesertEagle#431
LIKE_TYPE_PLAYER = 1


def build_like_protobuf(event_id, liked_uuid, like_type=LIKE_TYPE_PLAYER):
    """Protobuf CreateLike/IsThereLikeRequest: field 1 = EventId (varint),
    field 2 = LikedUser (string), field 3 = LikeType (varint).
    Bentuk IsThere diasumsikan sama dengan Create (dari dump, kedua
    request memakai field yang sama); kalau server tidak menjawab
    IsThere, Create tetap jalan — hasil Create + IsThere-sesudah yang
    menentukan."""
    ub = liked_uuid.encode("utf-8")
    return (b"\x08" + L.varint(event_id)
            + b"\x12" + L.varint(len(ub)) + ub
            + b"\x18" + L.varint(like_type))


def decode_room_event_data(frame, expected_name):
    """Ambil data protobuf dari satu frame RoomEvent (0x08) kalau
    namanya cocok; kembalikan None kalau bukan frame itu."""
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
    field 3 = LikeType. Sukses = ErrorMessage kosong."""
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
    field 2 = LikedUser, field 3 = ErrorMessage, field 4 = LikeType."""
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
    return out


def get_bots(target_uuid, limit=3):
    """Ambil akun bot dari Supabase panel lewat RPC get_fresh_bots
    (cara yang sama dengan follow_cloud.py). Kembalikan list dict
    {email, password, device_id}. Env SUPABASE_URL/SUPABASE_SERVICE_KEY
    = kredensial DB panel (bukan DB toko)."""
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        print("[-] SUPABASE_URL / SUPABASE_SERVICE_KEY tidak ada di env.")
        return []
    req = urllib.request.Request(
        url.rstrip("/") + "/rest/v1/rpc/get_fresh_bots",
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


async def like_once(session, bot, target_uuid):
    """Satu percobaan penuh: login s.d. IsThere -> Create -> IsThere.
    Kembalikan dict hasil atau None kalau gagal di tengah jalan."""
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
    try:
        s.sendall(L.build_login_frame(user_jwt))
        print("[+] Event login terkirim.")
    except Exception as e:
        print(f"[-] Gagal kirim event login: {e}")
        s.close()
        return None
    resp, closed, pongs = L.read_frames(s, 5)
    tbugv, room = L.scan_login_window(resp)
    print(f"[*] Login: tbugv={'YA' if tbugv else 'tidak'}, "
          f"auto-join={'room ' + str(room) if room is not None else 'tidak'}")
    if closed:
        print("[!] Koneksi ditutup setelah login.")
        s.close()
        return None
    if room is None:
        room = L.tcp_join_only(s, gsid)
    if room is None:
        print("[!] Join gagal di instance ini.")
        s.close()
        return None
    print(f"[+] Bergabung room {room}.")
    resp, closed, pongs = L.read_frames(s, 2)
    if closed:
        s.close()
        return None

    def do_is_there():
        frame = L.build_room_event_varint(
            "like", build_like_protobuf(2, target_uuid))
        try:
            s.sendall(frame)
            s.sendall(frame)  # query: aman dikirim 2x
        except Exception:
            return None
        resp, closed, _ = L.read_frames(s, 12)
        for p in resp:
            dec = decode_is_there_result(p)
            if dec is not None:
                print(f"    is_there: exists={dec['exists']} "
                      f"liked_user={dec['liked_user']} error={dec['error']!r}")
                return dec
        return None

    def do_create():
        frame = L.build_room_event_varint(
            "like", build_like_protobuf(0, target_uuid))
        try:
            s.sendall(frame)  # SEKALI SAJA (aksi tulis, bukan query)
        except Exception:
            return None
        resp, closed, _ = L.read_frames(s, 12)
        for p in resp:
            dec = decode_create_like_result(p)
            if dec is not None:
                print(f"    create: liked_user={dec['liked_user']} "
                      f"error={dec['error']!r} like_type={dec['like_type']}")
                return dec
        return None

    print("[*] Cek IsThere (sudah pernah like?) ...")
    before = do_is_there()
    if before and before["exists"] is True:
        print("[!] Bot ini SUDAH pernah like target — lewati ke bot berikut.")
        s.close()
        return {"already": True}
    print("[*] Kirim Create like (SEKALI) ...")
    created = do_create()
    print("[*] Cek IsThere sesudah Create ...")
    after = do_is_there()
    s.close()
    return {"already": False, "before": before, "created": created,
            "after": after}


async def main():
    target = (sys.argv[1] if len(sys.argv) > 1 else
              os.getenv("TARGET_UUID", TARGET_DEFAULT))
    print(f"[*] Target like profil: {target}")
    bots = get_bots(target, limit=8)
    if not bots:
        print("[!] Tidak ada bot — berhenti.")
        return 1
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        for bi, bot in enumerate(bots, 1):
            print(f"[*] --- Bot {bi}/{len(bots)} ---")
            for attempt in range(1, 3):
                print(f"[*] Percobaan instance ke-{attempt} (bot {bi})")
                res = await like_once(session, bot, target)
                if res is None:
                    continue  # instance gagal -> alokasi ulang
                if res.get("already"):
                    break  # ganti bot
                created = res.get("created") or {}
                after = res.get("after") or {}
                ok_create = (not created.get("error")
                             and created.get("liked_user") == target)
                ok_after = after.get("exists") is True
                dt = time.time() - t0
                print("=" * 48)
                print(f"HASIL LIKE: target {target}")
                print(f"  IsThere sebelum: "
                      f"{(res.get('before') or {}).get('exists')}")
                print(f"  Create: error={created.get('error')!r} "
                      f"liked_user={created.get('liked_user')}")
                print(f"  IsThere sesudah: {after.get('exists')}")
                print(f"  Kesimpulan: "
                      f"{'LIKE TERKIRIM' if (ok_create or ok_after) else 'BELUM TERBUKTI'}")
                print(f"  Waktu total: {dt:.0f} detik")
                print("=" * 48)
                if ok_create or ok_after:
                    return 0
                print("[!] Create belum terbukti di bot ini — coba bot berikut.")
                break
    print("[!] Semua bot/instance gagal membuktikan like.")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
