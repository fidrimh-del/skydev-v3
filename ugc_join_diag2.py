"""UGC Friend Join Test — bot gabung ke server DesertEagle via AllocateToUserIdRequest (join teman) + BURST INIT + SPAM magic_potion + activity/get BERULANG.

Perbedaan dari v4: v4 mematahkan timer tutup (survive 90,40 dtk,
run 40671390203) tapi server tetap tidak menjawab activity/get.
Teori v5: activity/get bot terlalu dini (klien game kirim sesudah
~115-213 frame traffic room; bot kirim di ~2 dtk, mungkin sebelum
mechanic UGC server siap sehingga request dibuang). v5 = v4 + SETTLE
5 dtk terima traffic room dulu, lalu activity/get + ulang tiap 5 dtk
selama observasi 30 dtk, plus ringkasan nama frame dari server.

Urutan burst (byte persis dari TBLog_0_u4jk.txt, baris 2461-2498):
  slot_interaction (58B, bawa UUID pemain -> dipakai UUID BOT dari frame
    tbugv, BUKAN UUID pemilik log) -> preset x8 (14B) -> fetch_events
  (14B) -> init_pos (10B) -> init_day_night (16B) -> pet (7B) ->
  slot_interaction lagi -> outfit (8B) -> vhc (7B) -> STRKS (7B) ->
  pet lagi -> activity/get (14B).
magic_potion SENGAJA tidak ikut (spam aksi pemain, bukan sinyal entry;
jadi variabel berikutnya bila v3 gagal).

Alur:
  1. Login FSTR + alokasi NORMAL (baseline + datacenterId).
  2. Alokasi ACTIVITY -> server activity.
  3. TCP connect -> event login -> auto-join (tbugv); tangkap UUID bot
     dari frame tbugv; fallback JoinRoom hanya kalau auto-join gagal.
  4. Kirim Analytics x2, lalu burst init, lalu activity/get.
  5. Baca balasan get-activity (maks ~25 dtk), catat survival (waktu
     dari login sampai tutup; zona maut v1/v2/diag = ~4,5 dtk).
  6. Bila masih hidup, tinggal di room 60 detik (pong jalan).

Kriteria sukses menurut pemilik: history join UGC target 6 -> 7.
Exit 0 hanya bila balasan "get-activity" diterima DAN activityId target
ditemukan di payload; selain itu exit 1.
Token/JWT TIDAK PERNAH dicetak ke log.
"""

import asyncio
import json
import re
import struct
import sys
import time
import uuid

import lookup_uuid as LU
from curl_cffi.requests import AsyncSession

ACTIVITY_ID_DEFAULT = "3e7cec3f-1ae9-4ba2-a19b-a6339e0f9b23"  # UGC Bartes (dipakai untuk cek activityId di payload terrain)
FRIEND_USER_ID = "aae2efe1-1abb-48c2-8799-ea192692a49c"  # DesertEagle#431 (pemilik TESGAME, teman probe)
STAY_SECONDS = 60
UUID_RE = re.compile(
    rb"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

# Burst init: payload RoomEvent lengkap (hex) dari TBLog, KECUALI
# slot_interaction (dibangun dinamis dengan UUID bot).
# Preset x8 DIHAPUS (join sukses kedua tidak mengirim preset).
# Pet: 6, lalu 19, 24 (nilai dari 2 join sukses).
# magic_potion: frame 27B klien game, dikirim berulang (spam).
FETCH_EVENTS_HEX = "080c66657463685f6576656e7473"
INIT_POS_HEX = "0808696e69745f706f73"
INIT_DAY_NIGHT_HEX = "080e696e69745f6461795f6e69676874"
PET_6_HEX = "08037065740806"
PET_19_HEX = "08037065740813"
PET_24_HEX = "08037065740818"
OUTFIT_HEX = "08066f7574666974"
VHC_HEX = "08037668630805"
STRKS_HEX = "08055354524b53"
MAGIC_POTION_HEX = "080c6d616769635f706f74696f6e080110ffffffffffffffffff01"


def frame_raw(hex_payload):
    raw = bytes.fromhex(hex_payload)
    return struct.pack(">I", len(raw)) + raw


def uuid_from_tbugv(frame):
    """Ambil UUID pemain dari frame EVENT 'tbugv' (atau None)."""
    if len(frame) < 8 or frame[0] != 0x05:
        return None
    ln, n = LU.read_varint(frame, 1)
    if ln is None or frame[1 + n:1 + n + ln] != b"tbugv":
        return None
    m = UUID_RE.search(frame[1 + n + ln:])
    return m.group(0).decode("ascii") if m else None


def find_get_activity(frames):
    """Cari frame RoomEvent 'get-activity'. Kembalikan (ketemu, ukuran)."""
    for p in frames:
        if (len(p) >= 14 and p[0] == 0x08 and p[1] == 0x0C
                and p[2:14] == b"get-activity"):
            return True, len(p)
    return False, 0


def summarize(frames):
    """Hitung nama frame dari server (EVENT/ROOMEVENT/type)."""
    counts = {}
    for p in frames:
        if len(p) >= 2 and p[0] in (0x05, 0x08):
            ln, n = LU.read_varint(p, 1)
            try:
                nm = p[1 + n:1 + n + ln].decode("utf-8") if ln is not None else "?"
            except Exception:
                nm = "?"
            key = ("EVENT " if p[0] == 0x05 else "ROOMEVENT ") + nm
        elif len(p) >= 1:
            key = f"TYPE0x{p[0]:02x}"
        else:
            key = "KOSONG"
        counts[key] = counts.get(key, 0) + 1
    return sorted(counts.items(), key=lambda kv: -kv[1])


def tcp_connect_fast(host, port):
    """Connect + kirim 0x03, baca sampai ack 0x03 (maks ~3 dtk).
    Kembalikan (socket, player_id, frame_jendela_connect) atau
    (None, None, []). TIDAK menunggu 6 dtk seperti LU.tcp_connect dan
    TIDAK membuang frame jendela connect (diag join-window membuktikan
    21-41 frame lewat di jendela itu)."""
    import socket as _socket
    try:
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((host, port))
    except Exception as e:
        print(f"[-] Gagal konek TCP: {e}")
        return None, None, []
    try:
        s.sendall(struct.pack(">I", 1) + b"\x03")
    except Exception as e:
        print(f"[-] Gagal kirim Connect: {e}")
        s.close()
        return None, None, []
    frames_all = []
    pid = None
    for _ in range(6):
        resp, closed, _ = LU.read_frames(s, 0.5)
        frames_all.extend(resp)
        for p in resp:
            if len(p) >= 5 and p[0] == 0x03:
                pid = int.from_bytes(p[1:5], "big")
        if pid is not None or closed:
            break
    if pid is not None:
        print(f"[*] Jendela Connect cepat: {len(frames_all)} frame, "
              f"playerId={pid}.")
        return s, pid, frames_all
    print("[-] Tidak ada frame Connected.")
    s.close()
    return None, None, []


def log_error_frame(fr):
    """Cetak payload frame get-activity-error (hex + string ASCII)."""
    ln, n = LU.read_varint(fr, 1)
    payload = fr[1 + n + ln:] if ln is not None else b""
    print(f"[!] ERROR-PAYLOAD len={len(payload)} hex={payload.hex()}")
    for run in re.findall(rb"[ -~]{4,}", payload):
        print(f"[!] ERROR-PAYLOAD ascii: {run.decode('ascii')}")


async def login_android(email, password, device_id, session):
    """Login FSTR + alokasi normal dengan device_info ANDROID (v7).
    Salinan LU.login_and_allocate dengan platform Android; LU TIDAK diubah
    (lookup_uuid.py produksi tetap DESKTOP)."""
    device_info = {
        "userAgent": "", "platform": "ANDROID", "system": "AndroidPlayer",
        "systemVersion": "Android OS 13 / API-33",
        "manufacturer": "Google", "deviceModel": "Pixel 7", "appVersion": LU.VERSION,
        "sdkVersion": "", "vendorId": "", "advertisingId": "",
        "pushToken": "", "cloudId": "", "carrierId": "",
        "timezone": "SE Asia Standard Time", "language": "EN",
        "country": "Unknown", "deviceId": device_id,
    }
    p1 = {"identityProvider": "FASTER",
          "credentials": {"type": "EMAIL", "email": email, "password": password},
          "deviceInfo": device_info}
    b_p1 = json.dumps(p1, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    h1 = LU.HEADERS_BASE.copy()
    h1["x-fstr-application-key"] = LU.APP_KEY
    h1["x-fstr-signature"] = LU.compute_signature("POST", "/v1/auth/sessions", b_p1)
    r1 = await session.post(LU.ACCOUNT_URL + "/v1/auth/sessions", data=b_p1, headers=h1, timeout=20)
    if r1.status_code != 200:
        print(f"[-] auth/sessions (Android) gagal: {r1.status_code}")
        return None, None
    auth_token = r1.json().get("authToken")
    await asyncio.sleep(0.2)
    p2 = {"authToken": auth_token, "deviceInfo": device_info}
    b_p2 = json.dumps(p2, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    h2 = LU.HEADERS_BASE.copy()
    h2["x-fstr-application-key"] = LU.APP_KEY
    h2["x-fstr-signature"] = LU.compute_signature("POST", "/v1/users/sessions", b_p2)
    r2 = await session.post(LU.ACCOUNT_URL + "/v1/users/sessions", data=b_p2, headers=h2, timeout=20)
    user_jwt = r2.json().get("userToken")
    if not user_jwt:
        print("[-] users/sessions (Android) tidak mengembalikan userToken")
        return None, None
    print("[+] Login Android OK, JWT didapat.")
    await asyncio.sleep(0.2)
    p3 = {"userToken": user_jwt, "gameServerProtocolVersion": LU.GAME_PROTOCOL, "isPrivate": False}
    b_p3 = json.dumps(p3, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    r3 = await session.post(LU.LOBBY_URL + "/lobby/allocation", data=b_p3,
                            headers=LU.HEADERS_BASE.copy(), timeout=20)
    print(f"[*] allocation status: {r3.status_code}")
    try:
        body = r3.json()
    except Exception:
        body = {}
    return user_jwt, body


def warm_up(user_jwt, alloc, bot):
    """Pemanasan sesi (v6): masuk room NORMAL dulu dengan JWT yang sama,
    seperti klien game yang selalu sudah online sebelum masuk UGC.
    Kembalikan True bila room sync normal didapat."""
    host, port = LU.find_host_port(alloc)
    gsid = alloc.get("gameServerId", "")
    print(f"[*] Pemanasan: server normal {host}:{port} ({gsid})")
    s, pid, _ = tcp_connect_fast(host, port)
    if s is None:
        return False
    try:
        s.sendall(LU.build_login_frame(user_jwt))
    except Exception as e:
        print(f"[-] Pemanasan: gagal kirim login: {e}")
        s.close()
        return False
    room, tbugv, closed = None, False, False
    for _ in range(8):
        resp, closed, _ = LU.read_frames(s, 1)
        tb, rm = LU.scan_login_window(resp)
        tbugv = tbugv or tb
        if rm is not None:
            room = rm
        if tbugv and room is not None:
            break
        if closed:
            break
    if room is None:
        print(f"[*] Pemanasan: room sync normal tidak datang (tbugv={tbugv}).")
        s.close()
        return False
    print(f"[*] Pemanasan: masuk room normal {room}, tinggal ~3 dtk.")
    try:
        s.sendall(LU.build_room_event_varint(
            "Analytics/sessionId", str(uuid.uuid4()).encode()))
        s.sendall(LU.build_room_event_varint(
            "Analytics/deviceId",
            str(uuid.uuid5(uuid.NAMESPACE_DNS, bot["email"])).encode()))
        potion = frame_raw(MAGIC_POTION_HEX)
        for _ in range(3):
            resp, closed, _ = LU.read_frames(s, 1)
            if closed:
                break
            s.sendall(potion)
    except Exception:
        pass
    s.close()
    return True


async def main():
    """DIAGNOSTIK JOIN activity-tier (TIDAK butuh Bartes online: server
    activity DesertEagle statis — terbukti dari sandbox saat ia offline).
    Tujuan: baca byte JoinedRoom (Success/RoomId/FailureReason/UsersInRoom)
    yang selama ini dibuang bot, + bandingkan dengan kontrol main-pool
    (JoinRoom ke room tak ada) sehingga byte FailureReason bisa diartikan.
    Token/JWT TIDAK PERNAH dicetak."""
    bot = LU.probe_bot()

    def dump_frames(tag, frames, limit=12):
        print(f"[*] {tag}: {len(frames)} frame")
        for fr in frames[:limit]:
            if not fr:
                continue
            print(f"    type=0x{fr[0]:02x} len={len(fr)} hex={fr[:120].hex()}")
        if len(frames) > limit:
            print(f"    ... +{len(frames) - limit} frame lain")

    def parse_joined(fr):
        # JoinedRoom wire: [0x06][Success 1B][RoomId 4B BE][FailureReason 1B][Users...]
        if not fr or fr[0] != 0x06 or len(fr) < 6:
            return None
        return {"success": fr[1] == 1, "room": int.from_bytes(fr[2:6], "big"),
                "fail_byte": fr[6] if len(fr) > 6 else None,
                "tail_hex": fr[7:40].hex() if len(fr) > 7 else ""}

    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        user_jwt, alloc = await login_android(
            bot["email"], bot["password"], bot["device_id"], session)
        if not user_jwt:
            print("HASIL: GAGAL login.")
            return 1
        body_friend = {"userToken": user_jwt,
                       "gameServerProtocolVersion": LU.GAME_PROTOCOL,
                       "isPrivate": False,
                       "destinationUserId": FRIEND_USER_ID}
        r = await session.post(
            LU.LOBBY_URL + "/lobby/allocation",
            data=json.dumps(body_friend, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
            headers=LU.HEADERS_BASE.copy(), timeout=20)
        act = r.json()
        print(f"[*] DIAG-A alokasi teman: {r.status_code} successful={act.get('successful')} "
              f"type={act.get('gameServerType')}")
        host, port = LU.find_host_port(act)
        gsid = act.get("gameServerId", "")
        print(f"[*] DIAG-A server: {host}:{port} ({gsid})")
        # Kontrol main-pool: alokasi normal untuk fase B
        r2 = await session.post(
            LU.LOBBY_URL + "/lobby/allocation",
            data=json.dumps({"userToken": user_jwt,
                             "gameServerProtocolVersion": LU.GAME_PROTOCOL,
                             "isPrivate": False}, separators=(",", ":")).encode("utf-8"),
            headers=LU.HEADERS_BASE.copy(), timeout=20)
        alloc_n = r2.json()
        host_n, port_n = LU.find_host_port(alloc_n)
        print(f"[*] DIAG-B server normal: {host_n}:{port_n} ({alloc_n.get('gameServerId')})")

    # ===== FASE A: activity-tier =====
    s, pid, conn_frames = tcp_connect_fast(host, port)
    if s is None:
        print("HASIL: GAGAL konek TCP activity-tier.")
        return 1
    dump_frames("DIAG-A jendela connect", conn_frames)
    s.sendall(LU.build_login_frame(user_jwt))
    print("[+] DIAG-A login terkirim.")
    all_a = []
    for _ in range(8):
        resp, closed, _ = LU.read_frames(s, 1)
        all_a += resp
        if closed:
            break
    dump_frames("DIAG-A jendela login (8 dtk)", all_a, limit=20)
    for room_try in (5, 0, 1):
        try:
            s.sendall(LU.build_join_frame(room_try))
        except Exception as e:
            print(f"[-] DIAG-A JoinRoom({room_try}) gagal kirim: {e}")
            break
        resp, closed, _ = LU.read_frames(s, 3)
        got = [parse_joined(fr) for fr in resp if fr and fr[0] == 0x06]
        print(f"[*] DIAG-A JoinRoom({room_try}): {len(resp)} frame, "
              f"JoinedRoom={got if got else 'TIDAK ADA'}, closed={closed}")
        if closed:
            break
    try:
        s.close()
    except Exception:
        pass

    # ===== FASE B: kontrol main-pool (room tak ada -> byte referensi) =====
    s, pid, conn_frames = tcp_connect_fast(host_n, port_n)
    if s is None:
        print("HASIL: GAGAL konek TCP main-pool.")
        return 1
    s.sendall(LU.build_login_frame(user_jwt))
    print("[+] DIAG-B login terkirim.")
    room_b = None
    for _ in range(12):
        resp, closed, _ = LU.read_frames(s, 1)
        tb, rm = LU.scan_login_window(resp)
        if rm is not None:
            room_b = rm
            break
        if closed:
            break
    print(f"[*] DIAG-B auto-join room={room_b}")
    try:
        s.sendall(LU.build_join_frame(9999))
    except Exception as e:
        print(f"[-] DIAG-B JoinRoom(9999) gagal kirim: {e}")
    resp, closed, _ = LU.read_frames(s, 3)
    got = [parse_joined(fr) for fr in resp if fr and fr[0] == 0x06]
    print(f"[*] DIAG-B JoinRoom(9999=room tak ada): {len(resp)} frame, "
          f"JoinedRoom={got if got else 'TIDAK ADA'}, closed={closed}")
    try:
        s.close()
    except Exception:
        pass
    print("HASIL: DIAGNOSTIK SELESAI (bandingkan fail_byte A vs B).")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
