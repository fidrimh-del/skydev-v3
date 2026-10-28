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


def read_varint(buf, off):
    v, sh = 0, 0
    while off < len(buf):
        x = buf[off]
        off += 1
        v |= (x & 0x7F) << sh
        if not (x & 0x80):
            return v, off
        sh += 7
        if sh > 35:
            return None, off
    return None, off


def unpack_batch(fr):
    """Pecah frame Batch 0x0d -> daftar pesan dalam (framing varint-length).
    Kembalikan (pesan, ok). Bila framing gagal di tengah, ok=False."""
    msgs, off = [], 1
    while off < len(fr):
        ln, off2 = read_varint(fr, off)
        if ln is None or ln <= 0 or off2 + ln > len(fr):
            return msgs, False
        msgs.append(fr[off2:off2 + ln])
        off = off2 + ln
    return msgs, True


def find_joined_in(frames):
    """Cari JoinedRoom sukses (0x06, p[1]==1) di frame top-level MAUPUN
    di dalam Batch 0x0d. Kembalikan (room, metode) atau (None, None)."""
    for fr in frames:
        if not fr:
            continue
        if fr[0] == 0x06 and len(fr) >= 6 and fr[1] == 1:
            return int.from_bytes(fr[2:6], "big"), "top-level"
        if fr[0] == 0x0d:
            msgs, ok = unpack_batch(fr)
            for m in msgs:
                if len(m) >= 6 and m[0] == 0x06 and m[1] == 1:
                    return int.from_bytes(m[2:6], "big"), "batch"
            if not ok:
                # Fallback heuristik: cari pola 06 01 + room di byte batch
                for i in range(len(fr) - 5):
                    if fr[i] == 0x06 and fr[i + 1] == 0x01:
                        return int.from_bytes(fr[i + 2:i + 6], "big"), "batch-heuristik"
    return None, None


async def main():
    """DIAG5: join PASIF via alokasi activityId-only (TANPA teman) + (TANPA Analytics/potion sebelum
    room — hipotesis: kiriman dini v5.1 menekan room assignment di tier ini).
    Tunggu JoinedRoom s.d. 25 dtk (top-level + dalam Batch), lalu bila
    join: burst + settle + activity/get x6 seperti v9. TIDAK butuh Bartes
    online (server activity statis). Token/JWT TIDAK PERNAH dicetak."""
    bot = LU.probe_bot()
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        user_jwt, alloc = await login_android(
            bot["email"], bot["password"], bot["device_id"], session)
        if not user_jwt:
            print("HASIL: GAGAL login.")
            return 1
        dc = alloc.get("gameServerDatacenterId")
        print(f"[*] DIAG5 normal baseline: {alloc.get('gameServerId')} (dc bawaan={dc}, DIPAKSA= digitalocean-blr1-02)")
        # Jalur NON-TEMAN: AllocateActivityRequest hanya butuh activityId
        # (TIDAK ada field teman/user di body) — jadi status pertemanan
        # akun probe TIDAK relevan di sini; ini simulasi bot non-teman.
        body_act = {"userToken": user_jwt, "activityId": ACTIVITY_ID_DEFAULT,
                    "datacenterId": "digitalocean-blr1-02",  # DIPAKSA blr1 (bukan dc bawaan)
                    "gameServerProtocolVersion": LU.GAME_PROTOCOL}
        r = await session.post(
            LU.LOBBY_URL + "/lobby/allocation",
            data=json.dumps(body_act, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
            headers=LU.HEADERS_BASE.copy(), timeout=20)
        act = r.json()
        print(f"[*] DIAG5 alokasi activity: {r.status_code} successful={act.get('successful')} "
              f"type={act.get('gameServerType')}")
        host, port = LU.find_host_port(act)
        print(f"[*] DIAG5 server: {host}:{port} ({act.get('gameServerId')})")

    s, pid, conn_frames = tcp_connect_fast(host, port)
    if s is None:
        print("HASIL: GAGAL konek TCP.")
        return 1
    s.sendall(LU.build_login_frame(user_jwt))
    print("[+] DIAG5 login terkirim (TANPA kiriman apa pun sesudahnya).")
    room, method, bot_uuid, t_join = None, None, None, None
    t0 = time.time()
    for _ in range(25):
        resp, closed, _ = LU.read_frames(s, 1)
        rm, md = find_joined_in(resp)
        if rm is not None:
            room, method, t_join = rm, md, time.time() - t0
        for fr in resp:
            u = uuid_from_tbugv(fr)
            if u:
                bot_uuid = u
        if room is not None:
            break
        if closed:
            break
    print(f"[*] DIAG5 join pasif: room={room} metode={method} "
          f"waktu={t_join if t_join is not None else '-'} dtk, "
          f"UUID bot={'terbaca' if bot_uuid else 'TIDAK'}, closed={closed}")
    if room is None:
        try:
            s.close()
        except Exception:
            pass
        print("HASIL: DIAG5 join pasif GAGAL (tidak ada JoinedRoom 25 dtk).")
        return 1

    # Sudah di room: burst + settle + activity/get seperti v9.
    potion = frame_raw(MAGIC_POTION_HEX)
    try:
        s.sendall(LU.build_room_event_varint(
            "Analytics/sessionId", str(uuid.uuid4()).encode()))
        s.sendall(LU.build_room_event_varint(
            "Analytics/deviceId",
            str(uuid.uuid5(uuid.NAMESPACE_DNS, bot["email"])).encode()))
        for _ in range(5):
            s.sendall(potion)
    except Exception:
        pass
    burst = []
    if bot_uuid:
        burst.append(LU.build_room_event_varint(
            "slot_interaction", b"\x08\x02\x1a\x24" + bot_uuid.encode("ascii")))
    burst += [frame_raw(FETCH_EVENTS_HEX), frame_raw(INIT_POS_HEX),
              frame_raw(INIT_DAY_NIGHT_HEX), frame_raw(PET_6_HEX)]
    if bot_uuid:
        burst.append(LU.build_room_event_varint(
            "slot_interaction", b"\x08\x02\x1a\x24" + bot_uuid.encode("ascii")))
    burst += [frame_raw(OUTFIT_HEX), frame_raw(VHC_HEX), frame_raw(STRKS_HEX)]
    try:
        for fr in burst:
            s.sendall(fr)
        print(f"[+] DIAG5 burst terkirim ({len(burst)} frame).")
    except Exception as e:
        print(f"[-] DIAG5 burst gagal: {e}")
    all_frames = []
    for _ in range(5):
        resp, closed, _ = LU.read_frames(s, 1)
        all_frames += resp
        try:
            s.sendall(potion)
        except Exception:
            break
    print(f"[*] DIAG5 settle: {len(all_frames)} frame, closed={closed}")
    got, gsize, error_count = False, 0, 0
    for i in range(6):
        try:
            s.sendall(LU.build_room_event_varint("activity/get", b""))
        except Exception:
            break
        print(f"[+] DIAG5 activity/get ke-{i + 1} terkirim.")
        for _ in range(5):
            resp, closed, _ = LU.read_frames(s, 1)
            all_frames += resp
            g, gs = find_get_activity(resp)
            if g:
                got, gsize = True, gs
                break
            for fr in resp:
                if fr and fr[0] == 0x08:
                    try:
                        l = fr[1]
                        if l < 0x80 and fr[2:2 + l] == b"get-activity-error":
                            error_count += 1
                    except Exception:
                        pass
            if closed:
                break
        if got or closed:
            break
    try:
        s.close()
    except Exception:
        pass
    if got:
        blob = b"".join(all_frames)
        found_id = ACTIVITY_ID_DEFAULT.encode("utf-8") in blob
        print(f"[+] DIAG5 TERRAIN DITERIMA ({gsize}B), activityId di payload={found_id}.")
        print("HASIL: DIAG5 TERRAIN VIA DC BLR1 = BOT NON-TEMAN BISA JOIN.")
        return 0
    print(f"[-] DIAG5 tanpa terrain (get-activity-error={error_count}x, closed={closed}).")
    print("HASIL: DIAG5 dc blr1 tetap error = BUTUH JALUR TEMAN / PERTEMANAN.")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
