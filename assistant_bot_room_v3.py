"""Assistant Bot UDP — join PASIF ke server DesertEagle (teman) via AllocateToUserId, lalu 120 detik kirim POSISI via UDP
(format C-message 21B hasil decode v60: header 08 01 43 00 + seq + X/Y/Z/rot float32 BIG-ENDIAN) ke gameServerUdpPort,
sambil merekam broadcast UDP masuk. Target posisi via env BOT_X/BOT_Y/BOT_Z/BOT_ROT (default titik diam akhir sesi log v60).
BUKAN UGC: tidak ada activity/get. Tujuannya satu: apakah wujud PKXDSHOP terlihat di dunia Bartes selama 120 detik itu.
"""


import asyncio
import os
import socket
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

def build_getroom_protobuf(friend_id):
    fb = friend_id.encode("utf-8")
    return b"\x08\x06\x12" + LU.varint(len(fb)) + fb


def parse_room_event(fr):
    if not fr or fr[0] != 0x08 or len(fr) < 2:
        return None
    ln, n = LU.read_varint(fr, 1)
    if ln is None:
        return None
    return fr[1 + n:1 + n + ln].decode("utf-8", "replace"), fr[1 + n + ln:]


def decode_result(data):
    out = {}
    for f, wt, v in LU.parse_protobuf_fields(data):
        if wt == 0:
            if isinstance(v, int):
                out[f"field{f}"] = v
            elif isinstance(v, (bytes, bytearray)) and len(v) == 1:
                out[f"field{f}"] = v[0]
            else:
                out[f"field{f}"] = v.hex() if isinstance(v, (bytes, bytearray)) else v
        elif wt == 2:
            try:
                out[f"field{f}"] = v.decode("utf-8", "replace")
            except Exception:
                out[f"field{f}"] = v.hex()
        else:
            out[f"field{f}_wt{wt}"] = v.hex() if isinstance(v, (bytes, bytearray)) else v
    return out


def pick_room_string(data):
    # Room = string yang tampak seperti id server (mengandung '::' + 'pkxd-tb-prod').
    # Jangan percaya nomor field (diag sempat salah label field2/field3).
    try:
        fields = decode_result(data)
    except Exception:
        return None
    for v in fields.values():
        if isinstance(v, str) and "::" in v and "pkxd-tb-prod" in v:
            return v
    return None

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
    """ASISTEN: join PASIF ke server activity (TANPA Analytics/potion sebelum
    room — hipotesis: kiriman dini v5.1 menekan room assignment di tier ini).
    Tunggu JoinedRoom s.d. 25 dtk (top-level + dalam Batch), lalu bila
    join: burst + settle. TIDAK butuh Bartes
    online (server activity statis). Token/JWT TIDAK PERNAH dicetak."""
    bot = LU.probe_bot()
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        user_jwt, alloc = await login_android(
            bot["email"], bot["password"], bot["device_id"], session)
        if not user_jwt:
            print("HASIL: GAGAL login.")
            return 1
        # v2: koneksi #1 pakai alokasi UMUM (tanpa tujuan) — BUKAN alokasi
        # teman. Alasan: v1 membuktikan AllocateToRoomId ditolak
        # ALREADY_ALLOCATED_TO_THE_ROOM bila bot sudah teralokasi ke room
        # itu lewat AllocateToUserId. Klien asli diduga mengalokasi room
        # dari server lain (sesi sendiri), bukan dari server teman.
        body_friend = {"userToken": user_jwt,
                       "gameServerProtocolVersion": LU.GAME_PROTOCOL,
                       "isPrivate": False}
        r = await session.post(
            LU.LOBBY_URL + "/lobby/allocation",
            data=json.dumps(body_friend, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
            headers=LU.HEADERS_BASE.copy(), timeout=20)
        act = r.json()
        print(f"[*] ASISTEN ROOM v3 alokasi umum: {r.status_code} successful={act.get('successful')} "
              f"type={act.get('gameServerType')}")
        host, port = LU.find_host_port(act)
        print(f"[*] ASISTEN server: {host}:{port} ({act.get('gameServerId')})")

    s, pid, conn_frames = tcp_connect_fast(host, port)
    if s is None:
        print("HASIL: GAGAL konek TCP.")
        return 1
    s.sendall(LU.build_login_frame(user_jwt))
    print("[+] ASISTEN login terkirim (TANPA kiriman apa pun sesudahnya).")
    # ROOM: koneksi #1 TANPA UDP (hanya untuk GetRoom); sessionId/deviceId
    # dibuat sekali, dipakai bersama di burst TCP koneksi #1 dan #2.
    session_id = str(uuid.uuid4())
    device_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, bot["email"]))
    u = None
    room, method, bot_uuid, t_join = None, None, None, None
    t0 = time.time()
    for _ in range(25):
        resp, closed, _ = LU.read_frames(s, 1)
        rm, md = find_joined_in(resp)
        if rm is not None:
            room, method, t_join = rm, md, time.time() - t0
        for fr in resp:
            tu = uuid_from_tbugv(fr)
            if tu:
                bot_uuid = tu
        if room is not None:
            break
        if closed:
            break
    print(f"[*] ASISTEN join pasif: room={room} metode={method} "
          f"waktu={t_join if t_join is not None else '-'} dtk, "
          f"UUID bot={'terbaca' if bot_uuid else 'TIDAK'}, closed={closed}")
    if room is None:
        try:
            s.close()
        except Exception:
            pass
        print("HASIL: ASISTEN join pasif GAGAL (tidak ada JoinedRoom 25 dtk).")
        return 1

    # Sudah di room: burst + settle.
    # sessionId/deviceId dibuat di init UDP awal, dipakai bersama di burst TCP.
    potion = frame_raw(MAGIC_POTION_HEX)
    try:
        s.sendall(LU.build_room_event_varint(
            "Analytics/sessionId", session_id.encode()))
        s.sendall(LU.build_room_event_varint(
            "Analytics/deviceId", device_id.encode()))
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
        print(f"[+] ASISTEN burst terkirim ({len(burst)} frame).")
    except Exception as e:
        print(f"[-] ASISTEN burst gagal: {e}")
    all_frames = []
    for _ in range(5):
        resp, closed, _ = LU.read_frames(s, 1)
        all_frames += resp
        try:
            s.sendall(potion)
        except Exception:
            break
    print(f"[*] ASISTEN settle: {len(all_frames)} frame, closed={closed}")
    # ===== JALUR ROOM: GetRoom -> AllocateToRoomId -> koneksi #2 =====
    # (meniru klien asli: GetRoom di koneksi lama, lalu alokasi berbasis
    # room, bukan berbasis teman seperti v5.1)
    try:
        s.sendall(LU.build_room_event_varint("friends", build_getroom_protobuf(FRIEND_USER_ID)))
        print("[+] ASISTEN ROOM v3 GetRoom terkirim (EventId=6, FriendId=UUID DesertEagle).")
    except Exception as e:
        print(f"[-] ASISTEN ROOM v3 GetRoom gagal dikirim: {e}")
        return 1
    room_str = None
    for _ in range(30):
        resp, closed, _ = LU.read_frames(s, 1)
        for fr in resp:
            ev = parse_room_event(fr)
            if ev is None:
                if fr and fr[0] == 0x0d:
                    msgs, _ok = unpack_batch(fr)
                    for m in msgs:
                        ev2 = parse_room_event(m)
                        if ev2 is not None and ev2[0] == "get_friend_room_result":
                            room_str = pick_room_string(ev2[1])
                continue
            if ev[0] == "get_friend_room_result":
                room_str = pick_room_string(ev[1])
        if room_str is not None or closed:
            break
    print(f"[*] ASISTEN ROOM v3 GetRoom Room={room_str}")
    try:
        s.close()
    except Exception:
        pass
    if not room_str:
        print("HASIL: ASISTEN ROOM v3 GetRoom TANPA JAWABAN — batal ke jalur room.")
        return 1
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        body_room = {"userToken": user_jwt,
                     "gameServerProtocolVersion": LU.GAME_PROTOCOL,
                     "isPrivate": False,
                     "destinationRoomId": room_str}
        r2 = await session.post(
            LU.LOBBY_URL + "/lobby/allocation",
            data=json.dumps(body_room, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
            headers=LU.HEADERS_BASE.copy(), timeout=20)
        act2 = r2.json()
        print(f"[*] ASISTEN ROOM v3 alokasi: {r2.status_code} successful={act2.get('successful')} "
              f"type={act2.get('gameServerType')} alasan={act2.get('failureReason') or act2.get('failureDescription') or '-'}")
        if not act2.get("successful"):
            print("HASIL: ASISTEN ROOM v3 alokasi GAGAL.")
            return 1
        host2, port2 = LU.find_host_port(act2)
        print(f"[*] ASISTEN ROOM v3 server: {host2}:{port2} ({act2.get('gameServerId')})")
    s, pid, conn_frames = tcp_connect_fast(host2, port2)
    if s is None:
        print("HASIL: ASISTEN ROOM v3 GAGAL konek TCP #2.")
        return 1
    s.sendall(LU.build_login_frame(user_jwt))
    print("[+] ASISTEN ROOM v3 login #2 terkirim.")
    udp_host = os.getenv("BOT_UDP_HOST") or host2
    udp_port = int(os.getenv("BOT_UDP_PORT") or 0) or act2.get("gameServerUdpPort") or port2
    src = "log mod" if os.getenv("BOT_UDP_PORT") else ("alokasi" if act2.get("gameServerUdpPort") else "fallback port TCP")
    print(f"[*] ASISTEN ROOM v3 UDP target: {udp_host}:{udp_port} (sumber: {src}; TCP {host2}:{port2})")
    u = None
    if udp_port:
        try:
            u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            u.settimeout(0.2)
            u.connect((udp_host, int(udp_port)))
            print(f"[+] ASISTEN ROOM v3 UDP connected {udp_host}:{udp_port}")
            try:
                u.send(struct.pack(">I", 1) + b"\x03")
                u.send(LU.build_login_frame(user_jwt))
                u.send(LU.build_room_event_varint("Analytics/sessionId", session_id.encode()))
                u.send(LU.build_room_event_varint("Analytics/deviceId", device_id.encode()))
                print("[+] ASISTEN ROOM v3 UDP init AWAL terkirim (sebelum join #2).")
            except Exception as e:
                print(f"[-] ASISTEN ROOM v3 UDP init awal gagal: {e}")
                u = None
        except Exception as e:
            print(f"[-] ASISTEN ROOM v3 UDP connect gagal: {e}")
            u = None
    else:
        print("[-] ASISTEN ROOM v3 alokasi TIDAK memuat gameServerUdpPort — fase UDP dilewati.")
    # v3: koneksi #2 BURST DULU (seperti klien asli stream 2: login ->
    # sessionId/deviceId/potion langsung, TANPA menunggu JoinedRoom),
    # baru tunggu JoinedRoom sambil spam potion. v2 membuktikan jalur
    # room TIDAK auto-assign bila bot diam pasif 28 dtk (closed=False).
    potion = frame_raw(MAGIC_POTION_HEX)
    try:
        s.sendall(LU.build_room_event_varint("Analytics/sessionId", session_id.encode()))
        s.sendall(LU.build_room_event_varint("Analytics/deviceId", device_id.encode()))
        for _ in range(5):
            s.sendall(potion)
    except Exception:
        pass
    burst = []
    if bot_uuid:
        burst.append(LU.build_room_event_varint("slot_interaction", b"\x08\x02\x1a\x24" + bot_uuid.encode("ascii")))
    burst += [frame_raw(FETCH_EVENTS_HEX), frame_raw(INIT_POS_HEX),
              frame_raw(INIT_DAY_NIGHT_HEX), frame_raw(PET_6_HEX)]
    if bot_uuid:
        burst.append(LU.build_room_event_varint("slot_interaction", b"\x08\x02\x1a\x24" + bot_uuid.encode("ascii")))
    burst += [frame_raw(OUTFIT_HEX), frame_raw(VHC_HEX), frame_raw(STRKS_HEX)]
    try:
        for fr in burst:
            s.sendall(fr)
        print(f"[+] ASISTEN ROOM v3 burst #2 terkirim ({len(burst)} frame, SEBELUM join).")
    except Exception as e:
        print(f"[-] ASISTEN ROOM v3 burst #2 gagal: {e}")
    room, method, bot_uuid, t_join = None, None, None, None
    t0 = time.time()
    for _ in range(25):
        resp, closed, _ = LU.read_frames(s, 1)
        rm, md = find_joined_in(resp)
        if rm is not None:
            room, method, t_join = rm, md, time.time() - t0
        for fr in resp:
            tu = uuid_from_tbugv(fr)
            if tu:
                bot_uuid = tu
        if room is not None:
            break
        if closed:
            break
        try:
            s.sendall(potion)
        except Exception:
            break
    print(f"[*] ASISTEN ROOM v3 join pasif #2: room={room} metode={method} "
          f"waktu={t_join if t_join is not None else '-'} dtk, "
          f"UUID bot={'terbaca' if bot_uuid else 'TIDAK'}, closed={closed}")
    if room is None:
        try:
            s.close()
        except Exception:
            pass
        print("HASIL: ASISTEN ROOM v3 join pasif #2 GAGAL.")
        return 1
    all_frames = []
    for _ in range(5):
        resp, closed, _ = LU.read_frames(s, 1)
        all_frames += resp
        try:
            s.sendall(potion)
        except Exception:
            break
    print(f"[*] ASISTEN ROOM v3 settle #2: {len(all_frames)} frame, closed={closed}")

# ===== FASE UDP: kirim posisi 21B via UDP 120 detik + rekam broadcast =====
    # v5: socket UDP + init sudah dilakukan AWAL (sebelum join); di sini
    # hanya kirim posisi lewat socket yang sama.
    tx = float(os.getenv("BOT_X", "8.31"))
    ty = float(os.getenv("BOT_Y", "22.13"))
    tz = float(os.getenv("BOT_Z", "-66.42"))
    trot = float(os.getenv("BOT_ROT", "319.3"))
    print(f"[*] ASISTEN target posisi: X={tx} Y={ty} Z={tz} rot={trot}")
    print(f"[*] ASISTEN UDP socket awal: {'siap' if u else 'TIDAK ADA (init awal gagal)'}")
    seq = 0
    udp_in = []
    udp_in_total = 0
    print("[*] ASISTEN fase UDP 120 dtk (posisi tiap ~0.7 dtk + potion TCP + auto-pong).")
    t0 = time.time()
    closed_early = None
    while time.time() - t0 < 120:
        resp, closed, _ = LU.read_frames(s, 0.5)
        if closed:
            closed_early = int(time.time() - t0)
            print(f"[-] ASISTEN TCP ditutup server lebih awal (detik ke-{closed_early}).")
            break
        try:
            s.sendall(LU.build_room_event_varint("magic_potion", bytes.fromhex("080110ffffffffffffffffff01")))
        except Exception:
            break
        if u is not None:
            try:
                payload = bytes([0x08, 0x01, 0x43, 0x00, seq]) + struct.pack(">ffff", tx, ty, tz, trot)
                u.send(struct.pack(">I", len(payload)) + payload)
                seq = (seq + 1) % 256
            except Exception as e:
                print(f"[-] ASISTEN UDP send gagal: {e}")
                u = None
            if u is not None:
                try:
                    while True:
                        data, _ = u.recvfrom(65535)
                        udp_in_total += 1
                        if data == bytes.fromhex("0000000101"):
                            try:
                                u.send(bytes.fromhex("0000000102"))
                            except Exception:
                                pass
                        if len(udp_in) < 50:
                            udp_in.append(data.hex())
                except socket.timeout:
                    pass
                except Exception:
                    pass
        el = int(time.time() - t0)
        if el % 10 == 0:
            print(f"[*] ASISTEN UDP online {el} dtk, closed=False, udp_masuk={udp_in_total}")
    print(f"[*] ASISTEN UDP masuk total: {udp_in_total} datagram (tampil maks 50).")
    for h in udp_in:
        print(f"    [UDP-IN] len={len(bytes.fromhex(h))} hex={h[:200]}")
    try:
        s.close()
    except Exception:
        pass
    if u is not None:
        try:
            u.close()
        except Exception:
            pass
    if closed_early is None:
        print("HASIL: ASISTEN UDP 120 dtk selesai -> BOT OFFLINE (diskonek sesuai rencana).")
    else:
        print(f"HASIL: ASISTEN UDP offline lebih awal (detik ke-{closed_early}) -> BOT OFFLINE.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
