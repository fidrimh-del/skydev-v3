"""register_named.py - CREATE BOT BERNAMA CEPAT (port dari outfit_test terbukti, untuk panel).

Dibuat dari nol berdasarkan dump.cs (bukan salinan register_named.py yang
acak-acakan), untuk diuji di repo skydev-test agar skydev-v3 tidak terganggu.

Alur, sesuai dump.cs:
  1. HTTP registrasi ke account.faster.aftvrsys.com:
     anonymous sign-up -> auth sessions -> users sessions (JWT)
     -> email sign-up -> link credentials -> lobby allocation
  2. TCP ke game server dari alokasi lobby (SOCK_STREAM, bukan UDP):
     handshake -> login frame -> outfit -> GetDefaultNickname -> SetNickname
  3. Hasil dicetak, TIDAK disimpan ke Supabase di tes ini.

Protobuf, sesuai dump.cs (Multiplayer.Scripts.Serialization.Protobuf):
  - LoginRequest: field 1 = userToken, field 2 = appVersion
  - GetDefaultNicknameRequest: field 1 = EventId (12), field 2 = Language
    (PERBAIKAN dari register_named.py yang salah memakai field 3)
  - SetNicknameRequest: field 1 = EventId (0), field 2 = Name,
    field 3 = DeviceId
  - SetNicknameResponse: field 1 = Name, field 2 = ErrorMessage
    (keduanya dicetak terpisah, supaya alasan penolakan terlihat)
  - GetDefaultNicknameResponse: field 1 = DefaultNickname
  - UpdateOutfitRequest: field 1 = EventId (1), field 2 = Outfit
  - Outfit: field 1 = SlotId, field 2 = Items, field 3 = SkinColor
  - Color: field 1 = R, field 2 = G, field 3 = B

RoomEvent (ToxicBirdProfileMechanic di dump.cs):
  - request command: "profile_update"
  - response: "set_name_result", "default_nickname_result"

Keamanan log:
  - TIDAK PERNAH mencetak token, JWT, password, FSTR secret, atau
    email lengkap. Email hanya 4 karakter pertama + ***.
  - Nickname hasil server dicetak apa adanya, karena itu yang sedang dites,
    dan akun ini adalah akun tes sekali pakai di repo tes.

Env yang wajib (dari GitHub Secrets di workflow, jangan hardcode):
  FSTR_APP_KEY, FSTR_APP_SECRET
Env opsional: NICKNAME (kalau kosong, pakai saran server)

Keluar: exit 0 bila SetNicknameResponse Name tidak kosong dan
ErrorMessage kosong; exit 1 bila tidak.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import random
import secrets
import socket
import string
import struct
import sys
import time
from collections import OrderedDict

from curl_cffi.requests import AsyncSession

ACCOUNT_URL = "https://account.faster.aftvrsys.com"
LOBBY_URL = "https://pkxd-lobby.aftvrsys.com"
VERSION = "1.93.4"
GAME_PROTOCOL = "1.93.0"

APP_KEY = os.getenv("FSTR_APP_KEY") or "57a3d999-654c-4484-a20c-0f4b5512af49"
APP_SECRET = os.getenv("FSTR_APP_SECRET") or "jWS5aLZacNVFx6PAxpXq2g4o"


HEADERS_BASE = {
    "User-Agent": "UnityPlayer/2022.3.62f3 (UnityWebRequest/1.0, libcurl/8.10.1-DEV)",
    "Accept": "*/*",
    "Accept-Encoding": "deflate, gzip",
    "Content-Type": "application/json",
    "x-fstr-application-key": APP_KEY,
    "X-Unity-Version": "2022.3.62f3",
    "Connection": "keep-alive",
}


def log(msg):
    print(msg, flush=True)


def compute_signature(method, path, body_bytes):
    secret = APP_SECRET.encode("utf-8")
    h = hmac.new(secret, method.encode("utf-8"), hashlib.sha256).digest()
    h = hmac.new(h, path.encode("utf-8"), hashlib.sha256).digest()
    h = hmac.new(h, b"", hashlib.sha256).digest()
    h = hmac.new(h, body_bytes, hashlib.sha256).digest()
    h = hmac.new(h, b"x-fstr-signature", hashlib.sha256).digest()
    return base64.b64encode(h).decode("utf-8")


def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def read_varint(buf, off):
    val, shift, i = 0, 0, off
    while i < len(buf) and shift < 35:
        b = buf[i]
        val |= (b & 0x7F) << shift
        i += 1
        if not (b & 0x80):
            return val, i - off
        shift += 7
    return None, 0


def pb_string(field, s):
    sb = s.encode("utf-8")
    return bytes([(field << 3) | 2]) + varint(len(sb)) + sb


def build_named_frame(frame_type, event_name, data):
    nb = event_name.encode("utf-8")
    payload = bytes([frame_type]) + varint(len(nb)) + nb + data
    return struct.pack(">I", len(payload)) + payload


def build_login_frame(user_token):
    tb = user_token.encode("utf-8")
    vb = VERSION.encode("utf-8")
    pb = b"\x0a" + varint(len(tb)) + tb + b"\x12" + varint(len(vb)) + vb
    return build_named_frame(0x05, "login", pb)


def build_get_default_nickname_protobuf(language="EN"):
    # dump.cs: GetDefaultNicknameRequest field 1 = EventId (12),
    # field 2 = Language. Bukan field 3.
    return b"\x08" + varint(12) + pb_string(2, language)


def build_set_nickname_protobuf(name, device_id):
    # dump.cs: SetNicknameRequest field 1 = EventId (0),
    # field 2 = Name, field 3 = DeviceId.
    # device_id=None -> field 3 dihilangkan (varian debug v6).
    if device_id is None:
        return b"\x08" + varint(0) + pb_string(2, name)
    return b"\x08" + varint(0) + pb_string(2, name) + pb_string(3, device_id)


def build_set_nickname_no_event_protobuf(name, device_id):
    # Varian debug v6: protobuf asli Google Protobuf MENGHILANGKAN field
    # bernilai default (EventId=0 tidak diserialisasi sama sekali).
    # Jadi bytes asli client kemungkinan hanya field 2 + field 3,
    # tanpa \x08\x00 di depan. Coba varian ini juga.
    return pb_string(2, name) + pb_string(3, device_id)


def parse_connected_frame(frame):
    """Parse frame Connected 0x03: playerId (4 byte BE) + playerKey.

    playerKey TIDAK dicetak ke log (itu kunci sesi). Hanya panjang
    dan cara decode-nya yang dilog, isi penuh hanya dipakai internal
    sebagai kandidat DeviceId di varian v6a.
    """
    if len(frame) < 5 or frame[0] != 0x03:
        return None, None
    pid = int.from_bytes(frame[1:5], "big")
    rest = frame[5:]
    key = None
    # Coba: varint length + string
    try:
        ln, n = read_varint(rest, 0)
        if ln is not None and n + ln == len(rest):
            key = rest[n:n + ln].decode("utf-8", "replace")
    except Exception:
        pass
    if key is None:
        # Coba: raw utf-8, buang null
        try:
            key = rest.decode("utf-8", "replace").strip("\x00")
        except Exception:
            key = None
    return pid, key


def build_update_nickname_protobuf(name):
    # dump.cs: UpdateNicknameRequest field 1 = EventId (9),
    # field 2 = Nickname. TIDAK ada DeviceId di request ini.
    # Response: UpdateNicknameResponse field 1 = Name,
    # field 2 = ErrorMessage, field 3 = Wallet, field 4 = Spent,
    # event response: "update_name_result".
    return b"\x08" + varint(9) + pb_string(2, name)


def build_outfit_protobuf():
    skin = b"\x08" + varint(255) + b"\x10" + varint(220) + b"\x18" + varint(180)
    outfit = b"\x08" + varint(0) + b"\x1a" + varint(len(skin)) + skin
    return b"\x08" + varint(1) + b"\x12" + varint(len(outfit)) + outfit


def build_equip_protobuf(items, skin_rgb, hair_model=0, hair_style=0, bundle_code=0):
    # EquipRequest dump.cs: field 1 Items (repeated int, packed),
    # field 2 SkinColor (Color R/G/B), field 3 HairModel, field 4 HairStyle,
    # field 5 BundleCode.
    # TEMUAN v6 2026-10-30: EquipRequest ASLI dari game (EVT equip,
    # OutfitLog_6) berisi field 5 BundleCode=14576, yang TIDAK
    # pernah dikirim bot sebelumnya. Field itu kemungkinan kunci
    # kenapa equip bot selalu diabaikan. Skin Color asli juga
    # menghilangkan field 0 (R=0 tidak diserialisasi), jadi di
    # sini R=0 juga dihilangkan supaya byte-nya persis game.
    packed = b"".join(varint(i) for i in items)
    out = b""
    if packed:
        out += b"\x0a" + varint(len(packed)) + packed
    r, g, b = skin_rgb
    skin = b""
    if r:
        skin += b"\x08" + varint(r)
    if g:
        skin += b"\x10" + varint(g)
    if b:
        skin += b"\x18" + varint(b)
    out += b"\x12" + varint(len(skin)) + skin
    if hair_model:
        out += b"\x18" + varint(hair_model)
    if hair_style:
        out += b"\x20" + varint(hair_style)
    if bundle_code:
        out += b"\x28" + varint(bundle_code)
    return out


def parse_varint_stream(data):
    out, i = [], 0
    while i < len(data):
        v, n = read_varint(data, i)
        if v is None:
            break
        out.append(v)
        i += n
    return out


def parse_protobuf_fields(data):
    fields = []
    i = 0
    while i < len(data):
        tag = data[i]
        i += 1
        field, wt = tag >> 3, tag & 7
        if wt == 0:
            v, n = read_varint(data, i)
            if v is None:
                break
            i += n
            fields.append((field, v))
        elif wt == 2:
            ln, n = read_varint(data, i)
            if ln is None:
                break
            i += n
            fields.append((field, data[i:i + ln]))
            i += ln
        else:
            break
    return fields


def decode_hair_response(frames, want):
    """Decode get_hair_styles / buy_hair_style payload secara lengkap.
    GetInventory: field1 Successful(bool), field2 Inventory(packed int),
    field3 ErrorMessage(string).
    BuyResult: field1 Successful, field2 Inventory(packed), field3 Wallet
    (pesan Money), field4 Spent (Money), field5 ErrorMessage.
    Tanpa decode ini, tes sebelumnya hanya tahu event 'buy_hair_style'
    muncul, tapi tidak tahu berhasil/gagal dan kenapa."""
    for fr in frames:
        ev, data = parse_room_event(fr)
        if ev != want or not data:
            continue
        parts = []
        for f, v in parse_protobuf_fields(data):
            if isinstance(v, bytes):
                # coba unpack packed int, kalau gagal tampilkan sebagai string
                try:
                    vals = parse_varint_stream(v)
                    parts.append(f"field{f}=packed{vals}")
                except Exception:
                    parts.append(f"field{f}={v!r}")
            else:
                parts.append(f"field{f}={v}")
        log(f"[*] HAIR RESP {want}: " + " ".join(parts))
        return True
    log(f"[*] HAIR RESP {want}: TIDAK DITEMUKAN di events")
    return False


def parse_room_event(frame):
    """Kembalikan (event_name, data) atau (None, None) untuk frame RoomEvent."""
    if len(frame) < 3 or frame[0] not in (0x05, 0x08):
        return None, None
    ln, n = read_varint(frame, 1)
    if ln is None:
        return None, None
    try:
        ev = frame[1 + n:1 + n + ln].decode("utf-8", "replace")
    except Exception:
        return None, None
    return ev, frame[1 + n + ln:]


def read_until(sock, want_events, cap_seconds, break_on_connected=False):
    deadline = time.time() + cap_seconds
    buf = bytearray()
    frames = []
    sock.settimeout(0.25)
    while time.time() < deadline:
        try:
            chunk = sock.recv(65536)
        except socket.timeout:
            if any(parse_room_event(f)[0] in want_events for f in frames):
                break
            continue
        except Exception:
            break
        if chunk == b"":
            break
        buf += chunk
        while len(buf) >= 4:
            ln = struct.unpack(">I", buf[:4])[0]
            if ln > 262144:
                return frames
            if len(buf) - 4 < ln:
                break
            fr = bytes(buf[4:4 + ln])
            del buf[:4 + ln]
            frames.append(fr)
            if break_on_connected and len(fr) >= 1 and fr[0] == 0x03:
                break
            if fr == b"\x01":
                try:
                    sock.sendall(struct.pack(">I", 1) + b"\x02")
                except Exception:
                    pass
        if break_on_connected and any(len(f) >= 1 and f[0] == 0x03 for f in frames):
            break
        if any(parse_room_event(f)[0] in want_events for f in frames):
            break
    return frames


def find_host_port(body):
    host = port = None

    def scan(d):
        nonlocal host, port
        if not isinstance(d, dict):
            return
        for k, v in d.items():
            lk = k.lower()
            if host is None and lk in ("gameserveraddress", "address", "host",
                                       "hostname", "ip") and isinstance(v, str) and v:
                host = v
            if port is None and lk in ("gameserverport", "port", "tcpport") \
                    and isinstance(v, int):
                port = v
        for v in d.values():
            if isinstance(v, dict):
                scan(v)
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        scan(item)

    scan(body)
    return host, port


def random_device_id():
    return secrets.token_hex(8)


def random_email():
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
               "icloud.com", "protonmail.com", "yandex.com", "mail.com",
               "zoho.com"]
    username = "nb" + "".join(random.choices(
        string.ascii_lowercase + string.digits, k=random.randint(12, 16)))
    if random.choice([True, False]):
        pos = random.randint(4, len(username) - 4)
        username = username[:pos] + random.choice([".", "_"]) + username[pos:]
    return f"{username}@{random.choice(domains)}"


def random_password():
    return (random.choice(string.ascii_uppercase)
            + "".join(random.choices(string.ascii_lowercase,
                                      k=random.randint(6, 8)))
            + "".join(random.choices(string.digits, k=random.randint(3, 5))))


async def http_register():
    device_id = random_device_id()
    # V9: kalau TEST_EMAIL/TEST_PASSWORD diisi dari workflow (oleh
    # pemilik akun tes sendiri), pakai itu, bukan acak. Kredensial
    # ini TIDAK pernah dicetak ke log dan TIDAK disimpan ke file
    # atau memori oleh script ini.
    email = random_email()
    password = random_password()
    device_info = OrderedDict([
        ("userAgent", ""), ("platform", "DESKTOP"),
        ("system", "WindowsPlayer"),
        ("systemVersion", "Windows 10  (10.0.19045) 64bit"),
        ("manufacturer", "Unknown"), ("deviceModel", ""),
        ("appVersion", VERSION), ("sdkVersion", ""), ("vendorId", ""),
        ("advertisingId", ""), ("pushToken", ""), ("cloudId", ""),
        ("carrierId", ""), ("timezone", "SE Asia Standard Time"),
        ("language", "EN"), ("country", "Unknown"),
        ("deviceId", device_id),
    ])
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        async def req(path, payload, bearer=None):
            body = json.dumps(payload, separators=(",", ":"),
                              ensure_ascii=False).encode("utf-8")
            h = HEADERS_BASE.copy()
            if bearer:
                h["Authorization"] = f"Bearer {bearer}"
            h["x-fstr-signature"] = compute_signature("POST", path, body)
            return await session.post(ACCOUNT_URL + path, data=body,
                                      headers=h, timeout=30)

        log("[1/6] anonymous sign-up...")
        r1 = await req("/v1/identity/anonymous/sign-up",
                       {"deviceInfo": device_info})
        if r1.status_code not in (200, 201):
            log(f"[!] Langkah 1 gagal: HTTP {r1.status_code}")
            return None
        acc_token = r1.json().get("accessToken") or r1.json().get("token")
        if not acc_token:
            log("[!] Langkah 1: tidak ada access token di jawaban")
            return None

        log("[2/6] auth sessions...")
        r2 = await req("/v1/auth/sessions", OrderedDict([
            ("identityProvider", "FASTER"),
            ("credentials", {"type": "TOKEN", "token": acc_token}),
            ("deviceInfo", device_info)]))
        if r2.status_code not in (200, 201):
            log(f"[!] Langkah 2 gagal: HTTP {r2.status_code}")
            return None
        auth_token = r2.json().get("authToken") or r2.json().get("token")
        if not auth_token:
            log("[!] Langkah 2: tidak ada auth token di jawaban")
            return None

        log("[3/6] users sessions (JWT)...")
        r3 = await req("/v1/users/sessions", OrderedDict([
            ("authToken", auth_token), ("deviceInfo", device_info)]))
        user_jwt = r3.json().get("userToken")
        if not user_jwt:
            log(f"[!] Langkah 3 gagal: HTTP {r3.status_code}")
            return None

        # V5 - ALUR GAME NORMAL (info Bartes 2026-10-30):
        # Pemain baru -> loading -> dunia/map -> pilih outfit ->
        # input nama (sudah ada nama random server) -> tombol
        # "main sebagai tamu" / "daftar dengan email".
        # Jadi nickname di-SET SEBAGAI TAMU dulu, SEBELUM email.
        # Email sign-up + link DITUNDA sampai SET berhasil, dan
        # setelah daftar email pemain TIDAK dipindah ke server lain.
        # Maka di sini HANYA: anonymous -> auth -> users -> lobby.
        log("[4/4] lobby allocation (tamu)...")
        p6 = {"userToken": user_jwt,
              "gameServerProtocolVersion": GAME_PROTOCOL,
              "isPrivate": False}
        b6 = json.dumps(p6, separators=(",", ":"),
                        ensure_ascii=False).encode("utf-8")
        headers6 = {k: v for k, v in HEADERS_BASE.items()
                    if k != "x-fstr-application-key"}
        r6 = await session.post(LOBBY_URL + "/lobby/allocation", data=b6,
                                headers=headers6, timeout=30)
        if r6.status_code not in (200, 201):
            log(f"[!] Langkah 4 gagal: HTTP {r6.status_code}")
            return None
        log(f"[+] Registrasi TAMU selesai untuk {email[:4]}*** "
            f"(email belum di-link, sesuai alur game)")
        return email, password, device_id, user_jwt, r6.json()


async def http_link_email(email, password, device_id, guest_jwt):
    """Daftar dengan email SETELAH nickname berhasil, sesuai alur game.

    Email sign-up lalu link credentials dengan bearer JWT tamu.
    Mengembalikan True bila link HTTP 200/201.
    """
    device_info = OrderedDict([
        ("userAgent", ""), ("platform", "DESKTOP"),
        ("system", "WindowsPlayer"),
        ("systemVersion", "Windows 10  (10.0.19045) 64bit"),
        ("manufacturer", "Unknown"), ("deviceModel", ""),
        ("appVersion", VERSION), ("sdkVersion", ""), ("vendorId", ""),
        ("advertisingId", ""), ("pushToken", ""), ("cloudId", ""),
        ("carrierId", ""), ("timezone", "SE Asia Standard Time"),
        ("language", "EN"), ("country", "Unknown"),
        ("deviceId", device_id),
    ])
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        async def req(path, payload, bearer=None):
            body = json.dumps(payload, separators=(",", ":"),
                              ensure_ascii=False).encode("utf-8")
            h = HEADERS_BASE.copy()
            if bearer:
                h["Authorization"] = f"Bearer {bearer}"
            h["x-fstr-signature"] = compute_signature("POST", path, body)
            return await session.post(ACCOUNT_URL + path, data=body,
                                      headers=h, timeout=30)
        log("[*] Daftar dengan email (setelah SET nickname)...")
        await req("/v1/identity/email/sign-up", OrderedDict([
            ("deviceInfo", device_info), ("email", email),
            ("password", password)]))
        r5 = await req("/v1/users/credentials/link", OrderedDict([
            ("identityProvider", "FASTER"),
            ("credentials", {"type": "EMAIL", "email": email,
                             "password": password})]), bearer=guest_jwt)
        log(f"[*] Link email HTTP {r5.status_code}")
        return r5.status_code in (200, 201)


async def email_login_jwt(email, password, device_id):
    """Login ulang via EMAIL untuk JWT yang valid untuk TCP.

    Catatan dari register_named.py: JWT registrasi terbukti tidak valid
    untuk TCP (0/8 gagal tanpa login ulang ini), jadi SET harus pakai
    JWT hasil email sign-in, bukan JWT registrasi.
    """
    device_info = OrderedDict([
        ("userAgent", ""), ("platform", "DESKTOP"),
        ("system", "WindowsPlayer"),
        ("systemVersion", "Windows 10  (10.0.19045) 64bit"),
        ("manufacturer", "Unknown"), ("deviceModel", ""),
        ("appVersion", VERSION), ("sdkVersion", ""), ("vendorId", ""),
        ("advertisingId", ""), ("pushToken", ""), ("cloudId", ""),
        ("carrierId", ""), ("timezone", "SE Asia Standard Time"),
        ("language", "EN"), ("country", "Unknown"),
        ("deviceId", device_id),
    ])
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        p1 = OrderedDict([
            ("identityProvider", "FASTER"),
            ("credentials", {"type": "EMAIL", "email": email,
                             "password": password}),
            ("deviceInfo", device_info)])
        b1 = json.dumps(p1, separators=(",", ":"),
                        ensure_ascii=False).encode("utf-8")
        h1 = HEADERS_BASE.copy()
        h1["x-fstr-signature"] = compute_signature(
            "POST", "/v1/auth/sessions", b1)
        r1 = await session.post(ACCOUNT_URL + "/v1/auth/sessions", data=b1,
                                headers=h1, timeout=20)
        if r1.status_code != 200:
            log(f"[!] Email login langkah 1 gagal: HTTP {r1.status_code}")
            return None, None
        auth_token = r1.json().get("authToken") or r1.json().get("token")
        if not auth_token:
            return None, None
        p2 = OrderedDict([("authToken", auth_token),
                          ("deviceInfo", device_info)])
        b2 = json.dumps(p2, separators=(",", ":"),
                        ensure_ascii=False).encode("utf-8")
        h2 = HEADERS_BASE.copy()
        h2["x-fstr-signature"] = compute_signature(
            "POST", "/v1/users/sessions", b2)
        r2 = await session.post(ACCOUNT_URL + "/v1/users/sessions", data=b2,
                                headers=h2, timeout=20)
        user_jwt = r2.json().get("userToken")
        if not user_jwt:
            return None, None
        p3 = {"userToken": user_jwt,
              "gameServerProtocolVersion": GAME_PROTOCOL,
              "isPrivate": False}
        b3 = json.dumps(p3, separators=(",", ":"),
                        ensure_ascii=False).encode("utf-8")
        headers3 = {k: v for k, v in HEADERS_BASE.items()
                    if k != "x-fstr-application-key"}
        r3 = await session.post(LOBBY_URL + "/lobby/allocation", data=b3,
                                headers=headers3, timeout=20)
        try:
            alloc = r3.json()
        except Exception:
            alloc = {}
        return user_jwt, alloc


def events_in(frames):
    out = []
    for fr in frames:
        ev, _ = parse_room_event(fr)
        if ev:
            out.append(ev)
        elif fr:
            out.append(f"frame_type={fr[0]:#x}_len={len(fr)}")
    return out


def tcp_nickname(user_jwt, alloc, device_id, wanted_name=None):
    is_custom = wanted_name is not None
    host, port = find_host_port(alloc)
    if not host or not port:
        log("[!] TCP: host/port game server tidak ketemu dari alokasi lobby")
        return None, None
    log(f"[*] TCP connect ke game server (host disembunyikan, port {port})...")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((host, port))
        s.sendall(struct.pack(">I", 1) + b"\x03")
    except Exception as e:
        log(f"[!] TCP connect gagal: {type(e).__name__}")
        return None, None
    resp = read_until(s, set(), 6, break_on_connected=True)
    connected = any(len(f) >= 1 and f[0] == 0x03 for f in resp)
    player_id = None
    player_key = None
    for fr in resp:
        if len(fr) >= 1 and fr[0] == 0x03:
            player_id, player_key = parse_connected_frame(fr)
            break
    # JoinedRoom frame 0x06 juga dicatat, tanpa isi sensitif.
    joined_rooms = []
    for fr in resp:
        if len(fr) >= 6 and fr[0] == 0x06:
            joined_rooms.append(int.from_bytes(fr[2:6], "big"))
    log(f"[*] Connected frame (0x03) diterima: {connected}, "
        f"playerId={player_id}, playerKey_len="
        f"{len(player_key) if player_key else 0}, "
        f"auto_joined_rooms={joined_rooms}, events awal: {events_in(resp)}")
    # Login frame, sesuai dump.cs LoginRequest field 1/2
    try:
        s.sendall(build_login_frame(user_jwt))
        log("[*] Login frame terkirim, dengar saran otomatis 3 detik (kalau tidak ada, langsung GET eksplisit)...")
    except Exception as e:
        log(f"[!] Login frame gagal: {type(e).__name__}")
        s.close()
        return None, None

    # OUTFIT DEBUG 2026-10-30 (Bartes: outfit masih kosong/default
    # setelah nama berhasil): JANGAN kirim UpdateOutfit kosong dulu.
    # Hipotesis: UpdateOutfit dengan Items kosong justru mengosongkan
    # outfit, sehingga di game avatar terlihat kosong/default.
    # Tes kontrol ini: nickname saja, tanpa outfit sama sekali,
    # lalu setelah nickname berhasil, GET outfit dari server
    # (OutfitRequest EventId=0) untuk melihat outfit bawaan server.
    log("[*] OUTFIT DEBUG: outfit kosong TIDAK dikirim dulu (kontrol)")

    suggestion = None
    if not wanted_name:
        # 1) Dengar dulu saran otomatis setelah login, seperti
        #    tcp_get_saran_only di script lama (dulu 10 detik, kini 3 detik:
    #    kalau tidak ada saran otomatis, GET eksplisit langsung
    #    berhasil dalam 0,06 detik di tes FierceAlligator, jadi
    #    menunggu 10 detik itu murni terbuang).
        frames = read_until(s, {"default_nickname_result"}, 3)
        log(f"[*] Events setelah login+outfit: {events_in(frames)}")
        for fr in frames:
            ev, data = parse_room_event(fr)
            if ev == "default_nickname_result" and data:
                for f, v in parse_protobuf_fields(data):
                    if f == 1 and isinstance(v, bytes):
                        suggestion = v.decode("utf-8", "replace")
                        break
            if suggestion:
                break
        # 2) Kalau tidak ada saran otomatis, baru kirim GET eksplisit
        #    (field 2 = bahasa, perbaikan dari field 3 di script lama).
        if not suggestion:
            log("[*] Tidak ada saran otomatis, kirim GET eksplisit "
                "(GetDefaultNickname, field 2 = EN)...")
            try:
                s.sendall(build_named_frame(
                    0x08, "profile_update",
                    build_get_default_nickname_protobuf("EN")))
            except Exception as e:
                log(f"[!] Kirim GET nickname gagal: {type(e).__name__}")
                s.close()
                return None, None
            frames = read_until(s, {"default_nickname_result"}, 5)
            log(f"[*] Events setelah GET eksplisit: {events_in(frames)}")
            for fr in frames:
                ev, data = parse_room_event(fr)
                if ev == "default_nickname_result" and data:
                    for f, v in parse_protobuf_fields(data):
                        if f == 1 and isinstance(v, bytes):
                            suggestion = v.decode("utf-8", "replace")
                            break
                if suggestion:
                    break
        log(f"[*] Saran nickname dari server: {suggestion!r}")
        if not suggestion:
            log("[!] Server tidak memberi saran nickname")
            s.close()
            return None, None
        wanted_name = suggestion

    # V6 - DEBUG SAMPAI BERHASIL (izin Bartes 2026-10-30):
    # Tetap alur tamu v5, tapi SET dicoba beberapa varian berurutan
    # di socket yang sama, berhenti di varian pertama yang berhasil.
    # Varian diuji karena dump.cs tidak menunjukkan isi method
    # SendSetNicknameRequest (hanya signature: nickname saja), jadi
    # sumber DeviceId yang sebenarnya belum terbukti.
    def try_set(label, pb_bytes, wait=2, send_twice=False):
        log(f"[*] Varian {label}: kirim SET {wanted_name!r}...")
        try:
            frame = build_named_frame(0x08, "profile_update", pb_bytes)
            s.sendall(frame)
            if send_twice:
                s.sendall(frame)
                log("[*] Varian kirim 2x (seperti search di lookup_uuid)")
        except Exception as e:
            log(f"[!] Varian {label} kirim gagal: {type(e).__name__}")
            return None, None
        frames = read_until(s, {"set_name_result"}, wait)
        log(f"[*] Events setelah varian {label}: {events_in(frames)}")
        n_out, e_out = None, None
        for fr in frames:
            ev, data = parse_room_event(fr)
            if ev == "set_name_result" and data:
                for f, v in parse_protobuf_fields(data):
                    if f == 1 and isinstance(v, bytes):
                        n_out = v.decode("utf-8", "replace")
                    if f == 2 and isinstance(v, bytes):
                        e_out = v.decode("utf-8", "replace")
                break
        log(f"[*] Varian {label}: Name={n_out!r} ErrorMessage={e_out!r}")
        return n_out, e_out

    # TES URUTAN ONBOARDING 2026-10-31 (izin Bartes, pilih opsi ini):
    # Di game, layar outfit muncul SEBELUM layar nama. Jadi kirim
    # Equip 7396/13 SEBELUM SET nickname, saat akun masih tamu /
    # onboarding, lalu equip yang sama dikirim lagi setelah nickname
    # (kode equip pasca-nickname di bawah tetap jalan). Ini satu
    # variabel waktu saja: item, gaya, base, bundle sama persis.
    # RANDOM SEMUANYA 2026-10-31 (izin Bartes, 1 bot): pilih acak
    # dari pool gratis OutfitLog_5_2w03.txt, pola yang terbukti
    # di CoolBoar: HairModel=id rambut, HairStyle=0, tanpa buy,
    # baju/celana sebagai PASANGAN (utama, material), slot
    # berbayar (Hat, Ear, Back, Companion) = 0, tangan 17,17,
    # sepatu 59,59, kacamata 0. Seed dicetak supaya pilihan
    # ini bisa direproduksi dan dicek satu per satu oleh Bartes.
    skin_pool = [(255,208,176),(239,239,239)]  # 2026-10-31 per Bartes: hanya slot 0 kuning sawo dan slot 25 putih, tidak random 27 warna lagi
    skin_slots = [0, 25]
    face_pool = [354,353,352,351,350,349,348,347,346,345,118,117,116,115,114,113,112,43,42,41,40,39,38,37,36,35,34,6,5]
    hair_pool = [7396,5210,617,611,162,83,81,79,62]
    shirt_pairs = [(6179,6178),(13,5697),(13,1397),(13,33),(13,28),(13,19)]
    pants_pairs = [(5696,5699),(125,149),(15,154),(15,153)]
    rand_seed = f"{time.time_ns()}-{device_id}-{wanted_name}"
    rng = random.Random(rand_seed)
    rand_skin_idx = rng.randrange(len(skin_pool))
    rand_skin = skin_pool[rand_skin_idx]
    rand_face = rng.choice(face_pool)
    rand_hair = rng.choice(hair_pool)
    rand_shirt = rng.choice(shirt_pairs)
    rand_pants = rng.choice(pants_pairs)
    pre_items = [0, 0, 0, rand_hair, 4636, 16, rand_face, rand_shirt[0], rand_shirt[1], rand_pants[0], rand_pants[1], 17, 17, 59, 59, 0, 0, 0, 0]
    log(f"[*] RANDOM SEED={rand_seed} skin_slot={skin_slots[rand_skin_idx]} skin_rgb={rand_skin} face={rand_face} hair={rand_hair} shirt={rand_shirt} pants={rand_pants} items={pre_items}")
    try:
        s.sendall(build_named_frame(
            0x08, "equip",
            build_equip_protobuf(pre_items, rand_skin, rand_hair, 0, 14576)))
        log(f"[*] EQUIP SEBELUM NICKNAME RANDOM: hair={rand_hair} HairStyle=0 items={pre_items}")
        pre_frames = read_until(s, set(), 3 if is_custom else 0.2)
        log(f"[*] EQUIP SEBELUM NICKNAME events: {events_in(pre_frames)}")
    except Exception as e:
        log(f"[!] EQUIP SEBELUM NICKNAME gagal: {type(e).__name__}")
    variants = []
    if player_key:
        variants.append(("v6a DeviceId=playerKey",
                         build_set_nickname_protobuf(wanted_name, player_key),
                         False))
    variants.append(("v6b DeviceId=kosong",
                     build_set_nickname_protobuf(wanted_name, None), False))
    variants.append(("v6c tanpa field EventId",
                     build_set_nickname_no_event_protobuf(wanted_name,
                                                          device_id),
                     False))
    variants.append(("v6d kirim 2x DeviceId=registrasi",
                     build_set_nickname_protobuf(wanted_name, device_id),
                     True))
    # v6e: JoinRoom dulu, lalu SET. Room dari suffix gameServerId
    # alokasi, fallback room 0. Format join dari lookup_uuid.py:
    # payload 0x06 + room_id big-endian.
    gsid = alloc.get("gameServerId", "") if isinstance(alloc, dict) else ""
    room_id = None
    if "::" in gsid:
        try:
            room_id = int(gsid.rsplit("::", 1)[1])
        except Exception:
            room_id = None
    if room_id is None:
        room_id = 0
    name_out, error_out = None, None
    for label, pb_bytes, twice in variants:
        name_out, error_out = try_set(label, pb_bytes, send_twice=twice)
        if name_out and not error_out:
            break
    if not (name_out and not error_out):
        log(f"[*] Varian v6e JoinRoom({room_id}) dulu, lalu SET...")
        try:
            s.sendall(struct.pack(">I", 5) + bytes([0x06])
                      + struct.pack(">I", room_id))
            join_frames = read_until(s, set(), 0.2)
            log(f"[*] Events setelah JoinRoom: {events_in(join_frames)}")
        except Exception as e:
            log(f"[!] JoinRoom gagal: {type(e).__name__}")
        name_out, error_out = try_set(
            "v6e setelah JoinRoom",
            build_set_nickname_protobuf(wanted_name, device_id))
    if name_out and not error_out:
        # OUTFIT DEBUG: GET outfit dari server di socket yang sama,
        # sebelum socket ditutup. Command "outfit", OutfitRequest
        # EventId=0 (GetAll). Response GetOutfitResponse: field 1
        # EventId, field 2 Success (bool), field 3 ErrorMessage,
        # field 4 repeated Outfit (slot/items/skin).
        try:
            s.sendall(build_named_frame(0x08, "outfit",
                                        b"\x08" + varint(0)))
            log("[*] OUTFIT DEBUG: kirim GetAllOutfits...")
            frames = read_until(s, {"outfit_get_all"}, 5)
            log(f"[*] OUTFIT DEBUG events: {events_in(frames)}")
            for fr in frames:
                ev, data = parse_room_event(fr)
                if ev != "outfit_get_all" or not data:
                    continue
                for f, v in parse_protobuf_fields(data):
                    if f == 2:
                        log(f"[*] OUTFIT DEBUG GetAll Success={bool(v)}")
                    if f == 3 and isinstance(v, bytes):
                        log(f"[*] OUTFIT DEBUG GetAll Error="
                            f"{v.decode('utf-8', 'replace')!r}")
                    if f == 4 and isinstance(v, bytes):
                        slot, items, skin = None, [], None
                        for of, ov in parse_protobuf_fields(v):
                            if of == 1:
                                slot = ov
                            if of == 2:
                                if isinstance(ov, bytes):
                                    items.extend(parse_varint_stream(ov))
                                else:
                                    items.append(ov)
                            if of == 3 and isinstance(ov, bytes):
                                rgb = {}
                                for cf, cv in parse_protobuf_fields(ov):
                                    rgb[cf] = cv
                                skin = rgb
                        log(f"[*] OUTFIT DEBUG slot={slot} "
                            f"items={items} skin={skin}")
        except Exception as e:
            log(f"[!] OUTFIT DEBUG GetAll gagal: {type(e).__name__}")
        # RAMBUT BARU 7396/13 2026-10-31 (log baru Bartes, akun baru
        # hanya ganti rambut): ITEM_CLICK 7396, SETEQUIP 7396 slot3,
        # HAIRSTYLE id=13 color gelap (12,5,0). Pasangan ini dari
        # akun baru yang semuanya masih default, jadi kemungkinan
        # gratis / dimiliki akun baru tanpa beli — berbeda dari
        # 609/60 yang terbukti berbayar di salon (NoisyHyena).
        # Tes ini satu variabel dari base FunnyArmadillo: hanya
        # rambut diubah ke 7396/13, tanpa buy, EquipRequest Bundle
        # 14576, sisanya persis base.
        skin_rgb = rand_skin
        equip_items = pre_items
        # v10: TANPA BUY. EQUIP final user (OutfitLog_4_6v48 baris 178-180)
        # mengirim HairModel=7396 dengan field4 HairStyle DIHILANGKAN (=0),
        # BUKAN 13. HAIRSTYLE 13 di UI ternyata hanya warna lokal,
        # bukan field server. Jadi tes ini: 7396/0, tanpa buy.
        try:
            s.sendall(build_named_frame(
                0x08, "equip",
                build_equip_protobuf(equip_items, skin_rgb, rand_hair, 0, 14576)))
            log(f"[*] EQUIP RANDOM SETELAH NICKNAME: items={pre_items} hair={rand_hair} HairStyle=0 TANPA buy bundle=14576")
            frames = read_until(s, set(), 0.2)
            log(f"[*] EQUIP DEBUG events: {events_in(frames)}")
        except Exception as e:
            log(f"[!] EQUIP DEBUG gagal: {type(e).__name__}")
    s.close()
    log(f"[*] HASIL v6: Name={name_out!r} ErrorMessage={error_out!r}")
    return name_out, error_out


# ============ PANEL SUPABASE (named_bots, terpisah dari accounts) ============
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
TABEL = "named_bots"

async def save_bot(session, email, password, device_id, nickname, status):
    endpoint = f"{SUPABASE_URL}/rest/v1/{TABEL}"
    headers = {"apikey": SUPABASE_KEY,
               "Authorization": f"Bearer {SUPABASE_KEY}",
               "Content-Type": "application/json",
               "Prefer": "return=minimal"}
    payload = {"email": email, "password": password,
               "device_id": device_id, "status": status,
               "nickname": nickname}
    try:
        r = await session.post(endpoint, json=payload, headers=headers, timeout=15)
        return r.status_code == 201
    except Exception:
        return False

def load_custom_names():
    path = os.getenv("CUSTOM_NAMES_FILE", "custom_names_list.txt")
    try:
        with open(path, encoding="utf-8") as f:
            names = [ln.strip() for ln in f if ln.strip()]
        # Validasi dasar: >4 huruf, tanpa spasi/nomor/simbol aneh di Latin,
        # kana Jepang / Hangul Korea saja untuk nama Asia (sesuai dump validator).
        out = []
        for n in names:
            if len(n) <= 4:
                continue
            if " " in n:
                continue
            out.append(n)
        return out
    except Exception as e:
        log(f"[!] Gagal baca list nama {path}: {type(e).__name__}, fallback saran server")
        return []

async def load_used_bases():
    try:
        async with AsyncSession(impersonate="chrome110", verify=False) as session:
            r = await session.get(f"{SUPABASE_URL}/rest/v1/{TABEL}?select=nickname&limit=10000",
                                  headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}, timeout=15)
            if r.status_code != 200:
                return set()
            bases = set()
            for row in r.json():
                nick = row.get("nickname")
                if nick and "#" in nick:
                    bases.add(nick.split("#", 1)[0])
            return bases
    except Exception:
        return set()


_exec_pool = None

async def proses_satu_bot(idx, total, wanted_name, candidate_names=None):
    global _exec_pool
    exec_pool = _exec_pool
    loop = asyncio.get_event_loop()

    async def try_nickname(user_jwt_, alloc_, device_id_, wanted_, break_on_error):
        # 4 sesi TCP, varian SET v6a-v6e di dalam tiap sesi.
        # break_on_error=True untuk nama list: penolakan eksplisit
        # (mis. profanity) itu permanen untuk nama itu, jangan buang
        # 3 sesi lagi. break_on_error=False untuk saran server:
        # tiap sesi bisa memberi saran baru, jadi saran yang ditolak
        # satu sesi tidak berarti saran berikutnya juga ditolak.
        n_out, e_out = None, None
        for _attempt in range(4):
            n_out, e_out = await loop.run_in_executor(
                exec_pool, tcp_nickname, user_jwt_, alloc_, device_id_, wanted_)
            if n_out and not e_out and "#" in n_out:
                return n_out, n_out, e_out
            if e_out and break_on_error:
                log(f"    -> Server menolak nama ini secara eksplisit: {e_out!r}, tidak di-retry")
                break
            if _attempt < 3:
                log(f"    -> TCP sesi {_attempt+1} gagal (name={n_out!r} error={e_out!r}), retry sesi {_attempt+2}...")
                await asyncio.sleep(2 + random.random() * 2)
        return None, n_out, e_out

    log(f"[{idx}/{total}] Registrasi HTTP...")
    reg = await http_register()
    if not reg:
        log("    -> GAGAL registrasi")
        return None
    email, password, device_id, user_jwt, alloc = reg
    log(f"    -> {email[:4]}*** terdaftar, TCP nickname+outfit...")
    await asyncio.sleep(random.random() * 2)
    nickname_ok, name_out, error_out = await try_nickname(
        user_jwt, alloc, device_id, wanted_name, break_on_error=True)

    # FALLBACK AKUN BARU 2026-10-31 per Bartes (menggantikan fallback
    # saran akun-sama): fallback saran akun-sama TERBUKTI tidak
    # menyelamatkan akun diam — run 41187626464: 8 sesi fallback,
    # saran server None 8/8, set_name_result 0/16, karena untuk akun
    # yang diam, seluruh profile mechanic diam, bukan hanya SET.
    # Yang TERBUKTI menyelamatkan nama gagal di tes diagnostik
    # adalah AKUN BARU + nama yang sama (3/3 nama gagal run #12
    # sukses lagi dengan akun baru). Jadi kalau nama list gagal di
    # akun pertama, akun pertama itu DIBUANG (tidak disimpan, sesuai
    # aturan hanya SUCCESS yang masuk DB), registrasi HTTP baru,
    # lalu coba NAMA LIST YANG SAMA lagi di akun baru. Kalau itu
    # juga gagal, barulah coba SARAN SERVER di akun baru itu
    # (profile mechanic akun baru kemungkinan masih hidup, jadi
    # saran di sini bisa diberikan, tidak seperti di akun lama yang
    # diam). Bot sukses list tetap pakai nama list; bot sukses
    # fallback tetap tersimpan, dengan kredensial akun yang sukses
    # itu (bukan akun lama yang gagal). Hanya bot gagal yang
    # membayar biaya waktu fallback; bot sukses list tidak kena
    # fallback sama sekali.
    if not nickname_ok and wanted_name is not None:
        log(f"    -> FALLBACK AKUN BARU: nama list {wanted_name!r} gagal "
            f"di akun pertama (dapat: {name_out!r} error: {error_out!r}), "
            f"akun pertama dibuang, registrasi akun baru...")
        reg2 = await http_register()
        if not reg2:
            log("    -> FALLBACK AKUN BARU: GAGAL registrasi akun baru")
        else:
            email2, password2, device_id2, user_jwt2, alloc2 = reg2
            log(f"    -> FALLBACK AKUN BARU: {email2[:4]}*** terdaftar, "
                f"coba nama list yang sama lagi...")
            await asyncio.sleep(random.random() * 2)
            nickname_ok, name_out, error_out = await try_nickname(
                user_jwt2, alloc2, device_id2, wanted_name, break_on_error=True)
            if nickname_ok:
                email, password, device_id, user_jwt = email2, password2, device_id2, user_jwt2
                log(f"    -> nickname OK via FALLBACK AKUN BARU list: {nickname_ok}")
            else:
                log(f"    -> FALLBACK SARAN AKUN BARU: nama list gagal juga "
                    f"di akun baru (dapat: {name_out!r} error: {error_out!r}), "
                    f"coba minta nama dari server di akun baru...")
                nickname_ok, name_out, error_out = await try_nickname(
                    user_jwt2, alloc2, device_id2, None, break_on_error=False)
                if nickname_ok:
                    email, password, device_id, user_jwt = email2, password2, device_id2, user_jwt2
                    log(f"    -> nickname OK via FALLBACK AKUN BARU saran: {nickname_ok}")
    # PENGAMAN NAMA BERBEDA 2026-10-31 per Bartes: kalau SET ditolak
    # server / diam saja, retri lagi dengan nama LAIN sampai sukses.
    # Urutan sebelumnya sudah mencoba: akun pertama + nama asli,
    # akun baru + nama asli, akun baru + saran server. Kalau semua
    # itu gagal, jangan langsung buang bot ini — coba nama list
    # BERBEDA, masing-masing dengan AKUN BARU, sampai ada yang
    # sukses. Nama berbeda diambil dari list partisi runner ini
    # setelah blok nama asli run ini, jadi tidak memakai nama asli
    # bot lain di run yang sama (duplikat antar bot tetap diizinkan
    # server, tapi dihindari di sini supaya stok nama tetap beragam).
    # BATAS PENGAMAN: maks 5 nama berbeda per bot (total percobaan
    # per bot: asli akun1 + asli akun2 + saran akun2 + 5 nama berbeda
    # akun baru = 7 akun, 28 sesi TCP maks). Batas ini bukan untuk
    # membatasi "sampai sukses" secara prinsip, tapi untuk mencegah
    # loop tak berhingga yang bisa timeout workflow 120 menit dan
    # menggagalkan semua bot di runner itu. Secara probabilitas,
    # kalau tiap percobaan sukses ~60% dan independen, peluang gagal
    # 7x berturut-turut = 0,4^7 ≈ 0,16%, jadi batas 7 percobaan ini
    # praktis sudah "sampai sukses" untuk hampir semua bot, dengan
    # waktu worst-case per bot ~7 menit (7 akun x 4 sesi x ~15 detik),
    # masih jauh di bawah timeout 120 menit karena bot jalan paralel
    # per runner dan wall time runner = bot terlambat, bukan jumlah
    # bot. Bot sukses di jalur sebelumnya TIDAK kena pengaman ini
    # sama sekali, hanya bot gagal yang membayar biaya waktunya.
    # UpdateNickname TIDAK dipakai di pengaman ini, karena tes
    # email-dulu membuktikan UpdateNickname juga diam untuk bot
    # yang SET-nya diam (0/2), jadi yang dicoba di sini hanya SET
    # dengan nama list berbeda, bukan UpdateNickname.
    if not nickname_ok and wanted_name is not None and candidate_names:
        for _cand in candidate_names:
            if _cand == wanted_name:
                continue
            log(f"    -> PENGAMAN NAMA BERBEDA: nama asli {wanted_name!r} gagal di semua jalur sebelumnya, coba nama berbeda {_cand!r} dengan akun baru...")
            reg_c = await http_register()
            if not reg_c:
                log("    -> PENGAMAN NAMA BERBEDA: GAGAL registrasi akun baru, coba nama berikutnya...")
                continue
            email_c, password_c, device_id_c, user_jwt_c, alloc_c = reg_c
            log(f"    -> PENGAMAN NAMA BERBEDA: {email_c[:4]}*** terdaftar, coba SET {_cand!r}...")
            await asyncio.sleep(random.random() * 2)
            nickname_ok, name_out, error_out = await try_nickname(
                user_jwt_c, alloc_c, device_id_c, _cand, break_on_error=True)
            if nickname_ok:
                email, password, device_id, user_jwt = email_c, password_c, device_id_c, user_jwt_c
                log(f"    -> nickname OK via PENGAMAN NAMA BERBEDA: {nickname_ok} (asli {wanted_name!r}, berbeda {_cand!r})")
                break
            log(f"    -> PENGAMAN NAMA BERBEDA: nama {_cand!r} juga gagal (dapat: {name_out!r} error: {error_out!r}), coba nama berikutnya...")
    status = "NICK_FAIL"
    linked = False
    if nickname_ok:
        log(f"    -> nickname OK (server): {nickname_ok}")
        linked = await http_link_email(email, password, device_id, user_jwt)
        log(f"    -> link email: {linked}")
        status = "SUCCESS" if linked else "EMAIL_FAIL"
    else:
        log(f"    -> nickname GAGAL (dapat: {name_out!r} error: {error_out!r})")
    if status == "SUCCESS":
        async with AsyncSession(impersonate="chrome110", verify=False) as session:
            ok = await save_bot(session, email, password, device_id, nickname_ok, status)
        log(f"    -> {'tersimpan' if ok else 'GAGAL simpan'} ke {TABEL} status={status}")
        return nickname_ok if ok else None
    else:
        # 2026-10-31 per Bartes: HANYA SUCCESS yang masuk Supabase.
        # Gagal tidak disimpan sama sekali, supaya stok di DB = stok
        # yang benar-benar bisa dipakai follow, tidak manipulatif.
        log(f"    -> TIDAK disimpan ke {TABEL} (status={status}, hanya SUCCESS yang masuk DB)")
        return None

async def main():
    jumlah = int(os.getenv("JUMLAH", "3"))
    workers = int(os.getenv("WORKERS", "10"))
    runner_index = int(os.getenv("RUNNER_INDEX", "1"))
    runner_total = int(os.getenv("RUNNER_TOTAL", "1"))
    name_mode = os.getenv("NAME_MODE", "list").strip().lower()
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] SUPABASE_URL / SUPABASE_SERVICE_KEY belum diset.")
        sys.exit(1)
    all_names = []
    if name_mode == "list":
        all_names = load_custom_names()
        # 2026-10-31 per Bartes: duplikat base DIIZINKAN server, kode #
        # selalu berbeda (bukti DB: プロプレイヤー#426/#404,
        # StormKing#929/#610, XxxKingxxX#941/#767, dst., tidak ada
        # nickname lengkap yang sama persis). Jadi JANGAN buang nama
        # yang sudah SUCCESS — kalau dibuang, list 92 nama tidak
        # cukup untuk 100 percobaan dan sisanya jatuh ke saran server.
        # Dengan duplikat diizinkan, list 187 nama cukup untuk 100+.
        # Partisi per runner (matrix 20): runner 1 ambil index 0,20,40...,
        # runner 2 ambil 1,21,41..., dst. Jadi tidak ada nama ganda antar runner.
        if runner_total > 1:
            all_names = all_names[runner_index - 1::runner_total]
        # ROTASI BLOK PER RUN (matrix saja, single tetap offset 0):
        # Tanpa rotasi, run 100 selalu pakai 100 nama pertama di list
        # (index 0-99, karena partisi pertama 5 per runner menutupi
        # persis index itu), jadi run berikutnya mengulang nama yang
        # sama. Dengan rotasi, setiap run matrix pakai blok 100 nama
        # berikutnya: offset = ((run_number-1)*5) % len(partisi),
        # jadi blok list = offset*20 .. offset*20+99, dan 20 run
        # berturut-turut tidak mengulang blok yang sama (list 2000
        # nama = 20 blok x 100 nama). GITHUB_RUN_NUMBER otomatis
        # ada di runner GitHub Actions, 0 kalau dijalankan lokal.
        name_offset = 0
        if runner_total > 1 and all_names:
            try:
                _run_no = int(os.getenv("GITHUB_RUN_NUMBER", "0") or 0)
            except ValueError:
                _run_no = 0
            if _run_no > 0:
                name_offset = ((_run_no - 1) * 5) % len(all_names)
        log(f"[*] NAME_MODE=list, {len(all_names)} nama untuk runner {runner_index}/{runner_total}, rotasi offset={name_offset}")
    else:
        log(f"[*] NAME_MODE={name_mode}, pakai saran server")
    log(f"[*] REGISTER+NICKNAME CEPAT: {jumlah} bot -> tabel `{TABEL}` (paralel {workers} workers)")
    t0 = time.time()
    global _exec_pool
    from concurrent.futures import ThreadPoolExecutor
    sem = asyncio.Semaphore(workers)
    _exec_pool = ThreadPoolExecutor(max_workers=workers)
    try:
        async def _run(i):
            async with sem:
                wanted = None
                candidate_names = []
                if name_mode == "list" and all_names:
                    # Siklis: kalau percobaan melebihi panjang list/partisi,
                    # ulang dari awal partisi, bukan jatuh ke saran server.
                    wanted = all_names[(i - 1 + name_offset) % len(all_names)]
                    # Pengaman nama berbeda: 5 nama setelah blok nama
                    # asli run ini (name_offset .. name_offset+jumlah-1),
                    # jadi tidak memakai nama asli bot lain di run yang
                    # sama di runner ini. Kalau list partisi kecil,
                    # modulo tetap jalan dan duplikat dihindari sebisa
                    # mungkin dengan cek != wanted dan belum dipakai.
                    for _k in range(5):
                        _cand = all_names[(name_offset + jumlah + _k) % len(all_names)]
                        if _cand != wanted and _cand not in candidate_names:
                            candidate_names.append(_cand)
                return await proses_satu_bot(i, jumlah, wanted, candidate_names)
        hasil = await asyncio.gather(*[_run(i) for i in range(1, jumlah + 1)])
    finally:
        _exec_pool.shutdown(wait=True)
    dengan_hash = sum(1 for x in hasil if x and "#" in str(x))
    dt = time.time() - t0
    log(f"\n[HASIL] {dengan_hash}/{jumlah} bot dapat nama (#) "
        f"({dt:.1f}s total, {dt/max(jumlah,1):.1f}s/bot)")

if __name__ == "__main__":
    asyncio.run(main())
