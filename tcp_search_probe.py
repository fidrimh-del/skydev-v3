"""Diagnostik TCP ToxicBird — TAHAP 3: cari UUID dari nama.

Temuan tahap 2 (TERKONFIRMASI dari server asli):
  - Framing luar: [int32 big-endian panjang payload][payload].
  - Kirim frame [len=1][0x03] -> server membalas Connected:
        [type=0x03][playerId int32 BE][playerKey = JWT]
    Bukti: JWT-nya HS512, klaim user_id SAMA PERSIS dengan
    playerId int32-nya (26621, lalu 26627 di koneksi berikut).
    Type 0x04 juga menghasilkan Connected (kemungkinan Reconnect).
  - Sapaan 6 byte tahap 1 (00 00 00 02 04 03) di tahap 2 tidak
    muncul dalam jendela 3 detik — kemungkinan datang terlambat
    (>3 detik) atau tidak konsisten. Script ini TIDAK bergantung
    padanya: ia membaca frame dalam loop sampai Connected ketemu.

Yang dicari tahap ini:
  - Kirim SearchFriend sebagai Event: eventName "friends"
    (FriendsRequestKey dari dump) + data protobuf:
        field 1 varint = 7 (SearchFriend)
        field 2 string = nama yang dicari (boleh nama#kode)
        contoh "tes#123" -> 08 07 12 07 74 65 73 23 31 32 33
  - Type Event belum diketahui -> dicoba type 5..10.
  - Encoding data belum diketahui -> dicoba dua varian:
        (a) string 7-bit length + data mentah (paling sesuai
            BinaryWriter .NET: Write(string)=7-bit len+UTF-8,
            Write(byte[])=mentah tanpa panjang)
        (b) string 7-bit length + int32 BE panjang + data
  - Kalau sweep utama gagal, fallback: eventName
    "search_friend_request" (belum terkonfirmasi di dump, tapi
    simetris dengan "search_friend_result") untuk type 5..8.
  - Sukses = ada frame balasan yang mengandung
    "search_friend_result" dan/atau UUID (pola 8-4-4-4-12 hex).

Yang TIDAK dicetak: JWT utuh / playerKey (hanya "JWT didapat,
user_id cocok/tidak"). UUID hasil lookup BOLEH dicetak — itu
justru output yang dicari (identifier publik pemain).

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY (Secrets),
SEARCH_NAME (input workflow, mis. tes#123).
App key/secret: env FSTR_* kalau ada, fallback hardcode sementara
untuk uji coba (repo private; hapus setelah berhasil).
"""

import json
import os
import re
import sys
import random
import asyncio
import hmac
import hashlib
import base64
import socket
import struct
import time
from collections import OrderedDict
from curl_cffi.requests import AsyncSession

try:
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    pass

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
# SEMENTARA UNTUK UJI COBA (repo private; nilai sama seperti script
# panel). Env selalu menang kalau sudah diisi sebagai Secrets.
APP_KEY = os.getenv("FSTR_APP_KEY") or "57a3d999-654c-4484-a20c-0f4b5512af49"
APP_SECRET = os.getenv("FSTR_APP_SECRET") or "jWS5aLZacNVFx6PAxpXq2g4o"

ACCOUNT_URL = "https://account.faster.aftvrsys.com"
LOBBY_URL = "https://pkxd-lobby.aftvrsys.com"
GAME_PROTOCOL = "1.93.0"
VERSION = "1.93.3"

HEADERS_BASE = {
    "User-Agent": "UnityPlayer/2022.3.62f3 (UnityWebRequest/1.0, libcurl/8.10.1-DEV)",
    "Accept": "*/*",
    "Accept-Encoding": "deflate, gzip",
    "Content-Type": "application/json",
    "X-Protocol-Version": GAME_PROTOCOL,
    "X-Unity-Version": "2022.3.62f3",
    "Connection": "keep-alive",
}

UUID_RE = re.compile(rb"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


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


def build_search_protobuf(name):
    """SearchFriendRequest: field1 varint 7, field2 string nama."""
    nb = name.encode("utf-8")
    return b"\x08\x07\x12" + varint(len(nb)) + nb


def build_event_frame(etype, event_name, data, variant):
    """Satu frame Event lengkap dengan panjang luar (int32 BE)."""
    nb = event_name.encode("utf-8")
    name_part = bytes([len(nb)]) + nb  # 7-bit length, cukup 1 byte (<128)
    if variant == "a":
        payload = bytes([etype]) + name_part + data
    else:  # variant b: data diawali int32 BE panjang
        payload = bytes([etype]) + name_part + struct.pack(">I", len(data)) + data
    return struct.pack(">I", len(payload)) + payload


def read_frames(sock, seconds):
    """Baca semua frame [len][payload] yang tiba dalam jendela waktu.
    Mengembalikan list payload (bytes)."""
    deadline = time.time() + seconds
    buf = bytearray()
    frames = []
    sock.settimeout(0.5)
    while time.time() < deadline:
        try:
            chunk = sock.recv(4096)
        except socket.timeout:
            continue
        except Exception:
            break
        if chunk == b"":
            break
        buf += chunk
        while len(buf) >= 4:
            ln = struct.unpack(">I", buf[:4])[0]
            if ln > 65536:
                return frames  # framing rusak, berhenti
            if len(buf) - 4 < ln:
                break
            frames.append(bytes(buf[4:4 + ln]))
            del buf[:4 + ln]
    return frames


def jwt_user_id(payload):
    """Ambil klaim user_id dari JWT di dalam frame Connected.
    Kembalikan None kalau bukan JWT / gagal decode. JWT tidak dicetak."""
    j = payload.find(b"eyJ")
    if j < 0:
        return None
    try:
        seg = payload[j:].decode().split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
        return claims.get("user_id")
    except Exception:
        return None


def describe_frame(p):
    """Deskripsi aman untuk log: tanpa JWT, potong hex panjang."""
    if b"eyJ" in p:
        j = p.find(b"eyJ")
        return f"type=0x{p[0]:02x} len={len(p)} prefix={p[:j].hex()} <JWT sensor>"
    h = p.hex()
    if len(h) > 200:
        h = h[:200] + "..."
    return f"type=0x{p[0]:02x} len={len(p)} hex={h}"


def scan_result(frames):
    """Cari UUID / penanda search_friend_result di frame balasan."""
    for p in frames:
        if b"search_friend_result" in p or UUID_RE.search(p):
            m = UUID_RE.search(p)
            return (m.group(0).decode() if m else None, p)
    return None, None


async def fetch_one_bot(session):
    endpoint = f"{SUPABASE_URL}/rest/v1/rpc/get_fresh_bots"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    payload = {"p_target_uuid": "00000000-0000-0000-0000-000000000000", "p_limit": 1}
    try:
        r = await session.post(endpoint, json=payload, headers=headers, timeout=20)
        if r.status_code == 200 and r.json():
            return r.json()[0]
    except Exception as e:
        print(f"[-] Gagal mengambil bot: {e}")
    return None


async def login_and_allocate(email, password, device_id, session):
    device_info = OrderedDict([
        ("userAgent", ""), ("platform", "DESKTOP"), ("system", "WindowsPlayer"),
        ("systemVersion", "Windows 10  (10.0.19045) 64bit"),
        ("manufacturer", "Unknown"), ("deviceModel", ""), ("appVersion", VERSION),
        ("sdkVersion", ""), ("vendorId", ""), ("advertisingId", ""),
        ("pushToken", ""), ("cloudId", ""), ("carrierId", ""),
        ("timezone", "SE Asia Standard Time"), ("language", "EN"),
        ("country", "Unknown"), ("deviceId", device_id),
    ])
    p1 = OrderedDict([("identityProvider", "FASTER"),
                      ("credentials", {"type": "EMAIL", "email": email, "password": password}),
                      ("deviceInfo", device_info)])
    b_p1 = json.dumps(p1, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    h1 = HEADERS_BASE.copy()
    h1["x-fstr-application-key"] = APP_KEY
    h1["x-fstr-signature"] = compute_signature("POST", "/v1/auth/sessions", b_p1)
    r1 = await session.post(ACCOUNT_URL + "/v1/auth/sessions", data=b_p1, headers=h1, timeout=20)
    if r1.status_code != 200:
        print(f"[-] auth/sessions gagal: {r1.status_code}")
        return None, None
    auth_token = r1.json().get("authToken")
    await asyncio.sleep(random.uniform(0.1, 0.3))
    p2 = OrderedDict([("authToken", auth_token), ("deviceInfo", device_info)])
    b_p2 = json.dumps(p2, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    h2 = HEADERS_BASE.copy()
    h2["x-fstr-application-key"] = APP_KEY
    h2["x-fstr-signature"] = compute_signature("POST", "/v1/users/sessions", b_p2)
    r2 = await session.post(ACCOUNT_URL + "/v1/users/sessions", data=b_p2, headers=h2, timeout=20)
    user_jwt = r2.json().get("userToken")
    if not user_jwt:
        print("[-] users/sessions tidak mengembalikan userToken")
        return None, None
    print("[+] Login OK, JWT didapat.")
    await asyncio.sleep(random.uniform(0.1, 0.3))
    p3 = {"userToken": user_jwt, "gameServerProtocolVersion": GAME_PROTOCOL, "isPrivate": False}
    b_p3 = json.dumps(p3, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    r3 = await session.post(LOBBY_URL + "/lobby/allocation", data=b_p3,
                            headers=HEADERS_BASE.copy(), timeout=20)
    print(f"[*] allocation status: {r3.status_code}")
    try:
        body = r3.json()
    except Exception:
        body = {}
    return user_jwt, body


def find_host_port(body):
    import re as _re
    host = port = None

    def ok_host(v):
        return isinstance(v, str) and v and "::" not in v and " " not in v

    def scan(d):
        nonlocal host, port
        if not isinstance(d, dict):
            return
        for k, v in d.items():
            lk = k.lower()
            if host is None and lk in ("gameserveraddress", "address",
                                       "host", "hostname", "ip") and ok_host(v):
                host = v
            if port is None and lk in ("gameserverport", "port",
                                       "tcpport") and isinstance(v, int):
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


async def main():
    name = os.getenv("SEARCH_NAME", "")
    if not SUPABASE_URL or not SUPABASE_KEY or not name:
        print("[!] Env SUPABASE_URL / SUPABASE_SERVICE_KEY / SEARCH_NAME wajib diisi.")
        sys.exit(1)
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        bot = await fetch_one_bot(session)
        if not bot:
            print("[-] Tidak ada bot tersedia di stok.")
            return
        print(f"[*] Pakai bot stok: {bot['email']}")
        _jwt, alloc = await login_and_allocate(
            bot["email"], bot["password"], bot["device_id"], session)
        if not alloc:
            return
        host, port = find_host_port(alloc)
        print(f"[*] Server target: {host}:{port}")
        if not host or not port:
            print("[-] Host/port tidak ketemu di allocation.")
            return

    # --- Handshake: kirim Connect (0x03), tunggu Connected ---
    try:
        s = socket.create_connection((host, port), timeout=5)
    except Exception as e:
        print(f"[-] TCP connect gagal: {e}")
        return
    s.sendall(struct.pack(">I", 1) + bytes([0x03]))
    frames = read_frames(s, 10)
    player_id = None
    for p in frames:
        if len(p) > 5 and p[0] == 0x03 and b"eyJ" in p:
            player_id = int.from_bytes(p[1:5], "big")
            uid = jwt_user_id(p)
            print(f"[+] Connected: playerId={player_id}, JWT user_id={uid} "
                  f"({'COCOK' if uid == player_id else 'TIDAK COCOK'})")
            break
        else:
            print(f"[*] Frame lain saat handshake: {describe_frame(p)}")
    if player_id is None:
        print("[-] Tidak mendapat frame Connected dalam 10 detik. Berhenti.")
        s.close()
        return

    # --- Sweep: cari Event type + varian yang dibalas search_friend_result ---
    protobuf = build_search_protobuf(name)
    print(f"[*] Protobuf SearchFriend untuk '{name}': {protobuf.hex()}")
    attempts = []
    for etype in [5, 6, 7, 8, 9, 10]:
        for variant in ["a", "b"]:
            attempts.append((etype, "friends", variant))
    for etype in [5, 6, 7, 8]:
        attempts.append((etype, "search_friend_request", "a"))

    for etype, ev_name, variant in attempts:
        frame = build_event_frame(etype, ev_name, protobuf, variant)
        try:
            s.sendall(frame)
        except Exception as e:
            print(f"[-] Gagal kirim type={etype} nama={ev_name} varian={variant}: {e}")
            break
        resp = read_frames(s, 3)
        if not resp:
            print(f"[-] type={etype} nama={ev_name} varian={variant}: timeout")
            continue
        uuid, hit = scan_result(resp)
        if uuid or hit is not None:
            print(f"[+] KETEMU! type={etype} nama={ev_name} varian={variant}")
            print(f"[+] UUID untuk '{name}': {uuid if uuid else '(frame ketemu, UUID tak ter-parse)'}")
            for p in resp:
                print(f"    frame: {describe_frame(p)}")
            s.close()
            print("[*] Search probe selesai — UUID ketemu.")
            return
        print(f"[*] type={etype} nama={ev_name} varian={variant}: dibalas "
              f"{len(resp)} frame, bukan hasil search: "
              + "; ".join(describe_frame(p) for p in resp[:2]))
    s.close()
    print("[-] Sweep selesai, belum ada yang mengembalikan search_friend_result.")
    print("    Kemungkinan berikut: encoding string/data berbeda, atau")
    print("    butuh JoinRoom dulu, atau nama event request berbeda.")


if __name__ == "__main__":
    asyncio.run(main())
