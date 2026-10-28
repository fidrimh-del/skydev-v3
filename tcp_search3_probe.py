"""Diagnostik TCP ToxicBird — TAHAP 8: request PERSIS game asli.

TEROBOSAN dari TBLog.txt (v55-fixed, tes Bartes 07:22 WIB):
  Game mengirim search sebagai RoomEvent bernama 'friends' yang
  DATANYA adalah protobuf SearchFriendRequest, dengan encoding
  nama memakai VARINT (7-bit length), bukan int32:
    OUT 22B: 08 07 'friends' 08 07 12 09 'Admin#000'
             |  |  |         |  |  |  |
             |  |  |         |  |  |  +-- FriendName (9 byte)
             |  |  |         |  |  +----- field 2, len 9
             |  |  |         |  +-------- field 1, EventId = 7 (SearchFriend)
             |  |  |         +----------- data = protobuf SearchFriendRequest
             |  |  +----------- nama RoomEvent 'friends'
             |  +-------------- varint len nama = 7
             +----------------- type 0x08 = RoomEvent
  Dan responsnya:
    IN 132B: 08 14 'search_friend_result' 08 01 1a 68 <RemotePlayer 104B>
             di dalam record: field 1 (36 byte) = UUID target,
             field 2 (9 byte) = 'Admin#000' (validasi nama!).
  Hasil untuk admin#000: UUID d0db3610-ac6d-4f83-ad52-12be6c29c8f1.

Kenapa tahap 6 gagal padahal sudah kirim 'friends' + protobuf?
  Tahap 6 memakai int32-BE untuk panjang nama (00 00 00 07),
  sedangkan game memakai varint (07). Tahap 4 mencoba varint
  tapi TANPA data protobuf. Kombinasi varint-nama + data
  protobuf BELUM PERNAH dicoba — itulah yang dicoba tahap ini.

Urutan tahap ini (setelah join terkonfirmasi):
  1. Kirim 'friends' KOSONG (varint-nama, tanpa data) — persis
     OUT 9B game. Harusnya dibalas 'get_friends_result' (daftar
     teman). Ini validasi encoding + kanal sebelum search.
  2. Kirim search (varint-nama + protobuf). Game mengirimnya
     2x, jadi probe juga kirim 2x kalau percobaan pertama diam.
  3. Varian cadangan: varint-nama + int32-len data + protobuf.

Sukses SEJATI = frame 'search_friend_result' yang MENGANDUNG
nama yang dicari persis + UUID dashed di dalamnya. UUID tanpa
nama itu = ambient, dihitung tapi DIABAIKAN (pelajaran tahap 5).

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY (Secrets),
SEARCH_NAME (input workflow, mis. admin#000).
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
    nb = name.encode("utf-8")
    return b"\x08\x07\x12" + varint(len(nb)) + nb


def build_room_event_varint(event_name, data, data_len_prefix=False):
    """Frame RoomEvent persis encoding game: [int32 BE total len]
    [0x08][varint len nama][nama][data]. Kalau data_len_prefix,
    data diawali int32 BE panjangnya (varian cadangan)."""
    nb = event_name.encode("utf-8")
    payload = bytes([0x08]) + varint(len(nb)) + nb
    if data:
        if data_len_prefix:
            payload += struct.pack(">I", len(data)) + data
        else:
            payload += data
    return struct.pack(">I", len(payload)) + payload


def build_join_frame(room_id):
    payload = bytes([0x06]) + struct.pack(">I", room_id)
    return struct.pack(">I", len(payload)) + payload


def read_frames(sock, seconds):
    deadline = time.time() + seconds
    buf = bytearray()
    frames = []
    closed = False
    sock.settimeout(0.5)
    while time.time() < deadline:
        try:
            chunk = sock.recv(65536)
        except socket.timeout:
            continue
        except Exception:
            break
        if chunk == b"":
            closed = True
            break
        buf += chunk
        while len(buf) >= 4:
            ln = struct.unpack(">I", buf[:4])[0]
            if ln > 262144:
                return frames, closed
            if len(buf) - 4 < ln:
                break
            frames.append(bytes(buf[4:4 + ln]))
            del buf[:4 + ln]
    return frames, closed


def jwt_user_id(payload):
    j = payload.find(b"eyJ")
    if j < 0:
        return None
    try:
        seg = payload[j:].decode().split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
        return claims.get("user_id")
    except Exception:
        return None


def describe_frame(p, limit=300):
    if b"eyJ" in p:
        j = p.find(b"eyJ")
        return f"type=0x{p[0]:02x} len={len(p)} prefix={p[:j].hex()} <JWT sensor>"
    h = p.hex()
    if len(h) > limit:
        h = h[:limit] + "..."
    return f"type=0x{p[0]:02x} len={len(p)} hex={h}"


def scan_search(frames, name):
    """Sukses SEJATI: frame 'search_friend_result' yang mengandung
    nama yang dicari persis. Kembalikan ('found', uuid) /
    ('notfound', None) / (None, jumlah_uuid_ambient)."""
    ambient = 0
    nb = name.encode("utf-8")
    for p in frames:
        if b"search_friend_result" in p and nb in p:
            m = UUID_RE.search(p)
            return ("found", m.group(0).decode() if m else None), ambient
        if UUID_RE.search(p):
            ambient += 1
    return (None, None), ambient


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
        gsid = alloc.get("gameServerId", "")
        print(f"[*] Server target: {host}:{port} (gameServerId: {gsid})")
        if not host or not port:
            print("[-] Host/port tidak ketemu di allocation.")
            return

    try:
        s = socket.create_connection((host, port), timeout=5)
    except Exception as e:
        print(f"[-] TCP connect gagal: {e}")
        return
    s.sendall(struct.pack(">I", 1) + bytes([0x03]))
    frames, _ = read_frames(s, 10)
    player_id = None
    for p in frames:
        if len(p) > 5 and p[0] == 0x03 and b"eyJ" in p:
            player_id = int.from_bytes(p[1:5], "big")
            uid = jwt_user_id(p)
            print(f"[+] Connected: playerId={player_id}, JWT user_id={uid} "
                  f"({'COCOK' if uid == player_id else 'TIDAK COCOK'})")
            break
    if player_id is None:
        print("[-] Tidak mendapat frame Connected dalam 10 detik. Berhenti.")
        s.close()
        return

    # --- JoinRoom sampai success (suffix, 15, 0, 1) ---
    room_candidates = []
    if gsid and "::" in gsid:
        try:
            room_candidates.append(int(gsid.rsplit("::", 1)[1]))
        except ValueError:
            pass
    for r in (15, 0, 1):
        if r not in room_candidates:
            room_candidates.append(r)
    joined_room = None
    for rid in room_candidates:
        print(f"[*] Coba JoinRoom({rid}) ...")
        try:
            s.sendall(build_join_frame(rid))
        except Exception as e:
            print(f"[-] Gagal kirim JoinRoom: {e}")
            break
        resp, closed = read_frames(s, 8)
        for p in resp:
            if len(p) >= 6 and p[0] == 0x06:
                success = p[1]
                room_back = int.from_bytes(p[2:6], "big")
                n_users = (len(p) - 6) // 4
                print(f"    -> JoinedRoom: success={success} roomId={room_back} "
                      f"users={n_users}")
                if success == 1:
                    joined_room = room_back
        if closed:
            print("[!] Server menutup koneksi saat join.")
            break
        if joined_room is not None:
            print(f"[+] BERGABUNG room {joined_room} (terkonfirmasi)")
            break
    if joined_room is None:
        print("[!] Tidak ada JoinRoom yang Success — tetap coba kirim search.")

    # --- Langkah 1: 'friends' KOSONG -> harus dibalas get_friends_result ---
    print("[*] Langkah 1: kirim 'friends' kosong (validasi encoding)...")
    try:
        s.sendall(build_room_event_varint("friends", b""))
    except Exception as e:
        print(f"[-] Gagal kirim friends kosong: {e}")
        s.close()
        return
    resp, closed = read_frames(s, 8)
    got_list = any(b"get_friends_result" in p for p in resp)
    print(f"    -> {len(resp)} frame, closed={closed}, "
          f"get_friends_result={'YA' if got_list else 'TIDAK'}")
    if got_list:
        print("[+] Kanal + encoding varint TERBUKTI (daftar teman diterima).")
    else:
        print("[!] Daftar teman tidak diterima — encoding/kanal masih salah, "
              "tapi tetap lanjut ke langkah search.")

    # --- Langkah 2: search persis game (varint-nama + protobuf), kirim 2x ---
    protobuf = build_search_protobuf(name)
    print(f"[*] Langkah 2: protobuf SearchFriend untuk '{name}': {protobuf.hex()}")
    variants = [
        ("varint-nama + protobuf mentah (persis game)", False),
        ("varint-nama + int32-len data (cadangan)", True),
    ]
    for label, len_prefix in variants:
        frame = build_room_event_varint("friends", protobuf, len_prefix)
        print(f"[*] Varian: {label} -> frame: {frame.hex()}")
        for attempt in (1, 2):
            try:
                s.sendall(frame)
            except Exception as e:
                print(f"[-] Gagal kirim (percobaan {attempt}): {e}")
                break
            resp, closed = read_frames(s, 6)
            (status, uuid), ambient = scan_search(resp, name)
            if status is not None:
                print(f"[+] HASIL SEARCH DITERIMA! varian='{label}' "
                      f"percobaan={attempt} nama='{name}'")
                if uuid:
                    print(f"[+] UUID untuk '{name}': {uuid}")
                else:
                    print("[*] Hasil search tanpa UUID — kemungkinan nama "
                          "tidak ditemukan (Success=false), tapi jalur "
                          "TERBUKTI berfungsi.")
                s.close()
                print("[*] Search3 probe selesai — jalur TERBUKTI.")
                return
            print(f"[-] {label} percobaan={attempt}: {len(resp)} frame, "
                  f"closed={closed}, UUID ambient diabaikan={ambient}")
            if closed:
                print("[!] Koneksi ditutup server — berhenti.")
                s.close()
                return
    s.close()
    print("[-] Search3 probe selesai, belum ada 'search_friend_result'.")
    print("    Kemungkinan tersisa: urutan/pra-langkah lain dari game "
          "(mis. subscribe 'friends' harus didahului sesuatu), atau "
          "server membedakan koneksi probe dari koneksi game.")


if __name__ == "__main__":
    asyncio.run(main())
