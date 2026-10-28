"""Diagnostik TCP ToxicBird — TAHAP 9: SNIFFER balasan server.

Konteks: tahap 8 GAGAL total padahal frame search-nya sudah
IDENTIK byte-per-byte dengan frame game asli (varint-nama +
protobuf SearchFriendRequest), dan join terkonfirmasi (success=1,
koneksi tetap terbuka, 130-182 frame ambient per jendela).
Jadi masalahnya BUKAN byte request — melainkan sesuatu yang
lain: (a) pra-langkah yang dikirim game setelah join (state
pemain/device/session) sebelum server merutekan event friends,
atau (b) event friends lewat transport lain (ToxicBirdClient
punya jalur UDP: SendMessageUnreliable / ListenForUdpData),
atau (c) server membalas dengan nama event lain (mis. error)
yang tidak dicari scanner tahap 8.

Karena itu probe ini TIDAK lagi sekadar ya/tidak: setiap
jendela baca, ia menginventarisasi SEMUA nama RoomEvent/Event
yang masuk (parse panjang nama varint maupun int32) plus
menandai frame apa pun yang mengandung 'friend' / 'search' /
'error' / 'fail'. Kalau server membalas apa pun, kita lihat.

Urutan (setelah join terkonfirmasi):
  1. 'friends' kosong (varint) — 10 detik.
  2. Search varint-nama + protobuf (persis game) — 2x, 10 detik.
  3. Varian: varint-nama + varint-len data + protobuf — 8 detik.
  4. Varian: Event (0x05) varint-nama + protobuf — 8 detik.

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
from collections import OrderedDict, Counter
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
FLAG_RE = re.compile(rb"friend|search|error|fail|denied", re.IGNORECASE)


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
    val = 0
    shift = 0
    i = off
    while i < len(buf) and shift < 35:
        b = buf[i]
        val |= (b & 0x7F) << shift
        i += 1
        if not (b & 0x80):
            return val, i - off
        shift += 7
    return None, 0


def build_search_protobuf(name):
    nb = name.encode("utf-8")
    return b"\x08\x07\x12" + varint(len(nb)) + nb


def build_room_event_varint(event_name, data, data_len_prefix=None):
    nb = event_name.encode("utf-8")
    payload = bytes([0x08]) + varint(len(nb)) + nb
    if data:
        if data_len_prefix == "varint":
            payload += varint(len(data)) + data
        elif data_len_prefix == "int32":
            payload += struct.pack(">I", len(data)) + data
        else:
            payload += data
    return struct.pack(">I", len(payload)) + payload


def build_event_varint(event_name, data):
    nb = event_name.encode("utf-8")
    payload = bytes([0x05]) + varint(len(nb)) + nb + data
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


def parse_event_name(p):
    """Ambil nama event dari frame RoomEvent (0x08) / Event (0x05).
    Coba varint dulu (encoding game), lalu int32. Kembalikan nama
    atau None."""
    if len(p) < 3 or p[0] not in (0x08, 0x05):
        return None
    v, nbytes = read_varint(p, 1)
    if v is not None and 1 <= v <= 64 and 1 + nbytes + v <= len(p):
        nb = p[1 + nbytes:1 + nbytes + v]
        if all(32 <= c < 127 for c in nb):
            return nb.decode("ascii", "replace")
    if len(p) >= 5:
        v2 = struct.unpack(">I", p[1:5])[0]
        if 1 <= v2 <= 64 and 5 + v2 <= len(p):
            nb = p[5:5 + v2]
            if all(32 <= c < 127 for c in nb):
                return nb.decode("ascii", "replace")
    return None


def inventory(frames, name):
    """Laporan jendela: hitung nama event masuk + flag frame
    menarik + cek hasil search ketat."""
    names = Counter()
    type_count = Counter()
    flagged = []
    search_hit = None
    nb = name.encode("utf-8")
    for p in frames:
        type_count[f"0x{p[0]:02x}"] += 1
        en = parse_event_name(p)
        if en:
            names[en] += 1
        if b"search_friend_result" in p and nb in p:
            m = UUID_RE.search(p)
            search_hit = m.group(0).decode() if m else "(tanpa UUID)"
        elif FLAG_RE.search(p):
            flagged.append(p)
    print(f"    tipe frame: {dict(type_count)}")
    print(f"    nama event masuk: {dict(names) if names else '(tidak ada yang ter-parse)'}")
    for p in flagged[:5]:
        h = p.hex()
        print(f"    FLAG frame len={len(p)}: {h[:200]}{'...' if len(h) > 200 else ''}")
    return search_hit


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

    def send_and_report(label, frame, seconds):
        print(f"[*] {label} -> frame: {frame.hex()}")
        try:
            s.sendall(frame)
        except Exception as e:
            print(f"[-] Gagal kirim: {e}")
            return None, True
        resp, closed = read_frames(s, seconds)
        print(f"    {len(resp)} frame, closed={closed}")
        hit = inventory(resp, name)
        return hit, closed

    # 1. friends kosong
    hit, closed = send_and_report(
        "Langkah 1: 'friends' kosong", build_room_event_varint("friends", b""), 10)
    if hit:
        print(f"[+] UUID untuk '{name}': {hit}")
        s.close()
        return
    if closed:
        print("[!] Koneksi ditutup — berhenti.")
        s.close()
        return

    # 2. search persis game, 2x
    protobuf = build_search_protobuf(name)
    for attempt in (1, 2):
        hit, closed = send_and_report(
            f"Langkah 2: search persis game (percobaan {attempt})",
            build_room_event_varint("friends", protobuf), 10)
        if hit:
            print(f"[+] HASIL SEARCH DITERIMA! UUID untuk '{name}': {hit}")
            s.close()
            print("[*] Search4 probe selesai — jalur TERBUKTI.")
            return
        if closed:
            print("[!] Koneksi ditutup — berhenti.")
            s.close()
            return

    # 3. varian varint-len data
    hit, closed = send_and_report(
        "Langkah 3: varint-nama + varint-len data",
        build_room_event_varint("friends", protobuf, "varint"), 8)
    if hit:
        print(f"[+] HASIL SEARCH DITERIMA! UUID untuk '{name}': {hit}")
        s.close()
        return
    if closed:
        s.close()
        return

    # 4. varian Event (0x05)
    hit, closed = send_and_report(
        "Langkah 4: Event (0x05) varint-nama + protobuf",
        build_event_varint("friends", protobuf), 8)
    if hit:
        print(f"[+] HASIL SEARCH DITERIMA! UUID untuk '{name}': {hit}")
        s.close()
        return

    s.close()
    print("[-] Search4 probe selesai, belum ada 'search_friend_result'.")
    print("    Lihat inventaris nama event di atas: kalau server membalas")
    print("    dengan nama lain (mis. error), itu petunjuk berikutnya.")
    print("    Kalau inventaris kosong dari nama RoomEvent, kemungkinan")
    print("    besar ada pra-langkah game yang belum direplikasi atau")
    print("    event friends lewat transport lain (UDP).")


if __name__ == "__main__":
    asyncio.run(main())
