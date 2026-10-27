"""Diagnostik TCP ToxicBird — TAHAP 5: perbaiki encoding string Event.

Temuan tahap 4 (join BERHASIL, search masih 0 frame):
  - JoinRoom(suffix gameServerId) -> JoinedRoom success=1.
    Jadi: type JoinRoom = 0x06 TERKONFIRMASI, dan suffix
    gameServerId memang roomId.
  - Setelah join, server mengirim dump state room (frame 0x0d,
    ~15 KB) berisi variabel pemain — termasuk nama (key 'm')
    dan UUID (key 'k'). Server MEMANG mengirim UUID lewat
    frame room, tapi target kita tidak di room itu.
  - Setelah join, kirim RoomEvent/Event 'friends' (string
    7-bit length) -> 0 frame SAMA SEKALI, tanpa error.
    Bukan "tidak ketemu" (itu tetap mengembalikan frame
    search_friend_result Success=false), melainkan request
    tidak sampai / tidak bisa diparse server.

Hipotesis yang diperbaiki tahap ini (berbasis bukti dump room):
  - Di dump room, string dikodekan [int32 BE panjang][bytes]
    (contoh: 00 00 00 03 + 'aaa', 00 00 00 25 + UUID 37 char) —
    BUKAN 7-bit length ala BinaryWriter .NET. Jadi frame Event
    tahap 3/4 (0x07 + 'friends') kemungkinan besar MALFORMED:
    server membaca panjang 0x07667269 (raksasa) lalu membuang.
  - Tahap ini mengirim eventName sebagai [int32 BE 7]['friends'],
    dengan data dua varian: mentah, dan [int32 BE panjang]+data.
  - Juga mendeteksi apakah server MENUTUP koneksi setelah kirim
    (tanda frame malformed) vs diam tapi koneksi tetap buka.

Alur: login + allocation -> Connect (0x03) -> JoinRoom(suffix,
lalu 0, 1) -> RoomEvent (0x08) / Event (0x05) int32-name untuk
nama penuh + nama tanpa #kode. Sukses = frame mengandung
'search_friend_result' dan/atau UUID.

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
FLAG_WORDS = (b"friend", b"result", b"error", b"fail")


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


def build_event_frame(etype, event_name, data, variant):
    """eventName int32-BE (bukti dari dump room), data dua varian."""
    nb = event_name.encode("utf-8")
    name_part = struct.pack(">I", len(nb)) + nb
    if variant == "a":
        payload = bytes([etype]) + name_part + data
    else:  # variant b: data diawali int32 BE panjang
        payload = bytes([etype]) + name_part + struct.pack(">I", len(data)) + data
    return struct.pack(">I", len(payload)) + payload


def build_join_frame(room_id):
    payload = bytes([0x06]) + struct.pack(">I", room_id)
    return struct.pack(">I", len(payload)) + payload


def read_frames(sock, seconds):
    """Kembalikan (frames, closed). closed=True kalau server menutup
    koneksi (recv b'') — sinyal frame malformed."""
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


def scan_result(frames):
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

    # --- JoinRoom ---
    room_candidates = []
    if gsid and "::" in gsid:
        try:
            room_candidates.append(int(gsid.rsplit("::", 1)[1]))
        except ValueError:
            pass
    for r in (0, 1):
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
        resp, closed = read_frames(s, 5)
        for p in resp:
            if len(p) >= 6 and p[0] == 0x06:
                success = p[1]
                room_back = int.from_bytes(p[2:6], "big")
                n_users = (len(p) - 6) // 4
                print(f"    -> JoinedRoom: success={success} roomId={room_back} "
                      f"users={n_users} frame_len={len(p)}")
                if success == 1:
                    joined_room = room_back
            elif len(p) > 1000:
                print(f"    -> frame besar: {describe_frame(p, 200)}")
        if closed:
            print("[!] Server MENUTUP koneksi saat join — coba koneksi baru?")
            break
        if joined_room is not None:
            print(f"[+] BERGABUNG room {joined_room}")
            break
    if joined_room is None:
        print("[!] Tidak ada JoinRoom yang Success — tetap coba kirim search.")

    # --- Search: RoomEvent/Event, nama int32, data varian a/b ---
    names = [name]
    if "#" in name:
        base = name.split("#", 1)[0]
        if base and base != name:
            names.append(base)
    for nm in names:
        protobuf = build_search_protobuf(nm)
        print(f"[*] Protobuf SearchFriend untuk '{nm}': {protobuf.hex()}")
        for etype, label in [(0x08, "RoomEvent"), (0x05, "Event")]:
            for variant in ["a", "b"]:
                frame = build_event_frame(etype, "friends", protobuf, variant)
                try:
                    s.sendall(frame)
                except Exception as e:
                    print(f"[-] Gagal kirim {label} varian={variant}: {e}")
                    break
                resp, closed = read_frames(s, 4)
                uuid, hit = scan_result(resp)
                if uuid or hit is not None:
                    print(f"[+] KETEMU! {label} varian={variant} nama='{nm}'")
                    print(f"[+] UUID untuk '{nm}': "
                          f"{uuid if uuid else '(frame ketemu, UUID tak ter-parse)'}")
                    for p in resp:
                        print(f"    frame: {describe_frame(p)}")
                    s.close()
                    print("[*] Event2 probe selesai — UUID ketemu.")
                    return
                interesting = [p for p in resp
                               if any(w in p.lower() for w in FLAG_WORDS)
                               and b"eyJ" not in p]
                print(f"[-] {label} varian={variant} nama='{nm}': "
                      f"{len(resp)} frame, closed={closed}, "
                      f"menarik={len(interesting)}")
                for p in interesting[:3]:
                    print(f"    menarik: {describe_frame(p)}")
                if closed:
                    print("[!] Koneksi ditutup server — kemungkinan frame "
                          "dianggap malformed. Berhenti.")
                    s.close()
                    return
    s.close()
    print("[-] Event2 probe selesai, belum ada search_friend_result.")
    print("    Opsi berikut: capture traffic game asli (PCAPdroid) saat")
    print("    cari teman di HP, untuk melihat frame persisnya.")


if __name__ == "__main__":
    asyncio.run(main())
