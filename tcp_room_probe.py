"""Diagnostik TCP ToxicBird — TAHAP 4: gabung room dulu, baru search.

Temuan kunci dari dump (yang menjelaskan gagalnya tahap 3):
  - ProcessSearchFriendResult(RoomEvent evt) — HASIL search datang
    sebagai RoomEvent, bukan Event biasa. Artinya request hampir
    pasti juga lewat RoomEvent, dan RoomEvent butuh keanggotaan
    room (client yang belum join room -> request dibuang server).
  - Tahap 3 mengirim Event/RoomEvent TANPA pernah join room ->
    tidak ada hasil, wajar. Banjir frame 15-29 per jendela di
    tahap 3 BUKAN balasan atas Event kita, melainkan traffic
    ambient server yang sibuk (presence pemain lain: type 0x0b/
    0x0c = connect/disconnect pemain lain, 0x01 = ping).

Yang dilakukan tahap ini:
  1. Login + allocation segar (bot stok) -> PRINT gameServerId.
  2. Connect (0x03) -> Connected (playerId + JWT cocok).
  3. JoinRoom: coba roomId dari suffix gameServerId (angka
     setelah '::' terakhir), lalu 0, lalu 1. Frame JoinRoom =
     [len][type=0x06][roomId int32 BE]. Tunggu JoinedRoom:
     [type=0x06][Success bool][RoomId int32][FailureReason byte].
     Berhenti di room pertama yang Success=true.
  4. Setelah join (atau sekalipun gagal join, tetap dicoba sekali):
     kirim RoomEvent (type 0x08) 'friends' + protobuf SearchFriend,
     lalu Event (type 0x05) 'friends' + protobuf, untuk nama
     SEARCH_NAME dan (kalau mengandung '#') nama tanpa kode.
     Sukses = frame mengandung 'search_friend_result' dan/atau UUID.
  5. Semua frame di jendela join + search pertama DICETAK penuh
     (JWT disensor) untuk diagnosis.

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
FLAG_WORDS = (b"friend", b"result", b"admin", b"error", b"fail")


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


def build_event_frame(etype, event_name, data):
    nb = event_name.encode("utf-8")
    payload = bytes([etype]) + bytes([len(nb)]) + nb + data
    return struct.pack(">I", len(payload)) + payload


def build_join_frame(room_id):
    payload = bytes([0x06]) + struct.pack(">I", room_id)
    return struct.pack(">I", len(payload)) + payload


def read_frames(sock, seconds):
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
                return frames
            if len(buf) - 4 < ln:
                break
            frames.append(bytes(buf[4:4 + ln]))
            del buf[:4 + ln]
    return frames


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


def describe_frame(p):
    if b"eyJ" in p:
        j = p.find(b"eyJ")
        return f"type=0x{p[0]:02x} len={len(p)} prefix={p[:j].hex()} <JWT sensor>"
    return f"type=0x{p[0]:02x} len={len(p)} hex={p.hex()}"


def scan_result(frames):
    for p in frames:
        if b"search_friend_result" in p or UUID_RE.search(p):
            m = UUID_RE.search(p)
            return (m.group(0).decode() if m else None, p)
    return None, None


def flag_interesting(frames):
    """Frame yang mengandung kata kunci (kemungkinan hasil/error)."""
    out = []
    for p in frames:
        low = p.lower()
        if any(w in low for w in FLAG_WORDS) and b"eyJ" not in p:
            out.append(p)
    return out


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
    frames = read_frames(s, 10)
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

    # --- JoinRoom: suffix gameServerId, lalu 0, lalu 1 ---
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
        resp = read_frames(s, 5)
        for p in resp:
            print(f"    join frame: {describe_frame(p)}")
            if len(p) >= 6 and p[0] == 0x06:
                success = p[1]
                room_back = int.from_bytes(p[2:6], "big")
                reason = p[6] if len(p) > 6 else None
                print(f"    -> JoinedRoom: success={success} roomId={room_back} "
                      f"failureReason={reason}")
                if success == 1:
                    joined_room = room_back
        if joined_room is not None:
            print(f"[+] BERGABUNG room {joined_room}")
            break
    if joined_room is None:
        print("[!] Tidak ada JoinRoom yang Success — tetap coba kirim search.")

    # --- Search via RoomEvent (0x08) lalu Event (0x05), dua varian nama ---
    names = [name]
    if "#" in name:
        base = name.split("#", 1)[0]
        if base and base != name:
            names.append(base)
    for nm in names:
        protobuf = build_search_protobuf(nm)
        print(f"[*] Protobuf SearchFriend untuk '{nm}': {protobuf.hex()}")
        for etype, label in [(0x08, "RoomEvent"), (0x05, "Event")]:
            frame = build_event_frame(etype, "friends", protobuf)
            try:
                s.sendall(frame)
            except Exception as e:
                print(f"[-] Gagal kirim {label}: {e}")
                break
            resp = read_frames(s, 4)
            uuid, hit = scan_result(resp)
            if uuid or hit is not None:
                print(f"[+] KETEMU! {label} nama='{nm}'")
                print(f"[+] UUID untuk '{nm}': "
                      f"{uuid if uuid else '(frame ketemu, UUID tak ter-parse)'}")
                for p in resp:
                    print(f"    frame: {describe_frame(p)}")
                s.close()
                print("[*] Room probe selesai — UUID ketemu.")
                return
            interesting = flag_interesting(resp)
            print(f"[-] {label} nama='{nm}': {len(resp)} frame, tanpa hasil. "
                  f"Frame menarik: {len(interesting)}")
            for p in interesting[:5]:
                print(f"    menarik: {describe_frame(p)}")
    s.close()
    print("[-] Room probe selesai, belum ada search_friend_result.")
    print("    Opsi berikut: capture traffic game asli (PCAPdroid) saat")
    print("    cari teman di HP, untuk melihat frame persisnya.")


if __name__ == "__main__":
    asyncio.run(main())
