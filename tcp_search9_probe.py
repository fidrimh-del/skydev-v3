"""Diagnostik TCP ToxicBird — TAHAP 14: AKUN FULL IDENTITAS (BUKAN STOK BOT).

HIPOTESIS YANG DIUJI: stok bot gagal bukan karena protokol,
melainkan karena akunnya — semua stok bot mungkin akun fresh /
tidak lengkap, sementara fitur friends butuh akun "full identitas".
Probe ini memakai SATU akun tetap yang disediakan pemiliknya
(akun bot beridentitas penuh), BUKAN get_fresh_bots.

KEAMANAN (PENTING):
  - Kredensial HANYA lewat GitHub Secrets: PROBE_EMAIL,
    PROBE_PASSWORD, PROBE_DEVICE_ID. TIDAK PERNAH ditulis di
    script, repo, atau log (email pun tidak dicetak ke log).
  - Probe ini READ-ONLY: login + allocation + connect + join +
    search saja. Tidak follow, tidak tulis history, tidak ubah
    apa pun di akun.
  - Kalau akun ini juga gagal -> hipotesis akun GUGUR total,
    dan penyelidikan kembali ke protokol (handshake UDP /
    urutan join dari rekaman game).

Alur: login akun tetap -> allocation (retry sampai 3 instance
kalau join gagal, seperti tahap 13) -> connect -> join ->
settle 5 detik (auto-pong) -> tes UDP ke host:9941 (4 varian) ->
kontrol TCP search 2x (20 detik).

Env: PROBE_EMAIL, PROBE_PASSWORD, PROBE_DEVICE_ID (Secrets),
SEARCH_NAME (input workflow). Supabase TIDAK dipakai di tahap ini.
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


def build_room_event_payload(event_name, data):
    nb = event_name.encode("utf-8")
    return bytes([0x08]) + varint(len(nb)) + nb + data


def build_room_event_varint(event_name, data):
    payload = build_room_event_payload(event_name, data)
    return struct.pack(">I", len(payload)) + payload


def build_join_frame(room_id):
    payload = bytes([0x06]) + struct.pack(">I", room_id)
    return struct.pack(">I", len(payload)) + payload


def read_frames(sock, seconds):
    deadline = time.time() + seconds
    buf = bytearray()
    frames = []
    closed = False
    pongs = 0
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
                return frames, closed, pongs
            if len(buf) - 4 < ln:
                break
            fr = bytes(buf[4:4 + ln])
            del buf[:4 + ln]
            frames.append(fr)
            if fr == b"\x01":
                try:
                    sock.sendall(struct.pack(">I", 1) + b"\x02")
                    pongs += 1
                except Exception:
                    pass
    return frames, closed, pongs


def read_both(tcp_sock, udp_sock, seconds):
    """Dengarkan TCP (frame + auto-pong) DAN UDP (datagram)
    dalam satu loop. Kembalikan (tcp_frames, udp_datagrams,
    closed, pongs)."""
    deadline = time.time() + seconds
    tcp_buf = bytearray()
    tcp_frames = []
    udp_data = []
    closed = False
    pongs = 0
    tcp_sock.settimeout(0.1)
    udp_sock.settimeout(0.1)
    while time.time() < deadline:
        try:
            chunk = tcp_sock.recv(65536)
            if chunk == b"":
                closed = True
                break
            tcp_buf += chunk
            while len(tcp_buf) >= 4:
                ln = struct.unpack(">I", tcp_buf[:4])[0]
                if ln > 262144:
                    break
                if len(tcp_buf) - 4 < ln:
                    break
                fr = bytes(tcp_buf[4:4 + ln])
                del tcp_buf[:4 + ln]
                tcp_frames.append(fr)
                if fr == b"\x01":
                    try:
                        tcp_sock.sendall(struct.pack(">I", 1) + b"\x02")
                        pongs += 1
                    except Exception:
                        pass
        except socket.timeout:
            pass
        except Exception:
            break
        try:
            dg, _addr = udp_sock.recvfrom(65536)
            if dg:
                udp_data.append(dg)
        except socket.timeout:
            pass
        except Exception:
            pass
    return tcp_frames, udp_data, closed, pongs


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


def scan_packets(packets, name):
    """Cari hasil search di daftar paket (TCP frame atau datagram
    UDP). Kembalikan UUID atau None. Juga laporkan nama event
    yang ter-parse."""
    names = Counter()
    nb = name.encode("utf-8")
    for p in packets:
        en = parse_event_name(p)
        if en:
            names[en] += 1
        if b"search_friend_result" in p and nb in p:
            m = UUID_RE.search(p)
            return (m.group(0).decode() if m else "(tanpa UUID)"), names
    return None, names


def inventory(frames, name):
    names = Counter()
    type_count = Counter()
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
    print(f"    tipe frame: {dict(type_count)}")
    print(f"    nama event masuk: {dict(names) if names else '(tidak ada yang ter-parse)'}")
    return search_hit


async def fetch_bots(session, limit):
    endpoint = f"{SUPABASE_URL}/rest/v1/rpc/get_fresh_bots"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    payload = {"p_target_uuid": "00000000-0000-0000-0000-000000000000", "p_limit": limit}
    try:
        r = await session.post(endpoint, json=payload, headers=headers, timeout=20)
        if r.status_code == 200 and r.json():
            return r.json()
    except Exception as e:
        print(f"[-] Gagal mengambil bot: {e}")
    return []


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


def find_udp_port(body):
    """Cari port UDP dari allocation (kunci mengandung 'udp',
    nilai int). Kalau tidak ada -> DEFAULT_UDP_PORT 9941 dari dump."""
    found = []

    def scan(d):
        if not isinstance(d, dict):
            return
        for k, v in d.items():
            if "udp" in k.lower() and isinstance(v, int) and v > 0:
                found.append(v)
            if isinstance(v, dict):
                scan(v)
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        scan(item)

    scan(body)
    return found[0] if found else 9941


def show_alloc_keys(body, depth=0, prefix=""):
    """Tampilkan struktur kunci allocation (nilai disensor kecuali
    host/port/id yang aman) — untuk mencari endpoint UDP."""
    if depth > 2 or not isinstance(body, dict):
        return
    for k, v in body.items():
        lk = k.lower()
        if any(t in lk for t in ("token", "secret", "password", "key", "jwt", "auth")):
            print(f"    {prefix}{k}: <sensor>")
        elif isinstance(v, dict):
            print(f"    {prefix}{k}: (dict)")
            show_alloc_keys(v, depth + 1, prefix + k + ".")
        elif isinstance(v, list):
            print(f"    {prefix}{k}: (list, {len(v)} item)")
            if v and isinstance(v[0], dict):
                show_alloc_keys(v[0], depth + 1, prefix + k + "[0].")
        else:
            print(f"    {prefix}{k}: {v}")


def tcp_connect_join(host, port, gsid, join_window=4, max_room=9):
    """Koneksi TCP + join. Kembalikan (socket, room) atau (None, None)."""
    try:
        s = socket.create_connection((host, port), timeout=5)
    except Exception as e:
        print(f"[-] TCP connect gagal: {e}")
        return None, None
    s.sendall(struct.pack(">I", 1) + bytes([0x03]))
    frames, _, pongs = read_frames(s, 10)
    print(f"[*] Jendela Connect: {len(frames)} frame, pong: {pongs}")
    player_id = None
    for p in frames:
        if len(p) > 5 and p[0] == 0x03 and b"eyJ" in p:
            player_id = int.from_bytes(p[1:5], "big")
            uid = jwt_user_id(p)
            print(f"[+] Connected: playerId={player_id}, JWT user_id={uid} "
                  f"({'COCOK' if uid == player_id else 'TIDAK COCOK'})")
            break
    if player_id is None:
        print("[-] Tidak mendapat frame Connected. Berhenti.")
        s.close()
        return None, None
    room_candidates = []
    if gsid and "::" in gsid:
        try:
            room_candidates.append(int(gsid.rsplit("::", 1)[1]))
        except ValueError:
            pass
    for r in range(0, max_room + 1):
        if r not in room_candidates:
            room_candidates.append(r)
    for rid in room_candidates:
        try:
            s.sendall(build_join_frame(rid))
        except Exception as e:
            print(f"[-] Gagal kirim JoinRoom: {e}")
            break
        resp, closed, pongs = read_frames(s, join_window)
        joined = None
        for p in resp:
            if len(p) >= 6 and p[0] == 0x06 and p[1] == 1:
                joined = int.from_bytes(p[2:6], "big")
        print(f"[*] JoinRoom({rid}): {len(resp)} frame, pong: {pongs}, "
              f"hasil={'SUKSES room ' + str(joined) if joined is not None else 'gagal'}")
        if closed:
            print("[!] Koneksi ditutup saat join.")
            break
        if joined is not None:
            return s, joined
    print("[!] Semua JoinRoom gagal.")
    return s, None


async def main():
    name = os.getenv("SEARCH_NAME", "")
    email = os.getenv("PROBE_EMAIL", "")
    password = os.getenv("PROBE_PASSWORD", "")
    device_id = os.getenv("PROBE_DEVICE_ID", "") or "9f31c2d4-probe-device"
    if not name or not email or not password:
        print("[!] Env SEARCH_NAME / PROBE_EMAIL / PROBE_PASSWORD wajib diisi (via GitHub Secrets).")
        sys.exit(1)
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        bot = {"email": email, "password": password, "device_id": device_id}
        print("[*] Pakai akun probe dari Secrets (email tidak ditampilkan di log).")
        for attempt in range(1, 4):
            print(f"[*] ===== Alokasi percobaan ke-{attempt} =====")
            _jwt, alloc = await login_and_allocate(
                bot["email"], bot["password"], bot["device_id"], session)
            if not alloc:
                print("[-] Allocation gagal, coba lagi.")
                continue
            host, port = find_host_port(alloc)
            gsid = alloc.get("gameServerId", "")
            udp_port = find_udp_port(alloc)
            print(f"[*] Server target: {host}:{port} | UDP target: {host}:{udp_port} "
                  f"(gameServerId: {gsid})")
            print("[*] Struktur allocation:")
            show_alloc_keys(alloc)
            if not host or not port:
                print("[-] Host/port tidak ketemu di allocation.")
                continue
            s, room = tcp_connect_join(host, port, gsid)
            if s is None:
                continue
            if room is None:
                print(f"[!] Join gagal di instance ini — tutup, alokasi ulang.")
                s.close()
                continue
            print(f"[+] BERGABUNG room {room} (terkonfirmasi) — lanjut tes UDP/TCP.")
            resp, closed, pongs = read_frames(s, 5)
            print(f"[*] Settle 5 detik: {len(resp)} frame, pong: {pongs}")
            if closed:
                print("[!] Koneksi ditutup saat settle.")
                s.close()
                continue
            # ---------- Tes UDP ke 9941 (atau port allocation) ----------
            try:
                u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                u.connect((host, udp_port))
                u.settimeout(0.1)
                print(f"[+] Socket UDP dibuka ke {host}:{udp_port}")
            except Exception as e:
                print(f"[-] Gagal buka socket UDP: {e}")
                u = None
            if u is not None:
                protobuf = build_search_protobuf(name)
                empty_payload = build_room_event_payload("friends", b"")
                search_payload = build_room_event_payload("friends", protobuf)
                udp_tests = [
                    ("UDP empty mentah (tanpa prefix)", empty_payload, 8),
                    ("UDP empty ber-prefix int32", struct.pack(">I", len(empty_payload)) + empty_payload, 8),
                    ("UDP search mentah (tanpa prefix)", search_payload, 12),
                    ("UDP search ber-prefix int32", struct.pack(">I", len(search_payload)) + search_payload, 12),
                ]
                done = False
                for label, dg, seconds in udp_tests:
                    print(f"[*] {label} -> {dg.hex()}")
                    try:
                        u.send(dg)
                    except Exception as e:
                        print(f"[-] Gagal kirim UDP: {e}")
                        continue
                    tframes, dgrams, closed, pongs = read_both(s, u, seconds)
                    print(f"    TCP: {len(tframes)} frame, pong: {pongs} | "
                          f"UDP: {len(dgrams)} datagram, closed={closed}")
                    hit, names = scan_packets(tframes, name)
                    if hit:
                        print(f"[+] HASIL SEARCH DITERIMA (via TCP)! UUID untuk '{name}': {hit}")
                        done = True
                        break
                    hit_u, names_u = scan_packets(dgrams, name)
                    if hit_u:
                        print(f"[+] HASIL SEARCH DITERIMA (via UDP port {udp_port})! "
                              f"UUID untuk '{name}': {hit_u}")
                        done = True
                        break
                    all_names = names + names_u
                    print(f"    nama event: {dict(all_names) if all_names else '(tidak ada)'}")
                    if closed:
                        break
                u.close()
                if done:
                    s.close()
                    print("[*] Search8 probe selesai — jalur TERBUKTI.")
                    return
                print("[-] Tes UDP selesai tanpa hasil.")
            # ---------- Kontrol TCP ----------
            protobuf = build_search_protobuf(name)
            frame = build_room_event_varint("friends", protobuf)
            try:
                s.sendall(frame)
                s.sendall(frame)
            except Exception as e:
                print(f"[-] Gagal kirim search TCP: {e}")
                s.close()
                continue
            resp, closed, pongs = read_frames(s, 20)
            print(f"[*] Kontrol TCP search 2x: {len(resp)} frame, closed={closed}, pong: {pongs}")
            hit = inventory(resp, name)
            s.close()
            if hit:
                print(f"[+] HASIL SEARCH DITERIMA (via TCP)! UUID untuk '{name}': {hit}")
                print("[*] Search8 probe selesai — jalur TERBUKTI.")
                return
            print("[-] Instance ini gagal total (UDP + TCP diam) — alokasi ulang.")
    print("[-] Search8 probe selesai, belum ada 'search_friend_result'.")
    print("    Kalau UDP 9941 diam juga, kemungkinan berikut: handshake")
    print("    UDP (InitUdpConnection) harus mengirim sesuatu dulu, atau")
    print("    event friends memang lewat jalur lain.")


if __name__ == "__main__":
    asyncio.run(main())
