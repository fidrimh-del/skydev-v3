"""Diagnostik TCP ToxicBird — TAHAP 15: EVENT "login" (TEMUAN DARI REKAMAN GAME).

TEROBOSAN DARI REKAMAN MAIN-MENU (TBLog_1_cnvi.txt, 1973 baris):
  Urutan game saat masuk world, PERSIS:
    1. OUT Connect (0x03, 1 byte)
    2. IN Connected (playerId + JWT HS512)
    3. OUT Event "login" (0x05, nama varint "login") + protobuf:
         field 1 (0x0a) = USER_SESSION JWT (token langkah-2
           /v1/users/sessions, ~1164 byte, type=USER_SESSION,
           iss=faster/playkids_xd/playkids-xd-android)
         field 2 (0x12) = "1.93.3" (versi game)
    4. IN Event "tbugv" (balasan server: playerId + UUID pemain)
    5. IN JoinedRoom — TANPA frame OUT JoinRoom sama sekali di
       seluruh rekaman (0 frame OUT 0x06)! Server menaruh klien
       ke room setelah login (atau JoinRoom game lewat jalur
       non-Serialize yang tak terlihat logger).
  SEMUA probe tahap 8-14 TIDAK PERNAH mengirim event "login".
  Itu menjelaskan semuanya: tanpa login, server menerima koneksi
  + join (level koneksi) tapi TIDAK merutekan event mechanic
  (friends) — sesi dianggap anonim. Juga menjelaskan kenapa
  playerId berubah tiap login: identitas sesi datang dari event
  login, bukan dari TCP Connect.
  Rekaman yang sama mengonfirmasi ulang: search Admin#000 ->
  UUID d0db3610-ac6d-4f83-ad52-12be6c29c8f1 (sama seperti
  rekaman v55-fixed). Percobaan add friend di rekaman itu TIDAK
  menghasilkan event friends apa pun (tap mungkin tidak terkirim)
  — tidak menghambat tujuan search.

Probe ini: login + allocation -> TCP connect -> kirim event
"login" (pakai userToken langkah-2) -> tunggu balasan "tbugv"
dan JoinedRoom (auto-join?) -> kalau belum join, JoinRoom biasa
(suffix + 0-9) -> settle -> friends kosong (harap
get_friends_result!) -> search 2x (harap search_friend_result!).

Akun: sama seperti tahap 14 (akun bot pemilik, fallback hardcode
di script atas perintah pemilik; env Secrets menang kalau ada).
Env: SEARCH_NAME (input workflow). Supabase TIDAK dipakai.
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


def build_event_varint(event_name, data):
    """Event (0x05) dengan nama varint — format yang sama dengan
    RoomEvent (0x08), hanya tipenya 0x05. Dipakai event 'login'."""
    nb = event_name.encode("utf-8")
    return bytes([0x05]) + varint(len(nb)) + nb + data


def build_login_protobuf(user_token):
    """Protobuf event login persis game: field 1 (0x0a) =
    USER_SESSION JWT, field 2 (0x12) = versi game '1.93.3'."""
    tb = user_token.encode("utf-8")
    vb = VERSION.encode("utf-8")
    return b"\x0a" + varint(len(tb)) + tb + b"\x12" + varint(len(vb)) + vb


def build_login_frame(user_token):
    payload = build_event_varint("login", build_login_protobuf(user_token))
    return struct.pack(">I", len(payload)) + payload


def tcp_connect(host, port):
    """Connect saja (tanpa join). Kembalikan (socket, player_id)
    atau (None, None). Auto-pong aktif selama jendela baca."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((host, port))
    except Exception as e:
        print(f"[-] Gagal konek TCP: {e}")
        return None, None
    try:
        s.sendall(struct.pack(">I", 1) + b"\x03")
    except Exception as e:
        print(f"[-] Gagal kirim Connect: {e}")
        s.close()
        return None, None
    resp, closed, pongs = read_frames(s, 10)
    pid = None
    for p in resp:
        if len(p) >= 5 and p[0] == 0x03:
            pid = int.from_bytes(p[1:5], "big")
    print(f"[*] Jendela Connect: {len(resp)} frame, pong: {pongs}")
    if pid is not None:
        print(f"[+] Connected: playerId={pid}")
        return s, pid
    print("[-] Tidak ada frame Connected.")
    s.close()
    return None, None


def tcp_join_only(s, gsid):
    """JoinRoom saja (dipakai kalau login tidak auto-join).
    Kembalikan roomId atau None."""
    room_candidates = []
    suffix = None
    if "::" in gsid:
        try:
            suffix = int(gsid.rsplit("::", 1)[1])
        except Exception:
            suffix = None
    if suffix is not None:
        room_candidates.append(suffix)
    for r in range(0, 10):
        if r not in room_candidates:
            room_candidates.append(r)
    for rid in room_candidates:
        try:
            s.sendall(build_join_frame(rid))
        except Exception as e:
            print(f"[-] Gagal kirim JoinRoom: {e}")
            break
        resp, closed, pongs = read_frames(s, 4)
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
            return joined
    print("[!] Semua JoinRoom gagal.")
    return None


def scan_login_window(resp):
    """Cari balasan login di jendela baca: Event 'tbugv' dan
    JoinedRoom (0x06 success=1). Kembalikan (tbugv, room)."""
    tbugv = False
    room = None
    for p in resp:
        if len(p) >= 6 and p[0] == 0x06 and p[1] == 1:
            room = int.from_bytes(p[2:6], "big")
        if len(p) >= 2 and p[0] == 0x05:
            try:
                l = p[1]
                if l < 0x80 and p[2:2 + l] == b"tbugv":
                    tbugv = True
            except Exception:
                pass
    return tbugv, room


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
    # Akun bot pemilik (fallback hardcode atas perintah pemilik;
    # env Secrets menang kalau ada). TIDAK dicetak ke log.
    email = os.getenv("PROBE_EMAIL", "") or "pkxdshop.id@gmail.com"
    password = os.getenv("PROBE_PASSWORD", "") or "Kontol@161"
    device_id = os.getenv("PROBE_DEVICE_ID", "") or "9f31c2d4-probe-device"
    if not name:
        print("[!] Env SEARCH_NAME wajib diisi (input workflow).")
        sys.exit(1)
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        bot = {"email": email, "password": password, "device_id": device_id}
        print("[*] Pakai akun probe (email tidak ditampilkan di log).")
        for attempt in range(1, 4):
            print(f"[*] ===== Alokasi percobaan ke-{attempt} =====")
            user_jwt, alloc = await login_and_allocate(
                bot["email"], bot["password"], bot["device_id"], session)
            if not alloc:
                print("[-] Allocation gagal, coba lagi.")
                continue
            host, port = find_host_port(alloc)
            gsid = alloc.get("gameServerId", "")
            print(f"[*] Server target: {host}:{port} (gameServerId: {gsid})")
            print("[*] Struktur allocation:")
            show_alloc_keys(alloc)
            if not host or not port:
                print("[-] Host/port tidak ketemu di allocation.")
                continue
            s, pid = tcp_connect(host, port)
            if s is None:
                continue
            # ---------- LANGKAH BARU: event login ----------
            try:
                s.sendall(build_login_frame(user_jwt))
                print("[+] Event 'login' terkirim (USER_SESSION JWT + versi 1.93.3).")
            except Exception as e:
                print(f"[-] Gagal kirim event login: {e}")
                s.close()
                continue
            resp, closed, pongs = read_frames(s, 6)
            tbugv, room = scan_login_window(resp)
            print(f"[*] Jendela login 6 detik: {len(resp)} frame, pong: {pongs}, "
                  f"tbugv={'YA' if tbugv else 'tidak'}, "
                  f"auto-join={'room ' + str(room) if room is not None else 'tidak'}")
            if closed:
                print("[!] Koneksi ditutup setelah login.")
                s.close()
                continue
            if room is None:
                print("[*] Tidak auto-join — coba JoinRoom biasa.")
                room = tcp_join_only(s, gsid)
            if room is None:
                print("[!] Join gagal di instance ini — tutup, alokasi ulang.")
                s.close()
                continue
            print(f"[+] BERGABUNG room {room} — lanjut tes friends/search.")
            resp, closed, pongs = read_frames(s, 5)
            print(f"[*] Settle 5 detik: {len(resp)} frame, pong: {pongs}")
            if closed:
                print("[!] Koneksi ditutup saat settle.")
                s.close()
                continue
            # ---------- Tes 1: friends kosong (harap get_friends_result) ----------
            try:
                s.sendall(build_room_event_varint("friends", b""))
            except Exception as e:
                print(f"[-] Gagal kirim friends kosong: {e}")
                s.close()
                continue
            resp, closed, pongs = read_frames(s, 12)
            names = {}
            for p in resp:
                if len(p) >= 2 and p[0] == 0x08:
                    try:
                        l = p[1]
                        if l < 0x80:
                            nm = p[2:2 + l].decode("utf-8", "replace")
                            names[nm] = names.get(nm, 0) + 1
                    except Exception:
                        pass
            print(f"[*] Friends kosong: {len(resp)} frame, pong: {pongs}, "
                  f"get_friends_result={'YA' if 'get_friends_result' in names else 'TIDAK'}")
            if closed:
                s.close()
                continue
            if "get_friends_result" in names:
                print("[+] LOGIN TERBUKTI: event friends dirutekan server!")
            # ---------- Tes 2: search (harap search_friend_result) ----------
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
            print(f"[*] Search 2x: {len(resp)} frame, closed={closed}, pong: {pongs}")
            hit = inventory(resp, name)
            s.close()
            if hit:
                print(f"[+] HASIL SEARCH DITERIMA! UUID untuk '{name}': {hit}")
                print("[*] Search10 probe selesai — jalur TERBUKTI (event login).")
                return
            print("[-] Instance ini gagal (search diam) — alokasi ulang.")
    print("[-] Search10 probe selesai, belum ada 'search_friend_result'.")
    print("    Kalau event login pun tidak mengubah apa-apa, tersangka")
    print("    berikut: isi login harus token lain, atau ada pesan")
    print("    tambahan sesudah login (Analytics / state pemain).")


if __name__ == "__main__":
    asyncio.run(main())
