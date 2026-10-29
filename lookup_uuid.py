"""lookup_uuid.py — Ambil UUID pemain PK XD dari nama (CEPAT).

Jalur yang sudah TERBUKTI: login FSTR -> allocation ->
TCP connect -> Event "login" (USER_SESSION JWT + 1.93.4) ->
LANGSUNG RoomEvent "friends" + protobuf SearchFriend (EventId=7)
-> search_friend_result -> UUID. Join TIDAK diperlukan
(skip-join test 2026-10-29: search dibalas tanpa room); join hanya
fallback bila search langsung kosong.

Versi ini DIPANGKAS untuk kecepatan dibanding probe:
  - Jalur cepat = search langsung sesudah login (tanpa join).
  - Tanpa tes UDP, tanpa Supabase, tanpa dump frame verbose.
  - Jendela early-exit (2026-10-29): connect/login/search berhenti
    SEGERA saat frame target tiba (cap tetap: connect 6, login 5,
    join 3 dtk per room, search 12 dtk); settle 2 dtk DIHAPUS
    (eksperimen skydev-test: 1,4-1,9 dtk vs baseline 27,2 dtk).
  - Join DIHAPUS dari jalur cepat (2026-10-29, skip-join test):
    search langsung sesudah login. Urutan fallback (v2):
    tunggu 6 dtk + search ulang -> join + search -> friends-kosong
    + search (join/friends-kosong 0 kasus penyelamatan di v2, hanya
    jaring pengaman terakhir; sesi lambat cukup ditunggu —
    kelambatan bersifat per-sesi, bukan per-server: 174.138.71.188
    pernah lambat 18-38 dtk dan pernah menjawab <1 dtk).
  - Satu varian query saja (server terbukti cocok case-insensitive,
    jadi kapitalisasi input tidak masalah).
  - Log ringkas + blok HASIL + waktu total + satu baris log
    ditambahkan ke uuid_lookup_log.txt.

CARA PAKAI:
  python lookup_uuid.py "Admin#000"      (satu argumen nama#kode)
  python lookup_uuid.py Admin 000        (dua argumen: nama + kode)
  python lookup_uuid.py                  (pakai env SEARCH_NAME / prompt)
  Di GitHub Actions: pakai lookup-uuid.yml (input search_name).

Kredensial: akun bot pemilik (env PROBE_* menang, kalau tidak ada
pakai nilai di script atas perintah pemilik; hapus nilai hardcode
saat masuk produksi dan pindah ke Secrets). Nilai kredensial
TIDAK PERNAH dicetak ke log.
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
import urllib.request
import urllib.error
from collections import OrderedDict, Counter
from curl_cffi.requests import AsyncSession

try:
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    pass

# SEMENTARA UNTUK UJI COBA (repo private; nilai sama seperti script
# panel). Env selalu menang kalau sudah diisi sebagai Secrets.
APP_KEY = os.getenv("FSTR_APP_KEY") or "57a3d999-654c-4484-a20c-0f4b5512af49"
APP_SECRET = os.getenv("FSTR_APP_SECRET") or "jWS5aLZacNVFx6PAxpXq2g4o"

ACCOUNT_URL = "https://account.faster.aftvrsys.com"
LOBBY_URL = "https://pkxd-lobby.aftvrsys.com"
GAME_PROTOCOL = "1.93.0"
VERSION = "1.93.4"

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


def get_panel_versions():
    """(game_version, protocol_version) dari tabel app_settings di DB
    panel (diubah dari dashboard, tanpa edit GitHub). Kembalikan
    (None, None) bila env/tabel tak tersedia — pemanggil memakai nilai
    bawaan (job tetap jalan, tidak mati)."""
    url = (os.getenv("PANEL_SUPABASE_URL")
           or os.getenv("SUPABASE_URL", "")).rstrip("/")
    key = (os.getenv("PANEL_SUPABASE_SERVICE_KEY")
           or os.getenv("SUPABASE_SERVICE_KEY", ""))
    if not url or not key:
        return None, None
    try:
        req = urllib.request.Request(
            url + "/rest/v1/app_settings?select=key,value"
            "&key=in.(game_version,protocol_version)",
            headers={"apikey": key, "Authorization": "Bearer " + key},
            method="GET")
        with urllib.request.urlopen(req, timeout=10) as r:
            rows = json.loads(r.read().decode("utf-8"))
        gv = pv = None
        for row in rows or []:
            v = (row.get("value") or "").strip() or None
            if row.get("key") == "game_version":
                gv = v
            elif row.get("key") == "protocol_version":
                pv = v
        return gv, pv
    except Exception as e:
        print(f"[!] Baca app_settings gagal ({e}) -> pakai versi bawaan "
              f"({VERSION}/{GAME_PROTOCOL}).")
        return None, None


def apply_panel_versions():
    """Terapkan versi dari DB panel (bila ada) ke modul ini, termasuk
    rebuild HEADERS_BASE yang dibangun sekali saat import (override
    variabel saja TIDAK cukup). Kembalikan (VERSION, GAME_PROTOCOL)
    yang aktif. Dipanggil sekali di awal main() setiap script."""
    global GAME_PROTOCOL, VERSION, HEADERS_BASE
    gv, pv = get_panel_versions()
    if gv:
        VERSION = gv
    if pv:
        GAME_PROTOCOL = pv
        HEADERS_BASE = {**HEADERS_BASE, "X-Protocol-Version": pv}
    print(f"[*] Versi aktif: game={VERSION} protokol={GAME_PROTOCOL}")
    return VERSION, GAME_PROTOCOL


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
    USER_SESSION JWT, field 2 (0x12) = versi game '1.93.4'."""
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
    resp, closed, pongs, _ = read_until(
        s, lambda fr: any(len(p) >= 5 and p[0] == 0x03 for p in fr), 6)
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
    Kembalikan roomId atau None.
    FAIL-FAST (2026-10-28): sifat join adalah per-instance, bukan
    per-room — instance yang sehat menerima suffix/room awal, instance
    yang buruk menolak SEMUA room (terbukti: 10/10 gagal di satu
    instance, lalu instance berikutnya langsung auto-join). Mencoba
    11 room hanya membuang ~30 detik di instance mati. Jadi cukup
    suffix + 4 room awal (maks 5 percobaan, ~15 detik); kalau semua
    gagal, pemanggil langsung alokasi ulang ke instance baru — itulah
    pemulihan yang terbukti, bukan memaksa instance yang menolak."""
    room_candidates = []
    suffix = None
    if "::" in gsid:
        try:
            suffix = int(gsid.rsplit("::", 1)[1])
        except Exception:
            suffix = None
    if suffix is not None:
        room_candidates.append(suffix)
    for r in range(0, 4):
        if r not in room_candidates:
            room_candidates.append(r)
    for rid in room_candidates:
        try:
            s.sendall(build_join_frame(rid))
        except Exception as e:
            print(f"[-] Gagal kirim JoinRoom: {e}")
            break
        resp, closed, pongs = read_frames(s, 3)
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


def parse_protobuf_fields(data):
    """Parse protobuf generik -> list (field, wiretype, value).
    wiretype 0 = varint, 2 = length-delimited."""
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
            fields.append((field, wt, v))
        elif wt == 2:
            ln, n = read_varint(data, i)
            if ln is None:
                break
            i += n
            fields.append((field, wt, data[i:i + ln]))
            i += ln
        else:
            break
    return fields


def decode_search_result(frame):
    """Decode satu frame RoomEvent 'search_friend_result' ->
    dict(success, uuid, name, field2) atau None kalau bukan
    frame hasil search."""
    if len(frame) < 2 or frame[0] != 0x08:
        return None
    ln, n = read_varint(frame, 1)
    if ln is None:
        return None
    if frame[1 + n:1 + n + ln] != b"search_friend_result":
        return None
    data = frame[1 + n + ln:]
    out = {"success": None, "uuid": None, "name": None, "field2": None}
    for f, wt, v in parse_protobuf_fields(data):
        if f == 1 and wt == 0:
            out["success"] = (v == 1)
        elif f == 2 and wt == 2:
            try:
                out["field2"] = v.decode("utf-8", "replace")
            except Exception:
                pass
        elif f == 3 and wt == 2:
            for f2, wt2, v2 in parse_protobuf_fields(v):
                if f2 == 1 and wt2 == 2:
                    try:
                        out["uuid"] = v2.decode("utf-8", "replace")
                    except Exception:
                        pass
                elif f2 == 2 and wt2 == 2:
                    try:
                        out["name"] = v2.decode("utf-8", "replace")
                    except Exception:
                        pass
    return out


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


def read_until(sock, pred, cap, slice_s=0.25):
    """Baca sampai pred(frames) True atau cap detik habis.
    Sama seperti read_frames (termasuk auto-pong), tapi BERHENTI
    SEGERA saat frame target tiba — jendela tetap lama selalu
    dihabiskan penuh meski jawaban sudah tiba di 0,1-0,3 detik
    (terbukti eksperimen skydev-test 2026-10-29: 27,2 -> 1,4 dtk).
    Kembalikan (frames, closed, pongs, hit)."""
    deadline = time.time() + cap
    buf = bytearray()
    frames = []
    closed = False
    pongs = 0
    sock.settimeout(slice_s)
    while time.time() < deadline:
        try:
            chunk = sock.recv(65536)
        except socket.timeout:
            if pred(frames):
                break
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
                return frames, closed, pongs, pred(frames)
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
        if pred(frames):
            break
    return frames, closed, pongs, pred(frames)


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


def resolve_name(argv):
    """Nama dari argumen: 'Nama#Kode' atau 'Nama Kode' (digabung
    '#'). Kalau tidak ada argumen: env SEARCH_NAME, kalau tidak
    ada juga: prompt input()."""
    args = argv[1:]
    if len(args) >= 2:
        return args[0] + "#" + args[1]
    if len(args) == 1:
        return args[0]
    env = os.getenv("SEARCH_NAME", "")
    if env:
        return env
    return input("Nama pemain (format Nama#Kode, mis. Admin#000): ").strip()


async def lookup_once(session, bot, name):
    """Satu percobaan penuh (login s.d. search). Kembalikan dict
    hasil decode atau None. Pemanggil yang mengulang kalau perlu
    (standalone main: 3x, poller: 2x per order)."""
    user_jwt, alloc = await login_and_allocate(
        bot["email"], bot["password"], bot["device_id"], session)
    if not alloc:
        print("[-] Allocation gagal.")
        return None
    host, port = find_host_port(alloc)
    gsid = alloc.get("gameServerId", "")
    suffix = gsid.rsplit("::", 1)[1] if "::" in gsid else "?"
    print(f"[*] Server: {host}:{port} (room suffix {suffix})")
    if not host or not port:
        print("[-] Host/port tidak ketemu di allocation.")
        return None
    s, pid = tcp_connect(host, port)
    if s is None:
        return None
    try:
        s.sendall(build_login_frame(user_jwt))
        print("[+] Event login terkirim.")
    except Exception as e:
        print(f"[-] Gagal kirim event login: {e}")
        s.close()
        return None
    # SKIP-JOIN (2026-10-29, terbukti eksperimen skydev-test): search
    # LANGSUNG sesudah login — server membalas SearchFriendRequest
    # tanpa keanggotaan room. Join hanya fallback bila search langsung
    # kosong (pengaman, bukan jalur utama lagi).

    def do_search():
        """Kirim search, tunggu hasil (cap 12 dtk, early-exit).
        Kembalikan (hit, closed)."""
        frame = build_room_event_varint("friends", build_search_protobuf(name))
        try:
            s.sendall(frame)
            s.sendall(frame)
        except Exception:
            return None, True

        def _pred(fr):
            for p in fr:
                dec = decode_search_result(p)
                if dec is not None and dec["success"] and dec["uuid"]:
                    return True
            return False

        resp, closed, _, _ = read_until(s, _pred, 12)
        for p in resp:
            dec = decode_search_result(p)
            if dec is not None:
                print(f"    decode: success={dec['success']} uuid={dec['uuid']} "
                      f"nama={dec['name']!r}")
                if dec["success"] and dec["uuid"]:
                    return dec, closed
        return None, closed

    print(f"[*] Cari: {name} ...")
    hit, closed = do_search()
    if hit is None and not closed:
        # Fallback 1 (v2: server lambat cukup ditunggu): tunggu 6 dtk
        # tanpa aksi — hasil pertama bisa tiba terlambat — lalu search ulang.
        print("[*] Search langsung kosong — fallback: tunggu 6 dtk lalu search ulang.")
        resp, closed, _ = read_frames(s, 6)
        for p in resp:
            dec = decode_search_result(p)
            if dec is not None and dec["success"] and dec["uuid"]:
                hit = dec
                break
        if hit is None and not closed:
            hit, closed = do_search()
    if hit is None and not closed:
        # Fallback 2: join dulu lalu search ulang (sesi yang sama).
        print("[*] Search masih kosong — fallback: join lalu search lagi.")
        resp, closed, pongs, _ = read_until(
            s, lambda fr: scan_login_window(fr)[1] is not None, 5)
        tbugv, room = scan_login_window(resp)
        print(f"[*] Login: tbugv={'YA' if tbugv else 'tidak'}, "
              f"auto-join={'room ' + str(room) if room is not None else 'tidak'}")
        if not closed:
            # Hasil search pertama bisa tiba terlambat selama jendela join.
            for p in resp:
                dec = decode_search_result(p)
                if dec is not None and dec["success"] and dec["uuid"]:
                    hit = dec
                    break
            if hit is None:
                if room is None:
                    room = tcp_join_only(s, gsid)
                if room is not None:
                    print(f"[+] Bergabung room {room}.")
                    hit, closed = do_search()
                else:
                    print("[!] Join gagal di instance ini.")
    if hit is None and not closed:
        # Fallback 3: friends-kosong dulu (ambil daftar), lalu search lagi.
        print("[*] Search masih kosong — fallback: friends-kosong lalu search lagi.")
        try:
            s.sendall(build_room_event_varint("friends", b""))
            read_frames(s, 6)
            hit, closed = do_search()
        except Exception:
            pass
    s.close()
    return hit


def probe_bot():
    """Akun bot lookup. Env Secrets menang; fallback hardcode atas
    perintah pemilik (repo private) — hapus nilai hardcode saat
    produksi dan pindah ke Secrets. TIDAK dicetak ke log."""
    return {
        "email": os.getenv("PROBE_EMAIL", "") or "pkxdshop.id@gmail.com",
        "password": os.getenv("PROBE_PASSWORD", "") or "Kontol@161",
        "device_id": os.getenv("PROBE_DEVICE_ID", "") or "9f31c2d4-probe-device",
    }


async def main():
    name = resolve_name(sys.argv)
    if not name:
        print("[!] Nama pemain wajib diisi.")
        sys.exit(1)
    bot = probe_bot()
    print(f"[*] Cari UUID untuk: {name}")
    t0 = time.time()
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        for attempt in range(1, 4):
            print(f"[*] --- Percobaan ke-{attempt} ---")
            hit = await lookup_once(session, bot, name)
            if hit:
                dt = time.time() - t0
                print("=" * 48)
                print(f"HASIL: {name} -> {hit['uuid']}")
                print(f"Nama kanonis: {hit['name']}")
                print(f"Waktu total: {dt:.0f} detik")
                print("=" * 48)
                try:
                    with open("uuid_lookup_log.txt", "a", encoding="utf-8") as f:
                        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | "
                                f"{name} -> {hit['uuid']} | {hit['name']}\n")
                    print("[*] Dicatat ke uuid_lookup_log.txt")
                except Exception as e:
                    print(f"[-] Gagal tulis log file: {e}")
                return
            print(f"[-] Percobaan ke-{attempt} belum berhasil.")
    print(f"[-] GAGAL: UUID untuk {name} tidak ditemukan "
          f"({time.time() - t0:.0f} detik, 3 percobaan).")


if __name__ == "__main__":
    asyncio.run(main())
