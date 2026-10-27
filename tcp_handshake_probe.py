"""Diagnostik TCP ToxicBird — TAHAP 2: cari pesan Connect client.

Temuan tahap 1 (dari server asli):
  - Setiap login+allocation memberi server BARU (IP/port/instans
    berubah tiap run) -> script SELALU login+allocation segar,
    TIDAK PERNAH hardcode host/port.
  - Server bicara duluan saat TCP connect, 6 byte:
        00 00 00 02 04 03
    Hipotesis framing (BELUM terbukti): [int32 big-endian panjang=2]
    + [payload 2 byte: 04 03]. Kalau balasan server nanti juga
    diawali int32 panjang yang cocok, hipotesis ini terkonfirmasi.

Yang dilakukan script ini:
  1. Login + allocation segar (bot stok, sama seperti tahap 1).
  2. Untuk tiap kandidat type byte 0..19: buka koneksi TCP BARU,
     baca sapaan server, kirim frame [int32 BE len=1][type],
     baca balasan (timeout 2 detik), catat: balasan / timeout /
     koneksi ditutup server.
  3. Cetak tabel ringkasan. Type yang menghasilkan balasan
     bermakna (mis. Connected berisi playerId+playerKey, atau
     challenge auth) adalah calon pesan Connect — dari situlah
     v3 (handshake lengkap + SearchFriend) dibangun.

Ini BUKAN brute-force kasar: 20 koneksi ringan ke server game
dengan akun bot sendiri + allocation valid, masing-masing 1 byte.
Akun yang dipakai adalah bot stok (throwaway), bukan akun utama.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY (Secrets).
App key/secret: env FSTR_* kalau ada, fallback hardcode sementara
untuk uji coba (repo private; hapus setelah berhasil).
"""

import json
import os
import sys
import random
import asyncio
import hmac
import hashlib
import base64
import socket
import struct
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


def compute_signature(method, path, body_bytes):
    secret = APP_SECRET.encode("utf-8")
    h = hmac.new(secret, method.encode("utf-8"), hashlib.sha256).digest()
    h = hmac.new(h, path.encode("utf-8"), hashlib.sha256).digest()
    h = hmac.new(h, b"", hashlib.sha256).digest()
    h = hmac.new(h, body_bytes, hashlib.sha256).digest()
    h = hmac.new(h, b"x-fstr-signature", hashlib.sha256).digest()
    return base64.b64encode(h).decode("utf-8")


def find_host_port(body):
    import re
    host = port = None
    ipv4 = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")

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


def parse_frame(chunk):
    """Coba baca chunk sebagai [int32 BE panjang][payload].
    Kembalikan (payload_hex, cocok?) — kalau tidak cocok, payload None
    dan pemanggil mencetak raw apa adanya."""
    if len(chunk) >= 4:
        ln = struct.unpack(">I", chunk[:4])[0]
        if ln == len(chunk) - 4 and 0 < ln <= 4096:
            return chunk[4:].hex(), True
        if ln <= 4096 and len(chunk) - 4 >= ln and ln > 0:
            return chunk[4:4 + ln].hex(), True
    return None, False


def ascii_of(data):
    return "".join(chr(b) if 32 <= b < 127 else "." for b in data)


def probe_type(host, port, t):
    """Satu kandidat: koneksi baru, baca sapaan, kirim [len=1][t]."""
    try:
        s = socket.create_connection((host, port), timeout=5)
    except Exception as e:
        return None, f"connect gagal: {e}"
    s.settimeout(3)
    greeting = None
    try:
        g = s.recv(512)
        greeting = g.hex() if g else "<tutup>"
    except socket.timeout:
        greeting = "<timeout>"
    except Exception as e:
        greeting = f"<err {e}>"
    # Kirim kandidat
    try:
        s.sendall(struct.pack(">I", 1) + bytes([t]))
    except Exception as e:
        s.close()
        return greeting, f"send gagal: {e}"
    s.settimeout(2)
    try:
        r = s.recv(512)
        if r == b"":
            outcome = "DITUTUP server"
        else:
            payload, ok = parse_frame(r)
            if ok:
                outcome = f"balasan frame payload={payload} ascii={ascii_of(bytes.fromhex(payload))}"
            else:
                outcome = f"balasan raw={r.hex()} ascii={ascii_of(r)}"
    except socket.timeout:
        outcome = "TIMEOUT (tidak ada balasan, koneksi masih buka?)"
    except Exception as e:
        outcome = f"err {e}"
    s.close()
    return greeting, outcome


async def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] Env SUPABASE_URL / SUPABASE_SERVICE_KEY wajib diisi.")
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
        print(f"[*] Server target: {host}:{port} "
              f"(id: {alloc.get('gameServerId')})")
        if not host or not port:
            print("[-] Host/port tidak ketemu di allocation.")
            return
    print("[*] Mulai probe type 0..19 (satu koneksi baru per type)...")
    results = []
    for t in range(20):
        greeting, outcome = probe_type(host, port, t)
        results.append((t, greeting, outcome))
        print(f"[t={t:02d} / 0x{t:02x}] sapaan={greeting} -> {outcome}")
    print()
    print("[*] RINGKASAN (yang BUKAN timeout/ditutup saja yang menarik):")
    for t, g, o in results:
        if "TIMEOUT" not in o and "DITUTUP" not in o and "gagal" not in o:
            print(f"    t={t:02d} -> {o}")
    print("[*] Probe selesai.")


if __name__ == "__main__":
    asyncio.run(main())
