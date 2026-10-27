"""Diagnostik TCP ToxicBird — TAHAP 1 (belum lookup UUID).

Latar: probe HTTP sudah 404 di semua kandidat endpoint -> tidak ada
endpoint HTTP untuk cari-nama-jadi-UUID. Jalur yang tersisa adalah
TCP ToxicBird + protobuf SearchFriend (mekanisme yang dipakai game).

MASALAH: dump game hanya berisi signature method, BUKAN isi method.
Jadi format framing byte-per-byte (FeatherSerialization) TIDAK bisa
diturunkan dari dump. Script ini TIDAK menebak framing — ia
mengumpulkan dua bukti dari server asli yang menentukan langkah
berikutnya:

  FASE 1: login (langkah 1-2, sudah terbukti di probe) + lobby
          allocation (langkah 3) -> TAMPILKAN body allocation
          (kunci-kuncinya + pratinjau teredaksi). Dari sinilah
          kita tahu: host/port TCP game server + dari mana
          playerId/playerKey didapat.
  FASE 2: kalau host/port ketemu di body allocation, buka koneksi
          TCP dan DENGARKAN apa yang dikirim server duluan
          (hex dump, maks 512 byte). Kalau server bicara duluan,
          byte itu mengungkap framing-nya. Kalau server diam
          (timeout), artinya server menunggu client kirim dulu —
          itu juga informasi untuk v2.

Yang TIDAK dicetak script ini: JWT utuh, password, token/key utuh.
Untuk kunci yang berbau token/key, hanya panjang + 8 karakter
pertama yang tampil. Host/port/ID dicetak penuh (bukan rahasia).

Env (GitHub Secrets):
  SUPABASE_URL, SUPABASE_SERVICE_KEY  (ambil 1 bot stok, sama
  seperti probe — read-only, tidak menghabiskan stok)
App key/secret game: env FSTR_APP_KEY / FSTR_APP_SECRET kalau ada,
fallback hardcode sementara untuk uji coba (repo private, sama
seperti probe; hapus setelah berhasil, pindah ke Secrets).

Cara jalan (Actions): lihat tcp-probe.yml (tanpa input, cukup
Run workflow, lalu baca log).
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

TOKEN_KEYS = ("token", "key", "secret", "jwt", "auth", "password", "session")


def compute_signature(method, path, body_bytes):
    secret = APP_SECRET.encode("utf-8")
    h = hmac.new(secret, method.encode("utf-8"), hashlib.sha256).digest()
    h = hmac.new(h, path.encode("utf-8"), hashlib.sha256).digest()
    h = hmac.new(h, b"", hashlib.sha256).digest()
    h = hmac.new(h, body_bytes, hashlib.sha256).digest()
    h = hmac.new(h, b"x-fstr-signature", hashlib.sha256).digest()
    return base64.b64encode(h).decode("utf-8")


def redact(key, value):
    """Pratinjau aman untuk log: token/key hanya panjang + 8 char awal."""
    if isinstance(value, str):
        lk = key.lower()
        if any(t in lk for t in TOKEN_KEYS):
            return f"<{len(value)} chars, awal: {value[:8]}...>"
        if len(value) > 120:
            return value[:120] + "..."
        return value
    if isinstance(value, (dict, list)):
        return f"<{type(value).__name__}, {len(value)} item>"
    return value


def show_body(body, indent=0):
    pad = "  " * indent
    if isinstance(body, dict):
        for k, v in body.items():
            if isinstance(v, (dict, list)):
                print(f"{pad}- {k}: {redact(k, v)}")
                if isinstance(v, dict) and indent < 1:
                    show_body(v, indent + 1)
                elif isinstance(v, list) and v and isinstance(v[0], dict) and indent < 1:
                    show_body(v[0], indent + 1)
            else:
                print(f"{pad}- {k} = {redact(k, v)}")
    else:
        print(f"{pad}{redact('body', body)}")


def find_host_port(body):
    """Cari kandidat host/port di body allocation (top-level + 1 level)."""
    host = port = None

    def scan(d):
        nonlocal host, port
        if not isinstance(d, dict):
            return
        for k, v in d.items():
            lk = k.lower()
            if host is None and isinstance(v, str) and any(
                t in lk for t in ("host", "address", "ip", "server")
            ) and "." in v:
                host = v
            if port is None and isinstance(v, int) and "port" in lk:
                port = v
            if isinstance(v, dict):
                scan(v)

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
    """Langkah 1-3 script panel, tapi body allocation DITAMPILKAN."""
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
        body = r3.text[:500]
    return user_jwt, body


def tcp_listen(host, port, timeout=8):
    """Buka TCP, diam, dengarkan apa yang dikirim server duluan."""
    print(f"[*] TCP connect ke {host}:{port} ...")
    try:
        s = socket.create_connection((host, port), timeout=5)
    except Exception as e:
        print(f"[-] TCP connect gagal: {e}")
        return
    s.settimeout(timeout)
    try:
        data = s.recv(512)
        if data:
            print(f"[+] Server bicara duluan ({len(data)} byte):")
            print("    HEX:", data.hex())
            printable = "".join(chr(b) if 32 <= b < 127 else "." for b in data)
            print("    ASCII:", printable)
        else:
            print("[*] Server menutup koneksi tanpa mengirim apa-apa.")
    except socket.timeout:
        print("[*] Timeout: server DIAM, menunggu client kirim dulu.")
        print("    (v2 berarti harus mengirim frame Connect dulu.)")
    except Exception as e:
        print(f"[-] Gagal baca dari server: {e}")
    finally:
        s.close()


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
        if alloc is None:
            return
        print("[*] Body allocation:")
        show_body(alloc)
        host, port = find_host_port(alloc)
        if host and port:
            print(f"[+] Kandidat host/port ketemu: {host}:{port}")
            tcp_listen(host, port)
        else:
            print("[!] Host/port TIDAK ketemu di body allocation.")
            print("    Laporkan body di atas — host TCP kemungkinan dari")
            print("    sumber lain (config game / endpoint lain).")
    print("[*] Diagnostik selesai.")


if __name__ == "__main__":
    asyncio.run(main())
