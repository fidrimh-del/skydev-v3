"""Probe: apakah ada endpoint HTTP untuk cari pemain berdasarkan nama?

Tujuan: sebelum membangun klien TCP (ToxicBird) yang lebih berat,
cek dulu apakah API HTTP game (keluarga gsn-*) punya endpoint
pencarian user berdasarkan nama. Kalau ada yang mengembalikan 200
+ data user (berisi UUID), maka script get-UUID bisa murni HTTP
(login -> search -> UUID -> Supabase), tanpa TCP protobuf.

Akun yang dipakai untuk login + search: SATU BOT dari stok Supabase
(keputusan Bartes — tidak perlu akun khusus). Caranya: panggil RPC
yang sama seperti script follow (get_fresh_bots) dengan target dummy
+ limit 1, lalu pakai bot pertama untuk login. Ini TIDAK menghabiskan
stok: lookup hanya login + search (read-only), tidak follow siapa pun
dan tidak menulis histori — histori follow tetap dicatat terpisah
setelah follow berhasil, seperti di script follow.

Yang TIDAK ada di file ini (sengaja, demi keamanan):
- URL + service key Supabase -> env SUPABASE_URL / SUPABASE_SERVICE_KEY
  (sama seperti script follow; simpan sebagai GitHub Secrets kalau
  jalan via Actions)
- app key / app secret game -> env FSTR_APP_KEY / FSTR_APP_SECRET
  kalau sudah dibuat sebagai Secrets; SELAMA UJI COBA, nilainya
  di-hardcode di bawah sebagai fallback (repo private, sama seperti
  script panel yang sudah menaruh nilai ini di kode). SETELAH probe
  berhasil, hapus nilai hardcode ini dan pindahkan ke Secrets —
  env selalu menang atas nilai hardcode.

Cara jalan (lokal):
  pip install curl_cffi
  SUPABASE_URL=... SUPABASE_SERVICE_KEY=... \
  FSTR_APP_KEY=... FSTR_APP_SECRET=... SEARCH_NAME=tes \
  python probe_search.py

Cara jalan (GitHub Actions): taruh file ini di repo panel yang sama,
buat workflow yang menjalankan script ini dengan env di atas dari
Secrets, lalu baca log-nya.

Output yang diharapkan dari kamu: endpoint mana (kalau ada) yang
status 200 + body-nya berisi user/UUID. Kalau semua 404 / tidak ada
data user -> rute HTTP gugur, lanjut ke rute TCP (ToxicBird).
"""

import json
import os
import sys
import asyncio
import hmac
import hashlib
import base64
import random
from collections import OrderedDict
from curl_cffi.requests import AsyncSession
try:
    # Opsional saja (supaya tidak ada warning verify=False).
    # curl_cffi tidak bergantung ke urllib3, jadi import ini
    # tidak boleh menggagalkan script kalau paketnya tidak ada.
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    pass

ACCOUNT_URL = "https://account.faster.aftvrsys.com"
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
# SEMENTARA UNTUK UJI COBA (repo private; nilai sama seperti di script
# panel). Env selalu menang kalau sudah diisi sebagai Secrets — setelah
# probe berhasil, HAPUS nilai hardcode di bawah dan pakai Secrets saja.
APP_KEY = os.getenv("FSTR_APP_KEY") or "57a3d999-654c-4484-a20c-0f4b5512af49"
APP_SECRET = os.getenv("FSTR_APP_SECRET") or "jWS5aLZacNVFx6PAxpXq2g4o"
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


async def login_and_get_jwt(email, password, device_id, session):
    """Langkah 1-2 dari script panel: auth/sessions -> users/sessions.
    Mengembalikan user JWT, atau None kalau gagal. Sengaja TIDAK
    memanggil lobby allocation (itu untuk main di game server;
    API sosial cukup dengan JWT ini, sama seperti follow)."""
    device_info = OrderedDict([
        ("userAgent", ""), ("platform", "DESKTOP"), ("system", "WindowsPlayer"),
        ("systemVersion", "Windows 10  (10.0.19045) 64bit"),
        ("manufacturer", "Unknown"), ("deviceModel", ""), ("appVersion", VERSION),
        ("sdkVersion", ""), ("vendorId", ""), ("advertisingId", ""),
        ("pushToken", ""), ("cloudId", ""), ("carrierId", ""),
        ("timezone", "SE Asia Standard Time"), ("language", "EN"),
        ("country", "Unknown"), ("deviceId", device_id),
    ])

    p1 = OrderedDict([
        ("identityProvider", "FASTER"),
        ("credentials", {"type": "EMAIL", "email": email, "password": password}),
        ("deviceInfo", device_info),
    ])
    b_p1 = json.dumps(p1, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    h1 = HEADERS_BASE.copy()
    h1["x-fstr-application-key"] = APP_KEY
    h1["x-fstr-signature"] = compute_signature("POST", "/v1/auth/sessions", b_p1)
    r1 = await session.post(ACCOUNT_URL + "/v1/auth/sessions", data=b_p1, headers=h1, timeout=20)
    if r1.status_code != 200:
        print(f"[-] auth/sessions gagal: {r1.status_code} {r1.text[:200]}")
        return None
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
        print(f"[-] users/sessions gagal: {r2.status_code} {r2.text[:200]}")
        return None
    return user_jwt


# Kandidat endpoint pencarian user (tebakan terarah dari keluarga API gsn-*).
# Kalau ada yang 200 + berisi data user, itulah endpoint-nya.
CANDIDATES = [
    "https://gsn-follow.aftvrsys.com/api/v1/users/search?query={name}",
    "https://gsn-follow.aftvrsys.com/api/v1/users/search?name={name}",
    "https://gsn-follow.aftvrsys.com/api/v1/search/users?query={name}",
    "https://gsn-follow.aftvrsys.com/api/v1/follow/search?query={name}",
    "https://gsn-posts.aftvrsys.com/api/v1/users/search?query={name}",
    "https://gsn-timelines.aftvrsys.com/api/v1/users/search?query={name}",
    "https://gsn-images.aftvrsys.com/api/v1/users/search?query={name}",
]


async def fetch_one_bot(session_http):
    """Ambil 1 bot dari stok via RPC yang sama seperti script follow.
    Target dummy dipakai karena lookup belum punya UUID target
    (UUID-lah yang sedang dicari); RPC ini hanya filter read-only,
    jadi bot yang diambil tetap segar untuk follow."""
    endpoint = f"{SUPABASE_URL}/rest/v1/rpc/get_fresh_bots"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    payload = {"p_target_uuid": "00000000-0000-0000-0000-000000000000", "p_limit": 1}
    try:
        r = await session_http.post(endpoint, json=payload, headers=headers, timeout=20)
        if r.status_code == 200 and r.json():
            return r.json()[0]
        return None
    except Exception as e:
        print(f"[-] Gagal mengambil bot dari database: {e}")
        return None


async def main():
    name = os.getenv("SEARCH_NAME", "")
    if not SUPABASE_URL or not SUPABASE_KEY or not name:
        print("[!] Env belum lengkap: SUPABASE_URL, SUPABASE_SERVICE_KEY, "
              "SEARCH_NAME wajib diisi.")
        sys.exit(1)
    if not APP_KEY or not APP_SECRET:
        print("[!] Env FSTR_APP_KEY / FSTR_APP_SECRET wajib diisi "
              "(salin dari script panel).")
        sys.exit(1)

    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        bot = await fetch_one_bot(session)
        if not bot:
            print("[-] Tidak ada bot tersedia di stok untuk login lookup.")
            return
        print(f"[*] Pakai bot stok untuk login lookup: {bot['email']}")
        jwt = await login_and_get_jwt(bot["email"], bot["password"], bot["device_id"], session)
        if not jwt:
            print("[-] Login gagal, probe dihentikan.")
            return
        print("[+] Login OK, JWT didapat. Mulai probe...")

        headers = HEADERS_BASE.copy()
        headers["Authorization"] = f"Bearer {jwt}"
        for url_tpl in CANDIDATES:
            url = url_tpl.format(name=name)
            try:
                r = await session.get(url, headers=headers, timeout=15)
                body = r.text[:300].replace("\n", " ")
                print(f"[{r.status_code}] {url} -> {body}")
            except Exception as e:
                print(f"[ERR] {url} -> {e}")

    print("\n[*] Probe selesai. Laporkan baris status 200 (kalau ada) + "
          "isi body-nya untuk langkah berikutnya.")


if __name__ == "__main__":
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
