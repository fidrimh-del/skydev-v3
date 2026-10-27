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
- app key / app secret game -> isi lewat env FSTR_APP_KEY / FSTR_APP_SECRET
  (salin dari script panel yang sudah kamu punya)
- URL + service key Supabase -> env SUPABASE_URL / SUPABASE_SERVICE_KEY
  (sama seperti script follow; simpan sebagai GitHub Secrets kalau
  jalan via Actions)

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
APP_KEY = os.getenv("FSTR_APP_KEY", "")
APP_SECRET = os.getenv("FSTR_APP_SECRET", "")
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
