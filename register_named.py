"""register_named.py — CREATE BOT + NICKNAME dalam satu alur.

Perbaikan dari register_referral.py + setup_nickname.py:
  1. SATU script: registrasi HTTP -> langsung TCP set nickname.
     Tidak perlu 2 workflow terpisah.
  2. Nickname langsung pakai format TERBUKTI:
     RoomEvent('profile_update') -> respons 'set_name_result'.
     Tidak buang waktu coba 8 kandidat.
  3. Timing KONSISTEN: delay tetap (bukan random), timeout tetap.
  4. Simpan ke tabel `accounts` (standar) + kolom nickname.

Jalankan di GitHub Actions via workflow register_named.yml.
Env: SUPABASE_URL, SUPABASE_SERVICE_KEY, JUMLAH (default 3).
Contoh: python register_named.py 5
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import random
import secrets
import socket
import string
import struct
import sys
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

from curl_cffi.requests import AsyncSession

try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    pass

# ============ KONFIGURASI ============
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
TABEL = "named_bots"

APP_KEY = os.getenv("FSTR_APP_KEY") or "57a3d999-654c-4484-a20c-0f4b5512af49"
APP_SECRET = os.getenv("FSTR_APP_SECRET") or "jWS5aLZacNVFx6PAxpXq2g4o"
ACCOUNT_URL = "https://account.faster.aftvrsys.com"
LOBBY_URL = "https://pkxd-lobby.aftvrsys.com"
VERSION = "1.93.4"
GAME_PROTOCOL = "1.93.0"

# Timing konsisten (detik) — TIDAK random
DELAY_ANTAR_BOT = 2.0
TIMEOUT_TCP_CONNECT = 5
TIMEOUT_TCP_READ = 8

HEADERS_BASE = {
    "User-Agent": "UnityPlayer/2022.3.62f3 (UnityWebRequest/1.0, libcurl/8.10.1-DEV)",
    "Accept": "*/*",
    "Accept-Encoding": "deflate, gzip",
    "Content-Type": "application/json",
    "x-fstr-application-key": APP_KEY,
    "X-Unity-Version": "2022.3.62f3",
    "Connection": "keep-alive",
}


def log(msg):
    print(msg, flush=True)


# ============ HELPER PROTOKOL ============
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
    val, shift, i = 0, 0, off
    while i < len(buf) and shift < 35:
        b = buf[i]
        val |= (b & 0x7F) << shift
        i += 1
        if not (b & 0x80):
            return val, i - off
        shift += 7
    return None, 0


def pb_string(field, s):
    sb = s.encode("utf-8")
    return bytes([(field << 3) | 2]) + varint(len(sb)) + sb


def build_named_frame(frame_type, event_name, data):
    nb = event_name.encode("utf-8")
    payload = bytes([frame_type]) + varint(len(nb)) + nb + data
    return struct.pack(">I", len(payload)) + payload


def build_login_frame(user_token):
    tb = user_token.encode("utf-8")
    vb = VERSION.encode("utf-8")
    pb = b"\x0a" + varint(len(tb)) + tb + b"\x12" + varint(len(vb)) + vb
    return build_named_frame(0x05, "login", pb)


def build_nickname_protobuf(name, device_id):
    # SetNicknameRequest: field1=0 (SetNickname), field2=name, field3=deviceId
    return b"\x08" + varint(0) + pb_string(2, name) + pb_string(3, device_id)


def build_outfit_protobuf():
    """UpdateOutfitRequest: f1=EventId(1=Update), f2=Outfit{f1=SlotId(0), f3=SkinColor{R=255,G=220,B=180}}.
    Mengsimulasikan konfirmasi outfit (seperti tombol random di game) agar server
    mengizinkan SetNicknameRequest. Items kosong = server pakai default."""
    # SkinColor: f1=R, f2=G, f3=B
    skin = b"\x08" + varint(255) + b"\x10" + varint(220) + b"\x18" + varint(180)
    # Outfit: f1=SlotId(0), f3=SkinColor
    outfit = b"\x08" + varint(0) + b"\x1a" + varint(len(skin)) + skin
    # UpdateOutfitRequest: f1=EventId(1), f2=Outfit
    return b"\x08" + varint(1) + b"\x12" + varint(len(outfit)) + outfit


def parse_protobuf_fields(data):
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
            fields.append((field, v))
        elif wt == 2:
            ln, n = read_varint(data, i)
            if ln is None:
                break
            i += n
            fields.append((field, data[i:i + ln]))
            i += ln
        else:
            break
    return fields


def read_until(sock, pred, cap):
    deadline = time.time() + cap
    buf = bytearray()
    frames = []
    sock.settimeout(0.25)
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
            break
        buf += chunk
        while len(buf) >= 4:
            ln = struct.unpack(">I", buf[:4])[0]
            if ln > 262144:
                return frames
            if len(buf) - 4 < ln:
                break
            fr = bytes(buf[4:4 + ln])
            del buf[:4 + ln]
            frames.append(fr)
            if fr == b"\x01":
                try:
                    sock.sendall(struct.pack(">I", 1) + b"\x02")
                except Exception:
                    pass
        if pred(frames):
            break
    return frames


def find_host_port(body):
    host = port = None

    def scan(d):
        nonlocal host, port
        if not isinstance(d, dict):
            return
        for k, v in d.items():
            lk = k.lower()
            if host is None and lk in ("gameserveraddress", "address", "host",
                                       "hostname", "ip") and isinstance(v, str) and v:
                host = v
            if port is None and lk in ("gameserverport", "port", "tcpport") \
                    and isinstance(v, int):
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


# ============ REGISTRASI HTTP ============
def random_device_id():
    return secrets.token_hex(8)


def acak_email():
    """Email unik anti-tabrakan dengan 60rb+ bot existing.
    Username 14-18 char (vs 8-12 dulu) + prefix 'nb' penanda named-bot."""
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
               "icloud.com", "protonmail.com", "yandex.com", "mail.com",
               "zoho.com"]
    username = "nb" + "".join(random.choices(
        string.ascii_lowercase + string.digits, k=random.randint(12, 16)))
    if random.choice([True, False]):
        pos = random.randint(4, len(username) - 4)
        username = username[:pos] + random.choice([".", "_"]) + username[pos:]
    return f"{username}@{random.choice(domains)}"


def acak_password():
    return (random.choice(string.ascii_uppercase)
            + "".join(random.choices(string.ascii_lowercase,
                                      k=random.randint(6, 8)))
            + "".join(random.choices(string.digits, k=random.randint(3, 5))))


def acak_nama():
    """Generate nama valid: >4 huruf, hanya [a-z], tanpa spasi/simbol.
    Server menolak nama <=4 huruf, karakter aneh, atau spasi."""
    nama = "Ref" + "".join(random.choices(string.ascii_lowercase, k=6))
    assert len(nama) > 4 and nama.isalpha(), f"nama tidak valid: {nama}"
    return nama


async def http_register(engine_session):
    """6 langkah registrasi. Return (email, password, device_id, user_jwt)
    atau (None, None, None, None) jika gagal."""
    device_id = random_device_id()
    email = acak_email()
    password = acak_password()

    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        try:
            device_info = OrderedDict([
                ("userAgent", ""), ("platform", "DESKTOP"),
                ("system", "WindowsPlayer"),
                ("systemVersion", "Windows 10  (10.0.19045) 64bit"),
                ("manufacturer", "Unknown"), ("deviceModel", ""),
                ("appVersion", VERSION), ("sdkVersion", ""), ("vendorId", ""),
                ("advertisingId", ""), ("pushToken", ""), ("cloudId", ""),
                ("carrierId", ""), ("timezone", "SE Asia Standard Time"),
                ("language", "EN"), ("country", "Unknown"),
                ("deviceId", device_id),
            ])

            async def req(path, payload, bearer=None):
                body = json.dumps(payload, separators=(",", ":"),
                                  ensure_ascii=False).encode("utf-8")
                h = HEADERS_BASE.copy()
                if bearer:
                    h["Authorization"] = f"Bearer {bearer}"
                h["x-fstr-signature"] = compute_signature("POST", path, body)
                return await session.post(ACCOUNT_URL + path, data=body,
                                          headers=h, timeout=30)

            # 1: anonymous sign-up
            r1 = await req("/v1/identity/anonymous/sign-up",
                           {"deviceInfo": device_info})
            if r1.status_code in (1015, 429, 403) or "html" in r1.text.lower():
                return None, None, None, None
            acc_token = r1.json().get("token")

            # 2: auth TOKEN
            r2 = await req("/v1/auth/sessions", OrderedDict([
                ("identityProvider", "FASTER"),
                ("credentials", {"type": "TOKEN", "token": acc_token}),
                ("deviceInfo", device_info)]))
            if r2.status_code in (1015, 429, 403):
                return None, None, None, None
            auth_token = r2.json().get("authToken")

            # 3: user session -> JWT
            r3 = await req("/v1/users/sessions", OrderedDict([
                ("authToken", auth_token), ("deviceInfo", device_info)]))
            user_jwt = r3.json().get("userToken")
            if not user_jwt:
                return None, None, None, None

            # 4: email sign-up
            await req("/v1/identity/email/sign-up", OrderedDict([
                ("deviceInfo", device_info),
                ("email", email), ("password", password)]))

            # 5: link kredensial
            r5 = await req("/v1/users/credentials/link", OrderedDict([
                ("identityProvider", "FASTER"),
                ("credentials", {"type": "EMAIL", "email": email,
                                 "password": password})]), bearer=user_jwt)
            if r5.status_code not in (200, 201):
                return None, None, None, None

            # 6: alokasi lobby (verifikasi)
            p6 = {"userToken": user_jwt,
                  "gameServerProtocolVersion": GAME_PROTOCOL,
                  "isPrivate": False}
            b6 = json.dumps(p6, separators=(",", ":"),
                            ensure_ascii=False).encode("utf-8")
            r6 = await session.post(LOBBY_URL + "/lobby/allocation", data=b6,
                                    headers={k: v for k, v in
                                             HEADERS_BASE.items()
                                             if k != "x-fstr-application-key"},
                                    timeout=30)
            if r6.status_code not in (200, 201):
                return None, None, None, None
            alloc = r6.json()
            return email, password, device_id, (user_jwt, alloc)
        except Exception as e:
            log(f"    [!] HTTP error: {str(e)[:60]}")
            return None, None, None, None


# ============ NICKNAME VIA TCP ============
def build_get_default_nickname_protobuf(device_id):
    """Minta nickname auto-generate: ProfileEventId.GetDefaultNickname=12."""
    return b"\x08" + varint(12) + pb_string(3, device_id)


def tcp_get_nickname(user_jwt, alloc, device_id):
    """Ambil nickname auto-generate dari server.
    Coba: (1) dengar default_nickname_result otomatis,
          (2) request GetDefaultNickname (eventId=12).
    Return nickname atau None."""
    host, port = find_host_port(alloc)
    if not host or not port:
        log(f"    [!] TCP: host/port tidak ketemu")
        return None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT_TCP_CONNECT)
        s.connect((host, port))
        s.sendall(struct.pack(">I", 1) + b"\x03")
    except Exception as e:
        log(f"    [!] TCP connect gagal: {e}")
        return None

    # Tunggu Connected
    resp = read_until(
        s, lambda fr: any(len(p) >= 5 and p[0] == 0x03 for p in fr), 6)
    if not any(len(p) >= 5 and p[0] == 0x03 for p in resp):
        log(f"    [!] TCP: tidak ada Connected")
        s.close()
        return None

    # Login event
    try:
        s.sendall(build_login_frame(user_jwt))
    except Exception as e:
        log(f"    [!] TCP login gagal: {e}")
        s.close()
        return None

    def cari_nickname(frames):
        """Cari default_nickname_result atau set_name_result di frames."""
        for p in frames:
            if len(p) < 3 or p[0] not in (0x05, 0x08):
                continue
            ln, n = read_varint(p, 1)
            if ln is None:
                continue
            try:
                ev_s = p[1 + n:1 + n + ln].decode("utf-8", "replace")
            except Exception:
                continue
            if ev_s not in ("default_nickname_result", "set_name_result"):
                continue
            data = p[1 + n + ln:]
            if not data:
                continue
            for f, v in parse_protobuf_fields(data):
                if f == 1 and isinstance(v, bytes):
                    try:
                        nama = v.decode("utf-8", "replace")
                        if nama:
                            return nama
                    except Exception:
                        pass
        return None

    # (1) Dengar 10 detik — server mungkin kirim default_nickname_result otomatis
    log(f"    [*] Menunggu default_nickname_result otomatis...")
    resp = read_until(s, lambda fr: False, 10)
    nama = cari_nickname(resp)
    if nama:
        log(f"    [+] Dapat nickname otomatis: {nama}")
        # KONFIRMASI: kirim SET agar tersimpan di server
        nama_fix = _kirim_set(s, nama, device_id, cari_nickname)
        s.close()
        return nama_fix or nama

    # (2) Minta explicit via GetDefaultNickname (eventId=12)
    req_pb = build_get_default_nickname_protobuf(device_id)
    for ftype, fname in [(0x08, "RoomEvent"), (0x05, "Event")]:
        try:
            s.sendall(build_named_frame(ftype, "profile_update", req_pb))
            log(f"    [*] Minta default nickname via {fname}...")
        except Exception as e:
            log(f"    [!] Kirim gagal: {e}")
            s.close()
            return None
        resp = read_until(s, lambda fr: False, 8)
        nama = cari_nickname(resp)
        if nama:
            log(f"    [+] Dapat nickname via {fname}: {nama}")
            # KONFIRMASI: kirim SET agar tersimpan di server
            nama_fix = _kirim_set(s, nama, device_id, cari_nickname)
            s.close()
            return nama_fix or nama

    s.close()
    log(f"    [-] Server tidak memberi nickname")
    return None


def _kirim_set(s, nama, device_id, cari_nickname):
    """Kirim SetNicknameRequest dan tunggu konfirmasi. Return nama terkonfirmasi atau None."""
    import time as _t
    # OUTFIT DULU (sekali saja)
    try:
        outfit_pb = build_outfit_protobuf()
        s.sendall(build_named_frame(0x08, "outfit", outfit_pb))
        log(f"    [*] Kirim outfit (konfirmasi)...")
        _t.sleep(2)
    except Exception as e:
        log(f"    [!] Kirim outfit gagal: {e} (lanjut SET)")
    try:
        set_pb = build_nickname_protobuf(nama, device_id)
        s.sendall(build_named_frame(0x08, "profile_update", set_pb))
        log(f"    [*] Kirim SET nickname: {nama}...")
    except Exception as e:
        log(f"    [!] Kirim SET gagal: {e}")
        return None
    # Tunggu konfirmasi (20 dtk, early-return jika tiba)
    resp = read_until(s, lambda fr: cari_nickname(fr) is not None, 20)
    nama_fix = cari_nickname(resp)
    if nama_fix and "#" in nama_fix:
        log(f"    [+] Nickname TERKONFIRMASI: {nama_fix}")
        return nama_fix
    log(f"    [!] SET gagal/tidak terkonfirmasi (nama: {nama_fix})")
    return None


async def email_login_jwt(email, password, device_id):
    """Login fresh via EMAIL -> user JWT (seperti setup_nickname.py)."""
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        device_info = OrderedDict([
            ("userAgent", ""), ("platform", "DESKTOP"),
            ("system", "WindowsPlayer"),
            ("systemVersion", "Windows 10  (10.0.19045) 64bit"),
            ("manufacturer", "Unknown"), ("deviceModel", ""),
            ("appVersion", VERSION), ("sdkVersion", ""), ("vendorId", ""),
            ("advertisingId", ""), ("pushToken", ""), ("cloudId", ""),
            ("carrierId", ""), ("timezone", "SE Asia Standard Time"),
            ("language", "EN"), ("country", "Unknown"),
            ("deviceId", device_id),
        ])
        p1 = OrderedDict([
            ("identityProvider", "FASTER"),
            ("credentials", {"type": "EMAIL", "email": email,
                             "password": password}),
            ("deviceInfo", device_info)])
        b1 = json.dumps(p1, separators=(",", ":"),
                        ensure_ascii=False).encode("utf-8")
        h1 = HEADERS_BASE.copy()
        h1["x-fstr-signature"] = compute_signature(
            "POST", "/v1/auth/sessions", b1)
        r1 = await session.post(ACCOUNT_URL + "/v1/auth/sessions", data=b1,
                                headers=h1, timeout=20)
        if r1.status_code != 200:
            return None, None
        auth_token = r1.json().get("authToken")

        p2 = OrderedDict([("authToken", auth_token),
                          ("deviceInfo", device_info)])
        b2 = json.dumps(p2, separators=(",", ":"),
                        ensure_ascii=False).encode("utf-8")
        h2 = HEADERS_BASE.copy()
        h2["x-fstr-signature"] = compute_signature(
            "POST", "/v1/users/sessions", b2)
        r2 = await session.post(ACCOUNT_URL + "/v1/users/sessions", data=b2,
                                headers=h2, timeout=20)
        user_jwt = r2.json().get("userToken")
        if not user_jwt:
            return None, None

        p3 = {"userToken": user_jwt,
              "gameServerProtocolVersion": GAME_PROTOCOL,
              "isPrivate": False}
        b3 = json.dumps(p3, separators=(",", ":"),
                        ensure_ascii=False).encode("utf-8")
        r3 = await session.post(
            LOBBY_URL + "/lobby/allocation", data=b3,
            headers={k: v for k, v in HEADERS_BASE.items()
                     if k != "x-fstr-application-key"}, timeout=20)
        try:
            alloc = r3.json()
        except Exception:
            alloc = {}
        return user_jwt, alloc


# ============ SIMPAN KE SUPABASE ============
async def save_bot(session, email, password, device_id, nickname, status):
    endpoint = f"{SUPABASE_URL}/rest/v1/{TABEL}"
    headers = {"apikey": SUPABASE_KEY,
               "Authorization": f"Bearer {SUPABASE_KEY}",
               "Content-Type": "application/json",
               "Prefer": "return=minimal"}
    payload = {"email": email, "password": password,
               "device_id": device_id, "status": status,
               "nickname": nickname}
    try:
        r = await session.post(endpoint, json=payload, headers=headers,
                               timeout=15)
        return r.status_code == 201
    except Exception:
        return False


# ============ MAIN ============
_exec_pool = None

async def proses_satu_bot(idx, total):
    global _exec_pool
    exec_pool = _exec_pool
    log(f"[{idx}/{total}] Registrasi HTTP...")
    email, password, device_id, jwt_alloc = await http_register(None)
    if not email:
        log(f"    -> GAGAL registrasi")
        return False

    user_jwt, alloc = jwt_alloc
    log(f"    -> {email[:4]}*** terdaftar, ambil nickname auto-generate...")

    # Jeda 30 detik agar akun fresh siap di server (10s terlalu singkat, banyak gagal SET)
    await asyncio.sleep(30)

    # Login ULANG via EMAIL untuk dapat JWT yang valid untuk TCP
    log(f"    -> login ulang via EMAIL...")
    user_jwt2, alloc2 = await email_login_jwt(email, password, device_id)
    if not user_jwt2:
        log(f"    [!] login ulang gagal, pakai JWT registrasi")
    else:
        user_jwt, alloc = user_jwt2, alloc2

    # Ambil nickname dengan RETRY 3x (fresh TCP socket tiap attempt).
    # Alasan: retry dalam _kirim_set pakai socket yang sama (mungkin stale).
    loop = asyncio.get_event_loop()
    nickname_ok = None
    for _r in range(3):
        nickname_ok = await loop.run_in_executor(
            exec_pool, tcp_get_nickname, user_jwt, alloc, device_id)
        if nickname_ok and "#" in nickname_ok:
            break
        log(f"    -> attempt {_r+1} belum dapat # (dapat: {nickname_ok}), retry...")
        if _r < 2:
            await asyncio.sleep(15)

    # KETAT: hanya nama dengan # yang berarti server benar-benar simpan
    # (set_name_result). Tanpa # = saran saja, JANGAN dianggap sukses.
    status = "SUCCESS"
    if nickname_ok and "#" in nickname_ok:
        log(f"    -> nickname OK (server): {nickname_ok}")
    else:
        log(f"    -> nickname GAGAL/tak terkonfirmasi server (dapat: {nickname_ok})")
        nickname_ok = None
        status = "NICK_FAIL"

    async with AsyncSession(impersonate="chrome110",
                            verify=False) as session:
        ok = await save_bot(session, email, password, device_id,
                            nickname_ok, status)
    log(f"    -> {'tersimpan' if ok else 'GAGAL simpan'} ke DB")
    # Return nickname (untuk hitung yang ada #)
    return nickname_ok if (ok and nickname_ok and "#" in nickname_ok) else None


async def main():
    jumlah = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    if len(sys.argv) > 2:
        # JUMLAH dari env workflow
        pass
    jumlah = int(os.getenv("JUMLAH", str(jumlah)))
    workers = int(os.getenv("WORKERS", "10"))

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] SUPABASE_URL / SUPABASE_SERVICE_KEY belum diset.")
        sys.exit(1)

    log(f"[*] REGISTER+NICKNAME: {jumlah} bot -> tabel `{TABEL}` (paralel {workers} workers)")
    t0 = time.time()

    global _exec_pool
    sem = asyncio.Semaphore(workers)
    # Executor eksplisit: default cuma 8 thread di runner 4-CPU
    _exec_pool = ThreadPoolExecutor(max_workers=workers)
    try:
        async def _run(i):
            async with sem:
                return await proses_satu_bot(i, jumlah)
        hasil = await asyncio.gather(*[_run(i) for i in range(1, jumlah + 1)])
    finally:
        _exec_pool.shutdown(wait=True)
    # hasil berisi nickname (str dengan #) atau None
    dengan_hash = sum(1 for x in hasil if x and "#" in str(x))

    dt = time.time() - t0
    log(f"\n[HASIL] {dengan_hash}/{jumlah} bot dapat nama (#) "
        f"({dt:.1f}s total, {dt/max(jumlah,1):.1f}s/bot)")


if __name__ == "__main__":
    asyncio.run(main())
