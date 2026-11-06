import json
import os
import sys
import random
import asyncio
import hmac
import hashlib
import base64
from collections import OrderedDict
from curl_cffi.requests import AsyncSession
import urllib3
import lookup_uuid as L

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# KONFIGURASI SUPABASE
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

class PKXDSkydevFollowEngine:
    def __init__(self):
        self.follow_url = "https://gsn-follow.aftvrsys.com"
        self.account_url = "https://account.faster.aftvrsys.com"
        self.lobby_url = "https://pkxd-lobby.aftvrsys.com"
        self.app_key = "57a3d999-654c-4484-a20c-0f4b5512af49"
        self.app_secret = "jWS5aLZacNVFx6PAxpXq2g4o" 
        self.game_protocol = "1.94.0"
        self.version = "1.94.0"
        self.impersonate_targets = ["chrome110", "chrome116", "safari15_5"]
        
        self.headers_base = {
            "User-Agent": "UnityPlayer/2022.3.62f3 (UnityWebRequest/1.0, libcurl/8.10.1-DEV)",
            "Accept": "*/*",
            "Accept-Encoding": "deflate, gzip",
            "Content-Type": "application/json",
            "X-Protocol-Version": self.game_protocol,
            "X-Unity-Version": "2022.3.62f3",
            "Connection": "keep-alive"
        }

    def update_versions(self, version, protocol):
        self.version = version
        self.game_protocol = protocol
        self.headers_base["X-Protocol-Version"] = protocol

    def compute_signature(self, method, path, body_bytes):
        secret = self.app_secret.encode('utf-8')
        h = hmac.new(secret, method.encode('utf-8'), hashlib.sha256).digest()
        h = hmac.new(h, path.encode('utf-8'), hashlib.sha256).digest()
        h = hmac.new(h, b"", hashlib.sha256).digest() 
        h = hmac.new(h, body_bytes, hashlib.sha256).digest()
        h = hmac.new(h, b"x-fstr-signature", hashlib.sha256).digest()
        return base64.b64encode(h).decode('utf-8')

    async def login_and_get_token(self, email, password, device_id, session):
        try:
            device_info = OrderedDict([
                ("userAgent", ""), ("platform", "DESKTOP"), ("system", "WindowsPlayer"),
                ("systemVersion", "Windows 10  (10.0.19045) 64bit"),
                ("manufacturer", "Unknown"), ("deviceModel", ""), ("appVersion", self.version),
                ("sdkVersion", ""), ("vendorId", ""), ("advertisingId", ""),
                ("pushToken", ""), ("cloudId", ""), ("carrierId", ""),
                ("timezone", "SE Asia Standard Time"), ("language", "EN"),
                ("country", "Unknown"), ("deviceId", device_id)
            ])

            # 1. Auth Sessions
            p1 = OrderedDict([("identityProvider", "FASTER"), ("credentials", {"type": "EMAIL", "email": email, "password": password}), ("deviceInfo", device_info)])
            b_p1 = json.dumps(p1, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
            h1 = self.headers_base.copy()
            h1["x-fstr-application-key"] = self.app_key
            h1["x-fstr-signature"] = self.compute_signature("POST", "/v1/auth/sessions", b_p1)
            
            r1 = await session.post(self.account_url + "/v1/auth/sessions", data=b_p1, headers=h1, timeout=20)
            if r1.status_code != 200: return None
            
            auth_token = r1.json().get("authToken")
            await asyncio.sleep(random.uniform(0.1, 0.3))

            # 2. Users Sessions
            p2 = OrderedDict([("authToken", auth_token), ("deviceInfo", device_info)])
            b_p2 = json.dumps(p2, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
            h2 = self.headers_base.copy()
            h2["x-fstr-application-key"] = self.app_key
            h2["x-fstr-signature"] = self.compute_signature("POST", "/v1/users/sessions", b_p2)
            
            r2 = await session.post(self.account_url + "/v1/users/sessions", data=b_p2, headers=h2, timeout=20)
            user_jwt = r2.json().get("userToken")
            if not user_jwt: return None

            await asyncio.sleep(random.uniform(0.1, 0.3))

            # 3. Lobby Allocation (dengan Retry bila terkena rate limit 429)
            p3 = {"userToken": user_jwt, "gameServerProtocolVersion": self.game_protocol, "isPrivate": False}
            b_p3 = json.dumps(p3, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
            h3 = self.headers_base.copy()
            
            for _ in range(3):
                r3 = await session.post(self.lobby_url + "/lobby/allocation", data=b_p3, headers=h3, timeout=20)
                if r3.status_code in [200, 201]:
                    return user_jwt
                elif r3.status_code == 429:
                    await asyncio.sleep(random.uniform(2.0, 3.5))
                else:
                    await asyncio.sleep(1.0)
            return None
        except:
            return None

    async def execute_follow(self, jwt_token, target_uid, session):
        headers = self.headers_base.copy()
        headers["Authorization"] = f"Bearer {jwt_token}"
        endpoint = f"{self.follow_url}/api/v1/follow/{target_uid}/follow"
        
        for _ in range(3):
            try:
                await asyncio.sleep(random.uniform(0.2, 0.5))
                r = await session.post(endpoint, headers=headers, data=b"", timeout=15)
                if r.status_code in [200, 201, 204]:
                    return True
                elif r.status_code == 429:
                    await asyncio.sleep(random.uniform(2.0, 3.5))
            except:
                await asyncio.sleep(0.5)
        return False

# ==========================================
# FUNGSI SUPABASE CLOUD
# ==========================================
async def fetch_available_bots(session_http, target_uuid, limit, partisi=None, jumlah_partisi=None):
    BOT_TYPE = os.getenv("BOT_TYPE", "accounts")
    rpc_name = "get_fresh_named_bots" if BOT_TYPE == "named_bots" else "get_fresh_bots"
    if partisi is not None:
        rpc_name += "_part"
    endpoint = f"{SUPABASE_URL}/rest/v1/rpc/{rpc_name}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    payload = {"p_target_uuid": target_uuid, "p_limit": limit}
    if partisi is not None:
        payload["p_partisi"] = partisi
        payload["p_jumlah_partisi"] = jumlah_partisi
    
    try:
        r = await session_http.post(endpoint, json=payload, headers=headers, timeout=20)
        if r.status_code == 200:
            return r.json()
        return []
    except Exception as e:
        print(f"[-] Gagal mengambil bot dari database: {e}")
        return []

async def record_history(session_http, target_uuid, new_success_emails):
    if not new_success_emails:
        return
    endpoint = f"{SUPABASE_URL}/rest/v1/follow_history"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates"
    }
    payload = [{"target_uuid": target_uuid, "account_email": email} for email in new_success_emails]
    try:
        await session_http.post(endpoint, json=payload, headers=headers, timeout=30)
    except Exception as e:
        print(f"[-] Gagal menyimpan histori: {e}")

# ==========================================
# WORKER UTAMA
# ==========================================
async def worker(engine, bot, target_uid, semaphore, success_set, lock, total_target):
    async with semaphore:
        async with lock:
            if len(success_set) >= total_target:
                return
            
        await asyncio.sleep(random.uniform(0.1, 0.8))
        
        async with AsyncSession(impersonate=random.choice(engine.impersonate_targets), verify=False) as session:
            token = await engine.login_and_get_token(bot["email"], bot["password"], bot["device_id"], session)
            if token:
                followed = await engine.execute_follow(token, target_uid, session)
                if followed:
                    async with lock:
                        if len(success_set) < total_target:
                            success_set.add(bot["email"])
                            print(f"\r[SKYDEV] Progres Follow: {len(success_set)}/{total_target} (Berhasil)", end="", flush=True)

async def main():
    if len(sys.argv) < 3:
        print("[!] Format Error! Gunakan: python follow_cloud.py <TARGET_UUID> <JUMLAH_ORDER> [PARTISI] [JUMLAH_PARTISI]")
        sys.exit(1)
        
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] EROR: Kredensial Supabase tidak ditemukan di Environment Variables!")
        sys.exit(1)

    target_uuid = sys.argv[1].strip()
    try:
        jumlah_order = int(sys.argv[2].strip())
    except ValueError:
        print("[!] Format Error: JUMLAH_ORDER harus berupa angka.")
        sys.exit(1)

    partisi = None
    jumlah_partisi = None
    if len(sys.argv) > 3:
        if len(sys.argv) != 5:
            print("[!] Format Error! Mode matrix butuh 2 argumen: <PARTISI> <JUMLAH_PARTISI>")
            sys.exit(1)
        try:
            partisi = int(sys.argv[3].strip())
            jumlah_partisi = int(sys.argv[4].strip())
        except ValueError:
            sys.exit(1)
        if jumlah_partisi < 1 or partisi < 0 or partisi >= jumlah_partisi:
            print("[!] Format Error: Partisi harus 0 <= PARTISI < JUMLAH_PARTISI.")
            sys.exit(1)

    engine = PKXDSkydevFollowEngine()
    L.apply_panel_versions()
    engine.update_versions(L.VERSION, L.GAME_PROTOCOL)

    print(f"[*] SKYDEV V3 CLOUD: Memulai target {target_uuid[:8]} (Total Pesanan: {jumlah_order})...")

    semaphore = asyncio.Semaphore(25)
    lock = asyncio.Lock()
    all_success = set()
    tried_emails = set()
    ronde = 0
    maks_ronde = 4

    while len(all_success) < jumlah_order and ronde < maks_ronde:
        ronde += 1
        defisit = jumlah_order - len(all_success)
        
        buffer = max(int(defisit * 0.3), 15)
        ambil_limit = defisit + buffer

        async with AsyncSession(verify=False) as session_http:
            bots_pool = await fetch_available_bots(session_http, target_uuid, ambil_limit, partisi, jumlah_partisi)

        # Saring bot: buang yang sudah pernah dicoba dalam sesi runner ini
        bots = [b for b in bots_pool if b.get("email") and b["email"] not in tried_emails]
        if not bots:
            print(f"\n[-] STOK HABIS: Tidak ada lagi bot fresh di database (Terkirim: {len(all_success)}).")
            break

        for b in bots:
            tried_emails.add(b["email"])

        print(f"\n[*] RONDE {ronde}: Menjalankan {len(bots)} bot (Target sisa: {defisit})...")
        
        sebelum_ronde = len(all_success)
        tasks = [asyncio.create_task(worker(engine, bot, target_uuid, semaphore, all_success, lock, jumlah_order)) for bot in bots]
        await asyncio.gather(*tasks)

        # Hanya kirimkan batch baru yang sukses di ronde ini ke DB histori
        sukses_baru = [email for email in all_success if email in [b["email"] for b in bots]]
        if sukses_baru:
            async with AsyncSession(verify=False) as session_http:
                await record_history(session_http, target_uuid, sukses_baru)

        if len(all_success) < jumlah_order:
            if len(all_success) == sebelum_ronde:
                print("\n[!] Tidak ada penambahan sukses di ronde ini. Menghentikan loop.")
                break
            print(f"\n[*] Target belum penuh ({len(all_success)}/{jumlah_order}), jeda pendinginan 4 detik...")
            await asyncio.sleep(4.0)

    total_sukses = len(all_success)
    persentase = (total_sukses / jumlah_order) * 100 if jumlah_order > 0 else 0

    print(f"\n\n================================================")
    print(f"[+] PROSES SELESAI! Total Berhasil Follow: {total_sukses}/{jumlah_order} ({persentase:.1f}%)")
    print(f"================================================")

    # Toleransi: jika mencapai >= 80% dari pesanan, anggap Runner sukses (Exit Code 0)
    if persentase >= 80.0:
        print("[+] Toleransi tercapai (>= 80%). Status Runner: HIJAU.")
        sys.exit(0)
    else:
        print("[!] Capaian di bawah 80%. Status Runner: MERAH.")
        sys.exit(1)

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
