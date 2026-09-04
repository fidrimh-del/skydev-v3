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
        self.game_protocol = "1.91.0" 
        self.version = "1.91.4"
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

            # 3. Lobby Allocation
            p3 = {"userToken": user_jwt, "gameServerProtocolVersion": self.game_protocol, "isPrivate": False}
            b_p3 = json.dumps(p3, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
            h3 = self.headers_base.copy()
            
            r3 = await session.post(self.lobby_url + "/lobby/allocation", data=b_p3, headers=h3, timeout=20)
            if r3.status_code in [200, 201]:
                return user_jwt
            return None
        except:
            return None

    async def execute_follow(self, jwt_token, target_uid, session):
        try:
            headers = self.headers_base.copy()
            headers["Authorization"] = f"Bearer {jwt_token}"
            endpoint = f"{self.follow_url}/api/v1/follow/{target_uid}/follow"
            
            await asyncio.sleep(random.uniform(0.1, 0.4))
            r = await session.post(endpoint, headers=headers, data=b"", timeout=15)
            
            # Jika respon 200, 201, atau 204 berarti sukses
            return r.status_code in [200, 201, 204]
        except:
            return False

# ==========================================
# FUNGSI SUPABASE CLOUD
# ==========================================
async def fetch_available_bots(session_http, target_uuid, limit):
    """Mengambil bot segar dari Supabase melalui fungsi RPC"""
    endpoint = f"{SUPABASE_URL}/rest/v1/rpc/get_fresh_bots"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    payload = {"p_target_uuid": target_uuid, "p_limit": limit}
    
    try:
        r = await session_http.post(endpoint, json=payload, headers=headers, timeout=20)
        if r.status_code == 200:
            return r.json()
        return []
    except Exception as e:
        print(f"[-] Gagal mengambil bot dari database: {e}")
        return []

async def record_history(session_http, target_uuid, success_emails):
    """Menyimpan riwayat bot yang berhasil follow ke Supabase"""
    if not success_emails: return
    
    endpoint = f"{SUPABASE_URL}/rest/v1/follow_history"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates" # Cegah error jika bot duplikat
    }
    
    # Format payload menjadi array of objects untuk di-insert massal
    payload = [{"target_uuid": target_uuid, "account_email": email} for email in success_emails]
    
    try:
        await session_http.post(endpoint, json=payload, headers=headers, timeout=30)
    except Exception as e:
        print(f"[-] Gagal menyimpan histori: {e}")

# ==========================================
# WORKER UTAMA
# ==========================================
async def worker(engine, bot, target_uid, semaphore, pbar, success_list):
    async with semaphore:
        async with AsyncSession(impersonate=random.choice(engine.impersonate_targets), verify=False) as session:
            # 1. Login Pintar (Otomatis dapat sesi baru)
            token = await engine.login_and_get_token(bot["email"], bot["password"], bot["device_id"], session)
            
            # 2. Eksekusi Follow
            if token:
                followed = await engine.execute_follow(token, target_uid, session)
                if followed:
                    success_list.append(bot["email"])
                    pbar["success"] += 1
            
            pbar["processed"] += 1
            print(f"\r[SKYDEV] Progres Follow: {pbar['success']}/{pbar['total']} (Berhasil)", end="", flush=True)

async def main():
    if len(sys.argv) < 3:
        print("[!] Format Error! Gunakan: python follow_cloud.py <TARGET_UUID> <JUMLAH_ORDER>")
        sys.exit()
        
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] EROR: Kredensial Supabase tidak ditemukan di Environment Variables!")
        sys.exit()

    target_uuid = sys.argv[1].strip()
    jumlah_order = int(sys.argv[2].strip())
    engine = PKXDSkydevFollowEngine()

    print(f"[*] SKYDEV V3 CLOUD: Menghubungkan ke Database untuk target {target_uuid[:8]}...")

    # Ambil stok bot dari Cloud
    async with AsyncSession(verify=False) as session_http:
        bots = await fetch_available_bots(session_http, target_uuid, jumlah_order)

    if not bots:
        print("[-] STOK HABIS: Tidak ada akun segar yang tersedia untuk target ini.")
        return

    print(f"[*] Berhasil menyaring {len(bots)} bot siap tembak. Memulai serangan...")

    semaphore = asyncio.Semaphore(150) # Batas server github
    success_list = []
    pbar = {"processed": 0, "success": 0, "total": len(bots)}

    # Eksekusi Paralel Massal
    tasks = [asyncio.create_task(worker(engine, bot, target_uuid, semaphore, pbar, success_list)) for bot in bots]
    await asyncio.gather(*tasks)

    print(f"\n\n[+] PROSES SELESAI! {pbar['success']} akun berhasil mem-follow.")
    
    # Simpan Histori ke Cloud
    if success_list:
        print(f"[*] Menyimpan riwayat ke database Supabase...")
        async with AsyncSession(verify=False) as session_http:
            await record_history(session_http, target_uuid, success_list)
        print("[+] Riwayat berhasil diamankan!")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
