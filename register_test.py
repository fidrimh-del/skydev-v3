import uuid
import json
import hmac
import hashlib
import base64
import random
import os
import sys
import asyncio
from collections import OrderedDict
from curl_cffi.requests import AsyncSession
import urllib3
import string

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# KONFIGURASI SUPABASE
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

async def save_to_supabase(session, email, password, device_id, status):
    """Fungsi untuk menyimpan akun langsung ke Supabase tanpa file lokal."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[-] EROR: SUPABASE_URL atau SUPABASE_SERVICE_KEY belum diset!")
        return False
    
    endpoint = f"{SUPABASE_URL}/rest/v1/accounts"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    payload = {
        "email": email,
        "password": password,
        "device_id": device_id,
        "status": status
    }
    
    try:
        r = await session.post(endpoint, json=payload, headers=headers, timeout=15)
        # 201 Created = Sukses masuk DB
        if r.status_code == 201:
            return True
        else:
            print(f"[!] Gagal save ke DB: {r.status_code} - {r.text}")
            return False
    except Exception as e:
        print(f"[!] Error koneksi ke Supabase: {str(e)}")
        return False

# ==========================================
# ENGINE REGISTER PKXD (TIDAK ADA PERUBAHAN LOGIKA GAME)
# ==========================================
class PKXDSkydevRegisterEngine:
    def __init__(self):
        self.account_url = "https://account.faster.aftvrsys.com"
        self.lobby_url = "https://pkxd-lobby.aftvrsys.com"
        self.app_key = "57a3d999-654c-4484-a20c-0f4b5512af49"
        self.app_secret = "jWS5aLZacNVFx6PAxpXq2g4o" 
        self.version = "1.91.4"
        self.game_protocol = "1.91.0" 
        self.impersonate_targets = ["chrome110", "chrome116", "safari15_5"]
        
        self.headers_base = {
            "User-Agent": "UnityPlayer/2022.3.62f3 (UnityWebRequest/1.0, libcurl/8.10.1-DEV)",
            "Accept": "*/*",
            "Accept-Encoding": "deflate, gzip",
            "Content-Type": "application/json",
            "x-fstr-application-key": self.app_key,
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

    async def send_request(self, session, method, path, payload, bearer=None):
        json_body = json.dumps(payload, separators=(',', ':'), ensure_ascii=False)
        body_bytes = json_body.encode('utf-8')
        headers = self.headers_base.copy()
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        headers["x-fstr-signature"] = self.compute_signature(method, path, body_bytes)
        
        return await session.request(method, self.account_url + path, data=body_bytes, headers=headers, timeout=30)

    async def register_bot(self, session):
        device_id = str(uuid.uuid4())
        domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com", "protonmail.com"]
        email = f"{uuid.uuid4().hex[:16]}@{random.choice(domains)}"
        
        karakter = string.ascii_letters + string.digits
        password = ''.join(random.choices(karakter, k=8))

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

            r1 = await self.send_request(session, "POST", "/v1/identity/anonymous/sign-up", {"deviceInfo": device_info})
            if "html" in r1.text.lower() or r1.status_code in [1015, 429, 403]:
                return f"BLOCKED_STEP1 (Status: {r1.status_code})", None, None

            acc_token = r1.json().get("token")

            p2 = OrderedDict([("identityProvider","FASTER"), ("credentials",{"type":"TOKEN","token":acc_token}), ("deviceInfo",device_info)])
            r2 = await self.send_request(session, "POST", "/v1/auth/sessions", p2)
            if "html" in r2.text.lower() or r2.status_code in [1015, 429, 403]:
                return f"BLOCKED_STEP2 (Status: {r2.status_code})", None, None
            
            auth_token = r2.json().get("authToken")

            p3 = OrderedDict([("authToken", auth_token), ("deviceInfo", device_info)])
            r3 = await self.send_request(session, "POST", "/v1/users/sessions", p3)
            user_jwt = r3.json().get("userToken")

            p4 = OrderedDict([("deviceInfo", device_info), ("email", email), ("password", password)])
            await self.send_request(session, "POST", "/v1/identity/email/sign-up", p4)

            p5 = OrderedDict([("identityProvider","FASTER"), ("credentials",{"type":"EMAIL","email":email,"password":password})])
            r5 = await self.send_request(session, "POST", "/v1/users/credentials/link", p5, bearer=user_jwt)

            if r5.status_code in [200, 201]:
                p6 = {"userToken": user_jwt, "gameServerProtocolVersion": self.game_protocol, "isPrivate": False}
                json_p6 = json.dumps(p6, separators=(',', ':'), ensure_ascii=False)
                h6 = self.headers_base.copy()
                if "x-fstr-application-key" in h6: del h6["x-fstr-application-key"]
                
                r6 = await session.post(self.lobby_url + "/lobby/allocation", data=json_p6.encode('utf-8'), headers=h6, timeout=30)
                
                if r6.status_code in [200, 201]:
                    return "SUCCESS", f"{email}|{password}|{device_id}", None
                else:
                    return "REGISTERED_ONLY", f"{email}|{password}|{device_id}", None
            else:
                return f"FAILED (Status: {r5.status_code})", None, None

        except Exception as e:
            return f"ERROR: {str(e)[:50]}", None, None

# ==========================================
# WORKER & MAIN LOOP
# ==========================================
async def worker_task(engine, attempt_id, semaphore):
    async with semaphore:
        await asyncio.sleep(random.uniform(1.0, 3.0))
        
        async with AsyncSession(impersonate=random.choice(engine.impersonate_targets), verify=False) as session:
            result, list_data, _ = await engine.register_bot(session)
            
            if result in ["SUCCESS", "REGISTERED_ONLY"] and list_data:
                # Pecah format "email|password|device_id"
                email, password, device_id = list_data.split("|")
                
                # Kirim ke database Supabase
                db_success = await save_to_supabase(session, email, password, device_id, result)
                if not db_success:
                    result += " (Gagal masuk DB)"
            
            return attempt_id, result

async def main():
    target_accounts = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] PERINGATAN: Kredensial Supabase (URL/KEY) tidak ditemukan.")
        print("[!] Pastikan Anda telah mengatur Environment Variables sebelum menjalankan script ini.")
        sys.exit(1)
        
    print(f"[*] SKYDEV V3 CLOUD: Memulai proses produksi {target_accounts} akun ke Supabase...")
        
    engine = PKXDSkydevRegisterEngine()
    semaphore = asyncio.Semaphore(5) 
    
    success_count = 0
    blocked_count = 0

    waktu_tunggu = random.uniform(1.0, 30.0)
    print(f"[*] Menyamarkan IP... Menunggu {waktu_tunggu:.1f} detik sebelum mengeksekusi.")
    await asyncio.sleep(waktu_tunggu)
    
    print(f"[*] Mulai mendaftarkan bot...\n")

    # Dihapus: file_lock karena Supabase sudah menangani write concurrency
    tasks = [asyncio.create_task(worker_task(engine, i+1, semaphore)) for i in range(target_accounts)]
    
    for completed_task in asyncio.as_completed(tasks):
        attempt_id, result = await completed_task
        
        if "SUCCESS" in result or "REGISTERED_ONLY" in result:
            success_count += 1
            print(f"[+] Akun {attempt_id} -> {result}")
        elif "BLOCKED" in result:
            blocked_count += 1
            print(f"[-] Akun {attempt_id} -> {result}")
        else:
            print(f"[!] Akun {attempt_id} -> {result}")
            
    print(f"\n[HASIL PRODUKSI] Sukses DB: {success_count} | Terblokir/Gagal: {blocked_count} | Total: {target_accounts}")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
