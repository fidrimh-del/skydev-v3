import json
import hmac
import hashlib
import base64
import time
import urllib3
import random
import os
import asyncio
import aiofiles
from collections import OrderedDict
from curl_cffi.requests import AsyncSession

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# FUNGSI PENGECEK JWT (0 Detik / Tanpa Internet)
# ==========================================
def ksatria_cek_token_expired(jwt_token):
    try:
        payload_b64 = jwt_token.split('.')[1]
        payload_b64 += '=' * (-len(payload_b64) % 4)
        payload = json.loads(base64.b64decode(payload_b64).decode('utf-8'))
        
        # Buffer aman 5 menit (300 detik) sebelum benar-benar expired
        if time.time() > (payload.get('exp', 0) - 300):
            return True # Expired
        return False # Masih Aktif
    except:
        return True

class PKXDSkydevLoginGithub:
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

    async def do_login(self, email, password, device_id, session):
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

            # STEP 1: AUTH SESSIONS
            p1 = OrderedDict([("identityProvider", "FASTER"), ("credentials", {"type": "EMAIL", "email": email, "password": password}), ("deviceInfo", device_info)])
            json_p1 = json.dumps(p1, separators=(',', ':'), ensure_ascii=False)
            body_p1 = json_p1.encode('utf-8')
            
            h1 = self.headers_base.copy()
            h1["x-fstr-application-key"] = self.app_key
            h1["x-fstr-signature"] = self.compute_signature("POST", "/v1/auth/sessions", body_p1)
            
            r1 = await session.post(self.account_url + "/v1/auth/sessions", data=body_p1, headers=h1, timeout=30)
            
            if "html" in r1.text.lower() or r1.status_code in [1015, 403, 429]: return "BLOCKED"
            if r1.status_code != 200: return "FAILED"

            auth_token = r1.json().get("authToken")
            await asyncio.sleep(random.uniform(0.1, 0.4)) 

            # STEP 2: USER SESSIONS
            p2 = OrderedDict([("authToken", auth_token), ("deviceInfo", device_info)])
            json_p2 = json.dumps(p2, separators=(',', ':'), ensure_ascii=False)
            body_p2 = json_p2.encode('utf-8')
            
            h2 = self.headers_base.copy()
            h2["x-fstr-application-key"] = self.app_key
            h2["x-fstr-signature"] = self.compute_signature("POST", "/v1/users/sessions", body_p2)
            
            r2 = await session.post(self.account_url + "/v1/users/sessions", data=body_p2, headers=h2, timeout=30)
            
            if "html" in r2.text.lower() or r2.status_code in [1015, 403, 429]: return "BLOCKED"

            user_jwt = r2.json().get("userToken")
            if not user_jwt: return "FAILED"
                
            await asyncio.sleep(random.uniform(0.1, 0.4))

            # STEP 3: ALLOCATION LOBBY
            p3 = {"userToken": user_jwt, "gameServerProtocolVersion": self.game_protocol, "isPrivate": False}
            json_p3 = json.dumps(p3, separators=(',', ':'), ensure_ascii=False)
            
            h3 = self.headers_base.copy()
            if "x-fstr-application-key" in h3: del h3["x-fstr-application-key"]
            
            r3 = await session.post(self.lobby_url + "/lobby/allocation", data=json_p3.encode('utf-8'), headers=h3, timeout=30)
            
            if r3.status_code in [200, 201]:
                return f"SUCCESS|{user_jwt}"
            else:
                return "FAILED"
        except:
            return "ERROR"

def load_accounts(filename):
    accounts = []
    if not os.path.exists(filename): return accounts
    with open(filename, "r") as f:
        for line in f:
            parts = line.strip().split('|')
            if len(parts) >= 3: accounts.append((parts[0], parts[1], parts[2]))
    return accounts

def load_current_sessions(filename):
    sessions = {}
    if not os.path.exists(filename): return sessions
    with open(filename, "r") as f:
        for line in f:
            parts = line.strip().split('|')
            if len(parts) == 2: sessions[parts[0]] = parts[1]
    return sessions

async def worker_task(engine, email, password, device_id, token_lama, semaphore, final_results, pbar):
    async with semaphore:
        # Pengecekan Pintar secara lokal sebelum menyentuh internet
        if token_lama and not ksatria_cek_token_expired(token_lama):
            pbar['processed'] += 1
            pbar['skipped'] += 1
            final_results[email] = token_lama
            print(f"\r[SKYDEV] Progres Sesi: {pbar['processed']}/{pbar['total']} | Segar: {pbar['skipped']} | Diperbarui: {pbar['success']}", end="", flush=True)
            return

        # Jika expired atau belum ada sesi, lakukan relogin async dengan micro-delay gerilya
        await asyncio.sleep(random.uniform(0.5, 2.5))
        async with AsyncSession(impersonate=random.choice(engine.impersonate_targets), verify=False) as session:
            res = await engine.do_login(email, password, device_id, session)
            pbar['processed'] += 1
            
            if "SUCCESS" in res:
                pbar['success'] += 1
                token_baru = res.split('|')[1]
                final_results[email] = token_baru
            else:
                # Jika gagal login, pertahankan token lama (jika ada) supaya tidak kehilangan data sepenuhnya
                if token_lama: final_results[email] = token_lama
                
            print(f"\r[SKYDEV] Progres Sesi: {pbar['processed']}/{pbar['total']} | Segar: {pbar['skipped']} | Diperbarui: {pbar['success']}", end="", flush=True)

async def main():
    list_file = "BOT/Akun_Baru/List.txt"
    session_file = "BOT/Akun_Baru/ActiveSessions.txt"

    accounts_list = load_accounts(list_file)
    current_sessions = load_current_sessions(session_file)

    if not accounts_list:
        print(f"[!] Gudang Kosong! Tidak ada file di {list_file}")
        return
        
    print(f"[*] Menemukan {len(accounts_list)} akun di dalam Gudang List.")
    print(f"[*] Menemukan {len(current_sessions)} data sesi di ActiveSessions.")
    
    engine = PKXDSkydevLoginGithub()
    
    # Batas aman paralel untuk single runner agar tidak memicu deteksi DDoS PKXD
    semaphore = asyncio.Semaphore(40) 
    final_results = {}
    
    pbar = {'processed': 0, 'success': 0, 'skipped': 0, 'total': len(accounts_list)}
    print(f"\n[*] Memulai pengecekan & penyegaran sesi massal V2...\n")

    tasks = []
    for email, password, device_id in accounts_list:
        token_lama = current_sessions.get(email)
        task = asyncio.create_task(worker_task(engine, email, password, device_id, token_lama, semaphore, final_results, pbar))
        tasks.append(task)

    await asyncio.gather(*tasks)

    # Simpan hasil akhir yang bersih dan ter-update secara mutlak
    os.makedirs(os.path.dirname(session_file), exist_ok=True)
    async with aiofiles.open(session_file, "w") as f:
        for email, token in final_results.items():
            await f.write(f"{email}|{token}\n")

    print(f"\n\n[+] PROSES SELESAI! Sesi segar disimpan. Total Diperbarui: {pbar['success']} | Dilewati: {pbar['skipped']}")

if __name__ == "__main__":
    if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
