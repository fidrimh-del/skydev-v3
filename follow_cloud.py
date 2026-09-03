import json
import time
import os
import sys
import base64
import random
import asyncio
import aiofiles
import hmac
import hashlib
from collections import OrderedDict
from curl_cffi.requests import AsyncSession
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# FUNGSI PENGECEK JWT (0 Detik / Tanpa Internet)
# ==========================================
def ksatria_cek_token_expired(jwt_token):
    # Proteksi khusus jika token masih "KOSONG" (Bot baru yang belum punya sesi)
    if not jwt_token or jwt_token == "KOSONG":
        return True 
        
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

class PKXDSkydevFollowGithub:
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

    # FUNGSI AUTO-RELOGIN (Dijalankan jika token expired di tengah jalan)
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

            p1 = OrderedDict([("identityProvider", "FASTER"), ("credentials", {"type": "EMAIL", "email": email, "password": password}), ("deviceInfo", device_info)])
            json_p1 = json.dumps(p1, separators=(',', ':'), ensure_ascii=False)
            body_p1 = json_p1.encode('utf-8')
            
            h1 = self.headers_base.copy()
            h1["x-fstr-application-key"] = self.app_key
            h1["x-fstr-signature"] = self.compute_signature("POST", "/v1/auth/sessions", body_p1)
            
            r1 = await session.post(self.account_url + "/v1/auth/sessions", data=body_p1, headers=h1, timeout=30)
            if r1.status_code != 200: return {"status": "FAILED"}

            auth_token = r1.json().get("authToken")
            await asyncio.sleep(random.uniform(0.1, 0.3)) 

            p2 = OrderedDict([("authToken", auth_token), ("deviceInfo", device_info)])
            json_p2 = json.dumps(p2, separators=(',', ':'), ensure_ascii=False)
            body_p2 = json_p2.encode('utf-8')
            
            h2 = self.headers_base.copy()
            h2["x-fstr-application-key"] = self.app_key
            h2["x-fstr-signature"] = self.compute_signature("POST", "/v1/users/sessions", body_p2)
            
            r2 = await session.post(self.account_url + "/v1/users/sessions", data=body_p2, headers=h2, timeout=30)
            user_jwt = r2.json().get("userToken")
            if not user_jwt: return {"status": "FAILED"}
                
            await asyncio.sleep(random.uniform(0.1, 0.3))

            p3 = {"userToken": user_jwt, "gameServerProtocolVersion": self.game_protocol, "isPrivate": False}
            json_p3 = json.dumps(p3, separators=(',', ':'), ensure_ascii=False)
            h3 = self.headers_base.copy()
            if "x-fstr-application-key" in h3: del h3["x-fstr-application-key"]
            
            r3 = await session.post(self.lobby_url + "/lobby/allocation", data=json_p3.encode('utf-8'), headers=h3, timeout=30)
            
            if r3.status_code in [200, 201]:
                return {"status": "SUCCESS", "jwt": user_jwt}
            return {"status": "FAILED"}
        except:
            return {"status": "ERROR"}

    # FUNGSI FOLLOW ASYNC
    async def do_follow(self, jwt_token, target_uid, session):
        try:
            headers = self.headers_base.copy()
            headers["Authorization"] = f"Bearer {jwt_token}"
            endpoint = f"{self.follow_url}/api/v1/follow/{target_uid}/follow"
            
            # Micro-delay natural manusia
            await asyncio.sleep(random.uniform(0.1, 0.4))
            
            r = await session.post(endpoint, headers=headers, data=b"", timeout=15)
            
            if "html" in r.text.lower() or r.status_code in [1015, 429, 403]:
                return "BLOCKED"

            if r.status_code in [200, 201, 204]:
                return "SUCCESS"
            elif r.status_code == 401:
                return "EXPIRED"
            else:
                return f"FAILED_{r.status_code}"
        except:
            return "ERROR"

# Fungsi update file ActiveSessions secara aman (Mendukung Penambahan Bot Baru)
async def update_token_di_file(filepath, email, token_baru, file_lock):
    async with file_lock:
        lines = []
        if os.path.exists(filepath):
            async with aiofiles.open(filepath, mode='r') as f:
                lines = await f.readlines()
                
        found = False
        async with aiofiles.open(filepath, mode='w') as f:
            for line in lines:
                parts = line.strip().split('|')
                if len(parts) >= 2 and parts[0] == email:
                    await f.write(f"{email}|{token_baru}\n")
                    found = True
                else:
                    await f.write(line)
            # Jika bot baru belum pernah tercatat di ActiveSessions, tambahkan baris baru
            if not found:
                await f.write(f"{email}|{token_baru}\n")

# Worker Pintar Pengeksekusi Follow Massal
async def worker_task(engine, email, jwt_token, target_uid, akun_detail, semaphore, file_lock, gudang_bot_path, pbar, berhasil_dipakai):
    async with semaphore:
        async with AsyncSession(impersonate=random.choice(engine.impersonate_targets), verify=False) as session:
            token_aktif = jwt_token
            
            # --- CEK TOKEN LOKAL (SMART SELF-HEALING) ---
            if ksatria_cek_token_expired(jwt_token):
                password = akun_detail.get('password')
                device_id = akun_detail.get('device_id')
                
                if password and device_id:
                    # Jalankan taktik gerilya acak mikro agar relogin massal tidak crash
                    await asyncio.sleep(random.uniform(0.2, 1.5))
                    hasil_relogin = await engine.do_login(email, password, device_id, session)
                    
                    if hasil_relogin.get("status") == "SUCCESS":
                        token_aktif = hasil_relogin.get("jwt")
                        # Simpan token segar ke berkas utama di background
                        asyncio.create_task(update_token_di_file(gudang_bot_path, email, token_aktif, file_lock))
                    else:
                        pbar['processed'] += 1
                        return
                else:
                    pbar['processed'] += 1
                    return

            # --- EKSEKUSI FOLLOW KILAT ---
            hasil_follow = await engine.do_follow(token_aktif, target_uid, session)
            pbar['processed'] += 1
            
            if hasil_follow == "SUCCESS":
                pbar['success'] += 1
                berhasil_dipakai.append(email)
                
            print(f"\r[SKYDEV] Progres Follow Sukses: {pbar['success']}/{pbar['total']}", end="", flush=True)

async def main():
    if len(sys.argv) < 3:
        print("[!] Format Error! Gunakan: python follow_cloud.py <TARGET_UUID> <JUMLAH_ORDER>")
        sys.exit()

    target_id = sys.argv[1].strip()
    jumlah_order = int(sys.argv[2].strip())

    gudang_bot_path = "BOT/Akun_Baru/ActiveSessions.txt"
    list_path = "BOT/Akun_Baru/List.txt"
    history_path = f"BOT/History/{target_id}.txt"
        
    # ==========================================
    # BACA GUDANG BOT (PENGGABUNGAN TOTAL LIST & SESSION)
    # ==========================================
    detail_akun = {}
    semua_bot_dict = {}

    # 1. Jadikan List.txt sebagai Master Data Pokok
    if os.path.exists(list_path):
        with open(list_path, "r") as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 3:
                    email = parts[0].strip()
                    detail_akun[email] = {"password": parts[1].strip(), "device_id": parts[2].strip()}
                    semua_bot_dict[email] = "KOSONG" # Tandai belum ada token

    # 2. Timpa dengan token aktif dari ActiveSessions.txt (jika ada)
    if os.path.exists(gudang_bot_path):
        with open(gudang_bot_path, "r") as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 2:
                    email = parts[0].strip()
                    token = parts[1].strip()
                    semua_bot_dict[email] = token

    # 3. Ubah menjadi list siap pakai
    semua_bot = [(email, token) for email, token in semua_bot_dict.items()]

    if not semua_bot:
        print("[!] Gudang bot kosong! Pastikan List.txt memiliki data.")
        sys.exit()

    print(f"[*] Berhasil memuat total {len(semua_bot)} bot dari kombinasi List & Session.")

    # 4. BACA HISTORY TARGET
    bot_bekas = set()
    if os.path.exists(history_path):
        with open(history_path, "r") as f:
            bot_bekas.update(f.read().splitlines())

    # 5. PENYARINGAN CERDAS (FILTERING)
    bot_siap_pakai = []
    for email, jwt in semua_bot:
        if email not in bot_bekas:
            bot_siap_pakai.append((email, jwt))
            if len(bot_siap_pakai) == jumlah_order: break

    if len(bot_siap_pakai) < jumlah_order:
        print(f"[!] STOK KURANG! Hanya menemukan {len(bot_siap_pakai)} bot segar untuk target ini.")
        if len(bot_siap_pakai) == 0: sys.exit()

    print(f"[*] Berhasil menyaring {len(bot_siap_pakai)} bot segar. Menyiapkan senjata Async V2...")

    # 6. EKSEKUSI UTAMA PARALEL
    engine = PKXDSkydevFollowGithub()
    
    # Batas aman konkurensi untuk single server GitHub Actions
    semaphore = asyncio.Semaphore(150) 
    file_lock = asyncio.Lock()
    
    berhasil_dipakai = []
    pbar = {'processed': 0, 'success': 0, 'total': len(bot_siap_pakai)}

    print(f"[*] Memulai Serangan Follow ke Target: {target_id}")

    tasks = []
    for email, jwt in bot_siap_pakai:
        akun_info = detail_akun.get(email, {})
        task = asyncio.create_task(worker_task(engine, email, jwt, target_id, akun_info, semaphore, file_lock, gudang_bot_path, pbar, berhasil_dipakai))
        tasks.append(task)

    await asyncio.gather(*tasks)

    print(f"\n\n[+] PROSES FOLLOW SELESAI! {pbar['success']} bot berhasil menembak target.")

    # 7. SIMPAN DATABASE HISTORY
    if berhasil_dipakai:
        os.makedirs("BOT/History", exist_ok=True)
        async with aiofiles.open(history_path, "a") as f:
            for email in berhasil_dipakai:
                await f.write(email + "\n")
        print(f"[*] Database diupdate! {len(berhasil_dipakai)} bot ini disimpan ke history.")

if __name__ == "__main__":
    if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
