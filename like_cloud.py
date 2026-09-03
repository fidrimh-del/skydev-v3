import time
import urllib3
import random
import os
import sys
import json
import base64
import asyncio
import aiofiles
import hmac
import hashlib
from collections import OrderedDict
from curl_cffi.requests import AsyncSession
from supabase import create_client, Client # TAMBAHAN MODULE SUPABASE

# Mematikan peringatan SSL
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
        
        # Buffer aman 5 menit (300 detik)
        if time.time() > (payload.get('exp', 0) - 300):
            return True # Expired
        return False # Masih Aktif
    except:
        return True

class PKXDSkydevLikeEngine:
    def __init__(self):
        self.api_url = "https://gsn-posts.aftvrsys.com"
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

    # FUNGSI AUTO-RELOGIN (Dewa Penyembuh Sesi)
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

    # MODE GET ASYNC: MENGAMBIL 50 POSTINGAN TERBARU (DENGAN MULTI-KEY IMAGE PROTEKSI)
    async def fetch_posts(self, target_uid, jwt_token):
        try:
            async with AsyncSession(impersonate=random.choice(self.impersonate_targets), verify=False) as session:
                headers = self.headers_base.copy()
                headers["Authorization"] = f"Bearer {jwt_token}"
                
                clean_uid = target_uid.replace('"', '').replace("'", "").strip()
                endpoint = f"{self.api_url}/api/v1/posts/profile/{clean_uid}?batchSize=50"
                
                r = await session.get(endpoint, headers=headers, timeout=15)
                
                if r.status_code == 200:
                    data = r.json()
                    extracted_data = []
                    
                    if "posts" in data:
                        for post in data["posts"]:
                            p_id = post.get("id", "")
                            likes = post.get("likes", 0)
                            
                            # PROTEKSI JARING GAMBAR (Menangkal error URL kosong)
                            content_data = post.get("content", {})
                            if isinstance(content_data, str):
                                try:
                                    content_data = json.loads(content_data)
                                except:
                                    content_data = {}

                            img_url = content_data.get("image", "")
                            if not img_url:
                                img_url = content_data.get("imageUrl", "")
                            if not img_url:
                                img_url = content_data.get("media", {}).get("url", "")
                            
                            if p_id:
                                extracted_data.append({
                                    "post_id": p_id,
                                    "image_url": img_url,
                                    "likes_count": likes
                                })
                        return 200, extracted_data
                    return 200, []
                elif r.status_code == 401:
                    return 401, None
                return r.status_code, None
        except Exception:
            return 000, None

    # MODE LIKE ASYNC: MENEMBAK LIKE KE POSTINGAN
    async def do_like(self, jwt_token, post_id, session):
        try:
            headers = self.headers_base.copy()
            headers["Authorization"] = f"Bearer {jwt_token}"
            endpoint = f"{self.api_url}/api/v1/posts/{post_id}/like"
            
            await asyncio.sleep(random.uniform(0.1, 0.4))
            r = await session.post(endpoint, headers=headers, data=b"", timeout=15)
            
            if r.status_code in [200, 201, 204]:
                return "SUCCESS"
            elif r.status_code == 401:
                return "EXPIRED"
            return f"FAILED_{r.status_code}"
        except:
            return "ERROR"

# Fungsi Penulisan Berkas Sesi Secara Aman (Mendukung Penambahan Bot Baru)
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
            if not found:
                await f.write(f"{email}|{token_baru}\n")

# Worker Pintar Pengeksekusi Like Massal + Perekam History
async def worker_like_task(engine, email, jwt_token, post_id, akun_detail, semaphore, file_lock, gudang_bot_path, pbar, berhasil_dipakai):
    async with semaphore:
        async with AsyncSession(impersonate=random.choice(engine.impersonate_targets), verify=False) as session:
            token_aktif = jwt_token
            
            if ksatria_cek_token_expired(jwt_token):
                password = akun_detail.get('password')
                device_id = akun_detail.get('device_id')
                
                if password and device_id:
                    await asyncio.sleep(random.uniform(0.2, 1.5))
                    hasil_relogin = await engine.do_login(email, password, device_id, session)
                    if hasil_relogin.get("status") == "SUCCESS":
                        token_aktif = hasil_relogin.get("jwt")
                        asyncio.create_task(update_token_di_file(gudang_bot_path, email, token_aktif, file_lock))
                    else:
                        pbar['processed'] += 1
                        return "FAILED_RELOGIN"
                else:
                    pbar['processed'] += 1
                    return "NO_CREDENTIALS"

            hasil = await engine.do_like(token_aktif, post_id, session)
            pbar['processed'] += 1
            if hasil == "SUCCESS":
                pbar['success'] += 1
                berhasil_dipakai.append(email) 
                
            print(f"\r[SKYDEV] Progres Like: {pbar['success']}/{pbar['total']}", end="", flush=True)
            return hasil

async def main():
    if len(sys.argv) < 3:
        print("[!] Format Error!")
        sys.exit()

    mode = sys.argv[1].upper()
    engine = PKXDSkydevLikeEngine()
    
    gudang_bot_path = "BOT/Akun_Baru/ActiveSessions.txt"
    list_path = "BOT/Akun_Baru/List.txt"
    
    detail_akun = {}
    semua_bot_dict = {}

    if os.path.exists(list_path):
        with open(list_path, "r") as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 3:
                    email = parts[0].strip()
                    detail_akun[email] = {"password": parts[1].strip(), "device_id": parts[2].strip()}
                    semua_bot_dict[email] = "KOSONG"

    if os.path.exists(gudang_bot_path):
        with open(gudang_bot_path, "r") as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 2:
                    email = parts[0].strip()
                    token = parts[1].strip()
                    semua_bot_dict[email] = token

    semua_bot = [(email, token) for email, token in semua_bot_dict.items()]

    if not semua_bot:
        print("[!] Gudang bot kosong! Pastikan List.txt memiliki data.")
        sys.exit()

    print(f"[*] Berhasil memuat total {len(semua_bot)} bot dari kombinasi List & Session.")

    # ==========================================
    # INTERMEZZO MODE GET (TERKONEKSI SUPABASE)
    # ==========================================
    if mode == "GET":
        target_uid = sys.argv[2].strip()
        print(f"[*] Misi: Tarik data postingan dari -> {target_uid}")
        
        email_master, jwt_master = semua_bot[0]
        print(f"[*] Menggunakan Agen Tunggal: {email_master}")
        
        if ksatria_cek_token_expired(jwt_master):
            print("[*] Token Agen Master Expired/Kosong. Memicu login ulang darurat...")
            akun_info = detail_akun.get(email_master, {})
            if akun_info:
                async with AsyncSession(impersonate="chrome110", verify=False) as sess:
                    res = await engine.do_login(email_master, akun_info['password'], akun_info['device_id'], sess)
                    if res.get("status") == "SUCCESS":
                        jwt_master = res.get("jwt")
                        lock_master = asyncio.Lock()
                        await update_token_di_file(gudang_bot_path, email_master, jwt_master, lock_master)
                        print("[+] Token Agen Master berhasil diperbarui!")

        status, data = await engine.fetch_posts(target_uid, jwt_master)
        
        if status == 200:
            print(f"[+] Akses Diterima! Menarik {len(data)} postingan.")
            
            # --- BLOK EKSEKUSI SUPABASE ---
            sb_url = os.environ.get("SUPABASE_URL")
            sb_key = os.environ.get("SUPABASE_KEY")
            
            if sb_url and sb_key:
                try:
                    supabase_client: Client = create_client(sb_url, sb_key)
                    payload = {
                        "target_uid": target_uid.replace('"', ''),
                        "posts_data": data
                    }
                    supabase_client.table("temp_youni_posts").upsert(payload).execute()
                    print("[+] Berhasil menyimpan Data Postingan secara real-time ke Database Supabase!")
                except Exception as e:
                    print(f"[-] Gagal push ke Supabase: {e}")
            else:
                print("[-] PERINGATAN: Kredensial Supabase (URL/KEY) tidak ditemukan di Environment GitHub Actions!")
                
        else:
            print(f"[-] Gagal Mengambil Data Postingan (Status Code: {status}).")

    # ==========================================
    # INTERMEZZO MODE LIKE (SERANGAN MASSAL + HISTORY)
    # ==========================================
    elif mode == "LIKE":
        post_ids_raw = sys.argv[2].strip()
        jumlah_order = int(sys.argv[3].strip())
        
        target_posts = [p for p in post_ids_raw.split(',') if p.strip()]
        
        semaphore = asyncio.Semaphore(150)
        file_lock = asyncio.Lock()
        
        for p_id in target_posts:
            print(f"\n[*] --- Menyerang Postingan: {p_id} ---")
            
            history_path = f"BOT/History_Like/{p_id}.txt"
            bot_bekas = set()
            if os.path.exists(history_path):
                with open(history_path, "r") as f:
                    bot_bekas.update(f.read().splitlines())
            
            bot_siap_pakai = []
            for email, jwt in semua_bot:
                if email not in bot_bekas:
                    bot_siap_pakai.append((email, jwt))
                    if len(bot_siap_pakai) == jumlah_order: break

            if len(bot_siap_pakai) == 0:
                print(f"[!] Seluruh stok bot di gudang sudah me-like postingan ini. SKIP!")
                continue
                
            if len(bot_siap_pakai) < jumlah_order:
                print(f"[-] Peringatan: Hanya menemukan {len(bot_siap_pakai)} bot segar dari {jumlah_order} yang diminta.")

            pbar = {'processed': 0, 'success': 0, 'total': len(bot_siap_pakai)}
            berhasil_dipakai = []
            
            tasks = []
            for email, jwt in bot_siap_pakai:
                akun_info = detail_akun.get(email, {})
                task = asyncio.create_task(worker_like_task(engine, email, jwt, p_id, akun_info, semaphore, file_lock, gudang_bot_path, pbar, berhasil_dipakai))
                tasks.append(task)
                
            await asyncio.gather(*tasks)
            print(f"\n[+] Tembakan selesai untuk {p_id[:8]}! Sukses: {pbar['success']}")
            
            if berhasil_dipakai:
                os.makedirs("BOT/History_Like", exist_ok=True)
                async with aiofiles.open(history_path, "a") as f:
                    for email in berhasil_dipakai:
                        await f.write(email + "\n")
                print(f"[*] Database History Like diupdate! {len(berhasil_dipakai)} bot dicatat.")
            
        print("\n[+] Misi LIKE Massal Selesai.")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
