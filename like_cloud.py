import time
import urllib3
import random
import os
import sys
import json
import base64
import asyncio
import hmac
import hashlib
from collections import OrderedDict
from curl_cffi.requests import AsyncSession

# Mematikan peringatan SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# KONFIGURASI SUPABASE CLOUD
# ==========================================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

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
            if "x-fstr-application-key" in h3: del h3["x-fstr-application-key"]
            
            r3 = await session.post(self.lobby_url + "/lobby/allocation", data=b_p3, headers=h3, timeout=20)
            
            if r3.status_code in [200, 201]:
                return user_jwt
            return None
        except:
            return None

    # MODE GET: MENGAMBIL POSTINGAN TERBARU
    async def fetch_posts(self, target_uid, jwt_token, session):
        try:
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
                        
                        content_data = post.get("content", {})
                        if isinstance(content_data, str):
                            try: content_data = json.loads(content_data)
                            except: content_data = {}

                        img_url = content_data.get("image", "")
                        if not img_url: img_url = content_data.get("imageUrl", "")
                        if not img_url: img_url = content_data.get("media", {}).get("url", "")
                        
                        if p_id:
                            extracted_data.append({
                                "post_id": p_id,
                                "image_url": img_url,
                                "likes_count": likes
                            })
                    return 200, extracted_data
                return 200, []
            return r.status_code, None
        except Exception:
            return 000, None

    # MODE LIKE: MENEMBAK LIKE KE POSTINGAN
    async def do_like(self, jwt_token, post_id, session):
        try:
            headers = self.headers_base.copy()
            headers["Authorization"] = f"Bearer {jwt_token}"
            endpoint = f"{self.api_url}/api/v1/posts/{post_id}/like"
            
            await asyncio.sleep(random.uniform(0.1, 0.4))
            r = await session.post(endpoint, headers=headers, data=b"", timeout=15)
            
            return r.status_code in [200, 201, 204]
        except:
            return False

# ==========================================
# FUNGSI SUPABASE (REST API)
# ==========================================
async def sb_upsert_youni_posts(session_http, target_uid, posts_data):
    if not SUPABASE_URL or not SUPABASE_KEY: return
    endpoint = f"{SUPABASE_URL}/rest/v1/temp_youni_posts"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    payload = {"target_uid": target_uid, "posts_data": posts_data}
    try:
        await session_http.post(endpoint, json=payload, headers=headers, timeout=20)
    except Exception as e:
        print(f"[-] Gagal push Youni Posts ke Supabase: {e}")

async def sb_get_fresh_like_bots(session_http, post_id, limit):
    endpoint = f"{SUPABASE_URL}/rest/v1/rpc/get_fresh_like_bots"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    payload = {"p_post_id": post_id, "p_limit": limit}
    try:
        r = await session_http.post(endpoint, json=payload, headers=headers, timeout=20)
        if r.status_code == 200: return r.json()
        return []
    except:
        return []

async def sb_record_like_history(session_http, post_id, success_emails):
    if not success_emails: return
    endpoint = f"{SUPABASE_URL}/rest/v1/like_history"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates"
    }
    payload = [{"post_id": post_id, "account_email": email} for email in success_emails]
    try:
        await session_http.post(endpoint, json=payload, headers=headers, timeout=30)
    except:
        pass

async def get_master_bot(session_http):
    """Mengambil 1 bot acak dari Supabase untuk digunakan saat Get Posts (tanpa menghabiskan kuota follow)"""
    endpoint = f"{SUPABASE_URL}/rest/v1/accounts?select=email,password,device_id&status=eq.SUCCESS&limit=1"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    try:
        r = await session_http.get(endpoint, headers=headers, timeout=15)
        if r.status_code == 200 and len(r.json()) > 0: return r.json()[0]
        return None
    except:
        return None

# ==========================================
# WORKER LIKE
# ==========================================
async def worker_like_task(engine, bot, post_id, semaphore, pbar, success_list):
    async with semaphore:
        async with AsyncSession(impersonate=random.choice(engine.impersonate_targets), verify=False) as session:
            # Smart Login
            token = await engine.login_and_get_token(bot["email"], bot["password"], bot["device_id"], session)
            
            if token:
                liked = await engine.do_like(token, post_id, session)
                if liked:
                    success_list.append(bot["email"])
                    pbar['success'] += 1
            
            pbar['processed'] += 1
            print(f"\r[SKYDEV] Progres Like Post {post_id[:6]}: {pbar['success']}/{pbar['total']}", end="", flush=True)

async def main():
    if len(sys.argv) < 3:
        print("[!] Format Error!")
        sys.exit()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] EROR: Kredensial Supabase tidak ditemukan!")
        sys.exit()

    mode = sys.argv[1].upper()
    engine = PKXDSkydevLikeEngine()
    
    # ==========================================
    # MODE GET POSTS (SUPABASE CLOUD)
    # ==========================================
    if mode == "GET":
        target_uid = sys.argv[2].strip()
        print(f"[*] Misi: Tarik data postingan dari -> {target_uid}")
        
        async with AsyncSession(verify=False) as session_http:
            master_bot = await get_master_bot(session_http)
            
            if not master_bot:
                print("[-] Tidak ada akun bot di database untuk digunakan sebagai Master.")
                return

            print(f"[*] Login Agen Master: {master_bot['email']}...")
            async with AsyncSession(impersonate="chrome110", verify=False) as session_game:
                master_token = await engine.login_and_get_token(master_bot["email"], master_bot["password"], master_bot["device_id"], session_game)
                
                if master_token:
                    status, data = await engine.fetch_posts(target_uid, master_token, session_game)
                    if status == 200:
                        print(f"[+] Berhasil menarik {len(data)} postingan. Menyimpan ke Supabase...")
                        await sb_upsert_youni_posts(session_http, target_uid, data)
                        print("[+] Data Postingan sukses masuk ke Database!")
                    else:
                        print(f"[-] Gagal Mengambil Data Postingan (Kode: {status}).")
                else:
                    print("[-] Master bot gagal login.")

    # ==========================================
    # MODE LIKE (SUPABASE CLOUD)
    # ==========================================
    elif mode == "LIKE":
        post_ids_raw = sys.argv[2].strip()
        jumlah_order = int(sys.argv[3].strip())
        
        target_posts = [p for p in post_ids_raw.split(',') if p.strip()]
        semaphore = asyncio.Semaphore(150)
        
        async with AsyncSession(verify=False) as session_http:
            for p_id in target_posts:
                print(f"\n[*] --- Memulai Operasi Like Postingan: {p_id} ---")
                
                # Tarik bot yang belum pernah like postingan ini
                bots = await sb_get_fresh_like_bots(session_http, p_id, jumlah_order)
                
                if not bots:
                    print(f"[!] Stok bot habis atau semua bot sudah me-like postingan ini. SKIP!")
                    continue
                    
                if len(bots) < jumlah_order:
                    print(f"[-] Peringatan: Hanya menemukan {len(bots)} bot segar.")

                pbar = {'processed': 0, 'success': 0, 'total': len(bots)}
                success_list = []
                
                tasks = [asyncio.create_task(worker_like_task(engine, bot, p_id, semaphore, pbar, success_list)) for bot in bots]
                await asyncio.gather(*tasks)
                
                print(f"\n[+] Tembakan selesai! {pbar['success']} Likes berhasil dikirim.")
                
                # Simpan History ke Supabase
                if success_list:
                    await sb_record_like_history(session_http, p_id, success_list)
                    print(f"[*] Riwayat berhasil dicatat ke Supabase.")
                
        print("\n[+] Misi LIKE Massal Selesai.")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
