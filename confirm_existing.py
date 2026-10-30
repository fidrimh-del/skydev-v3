#!/usr/bin/env python3
"""Konfirmasi nickname untuk bot existing di named_bots.
Menggunakan fungsi dari register_named.py (yang sudah di-fix dengan SET).
"""
import asyncio, json, os, sys

# Import dari register_named
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from register_named import (
    tcp_get_nickname, email_login_jwt, save_bot, log,
    SUPABASE_URL, SUPABASE_KEY, TABEL
)
from curl_cffi.requests import AsyncSession

def sb_get_bots(limit=0):
    import urllib.request
    q = f"{TABEL}?select=id,email,password,device_id,nickname&order=id.asc"
    if limit > 0: q += f"&limit={limit}"
    url = f"{SUPABASE_URL}/rest/v1/{q}"
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    req = urllib.request.Request(url, headers=h)
    try:
        r = urllib.request.urlopen(req, timeout=30)
        return json.loads(r.read().decode() or "[]")
    except Exception as e:
        log(f"[!] Gagal ambil bot: {e}")
        return []

async def konfirmasi_satu(bot, sem, idx, total):
    async with sem:
        email = bot["email"]
        log(f"[{idx}/{total}] {email[:4]}*** ...")
        try:
            user_jwt, alloc = await email_login_jwt(email, bot["password"], bot["device_id"])
            if not user_jwt:
                log(f"    -> GAGAL login")
                return False
            loop = asyncio.get_event_loop()
            nick = await loop.run_in_executor(None, tcp_get_nickname, user_jwt, alloc, bot["device_id"])
            if nick:
                # Update DB dengan nickname terkonfirmasi
                async with AsyncSession(impersonate="chrome110", verify=False) as sess:
                    await save_bot(sess, email, bot["password"], bot["device_id"], nick, "SUCCESS")
                log(f"    -> TERKONFIRMASI: {nick}")
                return True
            else:
                log(f"    -> GAGAL dapat nickname")
                return False
        except Exception as e:
            log(f"    -> ERROR: {e}")
            return False

async def main():
    batas = int(os.getenv("BATAS", "0"))
    workers = int(os.getenv("WORKERS", "10"))
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] SUPABASE_URL / SUPABASE_SERVICE_KEY belum diset."); sys.exit(1)
    bots = sb_get_bots(batas)
    # Filter: hanya yang nickname-nya tidak ada # (belum terkonfirmasi)
    bots = [b for b in bots if not (b.get("nickname") and "#" in b["nickname"])]
    if not bots:
        print("[!] Semua bot sudah terkonfirmasi."); return
    log(f"[*] KONFIRMASI: {len(bots)} bot, {workers} workers")
    sem = asyncio.Semaphore(workers)
    hasil = await asyncio.gather(*[konfirmasi_satu(b, sem, i+1, len(bots)) for i, b in enumerate(bots)])
    ok = sum(1 for x in hasil if x)
    log(f"\n[HASIL] {ok}/{len(bots)} terkonfirmasi")

if __name__ == "__main__":
    asyncio.run(main())
