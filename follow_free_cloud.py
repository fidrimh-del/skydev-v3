"""follow_free_cloud.py — Follow gratis 20, KHUSUS klaim Free Followers.

BEDA dengan follow_cloud.py (script massal lama):
- Hanya untuk 20, TIDAK PERNAH lebih: jumlah dikunci di konstanta
  TARGET_COUNT = 20, fetch dipotong ke sisa yang belum terkirim,
  dan worker berhenti begitu 20 tercapai. Script lama memakai
  paralel 150 + histori borongan di akhir (cocok untuk order ratusan/
  ribuan, tapi rawan meluber bila dipakai untuk 20).
- Paralel KECIL: Semaphore(5), bukan 150.
- Histori ditulis PER SUKSES, bukan borongan di akhir: bila job
  putus di tengah, bot yang sudah sukses sudah tercatat dan tidak
  akan diambil lagi di percobaan ulang — percobaan ulang hanya
  menambah SISA (20 - sudah_sukses), jadi total tetap tepat 20.
- Target boleh UUID atau Nama#Kode: bila Nama#Kode, UUID dicari
  dulu lewat lookup_uuid.lookup_once (jalur yang sama dengan
  lookup_poll / script like), jadi admin tidak perlu mengetik UUID.
- Status klaim di database TOKO diubah script ini sendiri:
  proses -> success bila total tepat 20; bila kurang (stok habis /
  gagal sebagian), status tetap 'proses' dan follow_sukses tercatat,
  supaya admin bisa klik Proses lagi untuk menambah sisanya saja.
  Bila UUID tidak ditemukan, klaim jadi 'gagal'.
- Hanya bot TANPA NAMA (accounts, RPC get_fresh_bots), sesuai aturan
  bot gratisan. Bot bernama TIDAK PERNAH dipakai di sini.

Pemakaian (dari workflow follow-free.yml):
  python follow_free_cloud.py "<TARGET_UUID atau Nama#Kode>" "<CLAIM_ID>"
CLAIM_ID boleh kosong bila dijalankan manual tanpa klaim (maka tidak
ada update status klaim, hanya follow + histori panel).

Env (dari Secrets repo, TIDAK ADA nilai kredensial di file ini):
  SUPABASE_URL / SUPABASE_SERVICE_KEY   = database PANEL (stok bot)
  SHOP_SUPABASE_URL / SHOP_SUPABASE_SERVICE_KEY = database TOKO (klaim)
  PANEL_SUPABASE_URL / PANEL_SUPABASE_SERVICE_KEY = versi game panel
  PROBE_* / FSTR_* = akun lookup (dibaca lookup_uuid.py)
"""

import asyncio
import json
import os
import re
import sys
import urllib.request
import urllib.error
from curl_cffi.requests import AsyncSession

import lookup_uuid as L
from follow_cloud import PKXDSkydevFollowEngine

PANEL_URL = os.getenv("SUPABASE_URL", "")
PANEL_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
TOKO_URL = os.getenv("SHOP_SUPABASE_URL", "")
TOKO_KEY = os.getenv("SHOP_SUPABASE_SERVICE_KEY", "")

TARGET_COUNT = 20
MAX_PARALLEL = 5

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def is_uuid(s):
    return bool(s and UUID_RE.match(s.strip()))


# ---------- database TOKO (klaim) ----------

def toko_request(method, path, payload=None):
    if not TOKO_URL or not TOKO_KEY:
        return None
    req = urllib.request.Request(
        TOKO_URL.rstrip("/") + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"apikey": TOKO_KEY, "Authorization": "Bearer " + TOKO_KEY,
                 "Content-Type": "application/json",
                 "Prefer": "return=representation"},
        method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode() or "null")
    except urllib.error.HTTPError as e:
        print(f"[-] Toko {method} {path} gagal: HTTP {e.code}")
        return None
    except Exception as e:
        print(f"[-] Toko {method} {path} gagal: {e}")
        return None


def toko_get_claim(claim_id):
    rows = toko_request(
        "GET", "/rest/v1/free_follower_claims?select=id,status,"
        "follow_sukses,player_uuid&" f"id=eq.{claim_id}")
    return rows[0] if rows else None


def toko_patch_claim(claim_id, patch):
    if not claim_id:
        return
    toko_request("PATCH",
                 f"/rest/v1/free_follower_claims?id=eq.{claim_id}", patch)


# ---------- database PANEL (bot + histori) ----------

async def fetch_bots(session_http, target_uuid, limit):
    endpoint = f"{PANEL_URL}/rest/v1/rpc/get_fresh_bots"
    headers = {"apikey": PANEL_KEY, "Authorization": f"Bearer {PANEL_KEY}",
               "Content-Type": "application/json"}
    try:
        r = await session_http.post(
            endpoint, json={"p_target_uuid": target_uuid, "p_limit": limit},
            headers=headers, timeout=20)
        if r.status_code == 200:
            bots = r.json() or []
            # Jaring pengaman: RPC tidak boleh mengembalikan lebih dari
            # sisa yang diminta, apa pun yang terjadi di sisi DB.
            return bots[:limit]
        print(f"[-] RPC get_fresh_bots HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"[-] Gagal mengambil bot: {e}")
        return []


async def record_history_one(session_http, target_uuid, email):
    endpoint = f"{PANEL_URL}/rest/v1/follow_history"
    headers = {"apikey": PANEL_KEY, "Authorization": f"Bearer {PANEL_KEY}",
               "Content-Type": "application/json",
               "Prefer": "resolution=ignore-duplicates"}
    try:
        await session_http.post(
            endpoint, json=[{"target_uuid": target_uuid,
                             "account_email": email}],
            headers=headers, timeout=30)
    except Exception as e:
        print(f"[-] Gagal mencatat histori {email}: {e} "
              "(follow di game sudah terjadi, histori dicoba lagi "
              "di percobaan ulang bila perlu)")


# ---------- worker ----------

async def worker(engine, bot, target_uuid, semaphore, state, claim_id):
    async with semaphore:
        async with state["lock"]:
            if state["baru"] >= state["sisa"]:
                return "skip-target-tercapai"
        import random
        async with AsyncSession(
                impersonate=random.choice(engine.impersonate_targets),
                verify=False) as session:
            token = await engine.login_and_get_token(
                bot["email"], bot["password"], bot["device_id"], session)
            if not token:
                return "fail-login"
            ok = await engine.execute_follow(token, target_uuid, session)
            if not ok:
                return "fail-follow"
        async with state["lock"]:
            if state["baru"] >= state["sisa"]:
                # Sisa sudah tercapai selagi worker ini jalan: follow
                # di game sudah terjadi untuk bot ini, tapi TIDAK
                # dihitung ke target (kelebihan 1 game-follow tidak
                # bisa dibatalkan; jaring pengaman fetch + lock membuat
                # ini praktis tidak terjadi karena total bot yang
                # diambil = sisa persis).
                return "skip-target-tercapai"
            state["baru"] += 1
            total = state["sudah"] + state["baru"]
        async with AsyncSession(verify=False) as session_http:
            await record_history_one(session_http, target_uuid, bot["email"])
        if claim_id:
            await asyncio.to_thread(
                toko_patch_claim, claim_id,
                {"follow_sukses": total})
        print(f"[+] Sukses {total}/{TARGET_COUNT} "
              f"(baru {state['baru']}/{state['sisa']})")
        return "sent"


async def resolve_uuid(target):
    if is_uuid(target):
        print(f"[*] Target sudah UUID: {target.strip()}")
        return target.strip()
    bot = L.probe_bot()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        for attempt in range(1, 4):
            hit = await L.lookup_once(session, bot, target)
            if hit:
                print(f"[+] UUID ditemukan: {target} -> {hit['uuid']}")
                return hit["uuid"]
            print(f"[-] Lookup percobaan {attempt} kosong.")
    return None


async def main_async(target, claim_id):
    if not PANEL_URL or not PANEL_KEY:
        print("[!] Kredensial panel tidak ditemukan di env.")
        return 1
    L.apply_panel_versions()
    engine = PKXDSkydevFollowEngine()
    engine.version = L.VERSION
    engine.game_protocol = L.GAME_PROTOCOL

    sudah = 0
    if claim_id:
        klaim = toko_get_claim(claim_id)
        if klaim:
            sudah = int(klaim.get("follow_sukses") or 0)
            print(f"[*] Klaim {claim_id}: sudah sukses {sudah}/"
                  f"{TARGET_COUNT} dari percobaan sebelumnya.")
    sisa = TARGET_COUNT - sudah
    if sisa <= 0:
        print("[+] Klaim sudah tepat 20 — tidak ada follow lagi.")
        if claim_id:
            toko_patch_claim(claim_id, {"status": "success"})
        return 0

    target_uuid = await resolve_uuid(target)
    if not target_uuid:
        print(f"[!] Target {target} TIDAK DITEMUKAN — tidak ada "
              "follow yang dikirim.")
        if claim_id:
            toko_patch_claim(claim_id, {"uuid_status": "not_found",
                                        "status": "gagal"})
        return 1
    if claim_id:
        toko_patch_claim(claim_id, {"player_uuid": target_uuid,
                                    "uuid_status": "found"})

    async with AsyncSession(verify=False) as session_http:
        bots = await fetch_bots(session_http, target_uuid, sisa)
    if not bots:
        print("[-] STOK HABIS: tidak ada bot tanpa nama yang segar "
              "untuk target ini. Status klaim tetap 'proses' supaya "
              "bisa dicoba lagi saat stok sudah ada.")
        return 1
    print(f"[*] Mengambil {len(bots)} bot (sisa {sisa}), "
          f"paralel maks {MAX_PARALLEL}.")

    state = {"baru": 0, "sisa": sisa, "sudah": sudah,
             "lock": asyncio.Lock()}
    semaphore = asyncio.Semaphore(MAX_PARALLEL)
    tasks = [asyncio.create_task(
        worker(engine, bot, target_uuid, semaphore, state, claim_id))
        for bot in bots]
    await asyncio.gather(*tasks)

    total = sudah + state["baru"]
    print("=" * 48)
    print(f"HASIL: target={target_uuid} sukses_baru={state['baru']} "
          f"total={total}/{TARGET_COUNT}")
    print("=" * 48)
    if claim_id:
        toko_patch_claim(claim_id, {"follow_sukses": total})
        if total >= TARGET_COUNT:
            from datetime import datetime, timezone
            toko_patch_claim(
                claim_id,
                {"status": "success",
                 "processed_at": datetime.now(timezone.utc).isoformat()})
            print("[+] Klaim otomatis menjadi success.")
            return 0
        print("[-] Belum tepat 20 — status tetap 'proses', admin "
              "bisa klik Proses lagi untuk menambah sisanya saja.")
        return 1
    return 0 if total >= TARGET_COUNT else 1


def main():
    if len(sys.argv) < 2:
        print("Pakai: python follow_free_cloud.py "
              "\"<TARGET_UUID atau Nama#Kode>\" \"<CLAIM_ID opsional>\"")
        return 1
    target = sys.argv[1].strip()
    claim_id = sys.argv[2].strip() if len(sys.argv) > 2 else ""
    return asyncio.run(main_async(target, claim_id))


if __name__ == "__main__":
    if os.name == "nt":
        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy())
    sys.exit(main())
