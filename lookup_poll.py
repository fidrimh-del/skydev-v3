"""lookup_poll.py — Poller UUID pesanan pkxd.shop (GitHub Actions).

Tiap run: ambil pesanan uuid_status='pending' (maks 5, terlama dulu),
lookup UUID tiap pesanan lewat lookup_uuid.py (jalur terbukti tahap 16),
lalu update barisnya:
  ketemu      -> player_uuid + uuid_status='found'
  tidak ada   -> uuid_status='not_found'
  gagal/error -> uuid_status='error' + uuid_error
Berhenti memulai order baru setelah 240 detik (batas aman job 5 menit);
sisa pesanan dilanjutkan run berikut (tiap 10 menit).

Jalankan terjadwal via .github/workflows/lookup-poll.yml (+ manual
dispatch). Env wajib (Secrets repo): SUPABASE_URL,
SUPABASE_SERVICE_KEY (service key melewati RLS untuk update).
Kredensial bot: dari lookup_uuid.probe_bot() (env Secrets menang,
fallback hardcode atas perintah pemilik — hapus saat produksi).
"""

import asyncio
import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lookup_uuid import lookup_once, probe_bot  # noqa: E402
from curl_cffi.requests import AsyncSession  # noqa: E402

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
BATCH_LIMIT = 5
TIME_BUDGET = 240


def sb_req(method, path, body=None):
    req = urllib.request.Request(SUPABASE_URL + path, method=method)
    req.add_header("apikey", SERVICE_KEY)
    req.add_header("Authorization", f"Bearer {SERVICE_KEY}")
    req.add_header("Content-Type", "application/json")
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.urlopen(req, data=data, timeout=30) as r:
            return json.loads(r.read().decode() or "[]")
    except urllib.error.HTTPError as e:
        print(f"[-] Supabase {method} {path}: HTTP {e.code} "
              f"{e.read().decode()[:200]}")
        return None
    except urllib.error.URLError as e:
        print(f"[-] Supabase {method} {path}: network error {e.reason}")
        return None


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


async def main():
    if not SUPABASE_URL or not SERVICE_KEY:
        print("[!] SUPABASE_URL / SUPABASE_SERVICE_KEY wajib diisi (Secrets).")
        sys.exit(1)
    orders = sb_req(
        "GET",
        "/rest/v1/orders?select=id,customer_pkxd_id"
        "&uuid_status=eq.pending&order=created_at.asc"
        f"&limit={BATCH_LIMIT}")
    if orders is None:
        sys.exit(1)
    if not orders:
        print("[*] Tidak ada pesanan pending — selesai.")
        return
    print(f"[*] {len(orders)} pesanan pending ditemukan.")
    bot = probe_bot()
    t0 = time.time()
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        for o in orders:
            if time.time() - t0 > TIME_BUDGET:
                print("[*] Batas waktu 240 detik — berhenti, "
                      "sisa dilanjut run berikut.")
                break
            oid, pkxd = o.get("id"), (o.get("customer_pkxd_id") or "")
            print(f"[*] Order {str(oid)[:8]}: {pkxd}")
            if "#" not in pkxd:
                sb_req("PATCH", f"/rest/v1/orders?id=eq.{oid}",
                       {"uuid_status": "error",
                        "uuid_error": "Format ID bukan Nama#Kode",
                        "uuid_checked_at": now_iso()})
                continue
            hit, err = None, None
            try:
                # 3 instance per order (fail-fast join membuat tiap
                # instance murah; peluang kena instance sehat naik).
                for _ in range(3):
                    hit = await lookup_once(session, bot, pkxd)
                    if hit:
                        break
            except Exception as e:
                err = str(e)[:200]
            if hit:
                sb_req("PATCH", f"/rest/v1/orders?id=eq.{oid}",
                       {"player_uuid": hit["uuid"],
                        "uuid_status": "found",
                        "uuid_checked_at": now_iso()})
                print(f"[+] {pkxd} -> {hit['uuid']}")
            elif err:
                sb_req("PATCH", f"/rest/v1/orders?id=eq.{oid}",
                       {"uuid_status": "error",
                        "uuid_error": err,
                        "uuid_checked_at": now_iso()})
                print(f"[-] {pkxd}: error {err}")
            else:
                sb_req("PATCH", f"/rest/v1/orders?id=eq.{oid}",
                       {"uuid_status": "not_found",
                        "uuid_checked_at": now_iso()})
                print(f"[-] {pkxd}: tidak ditemukan")
    print(f"[*] Poller selesai ({time.time() - t0:.0f} detik).")


if __name__ == "__main__":
    asyncio.run(main())
