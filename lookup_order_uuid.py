"""
lookup_order_uuid.py — Runner lookup UUID untuk order baru.

Alur yang diminta pemilik:
  Order dibuat > script pencari UUID di GitHub Actions jalan
  berdasarkan nama customer > jika UUID didapatkan, langsung
  kirim ke Telegram.

Kenapa runner ini ada:
  - Tabel orders tidak punya UUID saat INSERT, hanya
    customer_pkxd_id (Nama#Kode). Trigger Telegram di Supabase
    sudah diubah (lihat supabase/migrasi-uuid-order.sql di repo
    website) supaya DIAM saat INSERT tanpa UUID, dan baru
    mengirim saat customer_uuid terisi lewat UPDATE.
  - lookup_uuid.py yang sudah ada hanya mencetak HASIL dan
    menulis log lokal, tidak menulis balik ke orders. Runner
    ini yang menjembatani: jalankan lookup_uuid.py per order,
    lalu PATCH baris order tersebut di Supabase.
  - Begitu PATCH berhasil, trigger UPDATE di database yang
    mengirim notifikasi Telegram lengkap dengan tombol
    Salin UUID. Runner ini sendiri TIDAK mengirim Telegram
    langsung, supaya tidak ada notifikasi ganda.

Cara jalan:
  - Mode polling (workflow terjadwal): ambil order dengan
    customer_uuid IS NULL dan uuid_status = 'pending',
    urut dari yang terlama, maksimal LOOKUP_LIMIT order
    (default 3) per run.
  - Mode satu order (workflow_dispatch manual / retry):
    isi env ORDER_ID dan CUSTOMER_NAME, maka hanya order
    itu yang diproses, apa pun uuid_status-nya sekarang.
    Ini cara mengulang order yang sebelumnya gagal.

Env yang dipakai (dari GitHub Secrets di workflow, jangan
pernah ditulis hardcode di sini):
  SUPABASE_URL, SUPABASE_SERVICE_KEY
  (lookup_uuid.py sendiri memakai PROBE_*, FSTR_* lewat env
  yang sama seperti workflow lookup-uuid.yml)

Keamanan log:
  - Service key TIDAK PERNAH dicetak.
  - UUID lengkap TIDAK dicetak ke log Actions; hanya 8 karakter
    pertama untuk penanda. UUID lengkap hanya dikirim lewat
    PATCH ke Supabase, lalu Telegram yang menampilkannya
    ke admin lewat tombol Salin UUID.
  - Jika lookup gagal (script sudah mencoba 3x di dalamnya),
    order ditandai uuid_status='failed' dan TIDAK dikirimi
    Telegram, sesuai alur "jika didapatkan, langsung kirim".
    Admin bisa mengulang order itu lewat mode satu order
    (setelah memperbaiki nama, atau lewat dashboard dengan
    mengembalikan uuid_status ke 'pending').
"""

import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

UUID_RE = re.compile(
    r"HASIL:\s*.+?->\s*"
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.IGNORECASE,
)


def supabase_request(method, path, body=None):
    url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise SystemExit(
            "SUPABASE_URL / SUPABASE_SERVICE_KEY belum diisi di env. "
            "Isi lewat GitHub Secrets di workflow, jangan hardcode."
        )
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url + path,
        data=data,
        method=method,
        headers={
            "apikey": key,
            "Authorization": "Bearer " + key,
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode()
        return json.loads(raw) if raw else None


def fetch_pending_orders(limit):
    query = (
        "/rest/v1/orders"
        "?select=id,customer_pkxd_id,code"
        "&customer_uuid=is.null"
        "&uuid_status=eq.pending"
        "&order=created_at.asc"
        f"&limit={limit}"
    )
    rows = supabase_request("GET", query)
    return rows or []


def patch_order(order_id, payload):
    supabase_request(
        "PATCH",
        f"/rest/v1/orders?id=eq.{order_id}",
        body=payload,
    )


def run_lookup(customer_name):
    """Jalankan lookup_uuid.py yang sudah ada, kembalikan UUID atau None."""
    try:
        proc = subprocess.run(
            [sys.executable, "lookup_uuid.py", customer_name],
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired:
        print(f"[-] Timeout lookup untuk: {customer_name}")
        return None
    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
    match = UUID_RE.search(output)
    if match:
        return match.group(1).lower()
    return None


def process_one(order_id, customer_name, code=""):
    label = f"order {order_id} ({code})" if code else f"order {order_id}"
    if not customer_name or not customer_name.strip():
        print(f"[-] {label}: nama customer kosong, tandai failed.")
        patch_order(order_id, {"uuid_status": "failed"})
        return "failed"
    name = customer_name.strip()
    print(f"[*] {label}: lookup UUID untuk: {name}")
    uuid = run_lookup(name)
    if uuid:
        now = datetime.now(timezone.utc).isoformat()
        patch_order(
            order_id,
            {
                "customer_uuid": uuid,
                "uuid_status": "found",
                "uuid_lookup_at": now,
            },
        )
        print(
            f"[+] {label}: UUID didapatkan ({uuid[:8]}...), "
            "order diupdate — trigger Telegram di database yang "
            "akan mengirim notifikasi dengan tombol Salin UUID."
        )
        return "found"
    print(f"[-] {label}: UUID tidak ditemukan, tandai failed (tanpa Telegram).")
    patch_order(order_id, {"uuid_status": "failed"})
    return "failed"


def main():
    single_id = os.environ.get("ORDER_ID", "").strip()
    single_name = os.environ.get("CUSTOMER_NAME", "").strip()
    if single_id and single_name:
        result = process_one(single_id, single_name)
        print(f"Selesai mode satu order: {result}")
        return
    try:
        limit = int(os.environ.get("LOOKUP_LIMIT", "3"))
    except ValueError:
        limit = 3
    orders = fetch_pending_orders(limit)
    if not orders:
        print("Tidak ada order pending UUID. Selesai.")
        return
    print(f"Ditemukan {len(orders)} order pending UUID.")
    summary = {"found": 0, "failed": 0}
    for order in orders:
        try:
            result = process_one(
                order["id"],
                order.get("customer_pkxd_id", ""),
                order.get("code", ""),
            )
            summary[result] += 1
        except (urllib.error.HTTPError, urllib.error.URLError) as exc:
            print(f"[-] Gagal Supabase untuk order {order.get('id')}: {exc}")
            summary["failed"] += 1
        except Exception as exc:  # noqa: BLE001 - lanjut ke order berikutnya
            print(f"[-] Gagal tak terduga untuk order {order.get('id')}: {exc}")
            summary["failed"] += 1
    print(f"Selesai: found={summary['found']}, failed={summary['failed']}")


if __name__ == "__main__":
    main()
