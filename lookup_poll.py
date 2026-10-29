"""lookup_poll.py — Poller UUID pesanan pkxd.shop (GitHub Actions).

Tiap run: ambil pesanan uuid_status='pending' (maks 5, terlama dulu),
lookup UUID tiap pesanan lewat lookup_uuid.py (jalur terbukti tahap 16),
lalu update barisnya:
  ketemu      -> player_uuid + uuid_status='found'
  tidak ada   -> uuid_status='not_found'
  gagal/error -> uuid_status='error' + uuid_error
Berhenti memulai order baru setelah 240 detik (batas aman job 5 menit);
sisa pesanan dilanjutkan run berikut (tiap 10 menit).

NOTIFIKASI TELEGRAM (pindah dari trigger database ke Action, 2026-10-29):
setiap pesanan yang berhasil diubah statusnya menjadi found/not_found/
error langsung dikirimi pesan Telegram dari runner ini (isi persis sama
dengan format lama fungsi kirim_notif_telegram: judul, paket, username,
UUID, akun, WA, total, waktu WIB, tombol Salin ID/WA/Dashboard/Salin
UUID). Token dibaca dari private_settings toko lewat service key yang
sudah dipakai poller — TIDAK PERNAH di-print ke log (URL pun tidak).
Kirim di-retry 3x (backoff 3/8 detik, timeout 10 detik); kalau tetap
gagal, run ini exit 1 supaya kegagalan TERLIHAT di log Action (bukan
ditelan diam-diam), dan pesanan ditandai belum ternotifikasi sehingga
sweep run berikut mengirim ulangnya. Trigger UPDATE database
(trg_notif_telegram_uuid) DINONAKTIFKAN agar tidak kirim dobel.
Kolom penanda: orders.notif_sent_at (NULL = belum terkirim).

Sweep: tiap run juga mengirim ulang pesanan found/not_found/error
7 hari terakhir yang notif_sent_at-nya masih NULL (sisa run yang
crash tepat sesudah update status tapi sebelum kirim).

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
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lookup_uuid import lookup_once, probe_bot  # noqa: E402
from curl_cffi.requests import AsyncSession  # noqa: E402

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
BATCH_LIMIT = 5
TIME_BUDGET = 240
NOTIF_ATTEMPTS = 3
NOTIF_TIMEOUT = 10
NOTIF_BACKOFF = (3, 8)
SWEEP_DAYS = 7
SWEEP_LIMIT = 20
DASHBOARD_URL = "https://pkxd-shop-v2.vercel.app/#/admin/pesanan"

ORDER_FIELDS = (
    "id,code,customer_pkxd_id,customer_user_id,customer_whatsapp,"
    "package_snapshot,payment_method,subtotal,discount,total,promo_code,"
    "created_at,player_uuid,uuid_error,uuid_status"
)

_TG = None


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


def esc(s):
    return (str(s).replace("&", "&amp;")
            .replace("<", "&lt;").replace(">", "&gt;"))


def fmt_rp(n):
    try:
        return f"{int(n):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "0"


def fmt_wib(iso):
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        wib = dt.astimezone(timezone(timedelta(hours=7)))
        return wib.strftime("%d %b %Y, %H:%M")
    except (ValueError, TypeError):
        return "-"


def get_telegram_settings():
    """Ambil token/chat_id/aktif dari private_settings (cache per run).
    Nilai TIDAK PERNAH di-print."""
    global _TG
    if _TG is not None:
        return _TG
    rows = sb_req(
        "GET",
        "/rest/v1/private_settings?select=key,value"
        "&key=in.(telegram_bot_token,telegram_chat_id,telegram_aktif)")
    _TG = {}
    if rows:
        for r in rows:
            _TG[r.get("key")] = r.get("value")
    return _TG


def build_message(order, status):
    """Susun (teks, reply_markup) persis format lama kirim_notif_telegram."""
    snap = order.get("package_snapshot") or {}
    paket_label = (
        f"{snap.get('name', 'Paket PK XD')} — {snap.get('amount', '')}"
        + (" Like Postingan" if snap.get("type") == "likes" else " Followers"))
    pkxd = order.get("customer_pkxd_id") or "-"
    email_akun = None
    uid = order.get("customer_user_id")
    if uid:
        rows = sb_req("GET", f"/rest/v1/profiles?select=email&id=eq.{uid}")
        if rows:
            email_akun = rows[0].get("email")
    if status == "found":
        judul = "🆔 <b>ORDER BARU — UUID DIDAPAT</b>"
        status_line = "Status: <b>UUID ditemukan — siap diproses</b>."
    else:
        judul = "⚠️ <b>UUID TIDAK DITEMUKAN</b>"
        status_line = ("Status: <b>UUID tidak ditemukan — cek Username &amp; ID, "
                       "proses manual</b>.")
    teks = (
        f"{judul} — <code>{esc(order.get('code') or '-')}</code>\n\n"
        f"📦 Paket: <b>{esc(paket_label)}</b>\n"
        f"👤 Username &amp; ID: <code>{esc(pkxd)}</code>\n")
    if status == "found" and order.get("player_uuid"):
        teks += f"🆔 UUID Pemain: <code>{esc(order['player_uuid'])}</code>\n"
    if status in ("not_found", "error") and order.get("uuid_error"):
        teks += f"❌ Keterangan: {esc(order['uuid_error'])}\n"
    teks += (
        f"🧾 Akun: {esc(email_akun) if email_akun else 'Tamu (tanpa akun)'}\n")
    wa = order.get("customer_whatsapp")
    if wa:
        teks += f"📱 WhatsApp: <a href=\"https://wa.me/{wa}\">{wa}</a>\n"
    else:
        teks += "📱 WhatsApp: -\n"
    if (order.get("discount") or 0) > 0:
        teks += (f"🧮 Subtotal: Rp{fmt_rp(order.get('subtotal'))} · "
                 f"Diskon: -Rp{fmt_rp(order.get('discount'))}\n")
    teks += (f"💳 {order.get('payment_method') or '-'} · "
             f"💰 Total: <b>Rp{fmt_rp(order.get('total'))}</b>\n")
    if order.get("promo_code"):
        teks += f"🏷️ Promo: {order['promo_code']}\n"
    teks += f"⏰ Waktu: {fmt_wib(order.get('created_at'))} WIB\n\n{status_line}"

    baris = []
    if pkxd and pkxd != "-":
        baris.append({"text": "📋 Salin ID",
                      "copy_text": {"text": pkxd}})
    if wa:
        baris.append({"text": "💬 Chat WA", "url": f"https://wa.me/{wa}"})
    baris.append({"text": "🖥️ Dashboard", "url": DASHBOARD_URL})
    keyboard = [baris]
    if status == "found" and order.get("player_uuid"):
        keyboard.append([{"text": "📋 Salin UUID",
                          "copy_text": {"text": order["player_uuid"]}}])
    return teks, {"inline_keyboard": keyboard}


def send_telegram(text, markup):
    """Kirim 1 pesan dengan retry. TIDAK PERNAH log token/URL/body.
    Return True hanya bila Telegram menjawab ok=true."""
    tg = get_telegram_settings()
    token = tg.get("telegram_bot_token") or ""
    chat_id = tg.get("telegram_chat_id") or ""
    aktif = tg.get("telegram_aktif")
    if (aktif or "1") != "1" or not token or not chat_id:
        print("[*] Notif Telegram nonaktif/belum diatur — dilewati.")
        return None
    body = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "reply_markup": markup,
    }).encode()
    # Token wajib ada di path URL Bot API — ia hanya ada di memori
    # objek request ini dan TIDAK PERNAH di-print ke log mana pun.
    url = "https://api.telegram.org/bot" + token + "/sendMessage"
    for attempt in range(1, NOTIF_ATTEMPTS + 1):
        try:
            req = urllib.request.Request(url, data=body, method="POST")
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=NOTIF_TIMEOUT) as r:
                resp = json.loads(r.read().decode() or "{}")
            if resp.get("ok") is True:
                print("[+] Notif Telegram terkirim (Telegram 200).")
                return True
            print(f"[-] Notif percobaan {attempt}: Telegram menjawab "
                  f"ok={resp.get('ok')}.")
        except urllib.error.HTTPError as e:
            print(f"[-] Notif percobaan {attempt}: HTTP {e.code}.")
        except urllib.error.URLError as e:
            print(f"[-] Notif percobaan {attempt}: network error {e.reason}.")
        except Exception as e:
            print(f"[-] Notif percobaan {attempt}: {type(e).__name__}.")
        if attempt < NOTIF_ATTEMPTS:
            time.sleep(NOTIF_BACKOFF[attempt - 1])
    return False


def notify_order(order, status):
    """Kirim notif untuk 1 order; bila sukses, tandai notif_sent_at.
    Return True (terkirim), False (gagal setelah retry), None (dilewati
    karena nonaktif)."""
    text, markup = build_message(order, status)
    ok = send_telegram(text, markup)
    if ok is True:
        sb_req("PATCH", f"/rest/v1/orders?id=eq.{order['id']}",
               {"notif_sent_at": now_iso()})
        return True
    return ok


async def main():
    if not SUPABASE_URL or not SERVICE_KEY:
        print("[!] SUPABASE_URL / SUPABASE_SERVICE_KEY wajib diisi (Secrets).")
        return 1
    notif_failed = False
    orders = sb_req(
        "GET",
        "/rest/v1/orders?select=" + ORDER_FIELDS +
        "&uuid_status=eq.pending&order=created_at.asc"
        f"&limit={BATCH_LIMIT}")
    if orders is None:
        return 1
    if not orders:
        print("[*] Tidak ada pesanan pending.")
    else:
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
                res = sb_req("PATCH", f"/rest/v1/orders?id=eq.{oid}",
                             {"uuid_status": "error",
                              "uuid_error": "Format ID bukan Nama#Kode",
                              "uuid_checked_at": now_iso()})
                if res is not None:
                    o["uuid_status"] = "error"
                    o["uuid_error"] = "Format ID bukan Nama#Kode"
                    if notify_order(o, "error") is False:
                        notif_failed = True
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
                res = sb_req("PATCH", f"/rest/v1/orders?id=eq.{oid}",
                             {"player_uuid": hit["uuid"],
                              "uuid_status": "found",
                              "uuid_checked_at": now_iso()})
                print(f"[+] {pkxd} -> {hit['uuid']}")
                if res is not None:
                    o["player_uuid"] = hit["uuid"]
                    o["uuid_status"] = "found"
                    if time.time() - t0 > TIME_BUDGET:
                        print("[*] Batas waktu — notif ditunda ke sweep "
                              "run berikut.")
                        break
                    if notify_order(o, "found") is False:
                        notif_failed = True
            elif err:
                res = sb_req("PATCH", f"/rest/v1/orders?id=eq.{oid}",
                             {"uuid_status": "error",
                              "uuid_error": err,
                              "uuid_checked_at": now_iso()})
                print(f"[-] {pkxd}: error {err}")
                if res is not None:
                    o["uuid_status"] = "error"
                    o["uuid_error"] = err
                    if notify_order(o, "error") is False:
                        notif_failed = True
            else:
                res = sb_req("PATCH", f"/rest/v1/orders?id=eq.{oid}",
                             {"uuid_status": "not_found",
                              "uuid_checked_at": now_iso()})
                print(f"[-] {pkxd}: tidak ditemukan")
                if res is not None:
                    o["uuid_status"] = "not_found"
                    if notify_order(o, "not_found") is False:
                        notif_failed = True
        # Sweep: pesanan ter-resolve 7 hari terakhir yang notifnya
        # belum terkirim (sisa run yang crash sesudah update status).
        if time.time() - t0 <= TIME_BUDGET:
            since = (datetime.now(timezone.utc)
                     - timedelta(days=SWEEP_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
            leftovers = sb_req(
                "GET",
                "/rest/v1/orders?select=" + ORDER_FIELDS +
                "&uuid_status=in.(found,not_found,error)"
                "&notif_sent_at=is.null"
                f"&created_at=gte.{since}&order=created_at.asc"
                f"&limit={SWEEP_LIMIT}")
            if leftovers:
                print(f"[*] Sweep: {len(leftovers)} pesanan belum "
                      "ternotifikasi.")
                for o in leftovers:
                    st = o.get("uuid_status")
                    print(f"[*] Sweep notif {str(o.get('id'))[:8]}: {st}")
                    if notify_order(o, st) is False:
                        notif_failed = True
    print(f"[*] Poller selesai ({time.time() - t0:.0f} detik).")
    if notif_failed:
        print("[!] ADA NOTIFIKASI GAGAL setelah retry — cek log di atas; "
              "sweep run berikut akan mengirim ulangnya.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
