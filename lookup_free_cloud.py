"""lookup_free_cloud.py — Poller UUID klaim gratis (mirip lookup_poll.py
untuk order berbayar, tapi untuk tabel free_follower_claims).

Kenapa file baru, bukan mengubah lookup_poll.py: klaim gratis BUKAN
order (tidak ada di tabel orders, tidak memicu trigger order), jadi
poller order tidak pernah melihatnya. Itu sebabnya klaim yang dibuat
di preview diam saja: tidak ada Action yang jalan dan tidak ada
Telegram. Poller ini menutup celah itu, tanpa mengubah file lama.

Tiap run:
- Ambil klaim status='pending' yang uuid_status-nya NULL / 'pending'
  (maks 5, terlama dulu), lookup UUID lewat lookup_uuid.py, update:
    ketemu      -> player_uuid + uuid_status='found'
    tidak ada   -> uuid_status='not_found'
    gagal/error -> uuid_status='error'
- Kirim Telegram dari runner ini (format mirip notif order, tapi
  judulnya KLAIM GRATIS dan statusnya "follow belum jalan — tunggu
  admin klik Proses"). Bila Telegram gagal setelah retry, klaim
  ditandai belum ternotifikasi (notif_sent_at NULL) dan sweep run
  berikut mengirim ulangnya TANPA lookup ulang.
- Sweep: tiap run juga mengirim ulang klaim found/not_found 7 hari
  terakhir yang notif_sent_at-nya masih NULL.

Follow TIDAK dijalankan di sini — follow tetap di belakang tombol
Proses admin (workflow follow-free.yml), sesuai gerbang konfirmasi
admin. Poller ini hanya: cari UUID + beri tahu admin lewat Telegram.

Jalankan via .github/workflows/lookup-free.yml (cron 10 menit +
repository_dispatch 'new-claim' dari trigger database toko +
manual dispatch). Env (Secrets repo, TIDAK ADA nilai kredensial
di file ini):
  SUPABASE_URL / SUPABASE_SERVICE_KEY = database TOKO (klaim +
    private_settings Telegram). Di workflow, keduanya dipetakan dari
    SHOP_SUPABASE_URL / SHOP_SUPABASE_SERVICE_KEY.
  PANEL_SUPABASE_URL / PANEL_SUPABASE_SERVICE_KEY = versi game panel.
  PROBE_* / FSTR_* = akun lookup (dibaca lookup_uuid.py).
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
from lookup_uuid import lookup_once, probe_bot, apply_panel_versions  # noqa: E402
from curl_cffi.requests import AsyncSession  # noqa: E402

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
BATCH_LIMIT = 5
TIME_BUDGET = 240
NOTIF_ATTEMPTS = 3
NOTIF_TIMEOUT = 5
NOTIF_BACKOFF = (3, 8)
SWEEP_DAYS = 7
SWEEP_LIMIT = 20
DASHBOARD_URL = "https://pkxd.shop/#/admin"

CLAIM_FIELDS = (
    "id,user_id,customer_pkxd_id,customer_whatsapp,claimed_date,"
    "amount,created_at,player_uuid,uuid_status,notif_sent_at"
)

_TG = None


def sb_req(method, path, body=None):
    if not SUPABASE_URL or not SERVICE_KEY:
        return None
    req = urllib.request.Request(
        SUPABASE_URL + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"apikey": SERVICE_KEY,
                 "Authorization": "Bearer " + SERVICE_KEY,
                 "Content-Type": "application/json",
                 "Prefer": "return=representation"},
        method=method)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode() or "null")
    except urllib.error.HTTPError as e:
        print(f"[-] Supabase {method} {path} HTTP {e.code}.")
        return None
    except Exception as e:
        print(f"[-] Supabase {method} {path} gagal: {e}.")
        return None


def esc(s):
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;"))


def fmt_wib(iso):
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        wib = dt.astimezone(timezone(timedelta(hours=7)))
        return wib.strftime("%d %b %Y, %H:%M")
    except (ValueError, TypeError):
        return "-"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def get_telegram_settings():
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


def build_message(klaim, status):
    pkxd = klaim.get("customer_pkxd_id") or "-"
    email_akun = None
    uid = klaim.get("user_id")
    if uid:
        rows = sb_req("GET", f"/rest/v1/profiles?select=email&id=eq.{uid}")
        if rows:
            email_akun = rows[0].get("email")
    if status == "found":
        judul = "🆔 <b>KLAIM GRATIS — UUID DIDAPAT</b>"
        status_line = ("Status: <b>UUID ditemukan — follow belum jalan, "
                        "tunggu admin klik Proses di dashboard Klaim "
                        "Gratis</b>.")
    else:
        judul = "⚠️ <b>KLAIM GRATIS — UUID TIDAK DITEMUKAN</b>"
        status_line = ("Status: <b>UUID tidak ditemukan — cek Username "
                        "&amp; ID klaim, proses manual</b>.")
    teks = (
        f"{judul}\n\n"
        f"📦 Paket: <b>Gratis Harian — 20 Followers</b>\n"
        f"👤 Username &amp; ID: <code>{esc(pkxd)}</code>\n")
    if status == "found" and klaim.get("player_uuid"):
        teks += f"🆔 UUID Pemain: <code>{esc(klaim['player_uuid'])}</code>\n"
    teks += f"🧾 Akun: {esc(email_akun) if email_akun else 'Tamu (tanpa akun)'}\n"
    wa = klaim.get("customer_whatsapp")
    if wa:
        teks += f"📱 WhatsApp: <a href=\"https://wa.me/{wa}\">{wa}</a>\n"
    else:
        teks += "📱 WhatsApp: -\n"
    teks += (f"⏰ Waktu klaim: {fmt_wib(klaim.get('created_at'))} WIB\n\n"
             f"{status_line}")
    baris = []
    if pkxd and pkxd != "-":
        baris.append({"text": "📋 Salin ID", "copy_text": {"text": pkxd}})
    if wa:
        baris.append({"text": "💬 Chat WA", "url": f"https://wa.me/{wa}"})
    baris.append({"text": "🖥️ Dashboard", "url": DASHBOARD_URL})
    keyboard = [baris]
    if status == "found" and klaim.get("player_uuid"):
        keyboard.append([{"text": "📋 Salin UUID",
                          "copy_text": {"text": klaim["player_uuid"]}}])
    return teks, {"inline_keyboard": keyboard}


def send_telegram(text, markup):
    tg = get_telegram_settings()
    token = tg.get("telegram_bot_token") or ""
    chat_id = tg.get("telegram_chat_id") or ""
    aktif = tg.get("telegram_aktif")
    if (aktif or "1") != "1" or not token or not chat_id:
        print("[*] Notif Telegram nonaktif/belum diatur — dilewati.")
        return None
    body = json.dumps({
        "chat_id": chat_id, "text": text, "parse_mode": "HTML",
        "disable_web_page_preview": True, "reply_markup": markup,
    }).encode()
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
            print(f"[-] Notif percobaan {attempt}: Telegram ok="
                  f"{resp.get('ok')}.")
        except urllib.error.HTTPError as e:
            print(f"[-] Notif percobaan {attempt}: HTTP {e.code}.")
        except Exception as e:
            print(f"[-] Notif percobaan {attempt}: {type(e).__name__}.")
        if attempt < NOTIF_ATTEMPTS:
            time.sleep(NOTIF_BACKOFF[attempt - 1])
    return False


def notify_klaim(klaim, status):
    text, markup = build_message(klaim, status)
    ok = send_telegram(text, markup)
    if ok is True:
        sb_req("PATCH", f"/rest/v1/free_follower_claims?id=eq.{klaim['id']}",
               {"notif_sent_at": now_iso()})
        return True
    return ok


async def main():
    if not SUPABASE_URL or not SERVICE_KEY:
        print("[!] SUPABASE_URL / SUPABASE_SERVICE_KEY wajib diisi.")
        return 1
    apply_panel_versions()
    t0 = time.time()
    notif_failed = False

    klaim_pending = sb_req(
        "GET",
        "/rest/v1/free_follower_claims?select=" + CLAIM_FIELDS +
        "&status=eq.pending&or=(uuid_status.is.null,uuid_status.eq.pending)"
        "&order=created_at.asc" f"&limit={BATCH_LIMIT}")
    if klaim_pending is None:
        return 1
    print(f"[*] Klaim pending UUID: {len(klaim_pending)}")

    bot = probe_bot()
    for klaim in klaim_pending:
        if time.time() - t0 > TIME_BUDGET:
            print("[*] Batas waktu run tercapai, sisa dilanjut run berikut.")
            break
        pkxd = klaim.get("customer_pkxd_id") or ""
        print(f"[*] Lookup klaim {klaim['id'][:8]}: {pkxd}")
        hit = None
        async with AsyncSession(impersonate="chrome110",
                                verify=False) as session:
            for attempt in range(1, 3):
                hit = await lookup_once(session, bot, pkxd)
                if hit:
                    break
                print(f"[-] Lookup percobaan {attempt} kosong.")
        if hit:
            klaim["player_uuid"] = hit["uuid"]
            klaim["uuid_status"] = "found"
            sb_req("PATCH",
                   f"/rest/v1/free_follower_claims?id=eq.{klaim['id']}",
                   {"player_uuid": hit["uuid"], "uuid_status": "found"})
            print(f"[+] UUID: {pkxd} -> {hit['uuid']}")
            ok = notify_klaim(klaim, "found")
        else:
            klaim["uuid_status"] = "not_found"
            sb_req("PATCH",
                   f"/rest/v1/free_follower_claims?id=eq.{klaim['id']}",
                   {"uuid_status": "not_found"})
            print(f"[-] UUID tidak ditemukan: {pkxd}")
            ok = notify_klaim(klaim, "not_found")
        if ok is False:
            notif_failed = True

    # Sweep: kirim ulang klaim yang UUID-nya sudah ketemu tapi
    # Telegram-nya belum terkirim (run crash di tengah jalan).
    sejak = (datetime.now(timezone.utc) - timedelta(days=SWEEP_DAYS))
    sweep = sb_req(
        "GET",
        "/rest/v1/free_follower_claims?select=" + CLAIM_FIELDS +
        "&status=eq.pending&uuid_status=in.(found,not_found)"
        "&notif_sent_at=is.null"
        f"&created_at=gte.{sejak.isoformat()}"
        "&order=created_at.asc" f"&limit={SWEEP_LIMIT}") or []
    for klaim in sweep:
        if time.time() - t0 > TIME_BUDGET:
            break
        ok = notify_klaim(klaim, klaim["uuid_status"])
        if ok is False:
            notif_failed = True

    print("[+] Poller klaim selesai.")
    return 1 if notif_failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
