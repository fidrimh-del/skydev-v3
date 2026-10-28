"""like_profile_cloud.py — Kirim like profil PK XD massal via TCP (produksi).

Mesin yang dipakai (semuanya sudah TERBUKTI live, bukan tebakan):
  - Login/TCP/FrameReader/builder/decode: lookup_uuid.py + v2
    (sudah terbukti di probe run 1-5 + poller produksi).
  - Like: RoomEvent "like" + CreateLikeRequest (EventId=0, LikedUser=UUID,
    LikeType=1/Player); verifikasi andalan = IsThere-sesudah
    (balasan create_like_result tidak pernah tiba — jangan ditunggu).

CARA PAKAI:
  python like_profile_cloud.py <TARGET> <JUMLAH> [MAX_PARALEL]
    TARGET      UUID (36 karakter) atau Nama#Kode (dicari UUID-nya dulu
                lewat satu sesi lookup sebelum worker jalan).
    JUMLAH      jumlah like BARU yang diminta.
    MAX_PARALEL jumlah worker bersamaan, default 20, dijepit 1..20.
  Di GitHub Actions: pakai like-profile.yml (input target/jumlah/
  max_paralel). Di dashboard panel: kartu "Like profil" (tab Like)
  memanggil workflow itu lewat executeAndWait yang sudah ada.

ATURAN KERAS (jangan dilanggar):
  - Create dikirim SEKALI per bot (aksi tulis; 2x berisiko +2).
  - Satu bot dipakai tepat satu tugas (tidak ada dua thread memakai
    bot yang sama; tidak ada pasangan (bot, target) yang dikirim 2x
    dalam satu job).
  - Bot yang sudah pernah like target DILEWATI: dulu lewat histori
    Supabase (satu query massal, tanpa sesi), sisanya lewat
    IsThere-sebelum di dalam like_once (pemanasan + catat histori).
  - Histori HANYA ditulis sesudah IsThere-sesudah = True.
  - Kredensial TIDAK PERNAH dicetak ke log (hanya indeks bot).
  - Server menduplikasi per pasangan (bot, target): like kedua dari
    bot yang sama tidak menambah hitungan — jadi pengulangan tugas
    karena gagal tidak akan menggandakan like.

REPLENISHMENT (v3.1): selama terkirim < jumlah dan masih ada bot
  fresh, fetch ulang dengan limit membesar (deficit + 30, plafon
  fetch 5000 bot) dan jalankan bot fresh berikutnya (maks 4 ronde
  fetch); terakhir,
  bot yang gagal dicoba ulang sekali. Jaminan: MINIMAL jumlah
  bila stok bot cukup — bukan persis jumlah (overshoot wave
  paralel tetap berlaku; kelebihan itu bonus, bukan kerugian).

KELUAR (exit code):
  - 0 = terkirim >= jumlah yang diminta.
  - 1 = terkirim < jumlah (stok bot fresh habis, gagal berulang,
    atau target tidak ditemukan). Rincian pasti selalu tercetak
    di blok HASIL (terkirim / diminta / dilewati-histori /
    dilewati-sesi / gagal).

CATATAN JUJUR TENTANG WAKTU:
  - Waktu KERJA script: ~15 detik per like baru (satu worker);
    dengan 20 worker paralel, 100 like ≈ 100/20 x 15 detik ≈ 75 detik
    + overhead login (bukan 100 x 15 detik).
  - Counter profil di game adalah cache per sesi: angka tampil baru
    diperbarui saat buka ulang game atau ganti server (koreksi
    2026-10-28: BUKAN delay agregasi server). Jangan menilai job
    dari counter dalam sesi yang sama; nilailah dari blok HASIL
    (IsThere-sesudah) + log Actions, lalu konfirmasi counter
    sesudah buka ulang/ganti server.
"""

import asyncio
import json
import os
import random
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import lookup_uuid as L
import like_profile_probe as P

UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE)


# ---------------------------------------------------------------- fungsi murni
def is_uuid(s):
    return bool(UUID_RE.match((s or "").strip()))


def clamp_paralel(n, default=20):
    try:
        v = int(n)
    except (TypeError, ValueError):
        return default
    return max(1, min(20, v))


def parse_args(argv):
    """argv[1] = target, argv[2] = jumlah, argv[3] = max_paralel (opsional).
    Kembalikan (target, jumlah, max_paralel) atau None kalau tidak sah."""
    if len(argv) < 3:
        return None
    target = argv[1].strip()
    try:
        jumlah = int(argv[2])
    except ValueError:
        return None
    if not target or jumlah < 1:
        return None
    max_paralel = clamp_paralel(argv[3] if len(argv) > 3 else 20)
    return target, jumlah, max_paralel


def decide_exit(sent, jumlah):
    """0 = target terpenuhi, 1 = kurang (apa pun sebabnya)."""
    return 0 if sent >= jumlah else 1


def fresh_bots(pool, tried, history):
    """Bot yang belum pernah ditugaskan dan belum ada di histori.
    history=None -> filter histori dilewati (fallback IsThere)."""
    out = []
    for b in pool or []:
        email = b.get("email")
        if not email or email in tried:
            continue
        if history is not None and email in history:
            continue
        out.append(b)
    return out


def next_limit(limit, deficit):
    """Limit fetch ronde berikutnya: membesar sejumlah deficit + cadangan."""
    return min(limit + deficit + 30, 5000)


# ---------------------------------------------------------------- Supabase panel
def _supabase():
    return (os.getenv("SUPABASE_URL", "").rstrip("/"),
            os.getenv("SUPABASE_SERVICE_KEY", ""))


def history_set(target_uuid):
    """Satu query massal: {email bot yang sudah like target}.
    None = tabel belum ada / error -> fallback IsThere per bot."""
    url, key = _supabase()
    if not url or not key:
        return None
    q = ("/rest/v1/profile_like_history?select=bot_email"
         "&target_uuid=eq." + urllib.parse.quote(target_uuid, safe="")
         + "&limit=5000")
    req = urllib.request.Request(url + q, headers={
        "apikey": key, "Authorization": "Bearer " + key}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            rows = json.loads(r.read().decode("utf-8"))
        return {row["bot_email"] for row in rows if row.get("bot_email")}
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print("[!] Tabel profile_like_history BELUM ADA -> "
                  "jalan tanpa histori (fallback IsThere).")
        else:
            print(f"[!] history_set HTTP {e.code} -> fallback IsThere.")
        return None
    except Exception as e:
        print(f"[!] history_set gagal ({e}) -> fallback IsThere.")
        return None


# ---------------------------------------------------------------- resolusi nama
async def resolve_name(name, bot):
    """Nama#Kode -> dict lookup (uuid, name) atau None. Satu sesi
    lookup per percobaan, maks 3 percobaan (bot bergantian)."""
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        for attempt in range(1, 4):
            print(f"[*] Resolusi {name} (percobaan {attempt}) ...")
            hit = await L.lookup_once(session, bot, name)
            if hit and hit.get("uuid"):
                print(f"[+] {name} -> {hit['uuid']} "
                      f"(kanonis: {hit.get('name')})")
                return hit
    return None


# ---------------------------------------------------------------- worker per bot
async def bot_coro(bot, target_uuid):
    """Satu bot: maks 2 instance. Kembalikan 'sent' / 'already' / 'fail'."""
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        for attempt in range(1, 3):
            res = await P.like_once(session, bot, target_uuid)
            if res is None:
                continue  # instance gagal -> alokasi ulang
            if res.get("already"):
                return "already"
            after = res.get("after") or {}
            if after.get("exists") is True:
                return "sent"
            # Create tidak terbukti di instance ini -> coba instance baru
            # (aman: server menduplikasi per pasangan, tidak mengganda).
    return "fail"


def run_bot(idx, bot, target_uuid, state):
    """Satu tugas worker (dijalankan di thread pool). Menghormati
    target: kalau jumlah sudah terpenuhi, langsung lewati (tanpa sesi).
    Bot yang sempat jalan dicatat di tried (tidak ditugaskan lagi di
    ronde berikutnya); yang gagal dicatat di failed_map untuk ronde
    retry (bila nantinya terkirim, ia keluar dari failed_map — jadi
    hitungan Gagal di HASIL adalah bot yang AKHIRNYA gagal, bukan
    jumlah kejadian gagal)."""
    with state["lock"]:
        if state["sent"] >= state["jumlah"]:
            return "skip-target-met"
        state["tried"].add(bot["email"])
    time.sleep(random.uniform(0, 1))  # stagger login (hindari herd)
    t0 = time.time()
    try:
        hasil = asyncio.run(bot_coro(bot, target_uuid))
    except Exception as e:
        hasil = "fail"
        print(f"[!] Bot {idx}: exception {e}")
    dt = time.time() - t0
    with state["lock"]:
        if hasil == "sent":
            state["sent"] += 1
            state["failed_map"].pop(bot["email"], None)
        elif hasil == "already":
            state["skipped"] += 1
            state["failed_map"].pop(bot["email"], None)
        else:
            state["failed_map"][bot["email"]] = bot
    print(f"[*] Bot {idx}: {hasil} ({dt:.0f} detik) | "
          f"terkirim={state['sent']} dilewati={state['skipped']} "
          f"gagal={len(state['failed_map'])}")
    return hasil


def jalankan_tugas(tugas, target_uuid, state, max_paralel, nomor_awal):
    """Jalankan daftar tugas di thread pool; tunggu semua selesai.
    Kembalikan nomor tugas terakhir (penomoran berlanjut antar ronde
    agar log 'Bot N' tidak berulang)."""
    nomor = nomor_awal
    with ThreadPoolExecutor(max_workers=max_paralel) as pool:
        futs = []
        for bot in tugas:
            nomor += 1
            futs.append(pool.submit(run_bot, nomor, bot, target_uuid,
                                    state))
        for f in futs:
            try:
                f.result()
            except Exception as e:
                print(f"[!] Tugas worker exception: {e}")
    return nomor


# ---------------------------------------------------------------- main
def main():
    parsed = parse_args(sys.argv)
    if parsed is None:
        print("Pakai: python like_profile_cloud.py <TARGET> <JUMLAH> "
              "[MAX_PARALEL]")
        print("  TARGET = UUID atau Nama#Kode; JUMLAH >= 1; "
              "MAX_PARALEL dijepit 1..20 (default 20).")
        return 1
    target_in, jumlah, max_paralel = parsed
    t0 = time.time()
    print(f"[*] Like profil: target={target_in} jumlah={jumlah} "
          f"paralel_maks={max_paralel}")

    # 1. Daftar bot awal (untuk resolusi bila perlu).
    bots = P.get_bots(target_in, limit=10)
    if not bots:
        print("[!] Tidak ada bot — berhenti.")
        return 1

    # 2. Resolusi Nama#Kode -> UUID (lewati bila input sudah UUID).
    if is_uuid(target_in):
        target_uuid = target_in.strip()
        print(f"[*] Target sudah UUID: {target_uuid}")
    else:
        hit = asyncio.run(resolve_name(target_in, bots[0]))
        if not hit:
            print(f"[!] Target {target_in} TIDAK DITEMUKAN — berhenti "
                  "(tidak ada like dikirim).")
            return 1
        target_uuid = hit["uuid"]

    # 3. Ronde 1: daftar bot resmi (limit = jumlah + cadangan).
    limit = min(jumlah + 30, 5000)
    pool = P.get_bots(target_uuid, limit=limit)
    if not pool:
        print("[!] Tidak ada bot untuk target ini — berhenti.")
        return 1
    print(f"[*] {len(pool)} bot tersedia untuk job ini (limit {limit}).")

    # 4. Histori massal (tanpa sesi) — dibaca sekali di awal; baris
    #    yang ditulis selama job menyangkut bot yang sudah tried.
    sudah = history_set(target_uuid)
    if sudah is None:
        print("[*] Histori tidak dipakai (fallback IsThere per bot).")

    # 5. Ronde jalan paralel (maks 20). Selama terkirim < jumlah dan
    #    masih ada bot fresh, ambil lagi (replenishment): limit fetch
    #    membesar sejumlah deficit + cadangan; bot yang sudah tried
    #    dan yang ada di histori tidak ditugaskan lagi. Tugas yang
    #    antre dan mendapati target sudah terpenuhi lewati sendiri
    #    (murah, tanpa sesi).
    state = {"lock": threading.Lock(), "jumlah": jumlah,
             "sent": 0, "skipped": 0,
             "tried": set(), "failed_map": {}, "history_seen": set()}
    nomor = 0
    maks_ronde = 4
    for ronde in range(1, maks_ronde + 1):
        if ronde > 1:
            deficit = jumlah - state["sent"]
            limit = next_limit(limit, deficit)
            pool = P.get_bots(target_uuid, limit=limit)
            print(f"[*] RONDE {ronde}: fetch ulang (limit {limit}) -> "
                  f"{len(pool)} bot.")
        if sudah is not None:
            baru_histori = {b["email"] for b in pool
                            if b["email"] not in state["tried"]
                            and b["email"] in sudah}
            state["history_seen"] |= baru_histori
        tugas = fresh_bots(pool, state["tried"], sudah)
        if not tugas:
            print(f"[*] RONDE {ronde}: tidak ada bot fresh tersisa.")
            break
        print(f"[*] RONDE {ronde}: {len(tugas)} bot fresh jalan "
              f"(paralel {max_paralel}).")
        nomor = jalankan_tugas(tugas, target_uuid, state, max_paralel,
                               nomor)
        print(f"[*] RONDE {ronde} selesai: terkirim={state['sent']} "
              f"dilewati_sesi={state['skipped']} "
              f"gagal={len(state['failed_map'])}")
        if state["sent"] >= jumlah:
            break

    # 6. Ronde retry terakhir: bila masih kurang dan ada bot gagal,
    #    coba ulang sekali (sesi + alokasi fresh). Hanya sekali —
    #    gagal dua kali berarti instance/bot bermasalah, bukan
    #    sekadar apes.
    if state["sent"] < jumlah and state["failed_map"]:
        retry = list(state["failed_map"].values())
        print(f"[*] RONDE RETRY: {len(retry)} bot gagal dicoba ulang "
              f"sekali (sesi fresh).")
        nomor = jalankan_tugas(retry, target_uuid, state, max_paralel,
                               nomor)
        print(f"[*] RONDE RETRY selesai: terkirim={state['sent']} "
              f"gagal={len(state['failed_map'])}")

    dt = time.time() - t0
    sent = state["sent"]
    print("=" * 48)
    print(f"HASIL LIKE PROFIL: target {target_uuid}")
    print(f"  Diminta: {jumlah} | Terkirim: {sent} | "
          f"Dilewati histori: {len(state['history_seen'])} | "
          f"Dilewati sesi: {state['skipped']} | "
          f"Gagal: {len(state['failed_map'])}")
    print(f"  Waktu total: {dt:.0f} detik")
    print("=" * 48)
    if sent < jumlah:
        print(f"[!] Kurang {jumlah - sent} like dari target "
              "(stok bot fresh habis / gagal berulang — cek log).")
    return decide_exit(sent, jumlah)


if __name__ == "__main__":
    sys.exit(main())
