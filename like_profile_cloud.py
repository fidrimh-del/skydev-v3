"""
like_profile_cloud.py — Kirim like profil PK XD massal via TCP (produksi).
Versi stabil dengan chunking batch (maks 150 bot/ronde), pacing stagger, 
dan circuit-breaker proteksi overload antrean.
"""

import sys
import os
import json
import time
import random
import re
import asyncio
import threading
import urllib.request
import urllib.parse
import urllib.error
from concurrent.futures import ThreadPoolExecutor

import lookup_uuid as L
import like_profile_probe as P

UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def is_uuid(s):
  return bool(UUID_RE.match((s or "").strip()))


def clamp_paralel(n, default=20):
  try:
    v = int(n)
  except (TypeError, ValueError):
    return default
  return max(1, min(50, v))


def parse_args(argv):
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
  return 0 if sent >= jumlah else 1


def fresh_bots(pool, tried, history):
  out = []
  for b in pool or []:
    email = b.get("email")
    if not email or email in tried:
      continue
    if history is not None and email in history:
      continue
    out.append(b)
  return out


def _supabase():
  return (
      os.getenv("SUPABASE_URL", "").rstrip("/"),
      os.getenv("SUPABASE_SERVICE_KEY", ""),
  )


def history_set(target_uuid):
  url, key = _supabase()
  if not url or not key:
    return None
  out = set()
  offset = 0
  for _ in range(200):
    q = (
        "/rest/v1/profile_like_history?select=bot_email"
        "&target_uuid=eq."
        + urllib.parse.quote(target_uuid, safe="")
        + f"&limit=5000&offset={offset}"
    )
    req = urllib.request.Request(
        url + q,
        headers={"apikey": key, "Authorization": "Bearer " + key},
        method="GET",
    )
    try:
      with urllib.request.urlopen(req, timeout=20) as r:
        rows = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
      if e.code == 404:
        print(
            "[!] Tabel profile_like_history BELUM ADA -> jalan tanpa histori."
        )
      else:
        print(f"[!] history_set HTTP {e.code} -> fallback IsThere.")
      return None
    except Exception as e:
      print(f"[!] history_set gagal ({e}) -> fallback IsThere.")
      return None
    out |= {row["bot_email"] for row in rows if row.get("bot_email")}
    if len(rows) < 5000:
      break
    offset += 5000
  return out


async def resolve_name(name, bot):
  async with L.AsyncSession(impersonate="chrome110", verify=False) as session:
    for attempt in range(1, 4):
      print(f"[*] Resolusi {name} (percobaan {attempt}) ...")
      hit = await L.lookup_once(session, bot, name)
      if hit and hit.get("uuid"):
        print(f"[+] {name} -> {hit['uuid']} (kanonis: {hit.get('name')})")
        return hit
  return None


async def bot_coro(bot, target_uuid):
  answered_any = False
  prof = random.choice(("chrome110", "chrome116", "safari15_5"))
  async with L.AsyncSession(impersonate=prof, verify=False) as session:
    for attempt in range(1, 3):
      res = await P.like_once(session, bot, target_uuid)
      if res is None:
        continue
      if res.get("answered"):
        answered_any = True
      if res.get("already"):
        return "already"
      after = res.get("after") or {}
      if after.get("exists") is True:
        return "sent"
  return "fail" if answered_any else "fail-noanswer"


def run_bot(idx, bot, target_uuid, state):
  with state["lock"]:
    if state["sent"] >= state["jumlah"]:
      return "skip-target-met"
    state["tried"].add(bot["email"])

  # Jeda awal staggered agar socket tidak dibuka bersamaan
  time.sleep(random.uniform(0.3, 1.8))
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
      state["noanswer"].discard(bot["email"])
      state["consecutive_fails"] = 0
      if state["sent"] % 50 == 0 or state["sent"] >= state["jumlah"]:
        print(
            f"[*] Progres like profil: {state['sent']}/{state['jumlah']}"
            f" terkirim | gagal={len(state['failed_map'])}"
            f" dilewati_histori={len(state['history_seen'])}"
        )
    elif hasil == "already":
      state["skipped"] += 1
      state["failed_map"].pop(bot["email"], None)
      state["noanswer"].discard(bot["email"])
      state["consecutive_fails"] = 0
    else:
      state["failed_map"][bot["email"]] = bot
      state["consecutive_fails"] += 1
      if hasil == "fail-noanswer":
        state["noanswer"].add(bot["email"])
      print(f"[!] Bot {idx}: {hasil} ({dt:.0f}d)")

      # Circuit-breaker: Jika 15 bot gagal berturut-turut, tahan sejenak
      if state["consecutive_fails"] >= 15:
        print("[!] Lonjakan kegagalan terdeteksi, cooldown antrean 10 detik...")
        time.sleep(10)
        state["consecutive_fails"] = 0

  # Pendinginan singkat sebelum thread mengambil bot berikutnya
  time.sleep(random.uniform(0.2, 0.6))
  return hasil


def jalankan_tugas(tugas, target_uuid, state, max_paralel, nomor_awal):
  nomor = nomor_awal
  with ThreadPoolExecutor(max_workers=max_paralel) as pool:
    futs = []
    for bot in tugas:
      nomor += 1
      futs.append(pool.submit(run_bot, nomor, bot, target_uuid, state))
    for f in futs:
      try:
        f.result()
      except Exception as e:
        print(f"[!] Tugas worker exception: {e}")
  return nomor


def main():
  parsed = parse_args(sys.argv)
  if parsed is None:
    print(
        "Pakai: python like_profile_cloud.py <TARGET> <JUMLAH> [MAX_PARALEL]"
    )
    print(
        "  TARGET = UUID atau Nama#Kode; JUMLAH >= 1; MAX_PARALEL dijepit 1..50"
        " (default 20)."
    )
    return 1
  target_in, jumlah, max_paralel = parsed

  L.apply_panel_versions()
  L.QUIET = True
  t0 = time.time()
  print(
      f"[*] Like profil: target={target_in} jumlah={jumlah}"
      f" paralel_maks={max_paralel}"
  )

  # 1. Resolusi target
  bots = P.get_bots(target_in, limit=10)
  if not bots:
    print("[!] Tidak ada bot — berhenti.")
    return 1

  if is_uuid(target_in):
    target_uuid = target_in.strip()
    print(f"[*] Target sudah UUID: {target_uuid}")
  else:
    hit = asyncio.run(resolve_name(target_in, bots[0]))
    if not hit:
      print(f"[!] Target {target_in} TIDAK DITEMUKAN — berhenti.")
      return 1
    target_uuid = hit["uuid"]

  pakai_rpc_baru = True
  sudah = set()
  print(
      "[*] RPC fresh baru dipakai: filter like sudah di Supabase, history_set"
      " lokal dilewati."
  )

  state = {
      "lock": threading.Lock(),
      "jumlah": jumlah,
      "sent": 0,
      "skipped": 0,
      "tried": set(),
      "failed_map": {},
      "history_seen": set(),
      "noanswer": set(),
      "consecutive_fails": 0,
  }

  nomor = 0
  ronde = 0
  offset = 0
  HALAMAN = 5000

  # Loop batch per gelombang (Maksimal 150 bot per ronde)
  while True:
    if state["sent"] >= jumlah:
      break
    ronde += 1

    # Chunking: Ambil secukupnya sesuai sisa defisit, dijepit di maks 150
    defisit = jumlah - state["sent"]
    ambil_limit = min(max(defisit + 30, 50), 150)

    if pakai_rpc_baru:
      pool = P.get_profile_bots(target_uuid, limit=ambil_limit)
      if not pool:
        if ronde == 1:
          cek_lama = P.get_bots(target_uuid, limit=10)
          if cek_lama:
            print("[!] RPC baru kosong -> fallback ke jalur lama.")
            pakai_rpc_baru = False
            sudah = history_set(target_uuid)
            offset = 0
            continue
        print(f"[*] RONDE {ronde}: tidak ada bot fresh dari RPC — pool habis.")
        break

      tugas = fresh_bots(pool, state["tried"], None)
      if not tugas:
        if len(pool) < ambil_limit:
          print(f"[*] RONDE {ronde}: pool fresh habis.")
          break
        continue

      print(
          f"[*] RONDE {ronde}: {len(tugas)} bot fresh jalan (paralel"
          f" {max_paralel}, target defisit {defisit})."
      )
      nomor = jalankan_tugas(tugas, target_uuid, state, max_paralel, nomor)
      print(
          f"[*] RONDE {ronde} selesai: terkirim={state['sent']}"
          f" gagal={len(state['failed_map'])}"
      )

      if state["sent"] >= jumlah:
        break
      if len(pool) < ambil_limit:
        print("[*] RPC mengembalikan kurang dari limit — pool fresh habis.")
        break

      # Jeda pendinginan antar-ronde gelombang
      time.sleep(2.0)

    else:
      pool = P.get_bots_page(target_uuid, limit=HALAMAN, offset=offset)
      if not pool:
        print(f"[*] RONDE {ronde}: pool habis pada offset {offset}.")
        break
      offset += HALAMAN

      if sudah is not None:
        baru_histori = {
            b["email"]
            for b in pool
            if b["email"] not in state["tried"] and b["email"] in sudah
        }
        state["history_seen"] |= baru_histori

      tugas = fresh_bots(pool, state["tried"], sudah)
      if not tugas:
        continue

      # Jalankan dalam potongan maksimal 150 bot
      tugas_chunk = tugas[:150]
      print(
          f"[*] RONDE {ronde} (fallback): {len(tugas_chunk)} bot fresh jalan"
          f" (paralel {max_paralel})."
      )
      nomor = jalankan_tugas(
          tugas_chunk, target_uuid, state, max_paralel, nomor
      )
      print(
          f"[*] RONDE {ronde} selesai: terkirim={state['sent']}"
          f" gagal={len(state['failed_map'])}"
      )
      time.sleep(2.0)

  # Ronde retry terakhir untuk bot yang bukan noanswer
  if state["sent"] < jumlah and state["failed_map"]:
    retry = [
        b
        for e, b in state["failed_map"].items()
        if e not in state["noanswer"]
    ]
    if retry:
      print(
          f"[*] RONDE RETRY: {len(retry)} bot transient dicoba ulang sekali..."
      )
      nomor = jalankan_tugas(retry, target_uuid, state, max_paralel, nomor)
      print(
          f"[*] RONDE RETRY selesai: terkirim={state['sent']}"
          f" gagal={len(state['failed_map'])}"
      )

  dt = time.time() - t0
  sent = state["sent"]
  print("=" * 48)
  print(f"HASIL LIKE PROFIL: target {target_uuid}")
  print(
      f"  Diminta: {jumlah} | Terkirim: {sent} | Dilewati histori:"
      f" {len(state['history_seen'])} | Dilewati sesi: {state['skipped']} |"
      f" Gagal: {len(state['failed_map'])}"
  )
  print(f"  Waktu total: {dt:.0f} detik")
  print("=" * 48)
  if sent < jumlah:
    print(f"[!] Kurang {jumlah - sent} like dari target.")
  return decide_exit(sent, jumlah)


if __name__ == "__main__":
  sys.exit(main())
