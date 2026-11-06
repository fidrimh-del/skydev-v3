"""like_house_cloud.py — Kirim like RUMAH PK XD massal via TCP (produksi).

Revisi:
  - Exit code toleran: Sukses >= 80% mengembalikan exit code 0 (runner hijau).
  - Circuit Breaker: Batas kegagalan total agar tidak membuang waktu runner.
  - Optimasi jatah gate IP, pacing socket TCP, dan kuota ambil_limit proporsional.
"""

from concurrent.futures import ThreadPoolExecutor
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

import house_like_probe as H
import like_profile_probe as P
import lookup_uuid as L

UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------- fungsi murni
def is_uuid(s):
  return bool(UUID_RE.match((s or "").strip()))


def clamp_paralel(n, default=15):
  """Dijepit 1..25 dengan default 15 agar soket TCP tidak memicu rate-limit."""
  try:
    v = int(n)
  except (TypeError, ValueError):
    return default
  return max(1, min(25, v))


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
  max_paralel = clamp_paralel(argv[3] if len(argv) > 3 else 15)
  partisi = jumlah_partisi = None
  if len(argv) > 4:
    if len(argv) < 6:
      return None
    try:
      partisi = int(argv[4])
      jumlah_partisi = int(argv[5])
    except ValueError:
      return None
    if jumlah_partisi < 1 or partisi < 0 or partisi >= jumlah_partisi:
      return None
  return target, jumlah, max_paralel, partisi, jumlah_partisi


def decide_exit(sent, jumlah):
  """Toleransi exit code:

  Jika capaian >= 80% dari target, kembalikan exit 0 (sukses)
  agar runner GitHub Actions tetap HIJAU.
  """
  if jumlah <= 0:
    return 0
  if sent >= jumlah:
    return 0

  persentase = (sent / jumlah) * 100
  if persentase >= 80.0:
    print(
        f"[+] Toleransi tercapai: {sent}/{jumlah} ({persentase:.1f}%). Status"
        " Runner: HIJAU."
    )
    return 0

  return 1


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


# ---------------------------------------------------------------- Supabase panel
def _supabase():
  return (
      os.getenv("SUPABASE_URL", "").rstrip("/"),
      os.getenv("SUPABASE_SERVICE_KEY", ""),
  )


def history_set_house(target_uuid):
  url, key = _supabase()
  if not url or not key:
    return None
  out = set()
  offset = 0
  for _ in range(200):
    q = (
        "/rest/v1/house_like_history?select=bot_email"
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
            "[!] Tabel house_like_history BELUM ADA -> fallback IsThere."
        )
      else:
        print(f"[!] history_set_house HTTP {e.code} -> fallback IsThere.")
      return None
    except Exception as e:
      print(f"[!] history_set_house gagal ({e}) -> fallback IsThere.")
      return None
    out |= {row["bot_email"] for row in rows if row.get("bot_email")}
    if len(rows) < 5000:
      break
    offset += 5000
  return out


# ---------------------------------------------------------------- resolusi nama
async def resolve_name(name, bot):
  async with L.AsyncSession(impersonate="chrome110", verify=False) as session:
    for attempt in range(1, 4):
      print(f"[*] Resolusi {name} (percobaan {attempt}) ...")
      hit = await L.lookup_once(session, bot, name)
      if hit and hit.get("uuid"):
        print(f"[+] {name} -> {hit['uuid']} (kanonis: {hit.get('name')})")
        return hit
  return None


# ---------------------------------------------------------------- worker per bot
async def bot_coro(bot, target_uuid):
  answered_any = False
  prof = random.choice(("chrome110", "chrome116", "safari15_5"))
  async with L.AsyncSession(impersonate=prof, verify=False) as session:
    for attempt in range(1, 2):
      res = await H.like_house_once(session, bot, target_uuid)
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

  time.sleep(random.uniform(0.5, 2.0))
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
      if state["sent"] % 10 == 0 or state["sent"] >= state["jumlah"]:
        print(
            f"[*] Progres like rumah: {state['sent']}/{state['jumlah']} terkirim | "
            f"gagal={len(state['failed_map'])} dilewati_histori={len(state['history_seen'])}"
        )
    elif hasil == "already":
      state["skipped"] += 1
      state["failed_map"].pop(bot["email"], None)
      state["noanswer"].discard(bot["email"])
    else:
      state["failed_map"][bot["email"]] = bot
      if hasil == "fail-noanswer":
        state["noanswer"].add(bot["email"])
      print(f"[!] Bot {idx}: {hasil} ({dt:.0f}d)")
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


# ---------------------------------------------------------------- main
def main():
  parsed = parse_args(sys.argv)
  if parsed is None:
    print(
        "Pakai: python like_house_cloud.py <TARGET> <JUMLAH> [MAX_PARALEL] [PARTISI"
        " JUMLAH_PARTISI]"
    )
    return 1
  target_in, jumlah, max_paralel, partisi, jumlah_partisi = parsed
  L.apply_panel_versions()
  L.QUIET = True
  t0 = time.time()
  print(
      f"[*] Like rumah: target={target_in} jumlah={jumlah}"
      f" paralel_maks={max_paralel}"
  )
  if partisi is not None:
    print(
        f"[*] Mode matrix: partisi {partisi}/{jumlah_partisi} (RPC _part, tanpa"
        " fallback jalur lama)."
    )

  # 1. Daftar bot awal
  bots = P.get_bots(target_in, limit=10)
  if not bots:
    print("[!] Tidak ada bot — berhenti.")
    return 1

  # 2. Resolusi Nama#Kode -> UUID
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

  HALAMAN = 5000
  state = {
      "lock": threading.Lock(),
      "jumlah": jumlah,
      "sent": 0,
      "skipped": 0,
      "tried": set(),
      "failed_map": {},
      "history_seen": set(),
      "noanswer": set(),
  }
  nomor = 0
  ronde = 0
  offset = 0

  # Batas pengaman agar tidak looping berulang jika IP diblokir server
  MAX_RONDE = 4
  MAX_GAGAL_TOTAL = 20

  ambil_limit = max(int(jumlah * 1.3), 30)
  if partisi is not None:
    ambil_limit = min(ambil_limit, 65)

  while True:
    if state["sent"] >= jumlah or ronde >= MAX_RONDE:
      break

    if len(state["failed_map"]) >= MAX_GAGAL_TOTAL:
      print(
          f"[!] Batas gagal ({len(state['failed_map'])}) terlampaui."
          " Menghentikan pengambilan bot baru."
      )
      break

    ronde += 1
    defisit = jumlah - state["sent"]

    if pakai_rpc_baru:
      pool = H.get_house_bots(
          target_uuid,
          limit=ambil_limit,
          partisi=partisi,
          jumlah_partisi=jumlah_partisi,
      )
      if not pool:
        if ronde == 1 and partisi is None:
          cek_lama = P.get_bots(target_uuid, limit=10)
          if cek_lama:
            print("[!] RPC baru kosong tapi RPC lama ada bot -> fallback.")
            pakai_rpc_baru = False
            sudah = history_set_house(target_uuid)
            offset = 0
            continue
        print(
            f"[*] RONDE {ronde}: tidak ada bot fresh dari RPC baru — pool habis."
        )
        break
      tugas = fresh_bots(pool, state["tried"], None)
      if not tugas:
        if len(pool) < ambil_limit:
          print(
              f"[*] RONDE {ronde}: semua bot di batch ini sudah tried — pool"
              " habis."
          )
          break
        tambah = max(int(defisit * 1.3), 15)
        ambil_limit += tambah
        if partisi is not None:
          ambil_limit = min(ambil_limit, 70)
        continue
      print(
          f"[*] RONDE {ronde}: {len(tugas)} bot fresh jalan (paralel"
          f" {max_paralel}, ambil_limit {ambil_limit})."
      )
      nomor = jalankan_tugas(
          tugas, target_uuid, state, max_paralel, nomor
      )
      print(
          f"[*] RONDE {ronde} selesai: terkirim={state['sent']}"
          f" dilewati_sesi={state['skipped']} gagal={len(state['failed_map'])}"
      )
      if state["sent"] >= jumlah:
        break
      if len(pool) < ambil_limit:
        print("[*] RPC mengembalikan kurang dari limit — pool fresh habis.")
        break
      tambah = max(int((jumlah - state["sent"]) * 1.3), 15)
      ambil_limit += tambah
      if partisi is not None:
        ambil_limit = min(ambil_limit, 70)
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
      print(
          f"[*] RONDE {ronde} (fallback): {len(tugas)} bot fresh jalan (paralel"
          f" {max_paralel})."
      )
      nomor = jalankan_tugas(
          tugas, target_uuid, state, max_paralel, nomor
      )
      print(
          f"[*] RONDE {ronde} selesai: terkirim={state['sent']}"
          f" dilewati_sesi={state['skipped']} gagal={len(state['failed_map'])}"
      )

  # Ronde retry ringan (hanya bot yang sempat menjawab IsThere)
  if (
      state["sent"] < jumlah
      and state["failed_map"]
      and len(state["failed_map"]) < MAX_GAGAL_TOTAL
  ):
    retry = [
        b
        for e, b in state["failed_map"].items()
        if e not in state["noanswer"]
    ]
    if retry:
      print(f"[*] RONDE RETRY: {len(retry)} bot gagal dicoba ulang...")
      nomor = jalankan_tugas(retry, target_uuid, state, max_paralel, nomor)
      print(
          f"[*] RONDE RETRY selesai: terkirim={state['sent']}"
          f" gagal={len(state['failed_map'])}"
      )

  dt = time.time() - t0
  sent = state["sent"]
  print("=" * 48)
  print(f"HASIL LIKE RUMAH: target {target_uuid}")
  print(
      f"  Diminta: {jumlah} | Terkirim: {sent} | "
      f"Dilewati histori: {len(state['history_seen'])} | "
      f"Dilewati sesi (sudah like rumah): {state['skipped']} | "
      f"Gagal: {len(state['failed_map'])}"
  )
  print(f"  Waktu total: {dt:.0f} detik")
  print("=" * 48)

  if sent < jumlah:
    print(
        f"[!] Kurang {jumlah - sent} like dari target (stok bot fresh habis /"
        " gagal berulang — cek log)."
    )

  return decide_exit(sent, jumlah)


if __name__ == "__main__":
  sys.exit(main())
