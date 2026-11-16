# -*- coding: utf-8 -*-
"""
visit_order.py — Loop kunjungan UGC per ORDER (varian SIMPAN-SESI,
GitHub Actions, akun ke-2 hellcatindonesia/skydev-loop).

Perintah Bartes 2026-11-15: ubah sistem visitor UGC lama (join-leave,
1 bot = 1 kunjungan) menjadi model 400 bot seperti visit_loop.py,
tapi berhenti begitu TARGET JUMLAH tercapai — bukan berdasarkan
durasi. Jadi order 10.000 kunjungan selesai pada target (atau
sedikit di bawah), bukan jalan terus sampai durasi habis.

Dasar yang terbukti (probe kirim-ulang run 43664052491):
counter SkyDev 351 -> 357 = +6 — tiap kirim ulang rangkaian
init/'C' di SATU sesi TCP dihitung sebagai kunjungan baru.

Cara kerja per bot (1 login + 1 sesi, lalu loop kirim):
1. Ambil bot fresh dari panel (RPC get_fresh_bots[_part], tanpa tulis
   history — bot tetap fresh untuk orderan).
2. Login HTTP (JWT) -> alokasi activity -> TCP connect -> login frame
   -> tunggu JoinedRoom. Satu bot = satu thread, satu sesi TCP.
3. Loop kirim sampai TARGET tercapai: sebelum tiap kiriman, bot
   RESERVASI satu slot di counter global (di bawah lock). Bila
   counter sudah >= target, bot berhenti — tidak ada kiriman baru.
   Bila kiriman gagal, reservasi DILEPAS agar slotnya bisa dipakai
   bot lain. Hasil akhir: terkirim == target (atau kurang 1-2 bila
   ada kegagalan di detik terakhir; shortfall dilaporkan di JSON
   agar panel bisa top-up, seperti aturan like).
4. Bila socket putus / kirim gagal: reconnect (login + alokasi +
   TCP baru) lalu lanjut, selama target belum tercapai dan batas
   waktu (--max-min, default 60 mnt) belum habis.

Satu runner menangani 1/20 target (workflow menghitung
target-per-runner = ceil(target_total / runner_count)); total
semua runner = target_total (+/- kecil). TIDAK kirim
AcknowledgeMilestone / FetchKPIs / ValidateMissingRewards.
TIDAK tulis history. JANGAN login sebagai pemilik world.
"""
import argparse
import run_guard
import asyncio
import concurrent.futures
import json
import os
import random
import struct
import threading
import time
import urllib.request

import lookup_uuid as L
import gift_diag as G

DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"


def parse_gsid(gsid):
    parts = (gsid or "").split("::")
    server = parts[1] if len(parts) >= 2 else ""
    room = None
    if len(parts) >= 3:
        try:
            room = int(parts[2])
        except Exception:
            room = None
    return server, room


def build_c_payload(x, y, z, teleported, sequence):
    """Payload posisi 'C' 18 byte (TERBUKTI):
    [Teleported][seq][X/Y/Z/Rot f32 BE]."""
    return (bytes([teleported & 0xFF, sequence & 0xFF])
            + struct.pack(">f", x) + struct.pack(">f", y)
            + struct.pack(">f", z) + struct.pack(">f", 0.0))


def fetch_bots(supabase_url, supabase_key, target_uuid, limit,
               partisi, jumlah_partisi, bot_type):
    """Ambil bot fresh dari panel via RPC (pola follow_cloud.py).
    Kembalikan list dict {email, password, device_id} (bisa kosong)."""
    rpc = ("get_fresh_named_bots" if bot_type == "named_bots"
           else "get_fresh_bots")
    if partisi is not None:
        rpc += "_part"
    payload = {"p_target_uuid": target_uuid, "p_limit": limit}
    if partisi is not None:
        payload["p_partisi"] = partisi
        payload["p_jumlah_partisi"] = jumlah_partisi
    req = urllib.request.Request(
        f"{supabase_url}/rest/v1/rpc/{rpc}",
        data=json.dumps(payload).encode("utf-8"), method="POST")
    req.add_header("apikey", supabase_key)
    req.add_header("Authorization", f"Bearer {supabase_key}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data if isinstance(data, list) else []
    except Exception as exc:
        print(f"[-] Gagal mengambil bot dari database: {exc}")
        return []


async def allocate_world(session, jwt, activity_id, target_user_id):
    """Alokasi masuk world (pola progression_visit_short_diag.py).
    Kembalikan (body, label) atau ({}, '')."""
    attempts = [
        ("/lobby/activity/allocation",
         {"userToken": jwt, "activityId": activity_id,
          "gameServerProtocolVersion": L.GAME_PROTOCOL,
          "datacenterId": "blr1"},
         "activity"),
        ("/lobby/allocation",
         {"userToken": jwt, "gameServerProtocolVersion": L.GAME_PROTOCOL,
          "isPrivate": False, "activityId": activity_id},
         "lobby+activityId"),
        ("/lobby/allocation",
         {"userToken": jwt, "gameServerProtocolVersion": L.GAME_PROTOCOL,
          "isPrivate": False, "destinationUserId": target_user_id},
         "lobby+destinationUserId"),
    ]
    for endpoint, body, label in attempts:
        b = json.dumps(body, separators=(",", ":")).encode("utf-8")
        try:
            r = await session.post(L.LOBBY_URL + endpoint, data=b,
                                   headers=L.HEADERS_BASE.copy(), timeout=20)
            resp = r.json()
        except Exception:
            continue
        if not isinstance(resp, dict):
            continue
        host, port = L.find_host_port(resp)
        if host and port:
            return resp, label
    return {}, ""


def send_visit_set(sock, device_id, seq):
    """Satu rangkaian kunjungan: init activity + SendDeviceId +
    'C' spawn teleported=1. Kembalikan (ok, error)."""
    try:
        for name in ["init_pos", "init_day_night", "preset",
                     "fetch_events"]:
            sock.sendall(L.build_room_event_varint(name, b""))
        time.sleep(0.3)
        sock.sendall(L.build_room_event_varint("activity/get", b""))
        sock.sendall(L.build_room_event_varint(
            "Analytics/deviceId",
            (device_id or DEFAULT_DEVICE_ID).encode("utf-8")))
        seq[0] = (seq[0] + 1) & 0xFF
        sock.sendall(L.build_room_event_varint(
            "C", build_c_payload(0.0, 2.0, 0.0, 1, seq[0])))
        return True, None
    except Exception as exc:
        return False, str(exc)


async def _establish(email, password, device_id, activity_id,
                     target_user_id):
    """Satu kali login HTTP + alokasi. Kembalikan
    (jwt, body, label, host, port) atau None bila gagal."""
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        user_jwt, _alloc = await L.login_and_allocate(
            email, password, device_id, session)
        if not user_jwt:
            return None
        body, endpoint = await allocate_world(session, user_jwt,
                                              activity_id,
                                              target_user_id)
        if not body:
            return None
        host, port = L.find_host_port(body)
        if not host or not port:
            return None
        return user_jwt, body, endpoint, host, port


def wait_joined(sock, rm):
    """Tunggu JoinedRoom (standalone 0x06 0x01 / batch 0x0d), maks
    ~5 dtk. Kembalikan True bila terkonfirmasi."""
    fr = G.FrameReader(sock)
    needle = (bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
              if rm is not None else b"")

    def _is_join_or_tbugv(f):
        if G._is_tbugv(f):
            return True
        if rm is not None and len(f) >= 6 and f[0] == 0x06 and f[1] == 1 \
                and int.from_bytes(f[2:6], "big") == rm:
            return True
        if rm is not None and f and f[0] == 0x0d and needle in bytes(f):
            return True
        return False

    got, closed = fr.read_until(_is_join_or_tbugv, 3)
    if closed:
        return False, fr
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
    if got is not None and not G._is_tbugv(got):
        return True, fr
    if rm is not None:
        for f in fr.drain(2):
            raw = bytes(f)
            if (len(raw) >= 6 and raw[0] == 0x06 and raw[1] == 1
                    and int.from_bytes(raw[2:6], "big") == rm) \
                    or (raw and raw[0] == 0x0d and needle in raw):
                return True, fr
    return False, fr


def _target_reached(stats, lock, target_count):
    with lock:
        return stats["sends_total"] >= target_count


def run_bot(bot, activity_id, target_user_id, target_count,
            end_time, gap, login_sem, stats, lock, tag):
    """Satu bot: simpan 1 sesi, loop kirim sampai TARGET tercapai.
    Reservasi satu slot counter sebelum tiap kiriman; kiriman
    gagal melepas reservasinya. Reconnect bila sesi putus.
    Keluar TANPA login bila target sudah tercapai selagi antre
    giliran login; berhenti setelah 5x gagal alokasi beruntun."""
    email = bot.get("email") or ""
    password = bot.get("password") or ""
    device_id = bot.get("device_id") or ""
    time.sleep(random.uniform(0, 2))
    with lock:
        stats["active"] = stats.get("active", 0) + 1
    sends = 0
    reconnects = 0
    errors = {}
    est_fail = 0
    while (not _target_reached(stats, lock, target_count)
           and time.time() < end_time):
        with login_sem:
            # Antrean semaphore bisa lama: target mungkin sudah
            # tercapai selagi menunggu giliran — keluar TANPA login
            # (inilah yang bikin log penuh login zombie di run 43703317315).
            if _target_reached(stats, lock, target_count):
                break
            try:
                est = asyncio.run(_establish(
                    email, password, device_id,
                    activity_id, target_user_id))
            except Exception as exc:
                est = None
                err = f"establish: {exc}"
                errors[err] = errors.get(err, 0) + 1
        if not est:
            errors["login/alloc gagal"] = \
                errors.get("login/alloc gagal", 0) + 1
            est_fail += 1
            if est_fail >= 5:
                errors["berhenti: alloc gagal 5x"] = \
                    errors.get("berhenti: alloc gagal 5x", 0) + 1
                break
            time.sleep(5)
            continue
        est_fail = 0
        user_jwt, body, endpoint, host, port = est
        _srv, rm = parse_gsid(body.get("gameServerId", ""))
        try:
            sock, pid = L.tcp_connect(host, port)
        except Exception as exc:
            errors[f"tcp: {exc}"] = errors.get(f"tcp: {exc}", 0) + 1
            time.sleep(5)
            continue
        if sock is None:
            errors["tcp connect gagal"] = \
                errors.get("tcp connect gagal", 0) + 1
            time.sleep(5)
            continue
        try:
            sock.sendall(L.build_login_frame(user_jwt))
            joined, fr = wait_joined(sock, rm)
        except Exception as exc:
            errors[f"login frame: {exc}"] = \
                errors.get(f"login frame: {exc}", 0) + 1
            try:
                sock.close()
            except Exception:
                pass
            time.sleep(5)
            continue
        if not joined:
            errors["joined tidak terkonfirmasi"] = \
                errors.get("joined tidak terkonfirmasi", 0) + 1
            try:
                sock.close()
            except Exception:
                pass
            time.sleep(5)
            continue
        reconnects += 1
        seq = [0]
        alive = True
        while (alive
               and not _target_reached(stats, lock, target_count)
               and time.time() < end_time):
            with lock:
                if stats["sends_total"] >= target_count:
                    alive = False
                    break
                stats["sends_total"] += 1
            ok, err = send_visit_set(sock, device_id, seq)
            if not ok:
                with lock:
                    stats["sends_total"] -= 1
                errors[f"kirim: {err}"] = \
                    errors.get(f"kirim: {err}", 0) + 1
                alive = False
                break
            sends += 1
            try:
                fr.drain(0.2)
            except Exception:
                alive = False
                break
            if sock.fileno() == -1:
                alive = False
                break
            time.sleep(gap)
        try:
            sock.close()
        except Exception:
            pass
    with lock:
        stats["per_bot"][tag] = {"sends": sends,
                                 "reconnects": reconnects,
                                 "errors": errors}
        stats["active"] = stats.get("active", 0) - 1
    return sends


def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--activity-id", required=True)
    ap.add_argument("--target-user-id", required=True)
    ap.add_argument("--target-count", type=int, required=True)
    ap.add_argument("--bots", type=int, default=20)
    ap.add_argument("--partisi", type=int, default=None)
    ap.add_argument("--jumlah-partisi", type=int, default=None)
    ap.add_argument("--gap", type=float, default=2)
    ap.add_argument("--bot-type", default="accounts")
    ap.add_argument("--max-min", type=float, default=60)
    args = ap.parse_args()

    if args.target_count <= 0:
        print("[!] EROR: --target-count harus > 0.")
        return 1
    supabase_url = os.getenv("SUPABASE_URL", "")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not supabase_url or not supabase_key:
        print("[!] EROR: SUPABASE_URL / SUPABASE_SERVICE_KEY kosong.")
        return 1
    bots = fetch_bots(supabase_url, supabase_key,
                      args.target_user_id, args.bots,
                      args.partisi, args.jumlah_partisi,
                      args.bot_type)
    print(f"[+] Bot didapat (partisi {args.partisi}/"
          f"{args.jumlah_partisi}): {len(bots)}")
    if not bots:
        print("[!] Tidak ada bot fresh — berhenti.")
        return 1

    target = args.target_count
    end_time = time.time() + args.max_min * 60
    stats = {"sends_total": 0, "per_bot": {}, "active": 0}
    lock = threading.Lock()
    login_sem = threading.Semaphore(5)
    t0 = time.time()

    def stats_printer():
        while (time.time() < end_time
               and not _target_reached(stats, lock, target)):
            time.sleep(30)
            with lock:
                total = stats["sends_total"]
            elapsed_h = max((time.time() - t0) / 3600, 1e-6)
            rate = total / elapsed_h
            remain = max(target - total, 0)
            eta_min = (remain / rate * 60) if rate > 0 else -1
            print(f"[=] STAT: kiriman={total}/{target} laju="
                  f"{rate:.0f}/jam eta={eta_min:.1f}mnt bot_aktif="
                  f"{stats.get('active', 0)}/{len(bots)}", flush=True)

    printer = threading.Thread(target=stats_printer, daemon=True)
    printer.start()
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(bots)) as pool:
        futs = []
        for i, bot in enumerate(bots, 1):
            tag = f"Bot {i}/{len(bots)}"
            futs.append(pool.submit(
                run_bot, bot, args.activity_id,
                args.target_user_id, target, end_time, args.gap,
                login_sem, stats, lock, tag))
        for f in concurrent.futures.as_completed(futs):
            try:
                f.result()
            except Exception as exc:
                print(f"[!] thread bot error: {exc}")
    elapsed_min = (time.time() - t0) / 60
    total = stats["sends_total"]
    rate = total / max((time.time() - t0) / 3600, 1e-6)
    errors_all = {}
    for _tag, pb in stats["per_bot"].items():
        for k, v in pb["errors"].items():
            errors_all[k] = errors_all.get(k, 0) + v
    hasil = {"activity_id": args.activity_id,
             "target_user_id": args.target_user_id,
             "target_count": target,
             "delivered": total,
             "shortfall": max(0, target - total),
             "overshoot": max(0, total - target),
             "bots_requested": args.bots,
             "bots_running": len(bots),
             "partisi": args.partisi,
             "jumlah_partisi": args.jumlah_partisi,
             "gap_s": args.gap,
             "max_min": args.max_min,
             "elapsed_min": round(elapsed_min, 1),
             "rate_per_hour": round(rate, 0),
             "reconnects_total": sum(
                 pb["reconnects"] for pb in stats["per_bot"].values()),
             "errors": errors_all,
             "per_bot": stats["per_bot"]}
    print("")
    print("========== HASIL ORDER KUNJUNGAN (SIMPAN-SESI) ==========")
    print(f"Target: {target}  terkirim: {total}  "
          f"kurang: {hasil['shortfall']}  lebih: {hasil['overshoot']}")
    print(f"Bot: {len(bots)}  durasi: {elapsed_min:.1f} mnt  "
          f"laju: {rate:.0f}/jam")
    print(f"Reconnects: {hasil['reconnects_total']}  "
          f"errors: {errors_all or '-'}")
    print("Bartes HARUS cek angka world pemilik di game (1 kiriman "
          "= 1 kunjungan; shortfall > 0 berarti top-up).")
    print("=========================================================")
    print("VISIT_ORDER_JSON:" + json.dumps(hasil,
                                           separators=(",", ":")))
    return 0 if total >= target else 1


if __name__ == "__main__":
    raise SystemExit(main())
