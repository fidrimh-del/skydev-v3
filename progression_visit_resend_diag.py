# -*- coding: utf-8 -*-
"""
progression_visit_resend_diag.py — Probe: apakah kirim ulang init/'C'
di SATU sesi TCP dihitung sebagai kunjungan baru?

Perintah EKSPLISIT Bartes 2026-11-15 08:38 WIB ("Ya coba tes saja",
menjawab ide login-sekali-simpan-sesi; counter SkyDev saat itu 351).

Latar:
- Tes 30 rejoin (run 43663025260) TERKONFIRMASI di game: 321 -> 351
  = +30. Rejoin akun sama lewat 20-25 tanpa batas per akun.
- Tapi tiap loop itu login + alokasi + TCP BARU (~9,6 dtk/loop).
- Ide Bartes: login sekali, simpan sesi, loop join UGC-nya saja.
- Cek dump: satu koneksi TCP = satu room; client asli buang koneksi
  tiap pindah room (CreateNewServerInstance); tidak ada pesan pindah
  room di koneksi yang sama. LeaveRoom ada di protokol tapi client
  tidak pernah memakainya.
- Probe ini menguji SATU-SATUNYA yang belum terbukti: apakah kirim
  ulang rangkaian yang sama (init activity + SendDeviceId + 'C'
  spawn) di sesi yang sama, tanpa disconnect, dihitung server sebagai
  kunjungan tambahan.

DESAIN (1 bot tes, 1 login, 1 alokasi, 1 sesi TCP):
1. Baseline: GetInitializationInfo sebagai bot tes -> catat
   `activity_visits_count` (diharapkan 0).
2. Login + alokasi activity + TCP connect + login frame ->
   tunggu JoinedRoom.
3. Kirim rangkaian (init_pos, init_day_night, preset, fetch_events,
   sleep 0.3, activity/get, SendDeviceId, 'C' spawn teleported=1)
   sebanyak N=6 kali, jeda 5 dtk antar kiriman, TANPA disconnect.
   Tiap kiriman dicatat ok/gagal + apakah koneksi masih hidup.
4. Disconnect, settle 45 dtk, baca KPI bot tes lagi (diharapkan 0,
   builder-side).

TES BACA + JOIN SAJA — JANGAN klaim/acknowledge milestone apa pun.
JANGAN login sebagai SkyDev. Angka SkyDev HANYA bisa dicek Bartes
di gamenya: baseline 351. Sukses-penuh = 357 (+6, tiap kirim ulang
dihitung). +1 = 352 berarti kunjungan dihitung per sesi/alokasi
(ide simpan-sesi GUGUR).
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G
import progression_diag as P

SKYDEV_USER_ID = "aae2efe1-1abb-48c2-8799-ea192692a49c"
ACTIVITY_ID = "3e7cec3f-1ae9-4ba2-a19b-a6339e0f9b23"
BOT_USER_ID = "71056756-53c7-478e-9356-82c87fb2dd63"
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


def extract_visits(init_info):
    """Ambil nilai KPI `activity_visits_count` dari
    GetInitializationInfoResponse yang sudah diparse.
    value=None (proto3 default dihilangkan) dihitung 0."""
    if not init_info or not isinstance(init_info, dict):
        return None
    if "parse_error" in init_info:
        return None
    for k in init_info.get("kpis") or []:
        if k.get("id") == "activity_visits_count":
            v = k.get("value")
            return 0 if v is None else v
    return None


async def read_kpi(session, email, password, device_id):
    """Baca `activity_visits_count` bot tes via GetInitializationInfo
    (pola progression_diag.py — BACA SAJA, tanpa klaim)."""
    hasil = await P.progression_once(session, email, password, device_id,
                                     ["", "1"])
    info = hasil.get("init_info") or {}
    return extract_visits(info), hasil


async def allocate_world(session, jwt, activity_id, target_user_id):
    """Alokasi masuk world. Coba activity allocation dulu (tidak butuh
    target online), lalu /lobby/allocation + activityId, lalu
    destinationUserId. Kembalikan (body, label) atau ({}, '')."""
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
        except Exception as exc:
            print(f"[-] alokasi {label} gagal: {exc}")
            continue
        if not isinstance(resp, dict):
            print(f"[-] alokasi {label}: balasan bukan dict.")
            continue
        host, port = L.find_host_port(resp)
        if host and port:
            print(f"[+] alokasi {label} OK: host={host} port={port}")
            return resp, label
        print(f"[-] alokasi {label}: tanpa host/port "
              f"(kunci={list(resp.keys())[:8]}), coba berikut.")
    return {}, ""


def build_c_payload(x, y, z, teleported, sequence):
    """Payload posisi 'C' 18 byte (TERBUKTI):
    [Teleported][seq][X/Y/Z/Rot f32 BE]."""
    return (bytes([teleported & 0xFF, sequence & 0xFF])
            + struct.pack(">f", x) + struct.pack(">f", y)
            + struct.pack(">f", z) + struct.pack(">f", 0.0))


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


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("VISIT_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("VISIT_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("VISIT_DEVICE_ID", ""))
    ap.add_argument("--activity-id",
                    default=os.getenv("VISIT_ACTIVITY_ID", ACTIVITY_ID))
    ap.add_argument("--target-user-id",
                    default=os.getenv("VISIT_TARGET_USER_ID",
                                      SKYDEV_USER_ID))
    ap.add_argument("--sends", type=int, default=6)
    ap.add_argument("--gap", type=float, default=5)
    ap.add_argument("--settle", type=float, default=45)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
             "activity_id": args.activity_id,
             "target_user_id": args.target_user_id,
             "sends_planned": args.sends, "gap_s": args.gap,
             "settle_s": args.settle,
             "visits_before": None, "visits_after": None,
             "delta": None, "sends": [], "sends_ok": 0,
             "login_ok": False, "alloc": "", "joined": False,
             "server": "", "room": None, "player_id": None,
             "interpretation": None, "error": None}
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        # 1. Baseline KPI bot tes.
        print("===== FASE 1: baseline KPI bot tes =====")
        before, _hb = await read_kpi(session, args.email, args.password,
                                     args.device_id)
        hasil["visits_before"] = before
        print(f"[+] activity_visits_count bot tes SEBELUM: {before}")
        # 2. SATU login + SATU alokasi + SATU sesi TCP.
        print("===== FASE 2: satu login + satu sesi TCP =====")
        user_jwt, _alloc = await L.login_and_allocate(
            args.email, args.password, args.device_id, session)
        if not user_jwt:
            hasil["error"] = "login gagal"
            print("[!] login gagal.")
            print("PROGRESSION_VISIT_RESEND_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        hasil["login_ok"] = True
        body, endpoint = await allocate_world(session, user_jwt,
                                              args.activity_id,
                                              args.target_user_id)
        if not body:
            hasil["error"] = "alokasi gagal (semua endpoint)"
            print("[!] alokasi gagal di semua endpoint.")
            print("PROGRESSION_VISIT_RESEND_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        hasil["alloc"] = endpoint
        srv, rm = parse_gsid(body.get("gameServerId", ""))
        hasil["server"] = srv
        hasil["room"] = rm
        print(f"[+] alokasi OK via {endpoint}: server={srv} room={rm}")
        host, port = L.find_host_port(body)
        if not host or not port:
            hasil["error"] = "host/port tidak ketemu"
            print("[!] host/port tidak ketemu.")
            print("PROGRESSION_VISIT_RESEND_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        sock, pid = L.tcp_connect(host, port)
        if sock is None:
            hasil["error"] = "TCP connect gagal"
            print("[!] TCP connect gagal.")
            print("PROGRESSION_VISIT_RESEND_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        hasil["player_id"] = pid
        print(f"[+] Connected: playerId={pid}.")
        try:
            sock.sendall(L.build_login_frame(user_jwt))
        except Exception as exc:
            hasil["error"] = f"kirim login frame gagal: {exc}"
            print(f"[!] kirim login frame gagal: {exc}")
            try:
                sock.close()
            except Exception:
                pass
            print("PROGRESSION_VISIT_RESEND_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        fr = G.FrameReader(sock)
        needle = (bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
                  if rm is not None else b"")

        def _is_join_or_tbugv(f):
            if G._is_tbugv(f):
                return True
            if rm is not None and len(f) >= 6 and f[0] == 0x06 \
                    and f[1] == 1 \
                    and int.from_bytes(f[2:6], "big") == rm:
                return True
            if rm is not None and f and f[0] == 0x0d \
                    and needle in bytes(f):
                return True
            return False

        got, closed = fr.read_until(_is_join_or_tbugv, 3)
        if closed:
            hasil["error"] = "koneksi tertutup saat tunggu login"
            print("[!] koneksi tertutup saat tunggu login.")
            try:
                sock.close()
            except Exception:
                pass
            print("PROGRESSION_VISIT_RESEND_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        if got is not None and G._is_tbugv(got):
            time.sleep(1)
        joined = False
        if got is not None and not G._is_tbugv(got):
            joined = True
        if not joined and rm is not None:
            for f in fr.drain(2):
                raw = bytes(f)
                if (len(raw) >= 6 and raw[0] == 0x06 and raw[1] == 1
                        and int.from_bytes(raw[2:6], "big") == rm) \
                        or (raw and raw[0] == 0x0d and needle in raw):
                    joined = True
                    break
        hasil["joined"] = joined
        print(f"[+] JoinedRoom({rm}) = {joined}.")
        if not joined:
            hasil["error"] = "JoinedRoom tidak terkonfirmasi"
            try:
                sock.close()
            except Exception:
                pass
            print("PROGRESSION_VISIT_RESEND_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        # 3. Kirim rangkaian kunjungan N kali di sesi yang SAMA.
        print(f"===== FASE 3: kirim rangkaian x{args.sends} "
              f"di sesi yang sama (jeda {args.gap} dtk) =====")
        seq = [0]
        for i in range(1, args.sends + 1):
            ok, err = send_visit_set(sock, args.device_id, seq)
            alive = sock.fileno() != -1
            hasil["sends"].append({"send": i, "ok": ok, "error": err,
                                   "socket_open": alive})
            if ok:
                hasil["sends_ok"] += 1
            print(f"[*] kirim {i}/{args.sends}: ok={ok} "
                  f"socket_open={alive} error={err}")
            if not ok or not alive:
                print("[!] kiriman gagal / socket tertutup — berhenti.")
                break
            if i < args.sends:
                try:
                    fr.drain(args.gap)
                except Exception:
                    break
        print("[+] semua kiriman selesai, disconnect.")
        try:
            sock.close()
        except Exception:
            pass
        # 4. Settle lalu baca KPI sesudah.
        print(f"===== FASE 4: settle {args.settle} dtk =====")
        await asyncio.sleep(args.settle)
        print("===== FASE 5: KPI bot tes SESUDAH =====")
        after, _ha = await read_kpi(session, args.email, args.password,
                                    args.device_id)
        hasil["visits_after"] = after
        print(f"[+] activity_visits_count bot tes SESUDAH: {after}")
    if hasil["visits_before"] is not None \
            and hasil["visits_after"] is not None:
        hasil["delta"] = hasil["visits_after"] - hasil["visits_before"]
    b, a, d = (hasil["visits_before"], hasil["visits_after"],
               hasil["delta"])
    if d is None:
        interp = ("TIDAK_DAPAT_DIBACA (KPI sebelum/sesudah tidak "
                  "terbaca; cek log di atas)")
    elif d == 0:
        interp = ("TETAP_0: (i) BUILDER_SIDE (counter menghitung "
                  "kunjungan KE world milik akun; bot tes tak punya "
                  "world jadi tetap 0) — penentu: angka SkyDev "
                  "Bartes; ATAU (ii) kirim ulang tidak tercatat.")
    elif d == 1:
        interp = ("HANYA_1: kunjungan dihitung PER SESI/alokasi — "
                  "kirim ulang di sesi yang sama TIDAK menambah. "
                  "Ide simpan-sesi GUGUR untuk penghitungan.")
    elif d == hasil["sends_planned"]:
        interp = ("KIRIM_ULANG_DIHITUNG: tiap kirim ulang rangkaian "
                  "di sesi yang sama tercatat sebagai kunjungan baru "
                  f"(delta +{d} = {hasil['sends_planned']} kiriman). "
                  "Loop sesi-tunggal MUNGKIN — lanjut bangun loop.js "
                  "varian simpan-sesi.")
    elif d > 0:
        interp = (f"NAIK_SEBAGIAN: delta +{d} dari "
                  f"{hasil['sends_planned']} kiriman — sebagian "
                  f"kirim ulang tercatat.")
    else:
        interp = (f"DELTA_NEGATIF: delta {d} — aneh, KPI turun; "
                  f"kemungkinan pembacaan tidak konsisten.")
    hasil["interpretation"] = interp
    print("")
    print("========== HASIL TES KIRIM ULANG ==========")
    print(f"Bot tes: {args.email} (userId {BOT_USER_ID})")
    print(f"World: {args.activity_id} (SkyDev#431)")
    print(f"Login: {hasil['login_ok']}  alokasi: {hasil['alloc']}  "
          f"joined: {hasil['joined']}  server: {hasil['server']} "
          f"room: {hasil['room']}")
    print(f"Kiriman direncanakan: {args.sends}  "
          f"kiriman OK: {hasil['sends_ok']}")
    for s in hasil["sends"]:
        print(f"  kirim {s.get('send')}: ok={s.get('ok')} "
              f"socket_open={s.get('socket_open')} "
              f"error={s.get('error')}")
    print(f"activity_visits_count bot tes SEBELUM: {b}")
    print(f"activity_visits_count bot tes SESUDAH: {a}")
    print(f"Delta: {d} (kirim ulang dihitung bila +{args.sends})")
    print(f"Interpretasi: {interp}")
    print("Bartes HARUS cek angka SkyDev di game: baseline 351 — "
          "352 (+1) = per sesi; 357 (+6) = kirim ulang dihitung.")
    print("===========================================")
    print("PROGRESSION_VISIT_RESEND_JSON:"
          + json.dumps(hasil, separators=(",", ":")))
    return 0 if hasil["sends_ok"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
