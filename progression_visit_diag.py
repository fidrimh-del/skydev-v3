# -*- coding: utf-8 -*-
"""
progression_visit_diag.py — Tes hitung kunjungan `activity_visits_count`
dengan 1 bot tes (pkxdshop.id@gmail.com). Perintah EKSPLISIT Bartes
2026-11-14 22:04 WIB: "Ya coba tes".

Latar (probe run 43607035189):
- KPI Progression `activity_visits_count` (type UGC=3): milestone [10]
  di 1.000.000 kunjungan berhadiah Role CreatorUgc (TERBUKTI).
- Arah counter BELUM terbukti (visitor-side vs builder-side).
- Baseline Bartes (angka SkyDev, hanya dia yang bisa lihat di game):
  total kunjungan level ke-4 ada di 129/4000. Sukses = setelah loop
  5x, naik ke 134 (129+5). Angka "4000" tidak sama dengan goal
  milestone [4] dari probe (1.000) — JANGAN perdebatkan; pakai
  baseline 129 dan kriteria +5 sebagai patokan. Delta +5 yang penting.
- Bot tes: `activity_visits_count` baseline 0 (probe 43607035189).

DESAIN TES (1 bot x 5 loop — menguji apakah RE-JOIN dihitung):
1. Baseline: GetInitializationInfo sebagai bot tes -> catat
   `activity_visits_count` (diharapkan 0).
2. Loop 5x (bot tes yang sama): join ke world SkyDev (activity
   3e7cec3f-1ae9-4ba2-a19b-a6339e0f9b23), tahan di dalam world ~15
   detik per loop (supaya kunjungan sempat tercatat — jangan join
   lalu langsung keluar), lalu disconnect, ulangi sampai 5x.
   Jeda antar loop ~3 detik.
3. Settle ~45 detik (server bisa cache/delay update KPI), lalu
   GetInitializationInfo lagi sebagai bot tes -> baca
   `activity_visits_count`.

TES BACA + JOIN SAJA — JANGAN klaim/acknowledge milestone apa pun
(TIDAK kirim AcknowledgeMilestone / FetchKPIs /
ValidateMissingRewards / AcknowledgeStackedLevelUp).
JANGAN login sebagai SkyDev — angka SkyDev (129->134) HANYA bisa
dicek Bartes di gamenya.

Alokasi join (robust, target TIDAK wajib online):
- Utama: POST /lobby/activity/allocation (cara client asli masuk
  activity; JoiningActivity=3) — TIDAK butuh SkyDev online.
- Fallback 1: POST /lobby/allocation + activityId (bentuk
  activity_standby.py, TERBUKTI).
- Fallback 2: POST /lobby/allocation + destinationUserId (SkyDev)
  — hanya jalan bila SkyDev online (alokasi mengembalikan server=''
  bila target offline).
JoinRoom TIDAK dikirim (server auto-place). Sesudah login: init
activity (init_pos, init_day_night, preset, fetch_events,
activity/get) + SendDeviceId + 'C' spawn, lalu hold dengan
heartbeat 'C' tiap 2 detik.

INTERPRETASI:
- KPI bot tes naik 0->5: visitor-side TERBUKTI — counter menghitung
  kunjungan yang DILAKUKAN akun itu, dan tiap re-join dihitung.
  Artinya angka SkyDev (129) TIDAK akan naik dari loop ini.
- KPI bot tes tetap 0: (i) builder-side (counter menghitung
  kunjungan KE world milik akun; bot tes tak punya world jadi tetap
  0), atau (ii) join tidak tercatat (stay terlalu pendek / server
  dedupe / cache). Penentu: angka SkyDev Bartes (129->134 =
  builder-side TERBUKTI dan loop berhasil; tetap 129 = join tidak
  tercatat / perlu stay lebih lama / cache lebih lama).
- KPI bot tes naik tapi bukan 5 (mis. 1-4): laporkan angka persisnya.
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
    Kembalikan int, atau None bila KPI tidak ada / parse gagal.
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


async def join_once(session, email, password, device_id, activity_id,
                    target_user_id, hold_s=15, loop_no=1):
    """Satu loop: login -> alokasi masuk world -> TCP login -> init
    activity + SendDeviceId + 'C' spawn -> tahan hold_s detik (drain
    frame + heartbeat 'C' tiap 2 dtk) -> disconnect. Satu loop gagal
    TIDAK menggagalkan loop lain (pemanggil yang melanjutkan)."""
    res = {"loop": loop_no, "login_ok": False, "alloc": "",
           "alloc_ok": False, "server": "", "room": None,
           "tcp_ok": False, "player_id": None, "joined": False,
           "hold_s": 0, "error": None}
    user_jwt, _alloc = await L.login_and_allocate(email, password,
                                                  device_id, session)
    if not user_jwt:
        res["error"] = "login gagal"
        print(f"[!] loop {loop_no}: login gagal.")
        return res
    res["login_ok"] = True
    body, endpoint = await allocate_world(session, user_jwt, activity_id,
                                          target_user_id)
    if not body:
        res["error"] = "alokasi gagal (semua endpoint)"
        print(f"[!] loop {loop_no}: alokasi gagal di semua endpoint.")
        return res
    res["alloc"] = endpoint
    res["alloc_ok"] = True
    srv, rm = parse_gsid(body.get("gameServerId", ""))
    res["server"] = srv
    res["room"] = rm
    print(f"[+] loop {loop_no}: alokasi OK via {endpoint}: "
          f"server={srv} room={rm}")
    host, port = L.find_host_port(body)
    if not host or not port:
        res["error"] = "host/port tidak ketemu"
        print(f"[!] loop {loop_no}: host/port tidak ketemu.")
        return res
    sock, pid = L.tcp_connect(host, port)
    if sock is None:
        res["error"] = "TCP connect gagal"
        print(f"[!] loop {loop_no}: TCP connect gagal.")
        return res
    res["tcp_ok"] = True
    res["player_id"] = pid
    print(f"[+] loop {loop_no}: Connected: playerId={pid}.")
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        res["error"] = f"kirim login frame gagal: {exc}"
        print(f"[!] loop {loop_no}: kirim login frame gagal: {exc}")
        try:
            sock.close()
        except Exception:
            pass
        return res
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
        res["error"] = "koneksi tertutup saat tunggu login"
        print(f"[!] loop {loop_no}: koneksi tertutup saat tunggu login.")
        try:
            sock.close()
        except Exception:
            pass
        return res
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
    res["joined"] = joined
    print(f"[+] loop {loop_no}: JoinedRoom({rm}) = {joined}.")
    # Init activity (persis urutan game, terbukti activity_standby.py)
    # + SendDeviceId.
    try:
        for name in ["init_pos", "init_day_night", "preset",
                     "fetch_events"]:
            sock.sendall(L.build_room_event_varint(name, b""))
        time.sleep(0.3)
        sock.sendall(L.build_room_event_varint("activity/get", b""))
        sock.sendall(L.build_room_event_varint(
            "Analytics/deviceId",
            (device_id or DEFAULT_DEVICE_ID).encode("utf-8")))
        print(f"[+] loop {loop_no}: init activity + SendDeviceId terkirim.")
    except Exception as exc:
        print(f"[!] loop {loop_no}: kirim init gagal: {exc}")
    # 'C' spawn (0,2,0) teleported=1, lalu hold dengan heartbeat 'C'
    # tiap 2 dtk (teleported=0).
    seq = [0]

    def send_c(teleported):
        seq[0] = (seq[0] + 1) & 0xFF
        sock.sendall(L.build_room_event_varint(
            "C", build_c_payload(0.0, 2.0, 0.0, teleported, seq[0])))

    try:
        send_c(1)
    except Exception:
        pass
    t_hold_start = time.time()
    last_hb = time.time()
    t_end = t_hold_start + hold_s
    while time.time() < t_end:
        now = time.time()
        if now - last_hb >= 2.0:
            try:
                send_c(0)
            except Exception:
                break
            last_hb = now
        try:
            fr.drain(0.5)
        except Exception:
            break
    res["hold_s"] = round(time.time() - t_hold_start, 1)
    print(f"[+] loop {loop_no}: hold selesai ({res['hold_s']} dtk di "
          f"world), disconnect.")
    try:
        sock.close()
    except Exception:
        pass
    return res


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
    ap.add_argument("--loops", type=int, default=5)
    ap.add_argument("--hold", type=float, default=15)
    ap.add_argument("--gap", type=float, default=3)
    ap.add_argument("--settle", type=float, default=45)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
             "activity_id": args.activity_id,
             "target_user_id": args.target_user_id,
             "loops_planned": args.loops, "hold_s": args.hold,
             "gap_s": args.gap, "settle_s": args.settle,
             "visits_before": None, "visits_after": None,
             "delta": None, "loops": [], "loops_ok": 0,
             "interpretation": None, "error": None}
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        # 1. Baseline KPI bot tes.
        print("===== FASE 1: baseline KPI bot tes =====")
        before, _hb = await read_kpi(session, args.email, args.password,
                                     args.device_id)
        hasil["visits_before"] = before
        print(f"[+] activity_visits_count bot tes SEBELUM loop: {before}")
        # 2. Loop join.
        print(f"===== FASE 2: loop join x{args.loops} "
              f"(hold {args.hold} dtk/loop) =====")
        for i in range(1, args.loops + 1):
            r = await join_once(session, args.email, args.password,
                                args.device_id, args.activity_id,
                                args.target_user_id, args.hold, i)
            hasil["loops"].append(r)
            if r.get("alloc_ok") and r.get("tcp_ok") and r.get("joined"):
                hasil["loops_ok"] += 1
            print(f"[*] loop {i}/{args.loops}: alloc={r.get('alloc')!r} "
                  f"joined={r.get('joined')} hold={r.get('hold_s')} dtk "
                  f"error={r.get('error')}")
            if i < args.loops:
                await asyncio.sleep(args.gap)
        # 3. Settle (server bisa cache/delay update KPI).
        print(f"===== FASE 3: settle {args.settle} dtk (tunggu server) "
              f"=====")
        await asyncio.sleep(args.settle)
        # 4. KPI bot tes sesudah loop.
        print("===== FASE 4: KPI bot tes SESUDAH loop =====")
        after, _ha = await read_kpi(session, args.email, args.password,
                                    args.device_id)
        hasil["visits_after"] = after
        print(f"[+] activity_visits_count bot tes SESUDAH loop: {after}")
    if hasil["visits_before"] is not None \
            and hasil["visits_after"] is not None:
        hasil["delta"] = hasil["visits_after"] - hasil["visits_before"]
    # Interpretasi tegas.
    b, a, d = (hasil["visits_before"], hasil["visits_after"],
               hasil["delta"])
    if d is None:
        interp = ("TIDAK_DAPAT_DIBACA (KPI sebelum/sesudah tidak terbaca; "
                  "cek log di atas)")
    elif d == hasil["loops_planned"]:
        interp = ("VISITOR_SIDE: counter menghitung kunjungan yang "
                  "DILAKUKAN akun itu; tiap re-join dihitung "
                  f"(delta +{d} = {hasil['loops_planned']} loop). "
                  "Angka SkyDev (129) TIDAK akan naik dari loop ini.")
    elif d == 0:
        interp = ("TETAP_0: (i) BUILDER_SIDE (counter menghitung kunjungan "
                  "KE world milik akun; bot tes tak punya world jadi "
                  "tetap 0) — penentu: angka SkyDev Bartes 129->134 = "
                  "builder-side TERBUKTI dan loop berhasil; tetap 129 = "
                  "join tidak tercatat; ATAU (ii) join tidak tercatat "
                  "(stay terlalu pendek / server dedupe / cache).")
    elif d > 0:
        interp = (f"NAIK_SEBAGIAN: delta +{d} dari "
                  f"{hasil['loops_planned']} loop — hanya sebagian join "
                  f"yang tercatat (kemungkinan cache/dedupe sebagian).")
    else:
        interp = (f"DELTA_NEGATIF: delta {d} — aneh, KPI turun; "
                  f"kemungkinan pembacaan sebelum/sesudah tidak konsisten.")
    hasil["interpretation"] = interp
    print("")
    print("========== HASIL TES KUNJUNGAN ==========")
    print(f"Bot tes: {args.email} (userId {BOT_USER_ID})")
    print(f"World: {args.activity_id} (SkyDev#431)")
    print(f"Loop direncanakan: {args.loops}  "
          f"loop sukses (alloc+TCP+joined): {hasil['loops_ok']}")
    for r in hasil["loops"]:
        print(f"  loop {r.get('loop')}: alloc={r.get('alloc')!r} "
              f"joined={r.get('joined')} hold={r.get('hold_s')} dtk "
              f"error={r.get('error')}")
    print(f"activity_visits_count bot tes SEBELUM: {b}")
    print(f"activity_visits_count bot tes SESUDAH: {a}")
    print(f"Delta: {d} (target +{args.loops} bila tiap re-join dihitung)")
    print(f"Interpretasi: {interp}")
    print("Bartes HARUS cek angka SkyDev di game: 129 -> 134 bila "
          "builder-side dan loop tercatat.")
    print("==========================================")
    print("PROGRESSION_VISIT_JSON:" + json.dumps(hasil,
                                                 separators=(",", ":")))
    return 0 if hasil["loops_ok"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
