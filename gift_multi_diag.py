# -*- coding: utf-8 -*-
"""
gift_multi_diag.py — Gift MULTI pet berurutan dalam SATU koneksi.

Bot login -> alokasi destinationUserId -> JoinRoom target -> untuk
SETIAP pet (sesuai urutan dipilih): kirim GiftingSent -> tunggu
GiftingResult (penerima konfirmasi popup ~30 dtk) -> bila diterima
lanjut ke pet berikut; bila DITOLAK / timeout, KIRIM ULANG pet yang
sama (maks --max-retries kali) sebelum lanjut. Bila tetap gagal
setelah retry, pet itu dicatat "failed" dan lanjut ke pet berikut
(supaya satu pet yang bandel tidak memblokir sisanya).

Satu koneksi TCP dipakai untuk semua pet (login + join sekali saja),
bukan login ulang per pet. Hasil per pet dikumpulkan lalu di-output
sebagai GIFT_MULTI_JSON + POST ke /api/pet-result (kind=gift_multi)
agar panel bisa menampilkan status tiap pet tanpa menunggu log GitHub.

Dibuat 2026-11-10 atas permintaan Bartes (multi-select gift di panel).
Dasar: gift_join_room_diag.py (single gift, terbukti) — alur login /
alokasi / join / kirim / tunggu hasil sama, hanya diulang per pet.
"""
import argparse
import asyncio
import json
import os
import time

import lookup_uuid as L
import gift_diag as G
import gift_join_room_diag as GJ


async def gift_multi_once(session, email, password, device_id, pet_ids,
                          recipient, wait_seconds, max_retries,
                          max_alloc_tries, target_server, target_room,
                          dest_user_id=""):
    """Gift banyak pet berurutan. Kembalikan dict hasil per pet."""
    hasil = {"email": email, "pet_ids": pet_ids, "recipient": recipient,
             "target_server": target_server, "target_room": target_room,
             "server_matched": False, "joined_room": None,
             "results": []}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil
    # --- alokasi ke server teman (destinationUserId) ---
    chosen = None
    srv0, room0 = GJ.parse_gsid(alloc.get("gameServerId", ""))
    print(f"[*] alokasi awal (tanpa dest): server={srv0} room={room0}")
    if target_server and srv0 == target_server:
        chosen = alloc
    if not chosen and dest_user_id:
        print(f"[*] coba alokasi destinationUserId={dest_user_id} ...")
        body = await GJ.allocate_to_user(session, user_jwt, dest_user_id)
        if body:
            srv, rm = GJ.parse_gsid(body.get("gameServerId", ""))
            print(f"[*] alokasi destinationUserId: server={srv} room={rm}")
            if not target_server:
                chosen = body
                target_server = srv
                if rm is not None:
                    target_room = rm
                hasil["target_server"] = target_server
                hasil["target_room"] = target_room
                print("[+] destinationUserId BERHASIL — bot dialokasikan "
                      f"ke server teman ({srv} room {target_room})!")
            elif srv == target_server:
                chosen = body
                print("[+] destinationUserId BERHASIL.")
    if not chosen and target_server:
        for i in range(2, max_alloc_tries + 1):
            body = await GJ.allocate_once(session, user_jwt,
                                          datacenter="blr1")
            if not body:
                continue
            srv, rm = GJ.parse_gsid(body.get("gameServerId", ""))
            print(f"[*] alokasi percobaan {i}: server={srv} room={rm}")
            if srv == target_server:
                chosen = body
                break
            await asyncio.sleep(0.5)
    if not chosen:
        print("[!] Server target TIDAK didapat / destinationUserId gagal. "
              "BERHENTI.")
        return hasil
    hasil["server_matched"] = True
    host, port = L.find_host_port(chosen)
    if not host or not port:
        print("[!] host/port tidak ketemu.")
        return hasil
    print(f"[+] Server cocok: {target_server} -> {host}:{port}")
    sock, _pid = L.tcp_connect(host, port)
    if sock is None:
        print("[!] TCP connect gagal.")
        return hasil
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        print(f"[!] kirim login frame gagal: {exc}")
        return hasil
    fr = G.FrameReader(sock)
    print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
    got, closed = fr.read_until(
        lambda f: G._is_tbugv(f) or G._is_joinedroom(f), 3)
    if closed:
        print("[!] koneksi tertutup saat tunggu login.")
        return hasil
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
    print(f"[*] kirim JoinRoom({target_room}) ...")
    try:
        sock.sendall(L.build_join_frame(target_room))
    except Exception as exc:
        print(f"[!] kirim JoinRoom gagal: {exc}")
        return hasil

    def _is_target_room(f):
        return (len(f) >= 6 and f[0] == 0x06 and f[1] == 1
                and int.from_bytes(f[2:6], "big") == target_room)

    joined, closed = fr.read_until(_is_target_room, 4)
    if closed:
        print("[!] koneksi tertutup saat JoinRoom.")
        return hasil
    if joined is not None:
        hasil["joined_room"] = target_room
        print(f"[+] JoinedRoom({target_room}) TERKONFIRMASI.")
    else:
        print("[!] TIDAK ada konfirmasi JoinedRoom — tetap lanjut gift.")

    def tunggu_hasil(wait_s):
        """Tunggu satu GiftingResult (EventId=1). Kembalikan
        (successful, error) atau (None, None) bila timeout."""
        t_end = time.time() + wait_s
        while time.time() < t_end:
            chunk = min(5, max(1, int(t_end - time.time())))
            for f in fr.drain(chunk):
                name, data = G.decode_room_event(f)
                if name != "gifting" or not data:
                    continue
                eid, ok, err = G.parse_gifting_result(data)
                if eid == 0:
                    print("[*] server menerima gift (Received). "
                          "Menunggu penerima konfirmasi di game...")
                elif eid == 1:
                    print(f"[+] GiftingResult: Successful={ok} "
                          f"Error={err!r}")
                    return ok, err
        return None, None

    # --- loop per pet ---
    for idx, pet_id in enumerate(pet_ids, start=1):
        rec = {"pet_id": pet_id, "status": "failed", "attempts": 0,
               "error": None}
        print(f"\n[*] === Pet {idx}/{len(pet_ids)}: {pet_id} ===")
        for attempt in range(1, max_retries + 2):
            rec["attempts"] = attempt
            req = G.build_gifting_sent(recipient, [pet_id])
            try:
                sock.sendall(L.build_room_event_varint("gifting", req))
            except Exception as exc:
                print(f"[!] kirim gift gagal: {exc}")
                rec["error"] = str(exc)
                break
            print(f"[*] gift pet {pet_id} terkirim (percobaan {attempt}). "
                  f"Menunggu konfirmasi penerima (maks {wait_seconds} dtk)...")
            ok, err = tunggu_hasil(wait_seconds)
            if ok is True:
                rec["status"] = "accepted"
                rec["error"] = None
                print(f"[+] Pet {pet_id} DITERIMA.")
                break
            if ok is False:
                rec["status"] = "rejected"
                rec["error"] = err or "ditolak penerima"
                print(f"[!] Pet {pet_id} DITOLAK ({rec['error']}).")
            else:
                rec["status"] = "timeout"
                rec["error"] = "tidak ada konfirmasi (timeout)"
                print(f"[!] Pet {pet_id} TIMEOUT (tidak ada konfirmasi).")
            if attempt <= max_retries:
                print(f"[*] Kirim ulang pet {pet_id} "
                      f"(retry {attempt}/{max_retries})...")
                time.sleep(1)
            else:
                rec["status"] = "failed"
                print(f"[!] Pet {pet_id} GAGAL setelah {attempt} percobaan. "
                      f"Lanjut ke pet berikut.")
        hasil["results"].append(rec)
    try:
        sock.close()
    except Exception:
        pass
    return hasil


async def main():
    # Baca versi game/protokol dari dashboard panel (app_settings).
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("GIFT_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("GIFT_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("GIFT_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--pet-ids", default=os.getenv("GIFT_PET_IDS", ""),
                    help="Daftar pet_id dipisah koma, sesuai urutan dipilih")
    ap.add_argument("--recipient", default=os.getenv("GIFT_RECIPIENT", ""))
    ap.add_argument("--target-server",
                    default=os.getenv("GIFT_TARGET_SERVER", ""))
    ap.add_argument("--target-room", type=int,
                    default=int(os.getenv("GIFT_TARGET_ROOM", "0")))
    ap.add_argument("--dest-user-id",
                    default=os.getenv(
                        "GIFT_DEST_USER_ID",
                        "aae2efe1-1abb-48c2-8799-ea192692a49c"))
    ap.add_argument("--max-alloc-tries", type=int,
                    default=int(os.getenv("GIFT_MAX_ALLOC_TRIES", "15")))
    ap.add_argument("--wait", type=int,
                    default=int(os.getenv("GIFT_WAIT", "35")))
    ap.add_argument("--max-retries", type=int,
                    default=int(os.getenv("GIFT_MAX_RETRIES", "2")))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    pet_ids = [p.strip() for p in args.pet_ids.split(",") if p.strip()]
    if not pet_ids or not args.recipient:
        print("[!] pet-ids dan recipient (UUID) wajib diisi.")
        return 1
    print(f"[*] Diag Gift MULTI — bot {args.email} | {len(pet_ids)} pet "
          f"-> {args.recipient} | tunggu {args.wait} dtk/percobaan | "
          f"retry maks {args.max_retries}")
    L.QUIET = False
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await gift_multi_once(
            session, args.email, args.password, args.device_id,
            pet_ids, args.recipient, args.wait, args.max_retries,
            args.max_alloc_tries, args.target_server, args.target_room,
            args.dest_user_id)
    ok = sum(1 for r in hasil["results"] if r["status"] == "accepted")
    print("=" * 56)
    print("HASIL GIFT MULTI")
    for r in hasil["results"]:
        print(f"  {r['pet_id']}: {r['status']} "
              f"(percobaan {r['attempts']})"
              f"{' — ' + r['error'] if r['error'] else ''}")
    print(f"  Diterima: {ok}/{len(pet_ids)} | "
          f"Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 56)
    out = {
        "email": hasil["email"],
        "pet_ids": hasil["pet_ids"],
        "recipient": hasil["recipient"],
        "server": hasil["target_server"],
        "room": hasil["target_room"],
        "server_matched": hasil["server_matched"],
        "joined_room": hasil["joined_room"],
        "results": hasil["results"],
        "accepted": [r["pet_id"] for r in hasil["results"]
                     if r["status"] == "accepted"],
        "failed": [r["pet_id"] for r in hasil["results"]
                   if r["status"] != "accepted"],
    }
    print("GIFT_MULTI_JSON: " + json.dumps(out, ensure_ascii=False))
    GJ._kirim_hasil_cepat("gift_multi", hasil["email"], out)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
