# -*- coding: utf-8 -*-
"""
gift_join_room_diag.py — Diag gift pet via TCP DARI ROOM YANG SAMA.

Bot login -> alokasi lobby DIULANG sampai gameServerId cocok dengan server
target (SkyDev), lalu TCP ke server itu -> JoinRoom(nomor room target) ->
kirim GiftingSent (RoomEvent 'gifting') -> tunggu GiftingResult.

Latar: gift test 1 & 2 (2026-11-09) terkirim (server kirim Received) tapi
SkyDev TIDAK melihat popup sama sekali, lalu Result 'Gifting cancelled by
timeout' setelah ~30 dtk. Dugaan Bartes: gift (RoomEvent) hanya sampai ke
penerima bila pengirim & penerima ada di ROOM yang sama. Bot selama ini
join room-nya sendiri (dari alokasinya), bukan room SkyDev. Diag ini
menguji dugaan itu: bot masuk ke server + room SkyDev dulu, baru gift.

gameServerId SkyDev (2026-11-09, World utama):
  digitalocean-blr1-02::pkxd-tb-prod-1.94.0-main-vvz4s-vh7s7::7
  -> server 'pkxd-tb-prod-1.94.0-main-vvz4s-vh7s7', room 7,
     datacenter 'blr1'.

Alokasi lobby TIDAK bisa minta server tertentu (hanya userToken +
protocol + isPrivate, opsional datacenterId). Jadi diag mengulang
alokasi (pakai JWT yang sama, datacenterId 'blr1') sampai server
cocok, maks N percobaan. Bila tidak cocok, BERHENTI (tidak kirim gift
dari server lain — itu hanya mengulang tes yang sudah gagal).

CARA UTAMA (join server teman, usul Bartes 2026-11-09): dump punya
`AllocateToUserIdRequest : GenericAllocateRequest` dengan field
`destinationUserId` — alokasi lobby dengan destinationUserId = UUID
teman menempatkan bot di server teman itu. Bot pkxdshop.id hanya
berteman dengan SkyDev, jadi ini jalur yang tepat. Diag mencoba
destinationUserId DULU; bila tidak mengembalikan server target,
baru fallback ke alokasi ulang.

Dibuat 2026-11-09 atas permintaan Bartes (tes gift akun tes -> SkyDev,
satu room). Jangan dipakai untuk order.
"""
import argparse
import asyncio
import json
import os
import time

import lookup_uuid as L
import gift_diag as G


def parse_gsid(gsid):
    """'dc::server::room' -> (server, room_int_atau_None)."""
    parts = (gsid or "").split("::")
    server = parts[1] if len(parts) >= 2 else ""
    room = None
    if len(parts) >= 3:
        try:
            room = int(parts[2])
        except Exception:
            room = None
    return server, room


async def allocate_once(session, jwt, datacenter=None):
    """Satu POST /lobby/allocation (main world, tanpa activityId).
    Kembalikan dict body (atau {} bila gagal)."""
    body = {"userToken": jwt,
            "gameServerProtocolVersion": L.GAME_PROTOCOL,
            "isPrivate": False}
    if datacenter:
        body["datacenterId"] = datacenter
    b = json.dumps(body, separators=(",", ":")).encode("utf-8")
    try:
        r = await session.post(L.LOBBY_URL + "/lobby/allocation", data=b,
                               headers=L.HEADERS_BASE.copy(), timeout=20)
        resp = r.json()
    except Exception as exc:
        print(f"[-] alokasi gagal: {exc}")
        return {}
    return resp if isinstance(resp, dict) else {}


async def allocate_to_user(session, jwt, dest_user_id):
    """Alokasi ke server seorang teman (AllocateToUserIdRequest, dump:
    GenericAllocateRequest + destinationUserId). POST /lobby/allocation
    dengan destinationUserId = UUID teman. Kembalikan dict body."""
    body = {"userToken": jwt,
            "gameServerProtocolVersion": L.GAME_PROTOCOL,
            "isPrivate": False,
            "destinationUserId": dest_user_id}
    b = json.dumps(body, separators=(",", ":")).encode("utf-8")
    try:
        r = await session.post(L.LOBBY_URL + "/lobby/allocation", data=b,
                               headers=L.HEADERS_BASE.copy(), timeout=20)
        resp = r.json()
    except Exception as exc:
        print(f"[-] alokasi destinationUserId gagal: {exc}")
        return {}
    return resp if isinstance(resp, dict) else {}


async def gift_join_room_once(session, email, password, device_id, pet_id,
                              recipient, wait_seconds, max_alloc_tries,
                              target_server, target_room, dest_user_id=""):
    """Satu percobaan gift dari room target. Kembalikan dict hasil."""
    hasil = {"email": email, "pet_id": pet_id, "recipient": recipient,
             "target_server": target_server, "target_room": target_room,
             "alloc_tries": 0, "alloc_servers": [], "server_matched": False,
             "joined_room": None, "sent": False, "result_event": None,
             "successful": None, "error": None, "gifting_events": [],
             "events": []}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil

    # --- alokasi ke server teman dulu (destinationUserId = join friend) ---
    # Mode panel (hanya UUID target): bila target_server kosong, server +
    # room diambil dari alokasi destinationUserId itu sendiri (alokasi itu
    # menempatkan bot di server teman; gameServerId berisi server + room).
    # Bila target_server diisi (mode diag lama), server harus cocok.
    chosen = None
    allocs = [alloc]
    srv0, room0 = parse_gsid(alloc.get("gameServerId", ""))
    print(f"[*] alokasi awal (tanpa dest): server={srv0} room={room0}")
    hasil["alloc_servers"].append(srv0)
    if target_server and srv0 == target_server:
        chosen = alloc
    if not chosen and dest_user_id:
        print(f"[*] coba alokasi destinationUserId={dest_user_id} "
              f"(join server teman) ...")
        body = await allocate_to_user(session, user_jwt, dest_user_id)
        if body:
            allocs.append(body)
            srv, rm = parse_gsid(body.get("gameServerId", ""))
            hasil["alloc_servers"].append(srv)
            print(f"[*] alokasi destinationUserId: server={srv} room={rm}")
            if not target_server:
                # Mode panel: percaya alokasi destinationUserId.
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
                print("[+] destinationUserId BERHASIL — bot dialokasikan "
                      "ke server teman!")
            else:
                print("[*] destinationUserId tidak mengembalikan server "
                      "target (mungkin field diabaikan / teman tidak "
                      "ditemukan) — lanjut ke alokasi ulang.")
    # --- fallback: alokasi ulang sampai server cocok (hanya bila target_server diisi) ---
    if not chosen and target_server:
        for i in range(2, max_alloc_tries + 1):
            body = await allocate_once(session, user_jwt, datacenter="blr1")
            hasil["alloc_tries"] = i
            if not body:
                continue
            allocs.append(body)
            srv, rm = parse_gsid(body.get("gameServerId", ""))
            hasil["alloc_servers"].append(srv)
            print(f"[*] alokasi percobaan {i}: server={srv} room={rm}")
            if srv == target_server:
                chosen = body
                break
            await asyncio.sleep(0.5)
    hasil["alloc_tries"] = len(allocs)
    if not chosen:
        if not target_server:
            print("[!] destinationUserId tidak mengembalikan server teman. "
                  "BERHENTI (tidak tahu server/room target).")
        else:
            print(f"[!] Server target {target_server} TIDAK didapat setelah "
                  f"{len(allocs)} alokasi. BERHENTI (tidak kirim gift dari "
                  f"server lain).")
        return hasil
    hasil["server_matched"] = True
    host, port = L.find_host_port(chosen)
    if not host or not port:
        print("[!] host/port tidak ketemu di allocation yang cocok.")
        return hasil
    print(f"[+] Server cocok: {target_server} -> {host}:{port}")

    # --- TCP + login ---
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

    def _catat(frame):
        name, data = G.decode_room_event(frame)
        if name is None:
            return
        hasil["events"].append((name, data.hex() if data else ""))
        if name != "gifting":
            return
        hasil["gifting_events"].append(data.hex() if data else "")
        eid, ok, err = G.parse_gifting_result(data)
        print(f"[+] RoomEvent 'gifting' diterima: EventId={eid} "
              f"Successful={ok} Error={err!r}")
        if eid == 1:
            hasil["result_event"] = 1
            hasil["successful"] = ok
            hasil["error"] = err
        elif eid == 0:
            print("[*] server menerima gift (Received). "
                  "Menunggu penerima konfirmasi di game...")

    # --- tunggu jendela login (tbugv / JoinedRoom) ---
    print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
    got, closed = fr.read_until(
        lambda f: G._is_tbugv(f) or G._is_joinedroom(f), 3)
    if closed:
        print("[!] koneksi tertutup saat tunggu login.")
        return hasil
    if got is not None and G._is_tbugv(got):
        print("[*] tbugv diterima — observasi 1 dtk lagi.")
        time.sleep(1)
    elif got is not None:
        print("[*] JoinedRoom diterima (auto-join).")
    else:
        print("[*] timeout jendela login (koneksi hidup) — lanjut.")

    # --- JoinRoom(room target) secara eksplisit ---
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
        print(f"[+] JoinedRoom({target_room}) TERKONFIRMASI — bot di room "
              f"yang sama dengan SkyDev.")
    else:
        print(f"[!] TIDAK ada konfirmasi JoinedRoom({target_room}) dalam "
              f"4 dtk. Bot mungkin tetap di room auto-join. Lanjut kirim "
              f"gift (hasil akan menunjukkan apakah room berpengaruh).")

    # --- kirim gift ---
    req = G.build_gifting_sent(recipient, [pet_id])
    print(f"[*] GiftingSent ({len(req)} B): {req.hex()}")
    print(f"[*] kirim gift pet {pet_id} ke {recipient} dari room "
          f"{target_room} ...")
    try:
        sock.sendall(L.build_room_event_varint("gifting", req))
    except Exception as exc:
        print(f"[!] kirim gift gagal: {exc}")
        return hasil
    hasil["sent"] = True
    print("[+] gift terkirim. Menunggu Result "
          f"(maks {wait_seconds} dtk, penerima harus konfirmasi di game)...")
    t_end = time.time() + wait_seconds
    while time.time() < t_end:
        if hasil["result_event"] is not None:
            break
        chunk = min(10, max(1, int(t_end - time.time())))
        for f in fr.drain(chunk):
            _catat(f)
    try:
        sock.close()
    except Exception:
        pass
    return hasil


def _kirim_hasil_cepat(kind, email, result):
    """POST hasil diag ke /api/pet-result (jalur cepat panel).
    Gagal kirim TIDAK menggagalkan diag (log tetap jadi fallback).
    Secret dari env PET_RESULT_SECRET (GitHub secret)."""
    secret = os.getenv("PET_RESULT_SECRET", "")
    if not secret:
        return
    try:
        import urllib.request
        data = json.dumps({"kind": kind, "email": email,
                           "result": result, "secret": secret}).encode()
        req = urllib.request.Request(
            "https://pkxd.shop/api/pet-result", data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=20) as resp:
            resp.read()
        print("[+] hasil cepat terkirim ke /api/pet-result")
    except Exception as exc:
        print(f"[*] kirim hasil cepat gagal (log tetap jadi fallback): {exc}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("GIFT_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("GIFT_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("GIFT_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--pet-id", default=os.getenv("GIFT_PET_ID", ""))
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
                    default=int(os.getenv("GIFT_WAIT", "60")))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    if not args.pet_id or not args.recipient:
        print("[!] pet-id dan recipient (UUID) wajib diisi.")
        return 1
    print(f"[*] Diag Gift Join-Room — bot {args.email} | pet {args.pet_id} "
          f"-> {args.recipient}")
    print(f"[*] Target: server {args.target_server} room {args.target_room} "
          f"| dest-user-id {args.dest_user_id} "
          f"| maks alokasi {args.max_alloc_tries} | tunggu {args.wait} dtk")
    L.QUIET = False
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await gift_join_room_once(
            session, args.email, args.password, args.device_id,
            args.pet_id, args.recipient, args.wait,
            args.max_alloc_tries, args.target_server, args.target_room,
            args.dest_user_id)
    print("=" * 56)
    print("HASIL GIFT JOIN-ROOM DIAG")
    print(f"  Bot: {hasil['email']} | pet: {hasil['pet_id']}")
    print(f"  Penerima: {hasil['recipient']}")
    print(f"  Target: server {hasil['target_server']} "
          f"room {hasil['target_room']}")
    print(f"  Alokasi dicoba: {hasil['alloc_tries']} | server cocok: "
          f"{hasil['server_matched']}")
    print(f"  Server-server alokasi: {hasil['alloc_servers']}")
    print(f"  JoinedRoom target: {hasil['joined_room']}")
    print(f"  Gift terkirim: {hasil['sent']}")
    print(f"  Result EventId: {hasil['result_event']} | "
          f"Successful: {hasil['successful']} | Error: {hasil['error']!r}")
    print(f"  RoomEvent 'gifting' diterima: {len(hasil['gifting_events'])}")
    for hx in hasil["gifting_events"]:
        print(f"    - gifting: {hx[:200]}")
    print(f"  Total RoomEvent diterima: {len(hasil['events'])}")
    print(f"  Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 56)
    # Baris JSON mesin (untuk panel Pet Gift "Run Gift"): satu baris,
    # mudah diparse dari log GitHub Actions tanpa regex rapuh.
    print("GIFT_JSON: " + json.dumps({
        "email": hasil["email"],
        "pet_id": hasil["pet_id"],
        "recipient": hasil["recipient"],
        "server": hasil["target_server"],
        "room": hasil["target_room"],
        "server_matched": hasil["server_matched"],
        "joined_room": hasil["joined_room"],
        "sent": hasil["sent"],
        "successful": hasil["successful"],
        "error": hasil["error"],
    }, ensure_ascii=False))
    # Jalur cepat: POST hasil ke /api/pet-result agar panel tidak perlu
    # menunggu arsip log GitHub (30 dtk–2 mnt). Secret dari env
    # PET_RESULT_SECRET (GitHub secret, diteruskan workflow).
    _kirim_hasil_cepat("gift", hasil["email"], {
        "email": hasil["email"],
        "pet_id": hasil["pet_id"],
        "recipient": hasil["recipient"],
        "server": hasil["target_server"],
        "room": hasil["target_room"],
        "server_matched": hasil["server_matched"],
        "joined_room": hasil["joined_room"],
        "sent": hasil["sent"],
        "successful": hasil["successful"],
        "error": hasil["error"],
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
