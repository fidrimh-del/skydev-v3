# -*- coding: utf-8 -*-
"""
follow_tfi.py — Tes kirim TargetFriendId ("tfi") dari bot.

Latar (2026-11-09):
- "tfi" = user variable TargetFriendId, UUID teman (SkyDev). Client asli
  men-SET "tfi" saat join teman (terbukti di log mod, 2x), tapi TIDAK
  dikirim lewat EMIT "tbulv" key-value dan TIDAK ada di frame "tbulv"
  posisi maupun join frame. Jalur kirim "tfi" yang sebenarnya belum
  ketahuan.
- Format EMIT "tbulv" key-value SUDAH terdecode dari log mod:
  [4-byte BE panjang key][key][value]. Nilai Bool = tag 04 + 1 byte
  (04 00 = false); nilai Int = tag 02 + 4 byte (02 00000000 = 0).
  Tag untuk String BELUM diketahui (tidak ada string yang ter-EMIT di
  log). Kandidat tag string dicoba di sini: 00, 01, 03, 05.
- Tes ini: bot join server SkyDev, lalu kirim RoomEvent "tbulv" berisi
  variable "tfi" = UUID SkyDev, dengan 4 kandidat encoding string,
  masing-masing dijeda 8 detik. Bartes (online) melihat apakah bot
  muncul/teleport ke dekatnya, dan kapan (dicocokkan ke waktu kirim).
- Bot TIDAK kirim posisi. Hanya kirim user variable "tfi" (fitur bawaan
  game: join teman -> teleport ke teman). Ini BUKAN kirim posisi palsu.
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G

SKYDEV_UUID = "aae2efe1-1abb-48c2-8799-ea192692a49c"


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


async def allocate_to_user(session, jwt, dest_user_id):
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


def build_tfi_emit(uuid_str, type_tag):
    """Bangun data EMIT user variable 'tfi' (String).
    Format: [4-byte BE keylen=3]['tfi'][type_tag][4-byte BE strlen][UTF-8].
    """
    key = b"tfi"
    val = uuid_str.encode("utf-8")
    return (struct.pack(">I", len(key)) + key
            + bytes([type_tag]) + struct.pack(">I", len(val)) + val)


async def follow_tfi_once(session, email, password, device_id, dest_user_id):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "server": "", "room": None, "joined_room": None,
             "sends": [], "tbulv_after": 0}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil
    print(f"[*] alokasi destinationUserId={dest_user_id} ...")
    body = await allocate_to_user(session, user_jwt, dest_user_id)
    if not body:
        print("[!] alokasi destinationUserId gagal/kosong.")
        return hasil
    srv, rm = parse_gsid(body.get("gameServerId", ""))
    hasil["server"] = srv
    hasil["room"] = rm
    print(f"[+] destinationUserId OK: server={srv} room={rm}")
    if not srv or rm is None:
        return hasil
    host, port = L.find_host_port(body)
    if not host or not port:
        print("[!] host/port tidak ketemu.")
        return hasil
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
    print(f"[*] kirim JoinRoom({rm}) ...")
    try:
        sock.sendall(L.build_join_frame(rm))
    except Exception as exc:
        print(f"[!] kirim JoinRoom gagal: {exc}")
        return hasil

    def _is_target_room(f):
        return (len(f) >= 6 and f[0] == 0x06 and f[1] == 1
                and int.from_bytes(f[2:6], "big") == rm)

    joined, closed = fr.read_until(_is_target_room, 4)
    if closed:
        print("[!] koneksi tertutup saat JoinRoom.")
        return hasil
    if joined is not None:
        hasil["joined_room"] = rm
        print(f"[+] JoinedRoom({rm}) TERKONFIRMASI.")
    else:
        print("[!] TIDAK ada konfirmasi JoinedRoom — lanjut kirim tfi.")

    # --- kirim 4 kandidat "tfi", jeda 8 dtk ---
    candidates = [0x00, 0x01, 0x03, 0x05]
    t0 = time.time()
    for idx, tag in enumerate(candidates):
        data = build_tfi_emit(dest_user_id, tag)
        frame = L.build_room_event_varint("tbulv", data)
        try:
            sock.sendall(frame)
        except Exception as exc:
            print(f"[!] kirim kandidat {idx} (tag {tag:#04x}) gagal: {exc}")
            continue
        t_send = time.time() - t0
        hasil["sends"].append({
            "idx": idx, "tag": tag, "t_rel": round(t_send, 1),
            "hex": data.hex()})
        print(f"[+] kandidat {idx} tag={tag:#04x} terkirim "
              f"t+{t_send:.1f} dtk hex={data.hex()}")
        # jeda 8 dtk sambil tetap baca frame (jaga koneksi)
        end = time.time() + 8
        while time.time() < end:
            for f in fr.drain(1):
                name, _d = G.decode_room_event(f)
                if name == "tbulv":
                    hasil["tbulv_after"] += 1
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. tbulv diterima selama tes: {hasil['tbulv_after']}")
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("FOLLOW_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("FOLLOW_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("FOLLOW_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--dest-user-id",
                    default=os.getenv("FOLLOW_DEST_USER_ID", SKYDEV_UUID))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await follow_tfi_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id)
    print("FOLLOWTFI_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
