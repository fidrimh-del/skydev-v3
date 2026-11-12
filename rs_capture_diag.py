# -*- coding: utf-8 -*-
"""
rs_capture_diag.py — Tangkap frame event RS di Sekolah + bandingkan Plaza.

Latar (2026-11-12, Bartes):
- Bot TIDAK render di Sekolah (wujud tidak terlihat, gendong tidak
  jalan), padahal di Plaza gendong BERFUNGSI. Bartes: ini sistem
  berbeda antara Plaza dan Sekolah, harus dicari tahu.
- Petunjuk: tes carry Sekolah (run 43193136577) mencatat event
  "RS" 639x dalam 60 dtk, tapi join-full-diag Sekolah
  (run 43194056696) mencatat NOL RS. Jadi RS BUKAN event rutin
  Sekolah — RS dipicu sesuatu yang dilakukan tes carry (invite?
  smart spawn? dengar lama?). RS bisa jadi kunci perbedaan
  Plaza vs Sekolah.
- Diag ini: join target (Sekolah/Plaza) -> smart spawn -> kirim
  invite gendong (seperti carry) -> rekam SEMUA frame RS (hex penuh,
  maks 5 contoh) + hitung event + rekam frame 0x0d/0x06.
- TIDAK perlu Bartes terima/tolak invite; hanya rekam frame.
  Fokus: apa itu RS, kapan muncul, dan apakah ada frame
  RoomUserUpdate / broadcast pemain.
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G
import carry_invite_diag as C

KONTOL_UUID = "c6c271e0-a3d9-4a93-bf64-1c715d1b7fbb"
BOT_UUID = "71056756-53c7-478e-9356-82c87fb2dd63"


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


async def rs_capture_once(session, email, password, device_id,
                          dest_user_id, bot_user_id, listen_s):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id, "server": "", "room": None,
             "joined_room": None, "spawn_pos": None, "spawn_mode": None,
             "invite_sent": False, "event_names": {},
             "rs_count": 0, "rs_samples": [],
             "rs_first_t_rel": None,
             "frame_type_counts": {}, "room_user_update_seen": False,
             "joinedroom_in_batch": False}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil
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
    t0 = time.time()
    # Jendela login: rekam frame, cari JoinedRoom di batch 0x0d.
    print("[*] jendela login 3 dtk (rekam semua frame)...")
    end = time.time() + 3
    while time.time() < end:
        for f in fr.drain(0.5):
            if not f:
                continue
            hasil["frame_type_counts"][f"0x{f[0]:02x}"] = \
                hasil["frame_type_counts"].get(f"0x{f[0]:02x}", 0) + 1
            if f[0] == 0x0d:
                needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
                if needle in bytes(f):
                    hasil["joinedroom_in_batch"] = True
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
    if joined is not None:
        hasil["joined_room"] = rm
        print(f"[+] JoinedRoom({rm}) TERKONFIRMASI.")
    else:
        print("[!] TIDAK ada konfirmasi JoinedRoom standalone — tetap lanjut.")
    # Smart spawn (seperti carry v2).
    sx, sy, sz, smode = C.smart_spawn(fr, srv, seconds=5)
    hasil["spawn_pos"] = [round(sx, 2), round(sy, 2), round(sz, 2)]
    hasil["spawn_mode"] = smode
    try:
        data = C.build_spawn_aaa(sx, sy, sz)
        sock.sendall(L.build_room_event_varint("tbulv", data))
        print(f"[+] Posisi spawn terkirim ({sx:.2f},{sy:.2f},{sz:.2f}) "
              f"[{smode}].")
    except Exception as exc:
        print(f"[!] kirim spawn gagal: {exc}")
    # Kirim invite (seperti carry) lalu dengarkan + rekam RS.
    invite_pb = C.build_consent_invite(bot_user_id, dest_user_id, 0)
    try:
        sock.sendall(L.build_room_event_varint("consent", invite_pb))
        hasil["invite_sent"] = True
        print("[+] INVITE gendong terkirim.")
    except Exception as exc:
        print(f"[!] kirim invite gagal: {exc}")
    print(f"[*] mendengarkan {listen_s} dtk — rekam RS + semua event...")
    end = time.time() + listen_s
    while time.time() < end:
        for f in fr.drain(0.5):
            if not f:
                continue
            hasil["frame_type_counts"][f"0x{f[0]:02x}"] = \
                hasil["frame_type_counts"].get(f"0x{f[0]:02x}", 0) + 1
            try:
                name, d = G.decode_room_event(f)
            except Exception:
                name, d = None, None
            if name:
                hasil["event_names"][name] = \
                    hasil["event_names"].get(name, 0) + 1
                if name == "RS":
                    hasil["rs_count"] += 1
                    if hasil["rs_first_t_rel"] is None:
                        hasil["rs_first_t_rel"] = round(time.time() - t0, 1)
                    if len(hasil["rs_samples"]) < 5:
                        raw = bytes(d) if isinstance(d, (bytes, bytearray)) else b""
                        hasil["rs_samples"].append({
                            "t_rel": round(time.time() - t0, 1),
                            "len": len(raw),
                            "hex": raw[:200].hex()})
                        print(f"    RS t+{round(time.time()-t0,1)} len={len(raw)} "
                              f"hex={raw[:120].hex()}")
                if "room_user" in name.lower() or "user_update" in name.lower():
                    hasil["room_user_update_seen"] = True
    print(f"[+] selesai. RS={hasil['rs_count']} (pertama t+{hasil['rs_first_t_rel']}), "
          f"event={hasil['event_names']}")
    try:
        sock.close()
    except Exception:
        pass
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("RS_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("RS_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("RS_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--dest-user-id",
                    default=os.getenv("RS_DEST_USER_ID", KONTOL_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("RS_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--listen", type=float, default=30)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await rs_capture_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id, args.listen)
    print("RS_CAPTURE_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
