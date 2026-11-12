# -*- coding: utf-8 -*-
"""
join_necessity_diag.py — Apakah JoinRoom diperlukan? + parse batch 0x0d.

Latar (diag v2, run 43189639014, 2026-11-12, Bartes):
- Join room SENDIRI: konfirmasi standalone [0x06][0x01][room 9].
- Join room KONTOL (destinationUserId): TIDAK ada konfirmasi
  standalone; 3x frame [0x06][0x00][0x03] (hex 060003), SATU per
  kirim JoinRoom, BUKAN gema (echo=0). Diduga penolakan JoinRoom
  (alasan 3) karena bot SUDAH di room.
- Bukti kunci: konfirmasi JoinedRoom room 10 ([0x06][0x01][room 10],
  hex 06010000000a) SUDAH ADA, tertanam di dalam frame batch 0x0d
  (len 30145) saat login window (t+1.1), SEBELUM bot kirim JoinRoom.
  Dan tbulv (data room) SUDAH masuk di login window, sebelum JoinRoom.
- Jadi bot kemungkinan SUDAH di room begitu login (server tempatkan
  berdasar alokasi lobby); JoinRoom eksplisit jadi berlebihan dan
  ditolak 060003. Bot selama ini salah diagnosis: mengira join
  gagal karena hanya cari konfirmasi standalone 0x06.
- Diag v3 ini membuktikan 2 hal:
  (A) Tanpa kirim JoinRoom SAMA SEKALI, apakah bot tetap terima
      data room (tbulv) + batch 0x0d berisi JoinedRoom?
      Bila ya -> JoinRoom tidak diperlukan; server auto-place.
  (B) Parse frame 0x0d (batch): cari sub-pesan JoinedRoom
      ([0x06][0x01][room]) di dalam payload-nya, laporkan posisi
      dan hex-nya. Bila ketemu -> konfirmasi memang datang via batch.
- TIDAK gendong / tidak invite. Hanya login + dengarkan + (di akhir)
  SATU JoinRoom untuk merekam responsnya.
"""
import argparse
import asyncio
import json
import os
import time

import lookup_uuid as L
import gift_diag as G

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


def find_joinedroom_in_batch(payload, rm):
    """Cari sub-pesan JoinedRoom [0x06][0x01][room 4 byte] di dalam
    payload frame 0x0d (batch). Kembalikan offset atau -1."""
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
    return bytes(payload).find(needle)


async def join_necessity_once(session, email, password, device_id,
                              dest_user_id, bot_user_id,
                              listen_no_join_s, listen_after_join_s):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id, "server": "", "room": None,
             "phase_a_no_join": {"tbulv": 0, "frames_0x0d": 0,
                                 "joinedroom_in_batch": False,
                                 "batch_offsets": [],
                                 "batch_hex_prefix": [],
                                 "event_names": {}},
             "phase_b_join_response": {"sent": False, "frames_0x06": [],
                                       "confirmed": False,
                                       "confirm_hex": None},
             "phase_c_after_join": {"tbulv": 0, "event_names": {}}}
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
    # FASE A: TANPA JoinRoom — dengarkan saja.
    print(f"[*] FASE A: TANPA JoinRoom — dengarkan {listen_no_join_s} dtk. "
          f"Apakah data room (tbulv) + batch 0x0d berisi JoinedRoom "
          f"tetap masuk?")
    end = time.time() + listen_no_join_s
    while time.time() < end:
        for f in fr.drain(0.5):
            if not f:
                continue
            if f[0] == 0x0d:
                hasil["phase_a_no_join"]["frames_0x0d"] += 1
                off = find_joinedroom_in_batch(f, rm)
                if off >= 0:
                    hasil["phase_a_no_join"]["joinedroom_in_batch"] = True
                    hasil["phase_a_no_join"]["batch_offsets"].append(off)
                    hasil["phase_a_no_join"]["batch_hex_prefix"].append(
                        bytes(f)[:80].hex())
                    print(f"[+] JoinedRoom(room {rm}) KETEMU di batch "
                          f"0x0d, offset {off}, "
                          f"hex={bytes(f)[:80].hex()[:120]}")
            try:
                name, d = G.decode_room_event(f)
            except Exception:
                name = None
            if name:
                hasil["phase_a_no_join"]["event_names"][name] = \
                    hasil["phase_a_no_join"]["event_names"].get(name, 0) + 1
                if name == "tbulv":
                    hasil["phase_a_no_join"]["tbulv"] += 1
    pa = hasil["phase_a_no_join"]
    print(f"[*] FASE A selesai: tbulv={pa['tbulv']}, "
          f"batch 0x0d={pa['frames_0x0d']}, JoinedRoom di batch="
          f"{pa['joinedroom_in_batch']}, event={pa['event_names']}")
    # FASE B: kirim JoinRoom SEKALI, rekam responsnya.
    print(f"[*] FASE B: kirim JoinRoom({rm}) SEKALI, rekam respons 6 dtk.")
    try:
        sock.sendall(L.build_join_frame(rm))
        hasil["phase_b_join_response"]["sent"] = True
    except Exception as exc:
        print(f"[!] kirim JoinRoom gagal: {exc}")
        return hasil
    end = time.time() + 6
    while time.time() < end:
        for f in fr.drain(0.5):
            if f and f[0] == 0x06:
                tag = "?"
                if len(f) >= 6 and f[1] == 1:
                    got = int.from_bytes(f[2:6], "big")
                    tag = ("JoinedRoom-KONFIRMASI" if got == rm
                           else f"JoinedRoom-room-lain({got})")
                elif bytes(f) == bytes(L.build_join_frame(rm)[4:]):
                    tag = "ECHO-JoinRoom-send"
                else:
                    tag = f"0x06-lain(len={len(f)},b1={f[1]:#04x})"
                hasil["phase_b_join_response"]["frames_0x06"].append({
                    "t_rel": round(time.time() - t0, 1), "tag": tag,
                    "hex": bytes(f).hex(), "len": len(f)})
                print(f"    join-resp t+{round(time.time()-t0,1)}: {tag} "
                      f"hex={bytes(f).hex()}")
                if tag == "JoinedRoom-KONFIRMASI":
                    hasil["phase_b_join_response"]["confirmed"] = True
                    hasil["phase_b_join_response"]["confirm_hex"] = \
                        bytes(f).hex()
    # FASE C: sesudah JoinRoom, dengarkan lagi (bandingkan tbulv).
    print(f"[*] FASE C: sesudah JoinRoom — dengarkan "
          f"{listen_after_join_s} dtk.")
    end = time.time() + listen_after_join_s
    while time.time() < end:
        for f in fr.drain(0.5):
            try:
                name, d = G.decode_room_event(f)
            except Exception:
                name = None
            if name:
                hasil["phase_c_after_join"]["event_names"][name] = \
                    hasil["phase_c_after_join"]["event_names"].get(name, 0) + 1
                if name == "tbulv":
                    hasil["phase_c_after_join"]["tbulv"] += 1
    pc = hasil["phase_c_after_join"]
    print(f"[*] FASE C selesai: tbulv={pc['tbulv']}, event={pc['event_names']}")
    try:
        sock.close()
    except Exception:
        pass
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("JOIN_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("JOIN_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("JOIN_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--dest-user-id",
                    default=os.getenv("JOIN_DEST_USER_ID", KONTOL_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("JOIN_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--listen-no-join", type=float, default=10)
    ap.add_argument("--listen-after-join", type=float, default=5)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await join_necessity_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id,
            args.listen_no_join, args.listen_after_join)
    print("JOIN_NECESSITY_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
