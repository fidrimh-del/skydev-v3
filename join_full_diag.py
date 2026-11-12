# -*- coding: utf-8 -*-
"""
join_full_diag.py — Diagnostik JOIN LENGKAP bot (untuk analisis dump.cs).

Latar (2026-11-12, Bartes): bot join normal di Plaza tapi di Sekolah
(activity) wujudnya tidak ke-render & gendong gagal. Analisis dump.cs:
- Alur client asli: Connect -> Login -> JoinRoom ->
  OnRoomJoined(JoinRoomResponse{Success, ErrorMessage}) -> StartScene ->
  LoadScene -> RunInitSteps (PlayerCreateInitStep, GetExistingPlayers,
  SpawnLocation, PlayerVisibility, ...).
- Remote player di-render client lain bila server kirim
  RoomUserUpdateResponse{AddedPlayer: IRemotePlayerServerState} ->
  client panggil AddRemotePlayer/CreateRemotePlayer.
- Bot selama ini: login -> JoinRoom -> TIDAK PERNAH dapat konfirmasi
  JoinedRoom -> kirim 1 frame posisi -> selesai. Diduga join bot tidak
  lengkap sehingga server tak broadcast bot sebagai pemain penuh di
  activity (bot tertahan di spawn awal / awan).
- Script ini TIDAK gendong / tidak invite. Hanya JOIN lalu MEREKAM
  SEMUA frame yang server kirim (login window, join window, pasca
  spawn), supaya ketahuan: apakah JoinedRoom benar dikirim (dan dalam
  format apa), apakah ada RoomUserUpdate / daftar pemain, frame apa
  saja yang server kirim sesudah JoinRoom di activity.
- Dipakai untuk merancang "join sangat lengkap" di Termux v3.6+.
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G

KONTOL_UUID = "c6c271e0-a3d9-4a93-bf64-1c715d1b7fbb"
BOT_UUID = "71056756-53c7-478e-9356-82c87fb2dd63"
SPAWN_X, SPAWN_Y, SPAWN_Z = 79.96, 27.13, -122.17


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


def build_spawn_aaa(x, y, z):
    """Frame 'aaa' posisi spawn (sama seperti Termux bot)."""
    f32 = lambda v: struct.pack("<f", v)
    sub_y = bytes.fromhex("08f980a5ab05") + b"\x1d" + f32(y)
    sub_x = bytes.fromhex("089b87987010b3f2de61") + b"\x1d" + f32(x)
    sub_r = bytes.fromhex(
        "08ffffffffffffffffff0110ffffffffffffffffff01")
    sub_z = bytes.fromhex("08a2b0b7b0fdffffffff01") + b"\x1d" + f32(z)
    sub_v = bytes.fromhex(
        "08ffffffffffffffffff0110ffffffffffffffffff01")
    pb = b""
    for field, sub in ((1, sub_y), (2, sub_x), (3, sub_r),
                       (4, sub_z), (5, sub_v)):
        pb += bytes([(field << 3) | 2, len(sub)]) + sub
    return struct.pack(">I", 3) + b"aaa" + b"\x05" + pb


def classify_frame(f):
    """Klasifikasi kasar frame mentah -> (label, ringkasan)."""
    if not f:
        return ("empty", "")
    b0 = f[0]
    if b0 == 0x01:
        return ("ping", "")
    if b0 == 0x05:
        ln, m = L.read_varint(f, 1) if hasattr(L, "read_varint") else (None, 0)
        try:
            nm = f[1 + m:1 + m + ln].decode("utf-8", "replace") if ln else "?"
        except Exception:
            nm = "?"
        return ("tbugv?" if nm == "tbugv" else f"0x05:{nm}", "")
    if b0 == 0x06:
        if len(f) >= 2 and f[1] == 1:
            rm = int.from_bytes(f[2:6], "big") if len(f) >= 6 else None
            return ("JoinedRoom?", f"room={rm}")
        return ("0x06", f"len={len(f)}")
    if b0 == 0x08:
        try:
            name, d = G.decode_room_event(f)
        except Exception:
            name, d = None, None
        if name:
            return (f"RoomEvent:{name}", f"len={len(d) if d else 0}")
        return ("RoomEvent:?", "")
    return (f"0x{b0:02x}", f"len={len(f)}")


async def join_full_once(session, email, password, device_id,
                         dest_user_id, bot_user_id, listen_s):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id, "server": "", "room": None,
             "joined_room": None, "joined_room_frame_hex": None,
             "login_window_frames": [], "join_window_frames": [],
             "post_spawn_frames": [], "event_names": {},
             "tbulv_player_ids": [], "room_user_update_seen": False}
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

    def _collect(fr, seconds, bucket, max_frames=60):
        """Kumpulkan semua frame selama `seconds` dtk ke bucket."""
        end = time.time() + seconds
        n = 0
        while time.time() < end and n < max_frames:
            for f in fr.drain(0.5):
                label, ring = classify_frame(f)
                bucket.append({"t_rel": round(time.time() - t0, 1),
                               "label": label, "ring": ring,
                               "hex": f[:48].hex()})
                n += 1
                if label.startswith("RoomEvent:"):
                    nm = label.split(":", 1)[1]
                    hasil["event_names"][nm] = \
                        hasil["event_names"].get(nm, 0) + 1
                if label == "JoinedRoom?" and hasil["joined_room"] is None:
                    try:
                        hasil["joined_room"] = int.from_bytes(
                            f[2:6], "big")
                        hasil["joined_room_frame_hex"] = f[:32].hex()
                    except Exception:
                        pass
                if "RoomUserUpdate" in label or "room_user" in label:
                    hasil["room_user_update_seen"] = True
            # drain habis -> lanjut sampai end
        return n

    print("[*] FASE 1: login window — rekam semua frame (3 dtk)...")
    _collect(fr, 3, hasil["login_window_frames"])
    for e in hasil["login_window_frames"]:
        print(f"    login t+{e['t_rel']}: {e['label']} {e['ring']} "
              f"hex={e['hex'][:64]}")
    print(f"[*] kirim JoinRoom({rm}) ...")
    try:
        sock.sendall(L.build_join_frame(rm))
    except Exception as exc:
        print(f"[!] kirim JoinRoom gagal: {exc}")
        return hasil
    print("[*] FASE 2: join window — rekam semua frame (10 dtk)...")
    _collect(fr, 10, hasil["join_window_frames"])
    for e in hasil["join_window_frames"]:
        print(f"    join t+{e['t_rel']}: {e['label']} {e['ring']} "
              f"hex={e['hex'][:64]}")
    if hasil["joined_room"] is not None:
        print(f"[+] JoinedRoom TERKONFIRMASI room={hasil['joined_room']} "
              f"hex={hasil['joined_room_frame_hex']}")
    else:
        print("[!] TIDAK ada konfirmasi JoinedRoom di 10 dtk.")
    # Kirim spawn seperti bot Termux, lalu rekam lagi.
    try:
        data = build_spawn_aaa(SPAWN_X, SPAWN_Y, SPAWN_Z)
        sock.sendall(L.build_room_event_varint("tbulv", data))
        print(f"[+] Posisi spawn terkirim (Plaza "
              f"{SPAWN_X},{SPAWN_Y},{SPAWN_Z}).")
    except Exception as exc:
        print(f"[!] kirim spawn gagal: {exc}")
    print(f"[*] FASE 3: pasca-spawn — rekam semua frame ({listen_s} dtk)...")
    _collect(fr, listen_s, hasil["post_spawn_frames"])
    # Hitung pemain tbulv dari event_names + frame RoomEvent tbulv.
    # (player id diambil dari frame tbulv saat decode di bawah.)
    for e in hasil["post_spawn_frames"]:
        if e["label"] == "RoomEvent:tbulv":
            try:
                raw = bytes.fromhex(e["hex"])
                # hex hanya 48 byte pertama; id di byte 2-4 payload
                # (tidak bisa diandalkan dari potongan) — lewati.
                pass
            except Exception:
                pass
    print(f"[+] selesai. event_names={hasil['event_names']}")
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
    ap.add_argument("--listen", type=float, default=10)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await join_full_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id, args.listen)
    print("JOIN_FULL_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
