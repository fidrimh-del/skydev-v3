# -*- coding: utf-8 -*-
"""
spawn_diag.py — Bot spawn normal di titik spawn (Central Square, plaza).

Latar (2026-11-09):
- Bot yang join tanpa kirim posisi parkir di (0,0,0) (terbukti di tes
  follow_tfi v1/v2). Server TIDAK menempatkan bot di spawn point saat
  join (JoinRoom hanya bawa roomId, tanpa field spawn — dump.cs).
- Spawn ditentukan CLIENT-SIDE (CentralSquareSpawnLocationInitStep:
  TrySpawnAtFriendLocation / SpawnAtCentralSquare), lalu client kirim
  posisi spawn-nya lewat frame "aaa" (RoomEvent "tbulv"). Itu alur
  join NORMAL client — BUKAN lokasi palsu / teleport.
- Koordinat titik spawn plaza didapat dari mod CoordLog v3 (tombol
  "Ambil Posisi", Bartes 2026-11-09 22:11): X=79.96 Y=27.13 Z=-122.17.
- Tes ini: bot login -> alokasi destinationUserId (SkyDev) -> JoinRoom
  -> kirim SATU frame "aaa" berisi titik spawn tetap itu, sekali saja.
  TIDAK ada loop, TIDAK ada kirim ulang, TIDAK ada koordinat live
  pemain. Bot lalu diam di titik spawn sambil membaca frame "tbulv"
  (verifikasi bot-side: ada pemain di dekat titik spawn?).
- Frame "aaa" dibangun meniru PERSIS frame kirim asli client
  ([TBULV-BASE] UserVarLog v2, frame ke-3) — hanya ketiga float
  koordinat (X/Y/Z) yang diganti; varint/tag/panjang lain dibiarkan
  sama. Format terverifikasi: [4-byte BE len=3]["aaa"][0x05]
  [protobuf 5 sub-message], float di sub-field 3 (tag 0x1D):
  field 1 = Y, field 2 = X, field 4 = Z.
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
# Titik spawn plaza (Central Square) — dari CoordLog v3 Bartes 22:11 WIB.
SPAWN_X = 79.96
SPAWN_Y = 27.13
SPAWN_Z = -122.17


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
    """Bangun data RoomEvent "tbulv" berisi posisi spawn (frame "aaa").
    Meniru persis frame kirim asli client (UserVarLog v2 frame ke-3);
    hanya float X/Y/Z yang diganti dengan titik spawn.
    """
    f32 = lambda v: struct.pack("<f", v)
    # Sub-message: [varint-varint...] + [0x1D + float]. Disalin dari
    # frame asli (varint di dalamnya dibiarkan apa adanya).
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


async def spawn_once(session, email, password, device_id, dest_user_id,
                     sx, sy, sz, listen_s=15):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "server": "", "room": None, "joined_room": None,
             "spawn": {"x": sx, "y": sy, "z": sz},
             "spawn_sent": False, "spawn_t_rel": None,
             "tbulv_after": 0, "near_spawn": []}
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
    t0 = time.time()
    if joined is not None:
        hasil["joined_room"] = rm
        print(f"[+] JoinedRoom({rm}) TERKONFIRMASI.")
    else:
        print("[!] TIDAK ada konfirmasi JoinedRoom — tetap kirim spawn.")
    # --- kirim SATU frame spawn (sekali saja, titik tetap) ---
    data = build_spawn_aaa(sx, sy, sz)
    frame = L.build_room_event_varint("tbulv", data)
    try:
        sock.sendall(frame)
        hasil["spawn_sent"] = True
        hasil["spawn_t_rel"] = round(time.time() - t0, 1)
        print(f"[+] frame SPAWN terkirim t+{time.time() - t0:.1f} dtk "
              f"X={sx} Y={sy} Z={sz}")
    except Exception as exc:
        print(f"[!] kirim frame spawn gagal: {exc}")
        return hasil
    # --- dengarkan: ada pemain di dekat titik spawn? ---
    from follow_diag3 import parse_tbulv_pos as _parse_pos
    near = {}
    end = time.time() + listen_s
    while time.time() < end:
        for f in fr.drain(1):
            name, d = G.decode_room_event(f)
            if name != "tbulv":
                continue
            hasil["tbulv_after"] += 1
            pid, pos = _parse_pos(d)
            if not pid or "X" not in pos or "Z" not in pos:
                continue
            dx = pos["X"] - sx
            dz = pos["Z"] - sz
            dist = (dx * dx + dz * dz) ** 0.5
            if dist < 15 and pid not in near:
                near[pid] = {"x": round(pos["X"], 1),
                             "y": pos.get("Y"),
                             "z": round(pos["Z"], 1),
                             "dist": round(dist, 1)}
    hasil["near_spawn"] = [{"id": k, **v} for k, v in near.items()]
    print(f"[+] selesai. tbulv diterima: {hasil['tbulv_after']}, "
          f"pemain dekat spawn: {len(near)}")
    try:
        sock.close()
    except Exception:
        pass
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("SPAWN_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("SPAWN_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("SPAWN_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--dest-user-id",
                    default=os.getenv("SPAWN_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--spawn-x", type=float, default=SPAWN_X)
    ap.add_argument("--spawn-y", type=float, default=SPAWN_Y)
    ap.add_argument("--spawn-z", type=float, default=SPAWN_Z)
    ap.add_argument("--listen", type=float, default=15)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await spawn_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.spawn_x, args.spawn_y, args.spawn_z,
            args.listen)
    print("SPAWN_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
