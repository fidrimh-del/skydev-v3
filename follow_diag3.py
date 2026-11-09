# -*- coding: utf-8 -*-
"""
follow_diag3.py — Diag tbulv langkah 3: identifikasi SkyDev lewat jalan lurus.

Latar (2026-11-09, dari log diag 1 & 2):
- RoomEvent 'tbulv' = update posisi pemain (~9-12/dtk). Header 12 byte,
  ID 2-byte pemain di byte 2-3; protobuf dari byte 12: field 1 subfield 3
  (fixed32) = Y (tinggi), field 2 subfield 3 = X, field 4 subfield 3 = Z.
  Terverifikasi offline terhadap log run 42577761988 (plaza) dan
  42578992843 (mall): posisi hasil parse cocok persis dengan yang
  dilaporkan di chat (mis. f150 307/1187, 2614 X=Z 122).
- Diag 2 hanya menangkap 25 frame (~2-3 dtk): semua pemain bergerak
  0,27-1,20 unit saja — terlalu pendek untuk membedakan siapa SkyDev.
  Mondar-mandir juga tidak membedakan, karena semua pemain mondar-mandir.
- ID 2-byte itu RoomUserId sementara: berubah tiap ganti server/room
  (plaza f150/f664/... vs mall 2614/25f2/...), jadi identifikasi harus
  diulang tiap kali bot join server baru. UUID permanen aae2efe1-... tidak
  muncul di frame tbulv.

Tes yang dirancang untuk diag ini (instruksi ke Bartes):
- Kembali ke PLAZA (world utama, join sudah terbukti 2x), lalu JALAN LURUS
  JAUH SATU ARAH selama ~15-20 detik tanpa belok, saat bot menangkap.
- Pemain yang perpindahannya (displacement) paling besar DAN paling lurus
  (displacement / panjang lintasan mendekati 1) adalah SkyDev.
- Alternatif bila jalan lurus sulit: berdiri di pojok sepi, atau lompat
  berulang (Y naik-turun tajam) — keduanya juga dihitung di ringkasan.

Bot login -> alokasi destinationUserId -> JoinRoom -> tangkap 100 frame
tbulv FULL -> analisis per pemain di DALAM script (bukan manual):
perpindahan, panjang lintasan, kelurusan, rentang Y, lalu cetak ranking.
Bot TIDAK bergerak / TIDAK kirim posisi. Hanya membaca.
"""
import argparse
import asyncio
import json
import math
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G


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


def _parse_protobuf(buf):
    """Parse protobuf generik: kembalikan dict field -> nilai (bytes untuk
    length-delimited, float untuk fixed32, int untuk varint)."""
    out = {}
    i = 0
    while i < len(buf):
        tag = buf[i]
        i += 1
        field = tag >> 3
        wire = tag & 7
        if wire == 0:
            v = 0
            s = 0
            while i < len(buf):
                b = buf[i]
                i += 1
                v |= (b & 0x7f) << s
                if not b & 0x80:
                    break
                s += 7
            out[field] = v
        elif wire == 5:
            if i + 4 > len(buf):
                break
            out[field] = struct.unpack("<f", buf[i:i + 4])[0]
            i += 4
        elif wire == 2:
            if i >= len(buf):
                break
            ln = buf[i]
            i += 1
            out[field] = buf[i:i + ln]
            i += ln
        elif wire == 1:
            i += 8
        else:
            break
    return out


def parse_tbulv_pos(data):
    """Satu frame tbulv (bytes) -> (id_2byte_hex, {'X':..,'Y':..,'Z':..})
    atau (id, {}) bila posisi tak ter-parse. Format terverifikasi dari
    log diag 2 (lihat docstring modul)."""
    if len(data) < 13:
        return "", {}
    pid = data[2:4].hex()
    top = _parse_protobuf(data[12:])
    pos = {}
    for field, key in ((1, "Y"), (2, "X"), (4, "Z")):
        sub = top.get(field)
        if isinstance(sub, (bytes, bytearray)):
            inner = _parse_protobuf(bytes(sub))
            if 3 in inner and isinstance(inner[3], float):
                pos[key] = inner[3]
    return pid, pos


def ringkas_posisi(frames_hex):
    """frames_hex: list hex frame tbulv FULL. Kembalikan ringkasan per
    pemain, diurutkan berdasarkan perpindahan terbesar."""
    by = {}
    for hx in frames_hex:
        try:
            data = bytes.fromhex(hx)
        except Exception:
            continue
        pid, pos = parse_tbulv_pos(data)
        if pid and len(pos) == 3:
            by.setdefault(pid, []).append((pos["X"], pos["Y"], pos["Z"]))
    out = []
    for pid, pts in by.items():
        x0, y0, z0 = pts[0]
        x1, y1, z1 = pts[-1]
        disp = math.hypot(x1 - x0, z1 - z0)
        path = sum(math.hypot(pts[i + 1][0] - pts[i][0],
                              pts[i + 1][2] - pts[i][2])
                   for i in range(len(pts) - 1))
        straight = (disp / path) if path > 0 else 0.0
        yrange = max(p[1] for p in pts) - min(p[1] for p in pts)
        out.append({"id": pid, "n": len(pts),
                    "start": [round(x0, 1), round(z0, 1)],
                    "end": [round(x1, 1), round(z1, 1)],
                    "disp": round(disp, 2), "path": round(path, 2),
                    "straight": round(straight, 2),
                    "y_range": round(yrange, 2)})
    out.sort(key=lambda r: r["disp"], reverse=True)
    return out


async def follow_diag3_once(session, email, password, device_id,
                            dest_user_id, n_frames):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "server": "", "room": None, "joined_room": None,
             "tbulv_count": 0, "tbulv_frames": [], "ranking": []}
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
        print("[!] TIDAK ada konfirmasi JoinedRoom — lanjut tangkap tbulv.")

    # --- tangkap N frame tbulv FULL ---
    print(f"[*] menangkap {n_frames} frame 'tbulv' FULL ...")
    print("[*] (SkyDev HARUS online + JALAN LURUS JAUH satu arah selama ini)")
    frames = []
    t_end = time.time() + 180
    while len(frames) < n_frames and time.time() < t_end:
        for f in fr.drain(2):
            name, data = G.decode_room_event(f)
            if name == "tbulv" and data:
                frames.append(data.hex())
                if len(frames) >= n_frames:
                    break
    try:
        sock.close()
    except Exception:
        pass
    hasil["tbulv_count"] = len(frames)
    hasil["tbulv_frames"] = frames
    print(f"[+] tertangkap {len(frames)} frame tbulv FULL.")
    ranking = ringkas_posisi(frames)
    hasil["ranking"] = ranking
    print("[*] ranking perpindahan (paling besar = kemungkinan SkyDev):")
    for r in ranking:
        print(f"    {r['id']}: n={r['n']} disp={r['disp']} path={r['path']} "
              f"lurus={r['straight']} Yrange={r['y_range']} "
              f"{r['start']} -> {r['end']}")
    if ranking:
        print(f"[+] kandidat SkyDev: {ranking[0]['id']} "
              f"(disp {ranking[0]['disp']}, lurus {ranking[0]['straight']})")
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("FOLLOW_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("FOLLOW_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("FOLLOW_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--dest-user-id",
                    default=os.getenv("FOLLOW_DEST_USER_ID",
                                      "aae2efe1-1abb-48c2-8799-ea192692a49c"))
    ap.add_argument("--n-frames", type=int,
                    default=int(os.getenv("FOLLOW_N_FRAMES", "100")))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await follow_diag3_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.n_frames)
    print("FOLLOWDIAG3_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    asyncio.run(main())
