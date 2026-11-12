# -*- coding: utf-8 -*-
"""
aaa_raw_capture.py — Tangkap bytes protobuf 'aaa' MENTAH pemain yang dance.

Latar (2026-11-13): bot kirim 'aaa' + field 7 = 43 langsung (57x, run
43332129270) tapi Bartes lihat bot TETAP DIAM. Jalur kirim 'tbulv'
terbukti jalan (idle 'aaa' mengubah state bot), jadi masalah ada di
ISI protobuf dance-nya. build_spawn_aaa mengisi field animasi dengan
KOORDINAT posisi (f1.f3=21.96, f2.f3=-78.49, f4.f3=-76.42) — warisan
pendekatan lama 'posisi di aaa' yang SUDAH terbukti salah; 'aaa' asli
punya progres animasi kecil (0.01-0.2). Diag ini: bot berdiri di samping
Bartes, rekam BYTES MENTAH (hex) seluruh protobuf 'aaa' yang field
7-nya hadir (pemain sedang dance), plus f6/f7 dan s1.f3, selama 60 dtk.
Bartes dance SATU gerakan terus-menerus. Hasilnya dibandingkan
byte-per-byte dengan 'aaa' buatan bot untuk menemukan perbedaan format.
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G
import tbulv_capture_diag as T

SKYDEV_UUID = "aae2efe1-1abb-48c2-8799-ea192692a49c"
BOT_UUID = "71056756-53c7-478e-9356-82c87fb2dd63"
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


def build_position_payload(x, y, z, rotation=0.0, teleported=1, sequence=0):
    return (bytes([teleported & 0xFF, sequence & 0xFF])
            + struct.pack(">f", x) + struct.pack(">f", y)
            + struct.pack(">f", z) + struct.pack(">f", rotation))


def parse_aaa_raw(data):
    """Parse payload tbulv -> (pid, protobuf_hex, f6, f7, s1_f3)."""
    try:
        if len(data) < 13:
            return None, None, None, None, None
        pid = bytes(data[2:4]).hex()
        pb_bytes = bytes(data[12:])
        pb = T.parse_protobuf(pb_bytes)
        s1_f3 = None
        sub1 = pb.get(1)
        if isinstance(sub1, (bytes, bytearray)):
            inner = T.parse_protobuf(bytes(sub1))
            if isinstance(inner.get(3), float):
                s1_f3 = round(inner.get(3), 4)
        return pid, pb_bytes.hex(), pb.get(6), pb.get(7), s1_f3
    except Exception:
        return None, None, None, None, None


def find_joinedroom_in_frames(frames, rm):
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
    for fr in frames:
        if len(fr) >= 6 and fr[0] == 0x06 and fr[1] == 1 and int.from_bytes(fr[2:6], "big") == rm:
            return True
        if fr and fr[0] == 0x0d and needle in bytes(fr):
            return True
    return False


async def capture_once(session, email, password, device_id,
                       dest_user_id, bot_user_id, listen_s=60,
                       max_samples=40):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "listen_s": listen_s, "tbulv_count": 0,
             "samples": []}
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
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")

    def _is_join_or_tbugv(f):
        if G._is_tbugv(f):
            return True
        if len(f) >= 6 and f[0] == 0x06 and f[1] == 1 and int.from_bytes(f[2:6], "big") == rm:
            return True
        if f and f[0] == 0x0d and needle in bytes(f):
            return True
        return False

    got, closed = fr.read_until(_is_join_or_tbugv, 3)
    if closed:
        print("[!] koneksi tertutup saat tunggu login.")
        return hasil
    t0 = time.time()
    joined_here = False
    if got is not None and not G._is_tbugv(got):
        joined_here = True
    if not joined_here:
        extra = fr.drain(2)
        joined_here = find_joinedroom_in_frames(extra, rm)
    hasil["joined_room"] = rm if joined_here else None
    print(f"[+] JoinedRoom({rm}) = {joined_here}. Rekam bytes mentah 'aaa' dance {listen_s} dtk — Bartes dance SATU gerakan terus-menerus...")
    px, py, pz = -79.99, 21.96, -76.42
    seq = [0]
    try:
        sock.sendall(L.build_room_event_varint(
            "C", build_position_payload(px, py, pz, teleported=1, sequence=0)))
        print(f"[+] Posisi 'C' terkirim ({px},{py},{pz}) Teleported=1.")
    except Exception as exc:
        print(f"[!] kirim posisi 'C' gagal: {exc}")
    end = time.time() + listen_s
    last_hb = time.time()
    seen_hex = set()
    while time.time() < end:
        if time.time() - last_hb >= 1.0:
            try:
                seq[0] = (seq[0] + 1) & 0xFF
                sock.sendall(L.build_room_event_varint(
                    "C", build_position_payload(px, py, pz, teleported=0,
                                                sequence=seq[0])))
                last_hb = time.time()
            except Exception:
                pass
        for f in fr.drain(0.5):
            name, d = G.decode_room_event(f)
            if name == "tbulv" and d:
                hasil["tbulv_count"] += 1
                pid, pb_hex, f6, f7, s1_f3 = parse_aaa_raw(bytes(d))
                if pid and f7 is not None and pb_hex:
                    # Simpan hanya protobuf UNIK (hemat log), maks N sampel.
                    if pb_hex not in seen_hex and len(hasil["samples"]) < max_samples:
                        seen_hex.add(pb_hex)
                        hasil["samples"].append({
                            "t": round(time.time() - t0, 1),
                            "pid": pid, "f6": f6, "f7": f7,
                            "s1_f3": s1_f3, "protobuf_hex": pb_hex})
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. tbulv diterima: {hasil['tbulv_count']}, "
          f"sampel 'aaa' dance unik: {len(hasil['samples'])}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("RAW_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("RAW_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("RAW_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("RAW_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("RAW_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--listen", type=float, default=60)
    ap.add_argument("--max-samples", type=int, default=40)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await capture_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id, args.listen,
            args.max_samples)
    print("AAA_RAW_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
