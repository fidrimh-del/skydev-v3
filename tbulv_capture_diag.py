# -*- coding: utf-8 -*-
"""
tbulv_capture_diag.py — Tangkap frame posisi 'tbulv' ASLI dari pemain lain
di room, untuk membedah struktur "aaa" yang benar.

Latar (2026-11-12, Bartes): bot kirim posisi spawn town square via
frame "aaa" (sekali + heartbeat tiap 1 dtk) tapi tetap di bawah tanah.
Berarti isi frame "aaa" SALAH. Dugaan: prefix sub-message di
build_spawn_aaa (f1 varint besar: Y=1432961145, X=235275163,
Z=0xffffffffd60dd822, f3/f5=-1) adalah nilai HARDCODE yang disalin
dari frame tangkapan lama — bila f1 adalah timestamp/sequence/ID
pemain, nilai stale itu membuat server menolak frame posisi bot.
Diag ini menangkap frame 'tbulv' ASLI dari pemain lain di room yang
sama, lalu membongkar struktur protobuf-nya (field 1/2/3 tiap
sub-message) agar bisa dibandingkan byte-per-byte dengan frame bot.
Alur: login -> alokasi destinationUserId (SkyDev) -> TCP login ->
(server auto-place; JoinedRoom dari batch 0x0d) -> dengar 25 dtk,
tangkap frame 'tbulv' mentah -> decode + laporkan. TIDAK kirim posisi
spawn (hanya dengar) supaya tangkapan murni dari pemain lain.
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


def parse_protobuf(buf):
    out = {}
    i = 0
    n = len(buf)
    while i < n:
        tag = buf[i]
        i += 1
        field = tag >> 3
        wire = tag & 7
        if wire == 0:
            v = 0
            s = 0
            while i < n:
                b = buf[i]
                i += 1
                v |= (b & 0x7F) << s
                if not (b & 0x80):
                    break
                s += 7
            out[field] = v
        elif wire == 2:
            if i >= n:
                break
            ln = buf[i]
            i += 1
            if ln & 0x80:
                out[field] = b""
                break
            chunk = buf[i:i + ln]
            i += ln
            try:
                out[field] = chunk.decode("utf-8")
            except Exception:
                out[field] = chunk
        elif wire == 5:
            if i + 4 <= n:
                out[field] = struct.unpack("<f", buf[i:i + 4])[0]
                i += 4
            else:
                break
        elif wire == 1:
            i += 8
        else:
            break
    return out


def decode_tbulv_payload(data):
    """Bongkar payload 'tbulv' mentah -> dict struktur.
    data = isi frame sesudah event name 'tbulv' (payload RoomEvent).
    Kembalikan dict dengan header + 5 sub-message (field 1/2/3)."""
    info = {"raw_hex": bytes(data).hex(), "raw_len": len(data)}
    try:
        # Header: 00 02 <ID 2 byte> 00 00 00 03 'aaa' 05  (12 byte)
        # lalu protobuf 5 sub-message.
        if len(data) < 13:
            info["error"] = "payload terlalu pendek"
            return info
        info["header_hex"] = bytes(data[:12]).hex()
        info["player_id_bytes"] = bytes(data[2:4]).hex()
        pb = parse_protobuf(bytes(data[12:]))
        subs = {}
        for field in (1, 2, 3, 4, 5):
            sub = pb.get(field)
            if isinstance(sub, (bytes, bytearray)):
                inner = parse_protobuf(bytes(sub))
                subs[field] = {str(k): (v if not isinstance(v, (bytes, bytearray)) else v.hex()) for k, v in inner.items()}
                subs[field]["_raw_hex"] = bytes(sub).hex()
            else:
                subs[field] = sub
        info["sub_messages"] = subs
    except Exception as exc:
        info["error"] = str(exc)
    return info


def find_joinedroom_in_frames(frames, rm):
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
    for fr in frames:
        if len(fr) >= 6 and fr[0] == 0x06 and fr[1] == 1 and int.from_bytes(fr[2:6], "big") == rm:
            return True
        if fr and fr[0] == 0x0d and needle in bytes(fr):
            return True
    return False


async def capture_once(session, email, password, device_id,
                       dest_user_id, bot_user_id, listen_s=25, max_frames=5):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "tbulv_count": 0, "captured": []}
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
    print(f"[+] JoinedRoom({rm}) = {joined_here}. Menangkap tbulv {listen_s} dtk...")
    end = time.time() + listen_s
    while time.time() < end and len(hasil["captured"]) < max_frames:
        for f in fr.drain(0.5):
            name, d = G.decode_room_event(f)
            if name == "tbulv" and d:
                hasil["tbulv_count"] += 1
                if len(hasil["captured"]) < max_frames:
                    info = decode_tbulv_payload(bytes(d))
                    info["t_rel"] = round(time.time() - t0, 1)
                    hasil["captured"].append(info)
                    print(f"[+] tbulv #{hasil['tbulv_count']} ditangkap "
                          f"(t+{info['t_rel']} dtk, len={info['raw_len']}).")
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. tbulv diterima: {hasil['tbulv_count']}, "
          f"ditangkap: {len(hasil['captured'])}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("TBULV_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("TBULV_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("TBULV_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("TBULV_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("TBULV_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--listen", type=float, default=25)
    ap.add_argument("--max-frames", type=int, default=5)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await capture_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id,
            args.listen, args.max_frames)
    print("TBULV_CAPTURE_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
