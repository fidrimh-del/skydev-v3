# -*- coding: utf-8 -*-
"""
dance_capture_diag.py — Tangkap SEMUA RoomEvent saat Bartes dance.

Latar (2026-11-12, Bartes): bot bisa kirim posisi ('C') dan berdiri
sejajar; kini Bartes tanya apakah bot bisa menirukan dance-nya (dance
bersama). Dance = "Reaction" (ReactionIndex int + AnimationItem, tag
Dance/Gesture/Pose; BroadcastLastSelectedAnimationItem RVA 0x6B42DAC ->
ReactionChangedMessage). TIDAK ada user-variable key untuk reaction di
konstanta ToxicBirdServer, jadi dance kemungkinan lewat RoomEvent
terpisah — nama + payload-nya belum diketahui.
Diag ini: login -> alokasi destinationUserId (SkyDev) -> TCP login ->
JoinedRoom (batch 0x0d) -> dengar N detik, catat SEMUA nama RoomEvent
+ hitungan + contoh payload mentah (hex) per nama event. Bartes dance
satu gerakan selama jendela dengar; event dance dikenali dari nama
event yang muncul / melonjak saat ia dance. Bartes berdiri PALING
DEKAT dengan bot, jadi event dance-nya yang dicari.
"""
import argparse
import asyncio
import json
import os
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


def find_joinedroom_in_frames(frames, rm):
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
    for fr in frames:
        if len(fr) >= 6 and fr[0] == 0x06 and fr[1] == 1 and int.from_bytes(fr[2:6], "big") == rm:
            return True
        if fr and fr[0] == 0x0d and needle in bytes(fr):
            return True
    return False


async def capture_once(session, email, password, device_id,
                       dest_user_id, bot_user_id, listen_s=30):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "listen_s": listen_s,
             "event_names": {}, "samples": {}, "frame_types": {}}
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
    print(f"[+] JoinedRoom({rm}) = {joined_here}. Mendengar SEMUA event {listen_s} dtk — Bartes dance sekarang...")
    end = time.time() + listen_s
    while time.time() < end:
        for f in fr.drain(0.5):
            raw = bytes(f)
            ftype = raw[0] if raw else None
            hasil["frame_types"][str(ftype)] = hasil["frame_types"].get(str(ftype), 0) + 1
            name, d = G.decode_room_event(f)
            if name:
                hasil["event_names"][name] = hasil["event_names"].get(name, 0) + 1
                if name not in hasil["samples"]:
                    hasil["samples"][name] = {
                        "t_rel": round(time.time() - t0, 1),
                        "len": len(d) if d else 0,
                        "hex_head": (bytes(d).hex()[:160] if d else ""),
                    }
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. event: {json.dumps(hasil['event_names'])}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("DANCE_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("DANCE_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("DANCE_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("DANCE_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("DANCE_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--listen", type=float, default=30)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await capture_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id, args.listen)
    print("DANCE_CAPTURE_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
