# -*- coding: utf-8 -*-
"""
friend_invite_diag.py — Bot kirim permintaan pertemanan (add friend).

Latar (2026-11-12, Bartes):
- Bot tanpa nama (accounts) harus berteman dulu dengan SkyDev sebelum
  bisa join server SkyDev via destinationUserId (join teman). Tanpa
  pertemanan, bot tidak bisa masuk server SkyDev untuk gendong pemain.
- Mekanisme add friend (dump.cs):
  * ToxicBirdServer COMMAND_KEY: FriendsRequestKey = "friends".
  * FriendRequestEventId: GetAll=0, Invite=1, Accept=2, Decline=3,
    BreakUp=5, GetRoom=6, SearchFriend=7, CancelInvitation=8,
    GetFriendInformation=9.
  * Invite (bot -> server): RoomEvent "friends", f1 EventId=1 (Invite),
    f2 userId (UUID target). PENTING (proto3): f1=1 HARUS ditulis
    eksplisit (bukan 0), f2 = UUID target sebagai string.
  * Hasil (server -> bot): RoomEvent "friend_invite_result"
    (InviteFriendExtensionCommand), berisi Success + Status
    (FriendshipStatus: Friend=0, RequestSent=1, RequestReceived=2,
    None=3, Fan=4) + ErrorMessage.
  * BELUM TERBUKTI apakah bot harus satu room dengan target. Probe ini
    join room alokasi biasa dulu (bukan room target), lalu kirim invite.
    Bila hasil sukses -> tidak perlu satu room. Bila gagal/tidak ada
    hasil -> kemungkinan perlu satu room (butuh jalur lain, mis. target
    undang bot ke room-nya).
- Alur tes: bot login -> alokasi biasa -> JoinRoom -> kirim invite
  pertemanan ke UUID target -> dengarkan "friend_invite_result".
  Target (Bartes/SkyDev) lalu TERIMA permintaan di game (daftar teman).
  Setelah berteman, bot bisa join server SkyDev via destinationUserId.
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


def parse_protobuf(buf):
    """Decode protobuf generik -> dict field -> nilai (int/str/bytes)."""
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


def _varint(v):
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _sfield(num, text):
    raw = text.encode("utf-8")
    return bytes([(num << 3) | 2]) + _varint(len(raw)) + raw


def build_friend_invite(target_uuid):
    """Bangun FriendRequest Invite (bot -> server).
    RoomEvent "friends": f1 EventId=1 (Invite), f2 userId (target UUID).
    """
    pb = b""
    pb += b"\x08" + _varint(1)   # f1 EventId=1 (Invite)
    pb += _sfield(2, target_uuid)  # f2 userId (target)
    return pb


async def friend_invite_once(session, email, password, device_id,
                             target_user_id, listen_s=30):
    hasil = {"email": email, "target_user_id": target_user_id,
             "server": "", "room": None, "joined_room": None,
             "invite_sent": False, "invite_t_rel": None,
             "invite_results": [],
             "event_names": {}}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil
    srv, rm = parse_gsid(alloc.get("gameServerId", ""))
    hasil["server"] = srv
    hasil["room"] = rm
    print(f"[+] alokasi OK: server={srv} room={rm}")
    if not srv or rm is None:
        return hasil
    host, port = L.find_host_port(alloc)
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
        print("[!] TIDAK ada konfirmasi JoinedRoom — tetap lanjut.")
    # Kirim invite pertemanan ke target.
    invite_pb = build_friend_invite(target_user_id)
    try:
        sock.sendall(L.build_room_event_varint("friends", invite_pb))
        hasil["invite_sent"] = True
        hasil["invite_t_rel"] = 0.0
        print(f"[+] INVITE teman terkirim ke {target_user_id[:8]}...")
    except Exception as exc:
        print(f"[!] kirim invite teman gagal: {exc}")
        return hasil
    print(f"[*] mendengarkan hasil {listen_s} dtk "
          f"(RoomEvent 'friend_invite_result').")
    end = time.time() + listen_s
    while time.time() < end:
        for f in fr.drain(1):
            name, d = G.decode_room_event(f)
            if not name:
                continue
            hasil["event_names"][name] = \
                hasil["event_names"].get(name, 0) + 1
            if name not in ("friend_invite_result", "friends"):
                continue
            entry = {"t_rel": round(time.time() - t0, 1),
                     "name": name,
                     "hex": d.hex() if isinstance(d, (bytes, bytearray)) else str(d)}
            try:
                fields = parse_protobuf(bytes(d))
                entry["fields"] = {str(k): (v if not isinstance(v, bytes) else v.hex())
                                   for k, v in fields.items()}
            except Exception as exc:
                entry["parse_error"] = str(exc)
            hasil["invite_results"].append(entry)
            print(f"[+] {name} t+{entry['t_rel']} dtk: "
                  f"{entry.get('fields', entry['hex'][:120])}")
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. invite: {hasil['invite_sent']}, "
          f"hasil: {len(hasil['invite_results'])}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("FRIEND_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("FRIEND_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("FRIEND_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--target-user-id",
                    default=os.getenv("FRIEND_TARGET_USER_ID", SKYDEV_UUID))
    ap.add_argument("--listen", type=float, default=30)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await friend_invite_once(
            session, args.email, args.password, args.device_id,
            args.target_user_id, args.listen)
    print("FRIEND_INVITE_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
