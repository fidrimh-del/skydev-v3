# -*- coding: utf-8 -*-
"""
friend_breakup_diag.py — Bot UNFRIEND pemain (putus pertemanan).

Latar (2026-11-12, Bartes): daftar teman SkyDev tampil KOSONG di game
(200+ teman hilang) walau sudah clear data game; Bartes sempat menduga
bug jaringan/lag. Sekarang Bartes menduga BUKAN jaringan, tapi bug
pengiriman teman yang mungkin tidak lengkap — karena akun kontol juga
mengalami bug yang sama (permintaan teman tidak muncul). Perintah Bartes:
login ke akun tulisan Jepang/Korea (bot bernama 늑, yang dulu add friend
SkyDev dan DITERIMA 13:09 WIB 2026-11-12), lalu UNFRIEND SkyDev dari
akun itu — untuk menguji apakah koneksi pertemanan yang "tidak lengkap"
itu yang merusak daftar teman.
Mekanisme unfriend (dump.cs):
  * ToxicBirdServer COMMAND_KEY: FriendsRequestKey = "friends".
  * FriendRequestEventId: GetAll=0, Invite=1, Accept=2, Decline=3,
    BreakUp=5, GetRoom=6, SearchFriend=7, CancelInvitation=8,
    GetFriendInformation=9.
  * BreakUp (bot -> server): RoomEvent "friends", f1 EventId=5 (BreakUp),
    f2 userId (UUID target). f1=5 HARUS ditulis eksplisit (bukan 0).
  * Hasil (server -> bot): RoomEvent "unfriend_result".
KONSEKUENSI: ini MENGHAPUS pertemanan bot <-> SkyDev. Dilakukan atas
perintah eksplisit Bartes (2026-11-12 17:56 WIB). Setelah ini bot 늑
tidak lagi berteman dengan SkyDev; bila perlu berteman lagi, kirim
invite baru dan Bartes terima di game.
Alur: bot login -> alokasi biasa -> JoinRoom -> kirim BreakUp ke UUID
target -> dengarkan "unfriend_result" 30 dtk.
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
NEUK_EMAIL = "nbp498vu5jnp439rsf@icloud.com"


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


def build_friend_breakup(target_uuid):
    """Bangun FriendRequest BreakUp (bot -> server).
    RoomEvent "friends": f1 EventId=5 (BreakUp), f2 userId (target UUID).
    """
    pb = b""
    pb += b"\x08" + _varint(5)   # f1 EventId=5 (BreakUp)
    pb += _sfield(2, target_uuid)  # f2 userId (target)
    return pb


async def friend_breakup_once(session, email, password, device_id,
                              target_user_id, listen_s=30):
    hasil = {"email": email, "target_user_id": target_user_id,
             "server": "", "room": None, "joined_room": None,
             "breakup_sent": False, "breakup_t_rel": None,
             "breakup_results": [],
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
    # Kirim BreakUp (unfriend) ke target.
    breakup_pb = build_friend_breakup(target_user_id)
    try:
        sock.sendall(L.build_room_event_varint("friends", breakup_pb))
        hasil["breakup_sent"] = True
        hasil["breakup_t_rel"] = 0.0
        print(f"[+] BREAKUP (unfriend) terkirim ke {target_user_id[:8]}... "
              f"(EventId=5).")
    except Exception as exc:
        print(f"[!] kirim breakup gagal: {exc}")
        return hasil
    print(f"[*] mendengarkan hasil {listen_s} dtk "
          f"(RoomEvent 'unfriend_result').")
    end = time.time() + listen_s
    while time.time() < end:
        for f in fr.drain(1):
            name, d = G.decode_room_event(f)
            if not name:
                continue
            hasil["event_names"][name] = \
                hasil["event_names"].get(name, 0) + 1
            if name not in ("unfriend_result", "friends"):
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
            hasil["breakup_results"].append(entry)
            print(f"[+] {name} t+{entry['t_rel']} dtk: "
                  f"{entry.get('fields', entry['hex'][:120])}")
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. breakup: {hasil['breakup_sent']}, "
          f"hasil: {len(hasil['breakup_results'])}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("FRIEND_EMAIL", NEUK_EMAIL))
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
        hasil = await friend_breakup_once(
            session, args.email, args.password, args.device_id,
            args.target_user_id, args.listen)
    print("FRIEND_BREAKUP_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
