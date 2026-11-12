# -*- coding: utf-8 -*-
"""
role_change_diag.py — Bot ubah role via user variable (tes CreatorUgc).

Latar (2026-11-12, Bartes): client mengirim role sebagai user variable.
Verifikasi disassembly dump BARU membuktikan:
  * Kunci role = "D", kunci tier = "rt" (bukan kata "role").
  * Tipe "D"/"rt" = ByteVariable (tag 4, 1 byte) — BUKAN IntVariable.
    Bukti: ToxicBirdServer.get_Role pakai GetByteVariableOrDefault("D", 0).
  * Pembungkus single update BUKAN protobuf — format biner custom:
    [BE32(name_len)][nama ASCII][variable: tag+nilai], TANPA sequence,
    TANPA prefix panjang variable. Format [sequence][name_len][nama]
    [var_len][var] adalah BULK (RefreshVariablesBulk), bukan single update.
  * Wrapper protobuf di complete_join_diag.py SALAH — server parse 4 byte
    pertama sebagai name_len raksasa lalu abaikan; itu kemungkinan besar
    penyebab gagalnya run 43204259273, bukan karena variabel bukan penentu.
  * Slug event 'uv' = kandidat sangat kuat (literal ada di metadata),
    tapi TIDAK terbukti statis (pointer runtime).
Enum Role: User=0, Influencer=1, Admin=2, MinorInfluencer=3,
CreatorUgc=4, EventTeam01=5, EventTeam02=6, EventTeam03=7,
HighScorePlayer=8, EventPass=9, Undefined=10.
Enum RoleTier: None=0, Rare=1, Epic=2, Legendary=3, Orbit=4,
Rocketeer=5, Stardust=6, RisingStar=7, Supernova=8, Galaxy=9.
Tes ini (pilihan Bartes): bot pkxdshop join server SkyDev di world utama
(Plaza), kirim role "D"=CreatorUgc (4) + tier "rt"=Legendary (3) sebagai
2 RoomEvent 'uv' terpisah, lalu tetap online 90 dtk supaya Bartes (SkyDev)
bisa cek di game apakah bot tampil sebagai CreatorUgc. Juga kirim
SendDeviceId dulu (bagian dari set lengkap client).
Byte yang dikirim (terverifikasi unit test):
  "D"=4  : 00 00 00 01 44 04 04
  "rt"=3 : 00 00 00 02 72 74 04 03
Deteksi RoomUserUpdate = heuristik (frame berisi UUID target/bot, atau
event name baru). Verdict utama = cek in-game Bartes di SkyDev.
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

ROLE_CREATOR_UGC = 4
TIER_LEGENDARY = 3


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


def build_spawn_aaa(x, y, z):
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


def parse_tbulv_position(data):
    if not data or len(data) < 13:
        return None
    try:
        top = parse_protobuf(bytes(data[12:]))

        def _f3(sub):
            if isinstance(sub, (bytes, bytearray)):
                inner = parse_protobuf(bytes(sub))
                v = inner.get(3)
                if isinstance(v, float):
                    return v
            return None

        y = _f3(top.get(1))
        x = _f3(top.get(2))
        z = _f3(top.get(4))
        if x is None or y is None or z is None:
            return None
        return (x, y, z)
    except Exception:
        return None


def smart_spawn(fr, server, seconds=5):
    from collections import Counter
    observed = []
    end = time.time() + seconds
    while time.time() < end:
        for f in fr.drain(0.5):
            name, d = G.decode_room_event(f)
            if name != "tbulv" or not d:
                continue
            pos = parse_tbulv_position(bytes(d))
            if pos:
                observed.append(pos)
    if not observed:
        return (79.96, 27.13, -122.17, "fallback-Plaza")
    if "activity" in (server or ""):
        x, y, z = min(observed, key=lambda p: p[1])
        return (x + 2, y + 2, z + 2, f"activity-lowest-Y dari {len(observed)}")
    counts = Counter((round(p[0]), round(p[1]), round(p[2]))
                     for p in observed)
    (rx, ry, rz), n = counts.most_common(1)[0]
    return (rx + 2, ry + 2, rz + 2,
            f"main-paling-sering({n}x) dari {len(observed)}")


def build_uservar(name, var_tag, var_value_bytes):
    """Pembungkus single update user variable (TERBUKTI via disassembly,
    BUKAN protobuf): [BE32(name_len)][nama ASCII][variable: tag+nilai].
    Tanpa sequence, tanpa prefix panjang variable.
    Tag: 1=string ASCII, 2=int 4 byte BIG-ENDIAN, 3=bool 1 byte,
    4=byte 1 byte."""
    name_b = name.encode("ascii")
    return (struct.pack(">I", len(name_b)) + name_b
            + bytes([var_tag]) + var_value_bytes)


def build_role_var(role_value):
    """Role "D" sebagai ByteVariable (tag 4)."""
    return build_uservar("D", 4, bytes([role_value & 0xFF]))


def find_joinedroom_in_frames(frames, rm):
    """Cari konfirmasi JoinedRoom untuk room `rm` di daftar frame.
    Mencakup: (a) frame standalone 0x06 [0x06][0x01][room 4 byte], dan
    (b) sub-pesan JoinedRoom yang tertanam di frame batch 0x0d
    (needle 06 01 + room). Kembalikan True bila ketemu.
    PERBAIKAN (audit Bartes 2026-11-12): client asli TIDAK kirim
    JoinRoom eksplisit — server menempatkan bot di room saat TCP login
    (dari alokasi lobby destinationUserId); JoinedRoom tiba di batch
    0x0d pada jendela login. JoinRoom eksplisit ditolak 060003."""
    needle = bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
    for fr in frames:
        if len(fr) >= 6 and fr[0] == 0x06 and fr[1] == 1 and int.from_bytes(fr[2:6], "big") == rm:
            return True
        if fr and fr[0] == 0x0d and needle in bytes(fr):
            return True
    return False


def build_roletier_var(tier_value):
    """Role tier "rt" sebagai ByteVariable (tag 4)."""
    return build_uservar("rt", 4, bytes([tier_value & 0xFF]))


async def role_change_once(session, email, password, device_id,
                           dest_user_id, bot_user_id,
                           role_value, tier_value,
                           device_event="Analytics/deviceId", uv_event="tbulv",
                           listen_s=90):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "spawn_pos": None, "spawn_mode": None,
             "device_event": device_event, "device_id_sent": None,
             "deviceid_sent": False, "deviceid_t_rel": None,
             "uv_event": uv_event,
             "role_value": role_value, "tier_value": tier_value,
             "role_hex": build_role_var(role_value).hex(),
             "tier_hex": build_roletier_var(tier_value).hex(),
             "role_sent": False, "role_t_rel": None,
             "tier_sent": False, "tier_t_rel": None,
             "room_user_update": {"seen": False, "first_t_rel": None,
                                  "first_after": None, "uuid": None,
                                  "frame_type": None, "event": None,
                                  "hex_head": None},
             "new_events": [],
             "event_names": {}, "frame_types": {}, "rs_count": 0}
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
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
    # TIDAK kirim JoinRoom eksplisit (PERBAIKAN audit 2026-11-12):
    # client asli tidak kirim JoinRoom; server auto-place dari alokasi
    # lobby dan JoinedRoom tiba di batch 0x0d pada jendela login.
    # JoinRoom eksplisit ditolak 060003. Cek JoinedRoom dari frame yang
    # sudah didapat + drain singkat sesudahnya.
    t0 = time.time()
    joined_here = False
    if got is not None and not G._is_tbugv(got):
        joined_here = True
    if not joined_here:
        extra = fr.drain(2)
        joined_here = find_joinedroom_in_frames(extra, rm)
    if joined_here:
        hasil["joined_room"] = rm
        print(f"[+] JoinedRoom({rm}) TERKONFIRMASI (batch 0x0d / standalone).")
    else:
        print("[!] JoinedRoom tidak ketemu di jendela login — tetap lanjut "
              "(server auto-place; tbulv akan membuktikan).")
    # SMART SPAWN (main world: posisi paling sering).
    sx, sy, sz, smode = smart_spawn(fr, srv, seconds=5)
    hasil["spawn_pos"] = [round(sx, 2), round(sy, 2), round(sz, 2)]
    hasil["spawn_mode"] = smode
    try:
        data = build_spawn_aaa(sx, sy, sz)
        sock.sendall(L.build_room_event_varint("tbulv", data))
        print(f"[+] Posisi spawn terkirim ({sx:.2f},{sy:.2f},{sz:.2f}) [{smode}].")
    except Exception as exc:
        print(f"[!] kirim spawn gagal: {exc}")
    state = {"event_names": {}, "frame_types": {}, "seen_events": set(),
             "new_events": [],
             "room_user_update": dict(hasil["room_user_update"])}
    watch = [dest_user_id, bot_user_id]

    def _listen(seconds, label):
        end = time.time() + seconds
        while time.time() < end:
            for f in fr.drain(0.5):
                raw = bytes(f)
                ftype = raw[0] if raw else None
                state["frame_types"][ftype] = state["frame_types"].get(ftype, 0) + 1
                name, d = G.decode_room_event(f)
                if name:
                    state["event_names"][name] = state["event_names"].get(name, 0) + 1
                    if name not in state["seen_events"]:
                        state["seen_events"].add(name)
                        state["new_events"].append(
                            {"t_rel": round(time.time() - t0, 1),
                             "after": label, "event": name})
                if name == "RS":
                    hasil["rs_count"] += 1
                if not state["room_user_update"]["seen"]:
                    for u in watch:
                        if u.encode("utf-8") in raw:
                            # Abaikan event consent (bukan RoomUserUpdate).
                            if name == "consent":
                                continue
                            state["room_user_update"] = {
                                "seen": True,
                                "first_t_rel": round(time.time() - t0, 1),
                                "first_after": label,
                                "uuid": u[:8] + "...",
                                "frame_type": ftype,
                                "event": name or "",
                                "hex_head": raw[:120].hex()}
                            print(f"[+] KANDIDAT RoomUserUpdate t+{state['room_user_update']['first_t_rel']} dtk "
                                  f"(sesudah {label}): UUID {u[:8]}... di frame type {ftype}, event '{name}'.")
                            break
    # (1) SendDeviceId.
    send_dev = device_id or DEFAULT_DEVICE_ID
    hasil["device_id_sent"] = send_dev
    try:
        sock.sendall(L.build_room_event_varint(
            device_event, send_dev.encode("utf-8")))
        hasil["deviceid_sent"] = True
        hasil["deviceid_t_rel"] = round(time.time() - t0, 1)
        print(f"[+] SendDeviceId terkirim: event '{device_event}', "
              f"deviceId='{send_dev}' (t+{hasil['deviceid_t_rel']} dtk).")
    except Exception as exc:
        print(f"[!] kirim SendDeviceId gagal: {exc}")
    _listen(3, "deviceid")
    # (2) Role "D" = CreatorUgc (4), ByteVariable, wrapper BENAR.
    try:
        role_pb = build_role_var(role_value)
        sock.sendall(L.build_room_event_varint(uv_event, role_pb))
        hasil["role_sent"] = True
        hasil["role_t_rel"] = round(time.time() - t0, 1)
        print(f"[+] Role terkirim: event '{uv_event}', var 'D'={role_value} "
              f"(CreatorUgc), hex={role_pb.hex()} (t+{hasil['role_t_rel']} dtk).")
    except Exception as exc:
        print(f"[!] kirim role gagal: {exc}")
    _listen(3, "role-D")
    # (3) Tier "rt" = Legendary (3), ByteVariable, wrapper BENAR.
    try:
        tier_pb = build_roletier_var(tier_value)
        sock.sendall(L.build_room_event_varint(uv_event, tier_pb))
        hasil["tier_sent"] = True
        hasil["tier_t_rel"] = round(time.time() - t0, 1)
        print(f"[+] Tier terkirim: event '{uv_event}', var 'rt'={tier_value} "
              f"(Legendary), hex={tier_pb.hex()} (t+{hasil['tier_t_rel']} dtk).")
    except Exception as exc:
        print(f"[!] kirim tier gagal: {exc}")
    _listen(3, "tier-rt")
    hasil["room_user_update"] = state["room_user_update"]
    hasil["new_events"] = state["new_events"]
    hasil["event_names"] = state["event_names"]
    hasil["frame_types"] = {str(k): v for k, v in state["frame_types"].items()}
    print(f"[*] Sesudah deviceid+role+tier: RoomUserUpdate terlihat = "
          f"{hasil['room_user_update']['seen']} "
          f"(pertama: {hasil['room_user_update']['first_after']}).")
    print(f"[*] Bot tetap online {listen_s} dtk supaya Bartes (SkyDev) bisa "
          f"cek role bot di game. Mendengarkan...")
    _listen(listen_s, "role-check")
    hasil["room_user_update"] = state["room_user_update"]
    hasil["new_events"] = state["new_events"]
    hasil["event_names"] = state["event_names"]
    hasil["frame_types"] = {str(k): v for k, v in state["frame_types"].items()}
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. deviceid: {hasil['deviceid_sent']}, "
          f"role: {hasil['role_sent']}, tier: {hasil['tier_sent']}, "
          f"RoomUserUpdate: {hasil['room_user_update']['seen']} "
          f"({hasil['room_user_update']['first_after']}), "
          f"RS: {hasil['rs_count']}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("ROLE_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("ROLE_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("ROLE_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("ROLE_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("ROLE_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--role", type=int, default=ROLE_CREATOR_UGC)
    ap.add_argument("--tier", type=int, default=TIER_LEGENDARY)
    ap.add_argument("--device-event", default="Analytics/deviceId")
    ap.add_argument("--uv-event", default="tbulv")
    ap.add_argument("--listen", type=float, default=90)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await role_change_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id,
            args.role, args.tier,
            args.device_event, args.uv_event, args.listen)
    print("ROLE_CHANGE_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
