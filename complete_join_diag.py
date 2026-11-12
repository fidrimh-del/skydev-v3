# -*- coding: utf-8 -*-
"""
complete_join_diag.py — Bot kirim SET LENGKAP sesudah join (eksperimen Sekolah).

Latar (2026-11-12, Bartes): disassembly libil2cpp.so (dump BARU, RVA benar)
membuktikan GetExistingPlayers TIDAK mengirim apa pun ke server (murni
lokal: iterasi UsersInRoom lalu invoke callback lokal). Jadi tidak ada
pesan "GetExistingPlayers" yang bisa disalin. Tapi disassembly menemukan
DUA hal yang client asli kirim sesudah join dan bot TIDAK PERNAH kirim:
  (a) SendDeviceId (0x74AB574): deviceId sebagai byte UTF-8 via
      EmitRoomEvent (reliable). Nama event TERBUKTI: 'Analytics/deviceId'
      (audit relokasi 2026-11-12; 'deviceid'/'device_id' SALAH).
  (b) User variable (0x74B5290 SendLocalPlayerStateLogicalUpdateMessage):
      variabel state pemain lokal dikirim sebagai RoomEvent via
      LocalUserVariablesFeature.EmitEvent -> EmitRoomEvent (reliable).
      Format serialisasi NILAI terbukti (DefaultUserVariableSerializer):
      byte[0]=tag (1=string ASCII, 2=int 4 byte BIG-ENDIAN, 3=bool 1 byte,
      4=byte 1 byte). Slug event TERBUKTI: 'tbulv'
      (LocalUserVariablesFeature.ctor hardcode 'tbulv'; 'uv' SALAH —
      audit relokasi 2026-11-12). Pembungkus single update TERBUKTI
      (disassembly dump BARU): [BE32(name_len)][nama ASCII]
      [variable: tag+nilai], TANPA sequence, TANPA prefix panjang
      variable — BUKAN protobuf (wrapper protobuf lama SALAH, diperbaiki
      2026-11-12). Tes ini kirim SATU variabel bool "sr"
      (subscriber)=true. Kunci variabel HARUS kunci pendek dari dump
      ("sr", bukan "subscriber"; "D" untuk role, "rt" untuk tier).
Alur tes: login -> alokasi destinationUserId (kontol) -> TCP login ->
(server auto-place; JoinedRoom dari batch 0x0d — TIDAK kirim JoinRoom
eksplisit, ditolak 060003) -> smart spawn (activity = Y terendah) ->
kirim 'Analytics/deviceId' -> dengar 5 dtk -> kirim 'tbulv' (var "sr")
-> dengar 5 dtk -> invite gendong + ACC telat 6 dtk -> dengar 60 dtk.
Bartes online di Sekolah dan cek di game apakah wujud bot terlihat /
apakah dia tergendong.
Deteksi RoomUserUpdate: format wire-nya TIDAK diketahui (server->client
saat pemain terdaftar). Detektor heuristik: frame/event BARU yang muncul
sesudah kirim 'Analytics/deviceId'/'tbulv', ATAU frame yang mengandung
UUID kontol / UUID bot sebagai string UTF-8 (RoomUserUpdate berisi UUID
pemain). Verdict utama tetap cek in-game Bartes, bukan log.
KOREKSI PENTING (audit 2026-11-12): tes complete-join SEBELUMNYA
(run 43204259273) gagal BUKAN karena variable bukan kunci, melainkan
karena DUA nama event SALAH ('deviceid' dan 'uv' — keduanya diabaikan
server) + wrapper protobuf SALAH + nama variabel "subscriber" SALAH.
Tes ini mengulang dengan nama event + wrapper + kunci yang BENAR.
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


def _bfield(num, raw):
    return bytes([(num << 3) | 2]) + _varint(len(raw)) + raw


def build_consent_invite(sender_uuid, recipient_uuid, consent_type=0):
    pb = b""
    pb += _sfield(2, sender_uuid)
    pb += _sfield(3, recipient_uuid)
    return pb


def build_consent_response(sender_uuid, recipient_uuid, accepted=True,
                           consent_type=0):
    pb = b""
    pb += b"\x08" + _varint(1)
    pb += _sfield(2, sender_uuid)
    pb += _sfield(3, recipient_uuid)
    pb += b"\x20" + _varint(1 if accepted else 0)
    pb += b"\x28" + _varint(consent_type)
    return pb


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


def build_uservar(name, var_tag, var_value_bytes):
    """Pembungkus single update user variable (TERBUKTI via disassembly
    dump BARU, BUKAN protobuf): [BE32(name_len)][nama ASCII]
    [variable: tag+nilai]. Tanpa sequence, tanpa prefix panjang variable.
    Format [sequence][name_len][nama][var_len][var] adalah BULK
    (RefreshVariablesBulk), bukan single update.
    Tag: 1=string ASCII, 2=int 4 byte BIG-ENDIAN, 3=bool 1 byte,
    4=byte 1 byte.
    PERBAIKAN 2026-11-12 (Bartes): wrapper protobuf lama SALAH — server
    parse 4 byte pertama sebagai name_len raksasa lalu abaikan pesan."""
    name_b = name.encode("ascii")
    return (struct.pack(">I", len(name_b)) + name_b
            + bytes([var_tag]) + var_value_bytes)


def listen_window(fr, t0, seconds, watch_uuids, state, label):
    """Dengar `seconds` dtk. Catat event names + frame types baru, dan
    cek heuristik RoomUserUpdate: frame yang mengandung UUID target/bot
    sebagai string UTF-8, atau event name yang belum pernah terlihat.
    `state` = dict bersama (event_names, frame_types, seen_events,
    room_user_update: {seen, first_t_rel, first_after, sample})."""
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
            # Heuristik RoomUserUpdate: UUID kontol/bot di dalam frame.
            if not state["room_user_update"]["seen"]:
                for u in watch_uuids:
                    if u.encode("utf-8") in raw:
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
    return state


async def complete_join_once(session, email, password, device_id,
                             dest_user_id, bot_user_id,
                             device_event="Analytics/deviceId",
                             uv_event="tbulv", uv_name="sr",
                             invite_wait=6, listen_s=60):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "spawn_pos": None, "spawn_mode": None,
             "device_event": device_event, "device_id_sent": None,
             "deviceid_sent": False, "deviceid_t_rel": None,
             "uv_event": uv_event, "uv_name": uv_name,
             "uv_sent": False, "uv_t_rel": None,
             "room_user_update": {"seen": False, "first_t_rel": None,
                                  "first_after": None, "uuid": None,
                                  "frame_type": None, "event": None,
                                  "hex_head": None},
             "new_events": [],
             "invite_sent": False, "invite_t_rel": None,
             "late_acc_sent": False, "late_acc_t_rel": None,
             "accepted_observed": False,
             "consent_events": [],
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
    # SMART SPAWN (seperti carry v2).
    sx, sy, sz, smode = smart_spawn(fr, srv, seconds=5)
    hasil["spawn_pos"] = [round(sx, 2), round(sy, 2), round(sz, 2)]
    hasil["spawn_mode"] = smode
    try:
        data = build_spawn_aaa(sx, sy, sz)
        sock.sendall(L.build_room_event_varint("tbulv", data))
        print(f"[+] Posisi spawn terkirim ({sx:.2f},{sy:.2f},{sz:.2f}) [{smode}].")
    except Exception as exc:
        print(f"[!] kirim spawn gagal: {exc}")
    # State pendengar bersama untuk deteksi RoomUserUpdate.
    state = {"event_names": {}, "frame_types": {}, "seen_events": set(),
             "new_events": [],
             "room_user_update": dict(hasil["room_user_update"])}
    watch = [dest_user_id, bot_user_id]
    # (1) SendDeviceId: deviceId UTF-8 via RoomEvent (reliable).
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
    listen_window(fr, t0, 5, watch, state, "deviceid")
    # (2) User variable 'tbulv': SATU variabel bool "sr" (subscriber)=true.
    # PERBAIKAN: nama kunci "sr" (UserVariableSubscriberKey), BUKAN
    # "subscriber"; wrapper biner BENAR (bukan protobuf).
    try:
        uv_pb = build_uservar(uv_name, 3, b"\x01")
        sock.sendall(L.build_room_event_varint(uv_event, uv_pb))
        hasil["uv_sent"] = True
        hasil["uv_t_rel"] = round(time.time() - t0, 1)
        print(f"[+] User variable terkirim: event '{uv_event}', "
              f"var '{uv_name}' bool=true (t+{hasil['uv_t_rel']} dtk).")
    except Exception as exc:
        print(f"[!] kirim user variable gagal: {exc}")
    listen_window(fr, t0, 5, watch, state, "tbulv")
    hasil["room_user_update"] = state["room_user_update"]
    hasil["new_events"] = state["new_events"]
    hasil["event_names"] = state["event_names"]
    hasil["frame_types"] = {str(k): v for k, v in state["frame_types"].items()}
    print(f"[*] Sesudah deviceid+'tbulv': RoomUserUpdate terlihat = "
          f"{hasil['room_user_update']['seen']} "
          f"(pertama: {hasil['room_user_update']['first_after']}).")
    # Kirim invite gendong ke target (bot sebagai pengundang).
    invite_pb = build_consent_invite(bot_user_id, dest_user_id, 0)
    try:
        sock.sendall(L.build_room_event_varint("consent", invite_pb))
        hasil["invite_sent"] = True
        hasil["invite_t_rel"] = round(time.time() - t0, 1)
        print(f"[+] INVITE gendong terkirim ke {dest_user_id[:8]}... "
              f"(bot {bot_user_id[:8]}... sebagai pengundang).")
    except Exception as exc:
        print(f"[!] kirim invite gendong gagal: {exc}")
        return hasil
    print(f"[*] mendengarkan {listen_s} dtk. ACC telat dikirim di "
          f"t+{invite_wait} dtk bila belum ada accept. Bartes: terima/"
          f"tolak/biarkan timeout invite dari bot di game.")
    end = time.time() + listen_s
    acc_deadline = t0 + invite_wait
    late_acc_done = False
    while time.time() < end:
        for f in fr.drain(1):
            name, d = G.decode_room_event(f)
            if not name:
                continue
            hasil["event_names"][name] = hasil["event_names"].get(name, 0) + 1
            if name == "RS":
                hasil["rs_count"] += 1
            # Terus cek heuristik RoomUserUpdate selama fase carry juga.
            if not hasil["room_user_update"]["seen"]:
                raw = bytes(f)
                for u in watch:
                    if u.encode("utf-8") in raw:
                        hasil["room_user_update"] = {
                            "seen": True,
                            "first_t_rel": round(time.time() - t0, 1),
                            "first_after": "carry-phase",
                            "uuid": u[:8] + "...",
                            "frame_type": raw[0] if raw else None,
                            "event": name or "",
                            "hex_head": raw[:120].hex()}
                        print(f"[+] KANDIDAT RoomUserUpdate t+{hasil['room_user_update']['first_t_rel']} dtk "
                              f"(fase carry): UUID {u[:8]}....")
                        break
            if name != "consent":
                continue
            entry = {"t_rel": round(time.time() - t0, 1),
                     "hex": d.hex() if isinstance(d, (bytes, bytearray)) else str(d)}
            try:
                fields = parse_protobuf(bytes(d))
                entry["fields"] = {str(k): (v if not isinstance(v, bytes) else v.hex())
                                   for k, v in fields.items()}
            except Exception as exc:
                entry["parse_error"] = str(exc)
            hasil["consent_events"].append(entry)
            print(f"[+] CONSENT t+{entry['t_rel']} dtk: {entry.get('fields', entry['hex'][:120])}")
            fields = entry.get("fields", {})
            try:
                ev_id = int(fields.get("1", 0))
                accepted = int(fields.get("4", 0))
            except Exception:
                ev_id = -1
                accepted = -1
            if ev_id == 1 and accepted == 1:
                hasil["accepted_observed"] = True
                print("[+] ACCEPT teramati (target terima invite). "
                      "ACC telat TIDAK dikirim.")
        if (not late_acc_done and not hasil["accepted_observed"]
                and time.time() >= acc_deadline):
            resp = build_consent_response(bot_user_id, dest_user_id, True, 0)
            frame = L.build_room_event_varint("consent", resp)
            try:
                sock.sendall(frame)
                hasil["late_acc_sent"] = True
                hasil["late_acc_t_rel"] = round(time.time() - t0, 1)
                print(f"[+] ACC TELAT terkirim di t+{hasil['late_acc_t_rel']} dtk "
                      f"(Auto ACC TCP). Total: 1.")
            except Exception as exc:
                print(f"[!] kirim ACC telat gagal: {exc}")
            late_acc_done = True
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. deviceid: {hasil['deviceid_sent']}, uv: {hasil['uv_sent']}, "
          f"RoomUserUpdate: {hasil['room_user_update']['seen']} "
          f"({hasil['room_user_update']['first_after']}), "
          f"invite: {hasil['invite_sent']}, accept teramati: {hasil['accepted_observed']}, "
          f"ACC telat: {hasil['late_acc_sent']}, RS: {hasil['rs_count']}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("COMPLETE_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("COMPLETE_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("COMPLETE_DEVICE_ID", "")
                    or DEFAULT_DEVICE_ID)
    ap.add_argument("--dest-user-id",
                    default=os.getenv("COMPLETE_DEST_USER_ID", KONTOL_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("COMPLETE_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--device-event", default="Analytics/deviceId")
    ap.add_argument("--uv-event", default="tbulv")
    ap.add_argument("--uv-name", default="sr")
    ap.add_argument("--invite-wait", type=float, default=6)
    ap.add_argument("--listen", type=float, default=60)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await complete_join_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id,
            args.device_event, args.uv_event, args.uv_name,
            args.invite_wait, args.listen)
    print("COMPLETE_JOIN_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
