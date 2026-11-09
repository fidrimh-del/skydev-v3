# -*- coding: utf-8 -*-
"""
carry_spawn_diag.py — Bot auto-terima gendong, lalu kirim posisi spawn.

Latar (2026-11-09/10):
- Spawn via frame "aaa" di join (v1/v2) TIDAK berhasil — bot tetap di
  (0,0,0) bawah tanah. Gendong (carry) via consent BERHASIL diterima
  dan arahnya sudah benar (v3: Bartes menggendong bot), TAPI wujud bot
  tidak muncul di tangan Bartes karena posisinya tetap bawah tanah:
  gendong hanya ubah status, tidak pindahkan posisi bot (bot tidak
  pernah kirim posisi).
- Ide tes ini: SETELAH bot diterima digendong (status sah, digerakkan
  pemain asli ke lokasi sah), bot kirim SATU frame posisi titik spawn
  tetap (79.96, 27.13, -122.17) — BUKAN posisi live pemain, BUKAN
  loop. Frame dikirim 3x berjeda (t+2, t+10, t+20 dtk sesudah terima)
  supaya salah satunya mendarat saat Bartes sudah berjalan ke spawn.
  Harapannya server mau menerima posisi dari pemain yang sedang
  digendong (konteks sah), tidak seperti saat join.
- Mekanisme gendong = sistem CONSENT (dump.cs). Respons di-ECHO dari
  undangan (f2=pengundang, f3=bot) — JANGAN dibalik (v2 terbalik).
  Frame "aaa" meniru persis frame kirim asli client ([TBULV-BASE]
  frame 3), hanya float X/Y/Z diganti (terverifikasi byte-identik).
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


def build_consent_response(sender_uuid, recipient_uuid, accepted=True,
                           consent_type=0):
    """ConsentInviteResponse. Sender/Recipient di-ECHO dari undangan."""
    def sfield(num, text):
        raw = text.encode("utf-8")
        return bytes([(num << 3) | 2]) + _varint(len(raw)) + raw

    pb = b""
    pb += b"\x08" + _varint(1)                       # f1 EventId=1
    pb += sfield(2, sender_uuid)                     # f2 Sender (echo)
    pb += sfield(3, recipient_uuid)                  # f3 Recipient (echo)
    pb += b"\x20" + _varint(1 if accepted else 0)     # f4 Accepted
    pb += b"\x28" + _varint(consent_type)             # f5 ConsentType
    return pb


def build_spawn_aaa(x, y, z):
    """Frame "aaa" posisi spawn (meniru frame kirim asli client)."""
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


async def carry_spawn_once(session, email, password, device_id,
                           dest_user_id, sx, sy, sz, listen_s=600):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "server": "", "room": None, "joined_room": None,
             "spawn": {"x": sx, "y": sy, "z": sz},
             "consent_events": [], "accepts_sent": 0,
             "spawn_sends": [], "event_names": {}, "tbulv_count": 0}
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
        print("[!] TIDAK ada konfirmasi JoinedRoom — tetap dengarkan.")
    print(f"[*] mendengarkan undangan gendong {listen_s} dtk. "
          f"Setelah diterima, bot kirim posisi spawn {sx},{sy},{sz}.")
    accept_t = None
    spawn_offsets = [2, 10, 20]
    spawn_idx = 0
    end = time.time() + listen_s
    while time.time() < end:
        # Kirim frame spawn terjadwal sesudah carry diterima.
        if accept_t is not None and spawn_idx < len(spawn_offsets):
            if time.time() - accept_t >= spawn_offsets[spawn_idx]:
                data = build_spawn_aaa(sx, sy, sz)
                frame = L.build_room_event_varint("tbulv", data)
                try:
                    sock.sendall(frame)
                    hasil["spawn_sends"].append(
                        {"offset": spawn_offsets[spawn_idx],
                         "t_rel": round(time.time() - t0, 1)})
                    print(f"[+] frame SPAWN #{spawn_idx + 1} terkirim "
                          f"(t+{time.time() - accept_t:.0f} dtk sesudah terima)")
                except Exception as exc:
                    print(f"[!] kirim frame spawn gagal: {exc}")
                spawn_idx += 1
        for f in fr.drain(1):
            name, d = G.decode_room_event(f)
            if not name:
                continue
            hasil["event_names"][name] = \
                hasil["event_names"].get(name, 0) + 1
            if name == "tbulv":
                hasil["tbulv_count"] += 1
                continue
            if name != "consent":
                continue
            entry = {"t_rel": round(time.time() - t0, 1)}
            try:
                fields = parse_protobuf(bytes(d))
                entry["fields"] = {str(k): (v if not isinstance(v, bytes) else v.hex())
                                   for k, v in fields.items()}
            except Exception as exc:
                entry["parse_error"] = str(exc)
            hasil["consent_events"].append(entry)
            print(f"[+] CONSENT t+{entry['t_rel']} dtk: {entry.get('fields', '')}")
            fields = entry.get("fields", {})
            try:
                ev_id = int(fields.get("1", 0))
                ctype = int(fields.get("4", 0))
            except Exception:
                ev_id = -1
                ctype = -1
            if ev_id == 0 and ctype == 0:
                sender = fields.get("2", "")
                recipient = fields.get("3", "")
                if isinstance(sender, str) and isinstance(recipient, str) \
                        and sender and recipient:
                    resp = build_consent_response(sender, recipient, True, 0)
                    frame = L.build_room_event_varint("consent", resp)
                    try:
                        sock.sendall(frame)
                        hasil["accepts_sent"] += 1
                        if accept_t is None:
                            accept_t = time.time()
                        print(f"[+] TERIMA gendong terkirim. "
                              f"Total terima: {hasil['accepts_sent']}")
                    except Exception as exc:
                        print(f"[!] kirim terima gendong gagal: {exc}")
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. consent: {len(hasil['consent_events'])}, "
          f"terima: {hasil['accepts_sent']}, "
          f"spawn terkirim: {len(hasil['spawn_sends'])}")
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("CARRY_SPAWN_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("CARRY_SPAWN_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("CARRY_SPAWN_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--dest-user-id",
                    default=os.getenv("CARRY_SPAWN_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--spawn-x", type=float, default=SPAWN_X)
    ap.add_argument("--spawn-y", type=float, default=SPAWN_Y)
    ap.add_argument("--spawn-z", type=float, default=SPAWN_Z)
    ap.add_argument("--listen", type=float, default=600)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await carry_spawn_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.spawn_x, args.spawn_y, args.spawn_z,
            args.listen)
    print("CARRYSPAWN_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
