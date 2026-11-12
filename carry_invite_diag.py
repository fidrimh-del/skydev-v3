# -*- coding: utf-8 -*-
"""
carry_invite_diag.py — Bot UNDANG gendong pemain (bot sebagai penggendong).

Latar (2026-11-12, Bartes):
- Bot sebagai PENERIMA gendong sudah TERBUKTI (carry_accept_diag.py):
  pemain kirim invite ke bot, bot accept, bot digendong pemain.
- Yang belum dites: bot sebagai PENGUNDANG — bot join server Bartes,
  kirim invite gendong ke Bartes, Bartes tergendong oleh bot.
- Mekanisme gendong = sistem CONSENT (dump.cs):
  * ConsentType: CarryPlayer=0, PetTrade=1, PairedGesture=2, PetBattle=3.
  * ToxicBirdConsentMechanic COMMAND_KEY = "consent".
  * ConsentClientEventId: ConsentInviteRequest=0, ConsentInviteResponse=1.
  * ConsentInviteRequest (bot -> server): f1 EventId=0, f2 Sender (UUID
    bot/pengundang), f3 Recipient (UUID target), f4 ConsentType=0.
    PENTING (proto3): field bernilai 0 TIDAK diserialisasi, jadi wire
    invite hanya berisi f2 + f3 — sama seperti ForwardInvite yang
    server kirim ke target.
  * ConsentInviteResponse (bot -> server, ACC telat / Auto ACC TCP):
    f1 EventId=1, f2 Sender (bot), f3 Recipient (target), f4 Accepted
    (bool), f5 ConsentType=0. Sama seperti trik Auto ACC di mod (v4):
    kirim ~6 dtk sesudah invite, sesudah target tolak/timeout.
- Alur tes: bot login -> alokasi destinationUserId (Bartes) -> JoinRoom
  -> kirim invite gendong ke Bartes -> dengarkan RoomEvent "consent".
  Bila Bartes accept (hasil f1=1, f4=true) sebelum batas tunggu, ACC
  telat TIDAK dikirim. Bila tolak (f4=false) atau tidak ada hasil
  sampai batas tunggu (~6 dtk), bot kirim ACC telat (Auto ACC TCP).
  Bartes lalu cek di game apakah dia tergendong oleh bot.
- BATASAN: bot TIDAK bisa jalan (diam di titik spawn / 0,0,0). Jadi
  bot hanya bisa MENAHAN pemain di tempat bot berada, bukan membawa
  mereka ke mana-mana. Ini probe, bukan fitur gerak.
- UUID bot TIDAK perlu ditebak: bawaan = PKXDSHOP #760
  (71056756-53c7-478e-9356-82c87fb2dd63), bisa di-override via argumen.
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


def _sfield(num, text):
    raw = text.encode("utf-8")
    return bytes([(num << 3) | 2]) + _varint(len(raw)) + raw


def build_consent_invite(sender_uuid, recipient_uuid, consent_type=0):
    """Bangun ConsentInviteRequest (bot -> server).
    f1 EventId=0 (dihilangkan, proto3), f2 Sender (bot), f3 Recipient
    (target), f4 ConsentType=0 (dihilangkan, proto3). Wire hanya f2+f3.
    """
    pb = b""
    pb += _sfield(2, sender_uuid)   # f2 Sender (bot, pengundang)
    pb += _sfield(3, recipient_uuid)  # f3 Recipient (target)
    # f4 ConsentType=0 dihilangkan (proto3, nilai 0 tidak diserialisasi)
    return pb


def build_consent_response(sender_uuid, recipient_uuid, accepted=True,
                           consent_type=0):
    """Bangun ConsentInviteResponse (bot -> server, ACC telat).
    f1 EventId=1, f2 Sender, f3 Recipient, f4 Accepted, f5 ConsentType.
    Sender/Recipient = bot (pengundang) / target — SAMA seperti invite,
    jangan dibalik (arah gendong ditentukan server dari pasangan ini).
    """
    pb = b""
    pb += b"\x08" + _varint(1)                       # f1 EventId=1
    pb += _sfield(2, sender_uuid)                     # f2 Sender (bot)
    pb += _sfield(3, recipient_uuid)                  # f3 Recipient (target)
    pb += b"\x20" + _varint(1 if accepted else 0)     # f4 Accepted
    pb += b"\x28" + _varint(consent_type)             # f5 ConsentType
    return pb


async def carry_invite_once(session, email, password, device_id,
                            dest_user_id, bot_user_id,
                            invite_wait=6, listen_s=60):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "bot_user_id": bot_user_id,
             "server": "", "room": None, "joined_room": None,
             "invite_sent": False, "invite_t_rel": None,
             "late_acc_sent": False, "late_acc_t_rel": None,
             "accepted_observed": False,
             "consent_events": [],
             "event_names": {}}
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
        print("[!] TIDAK ada konfirmasi JoinedRoom — tetap lanjut.")
    # Kirim invite gendong ke target (bot sebagai pengundang).
    invite_pb = build_consent_invite(bot_user_id, dest_user_id, 0)
    try:
        sock.sendall(L.build_room_event_varint("consent", invite_pb))
        hasil["invite_sent"] = True
        hasil["invite_t_rel"] = 0.0
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
            hasil["event_names"][name] = \
                hasil["event_names"].get(name, 0) + 1
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
            # Hasil invite (server -> bot): f1=1, f4 Accepted.
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
        # Batas tunggu invite habis: kirim ACC telat bila belum accept.
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
    print(f"[+] selesai. invite: {hasil['invite_sent']}, "
          f"accept teramati: {hasil['accepted_observed']}, "
          f"ACC telat: {hasil['late_acc_sent']}, "
          f"consent events: {len(hasil['consent_events'])}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("CARRY_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("CARRY_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("CARRY_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--dest-user-id",
                    default=os.getenv("CARRY_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("CARRY_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--invite-wait", type=float, default=6)
    ap.add_argument("--listen", type=float, default=60)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await carry_invite_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.bot_user_id,
            args.invite_wait, args.listen)
    print("CARRY_INVITE_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
