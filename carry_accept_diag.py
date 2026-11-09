# -*- coding: utf-8 -*-
"""
carry_accept_diag.py — Bot auto-terima undangan GENDONG (carry).

Latar (2026-11-09):
- Bot spawn via frame "aaa" belum berhasil (v1/v2 tidak muncul di titik
  spawn). Rencana Bartes: pakai fitur GENDONG bawaan game — dia klik
  gendong pada bot, bot OTOMATIS menerima, lalu dia bawa bot ke titik
  spawn dan lepas di sana, baru diagnosis dari situ.
- Mekanisme gendong = sistem CONSENT (dump.cs):
  * ConsentType: CarryPlayer=0, PetTrade=1, PairedGesture=2, PetBattle=3.
  * ToxicBirdConsentMechanic COMMAND_KEY = "consent".
  * ConsentClientEventId: ConsentInviteRequest=0, ConsentInviteResponse=1.
  * ConsentServerEventId: ConsentInvitationForward=0,
    ConsentInvitationResult=1.
  * ConsentForwardInvite (server -> bot): f1 EventId=0, f2 Sender (UUID
    pengundang), f3 Recipient (UUID bot), f4 ConsentType=0.
  * ConsentInviteResponse (bot -> server, terima/tolak): f1 EventId=1,
    f2 Sender (UUID bot), f3 Recipient (UUID pengundang),
    f4 Accepted (bool), f5 ConsentType=0.
- Alur tes: bot login -> alokasi destinationUserId (Bartes) -> JoinRoom
  -> dengarkan RoomEvent "consent". Saat undangan gendong diterima
  (ConsentForwardInvite, ConsentType CarryPlayer), bot LANGSUNG kirim
  ConsentInviteResponse Accepted=true. Bartes lalu gendong bot ke titik
  spawn dan lepas. Bot TIDAK kirim posisi; hanya merespons undangan
  yang dikirim Bartes (fitur game yang sah).
- UUID bot TIDAK perlu diketahui di muka: dibaca dari f3 (Recipient)
  undangan yang diterima; UUID pengundang dari f2 (Sender).
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
            # length varint (sederhana: 1 byte, cukup untuk UUID/string pendek)
            if ln & 0x80:
                # varint multi-byte
                ln = 0
                s = 0
                # ulangi baca length dari posisi sebelumnya (sudah maju 1)
                # (kasus ini jarang; pakai fallback)
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


def build_consent_response(sender_uuid, recipient_uuid, accepted=True,
                           consent_type=0):
    """Bangun ConsentInviteResponse (bot -> server).
    f1 EventId=1, f2 Sender, f3 Recipient, f4 Accepted, f5 ConsentType.
    """
    def varint(v):
        out = bytearray()
        while True:
            b = v & 0x7F
            v >>= 7
            if v:
                out.append(b | 0x80)
            else:
                out.append(b)
                return bytes(out)

    def sfield(num, text):
        raw = text.encode("utf-8")
        return bytes([(num << 3) | 2]) + varint(len(raw)) + raw

    pb = b""
    pb += b"\x08" + varint(1)                       # f1 EventId=1
    pb += sfield(2, sender_uuid)                     # f2 Sender (bot)
    pb += sfield(3, recipient_uuid)                  # f3 Recipient (pengundang)
    pb += b"\x20" + varint(1 if accepted else 0)     # f4 Accepted
    pb += b"\x28" + varint(consent_type)             # f5 ConsentType
    return pb


async def carry_accept_once(session, email, password, device_id,
                            dest_user_id, listen_s=600):
    hasil = {"email": email, "dest_user_id": dest_user_id,
             "server": "", "room": None, "joined_room": None,
             "consent_events": [], "accepts_sent": 0,
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
        print("[!] TIDAK ada konfirmasi JoinedRoom — tetap dengarkan.")
    print(f"[*] mendengarkan undangan gendong selama {listen_s} dtk. "
          f"Bartes: klik gendong pada PKXDSHOP di game.")
    end = time.time() + listen_s
    while time.time() < end:
        for f in fr.drain(1):
            name, d = G.decode_room_event(f)
            if not name:
                continue
            hasil["event_names"][name] = \
                hasil["event_names"].get(name, 0) + 1
            if name != "consent":
                continue
            # Ada RoomEvent "consent" — catat + parse.
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
            # Terima bila ini undangan gendong (Forward, ConsentType 0).
            # PENTING (proto3): field bernilai 0 TIDAK diserialisasi,
            # jadi "tidak ada" di wire = 0. Undangan ForwardInvite yang
            # asli hanya berisi f2 (Sender) + f3 (Recipient) — f1=0 dan
            # f4=0 dihilangkan server. v1 salah menunggu f1==0 DAN f4==0
            # secara eksplisit, sehingga tidak pernah menerima (log run
            # 42644573867: 22 event consent masuk, 0 diterima).
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
                    resp = build_consent_response(recipient, sender, True, 0)
                    frame = L.build_room_event_varint("consent", resp)
                    try:
                        sock.sendall(frame)
                        hasil["accepts_sent"] += 1
                        print(f"[+] TERIMA gendong terkirim (ke {sender[:8]}...). "
                              f"Total terima: {hasil['accepts_sent']}")
                    except Exception as exc:
                        print(f"[!] kirim terima gendong gagal: {exc}")
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. consent events: {len(hasil['consent_events'])}, "
          f"terima terkirim: {hasil['accepts_sent']}")
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("CARRY_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("CARRY_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("CARRY_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--dest-user-id",
                    default=os.getenv("CARRY_DEST_USER_ID", SKYDEV_UUID))
    ap.add_argument("--listen", type=float, default=600)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await carry_accept_once(
            session, args.email, args.password, args.device_id,
            args.dest_user_id, args.listen)
    print("CARRY_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
