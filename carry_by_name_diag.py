# -*- coding: utf-8 -*-
"""
carry_by_name_diag.py — Bot UNDANG gendong pemain BERDASARKAN NAMA.

Varian carry_invite_diag.py yang menerima Nama#Kode (bukan UUID).
Dipakai untuk tes Bartes 2026-11-12: bot pkxdshop (akun teruji) gendong
kontol#618144 (akun baru) — untuk bedakan masalah script vs akun target
yang mungkin butuh setup complete dulu.

Alur: login -> TCP ke ruang bot sendiri -> cari UUID target dari nama
(search "friends", hasil "search_friend_result") -> alokasi
destinationUserId (UUID target) -> TCP + JoinRoom server target ->
kirim invite gendong -> ACC telat ~6 dtk bila belum accept -> dengarkan.

CATATAN: "ACCEPT teramati" di log = server mencatat consent diterima.
Apakah target BENAR-BENAR tergendong di game harus dikonfirmasi Bartes
di game (log saja tidak cukup — terbukti di Termux: ACCEPT teramati tapi
di game tidak terjadi, dugaan beda room/server).
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


def tcp_login_join(fr, sock, user_jwt, room):
    """Kirim login frame, tunggu jendela login, JoinRoom. Kembalikan
    True bila JoinedRoom terkonfirmasi (atau False bila tidak, tapi
    koneksi masih hidup)."""
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        print(f"[!] kirim login frame gagal: {exc}")
        return False
    got, closed = fr.read_until(
        lambda f: G._is_tbugv(f) or G._is_joinedroom(f), 3)
    if closed:
        print("[!] koneksi tertutup saat tunggu login.")
        return False
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
    try:
        sock.sendall(L.build_join_frame(room))
    except Exception as exc:
        print(f"[!] kirim JoinRoom gagal: {exc}")
        return False

    def _is_target_room(f):
        return (len(f) >= 6 and f[0] == 0x06 and f[1] == 1
                and int.from_bytes(f[2:6], "big") == room)

    joined, closed = fr.read_until(_is_target_room, 4)
    if closed:
        print("[!] koneksi tertutup saat JoinRoom.")
        return False
    if joined is not None:
        print(f"[+] JoinedRoom({room}) TERKONFIRMASI.")
        return True
    print("[!] TIDAK ada konfirmasi JoinedRoom — tetap lanjut.")
    return True


def search_uuid(fr, sock, name, wait_s=12):
    """Cari UUID dari Nama#Kode. Kembalikan UUID atau None."""
    frame = L.build_room_event_varint(
        "friends", L.build_search_protobuf(name))
    try:
        sock.sendall(frame)
        sock.sendall(frame)
    except Exception as exc:
        print(f"[!] kirim search gagal: {exc}")
        return None
    end = time.time() + wait_s
    while time.time() < end:
        for f in fr.drain(1):
            try:
                dec = L.decode_search_result(f)
            except Exception:
                dec = None
            if dec is not None and dec.get("success") and dec.get("uuid"):
                return dec["uuid"]
    return None


async def carry_by_name_once(session, email, password, device_id,
                             dest_name, bot_user_id,
                             invite_wait=6, listen_s=60):
    hasil = {"email": email, "dest_name": dest_name,
             "bot_user_id": bot_user_id,
             "dest_user_id": None,
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
    # 1) TCP ke ruang bot sendiri, cari UUID target dari nama.
    srv0, rm0 = parse_gsid(alloc.get("gameServerId", ""))
    print(f"[*] ruang bot sendiri: server={srv0} room={rm0}")
    if not srv0 or rm0 is None:
        return hasil
    host, port = L.find_host_port(alloc)
    if not host or not port:
        print("[!] host/port tidak ketemu.")
        return hasil
    sock, _pid = L.tcp_connect(host, port)
    if sock is None:
        print("[!] TCP connect gagal.")
        return hasil
    fr = G.FrameReader(sock)
    if not tcp_login_join(fr, sock, user_jwt, rm0):
        try:
            sock.close()
        except Exception:
            pass
        return hasil
    print(f"[*] Cari UUID {dest_name}...")
    target_uuid = search_uuid(fr, sock, dest_name)
    try:
        sock.close()
    except Exception:
        pass
    if not target_uuid:
        print(f"[!] {dest_name} tidak ketemu.")
        return hasil
    hasil["dest_user_id"] = target_uuid
    print(f"[+] {dest_name} -> UUID {target_uuid[:8]}...")
    # 2) Alokasi ke server target, TCP + JoinRoom di sana.
    body = await allocate_to_user(session, user_jwt, target_uuid)
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
    fr = G.FrameReader(sock)
    if not tcp_login_join(fr, sock, user_jwt, rm):
        try:
            sock.close()
        except Exception:
            pass
        return hasil
    hasil["joined_room"] = rm
    # 3) Kirim invite gendong + ACC telat.
    invite_pb = build_consent_invite(bot_user_id, target_uuid, 0)
    try:
        sock.sendall(L.build_room_event_varint("consent", invite_pb))
        hasil["invite_sent"] = True
        hasil["invite_t_rel"] = 0.0
        print(f"[+] INVITE gendong terkirim ke {dest_name} "
              f"({target_uuid[:8]}...).")
    except Exception as exc:
        print(f"[!] kirim invite gendong gagal: {exc}")
        return hasil
    print(f"[*] mendengarkan {listen_s} dtk. ACC telat dikirim di "
          f"t+{invite_wait} dtk bila belum ada accept.")
    t0 = time.time()
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
            fields = entry.get("fields", {})
            try:
                ev_id = int(fields.get("1", 0))
                accepted = int(fields.get("4", 0))
            except Exception:
                ev_id = -1
                accepted = -1
            if ev_id == 1 and accepted == 1:
                hasil["accepted_observed"] = True
                print("[+] ACCEPT teramati (target terima invite).")
        if (not late_acc_done and not hasil["accepted_observed"]
                and time.time() >= acc_deadline):
            resp = build_consent_response(bot_user_id, target_uuid, True, 0)
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
    ap.add_argument("--dest-name",
                    default=os.getenv("CARRY_DEST_NAME", ""))
    ap.add_argument("--bot-user-id",
                    default=os.getenv("CARRY_BOT_USER_ID", BOT_UUID))
    ap.add_argument("--invite-wait", type=float, default=6)
    ap.add_argument("--listen", type=float, default=60)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    if not args.dest_name:
        print("[!] --dest-name wajib diisi (Nama#Kode target).")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await carry_by_name_once(
            session, args.email, args.password, args.device_id,
            args.dest_name, args.bot_user_id,
            args.invite_wait, args.listen)
    print("CARRY_BY_NAME_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
