# -*- coding: utf-8 -*-
"""
rename_unnamed_diag.py — Beri NAMA ke akun bot TANPA NAMA (sudah ada,
login email), lalu lihat apakah nama diterima server.

Latar (2026-11-12, Bartes): daftar teman SkyDev (200+) kosong bahkan
sesudah hapus data game; dugaan Bartes, akun SkyDev bug karena menerima
akun tanpa nama (bot 33e8a45c... yang kirim friend request 12:45).
Tes ini: login ke akun tanpa nama itu, beri nama (bebas), lihat apakah
server menerima. Bila nama diterima, akun itu jadi terlihat di daftar
teman dan mungkin daftar teman SkyDev pulih.

Dua jalur nama (dump.cs, ToxicBirdProfileMechanic, event "profile_update"):
- UpdateNicknameRequest: f1 EventId=9, f2 Nickname (TANPA DeviceId).
  Response "update_name_result": f1 Name, f2 ErrorMessage, f3 Wallet,
  f4 Spent. Ini jalur GANTI nama akun yang SUDAH ADA (UI game).
- SetNicknameRequest: f1 EventId=0, f2 Name, f3 DeviceId.
  Response "set_name_result": f1 Name, f2 ErrorMessage. Ini jalur
  registrasi akun BARU (terbukti di register_named.py).
Dicoba berurutan di socket yang sama: Update dulu (paling cocok untuk
akun lama), lalu Set dengan device, lalu Set tanpa field EventId.
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G


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


def build_update_nickname(name):
    # UpdateNicknameRequest: f1 EventId=9, f2 Nickname.
    return b"\x08" + _varint(9) + _sfield(2, name)


def build_set_nickname(name, device_id):
    # SetNicknameRequest: f1 EventId=0, f2 Name, f3 DeviceId.
    if device_id is None:
        return b"\x08" + _varint(0) + _sfield(2, name)
    return b"\x08" + _varint(0) + _sfield(2, name) + _sfield(3, device_id)


def build_set_nickname_no_event(name, device_id):
    # Varian tanpa field EventId (proto3 menghilangkan nilai 0).
    return _sfield(2, name) + _sfield(3, device_id)


def listen_for(fr, wanted, seconds):
    """Dengarkan RoomEvent bernama `wanted` selama `seconds` detik.
    Kembalikan (fields dict, event_names) atau (None, event_names)."""
    event_names = {}
    end = time.time() + seconds
    while time.time() < end:
        for f in fr.drain(0.5):
            name, d = G.decode_room_event(f)
            if not name:
                continue
            event_names[name] = event_names.get(name, 0) + 1
            if name != wanted:
                continue
            try:
                fields = parse_protobuf(bytes(d))
            except Exception:
                fields = {}
            return fields, event_names
    return None, event_names


async def rename_once(session, email, password, device_id, new_name):
    hasil = {"email": email, "new_name": new_name,
             "server": "", "room": None, "joined_room": None,
             "attempts": [], "final_name": None, "final_error": None,
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
    if joined is not None:
        hasil["joined_room"] = rm
        print(f"[+] JoinedRoom({rm}) TERKONFIRMASI.")
    else:
        print("[!] TIDAK ada konfirmasi JoinedRoom — tetap lanjut.")

    attempts = [
        ("update", "update_name_result",
         build_update_nickname(new_name)),
        ("set+device", "set_name_result",
         build_set_nickname(new_name, device_id)),
        ("set-no-event", "set_name_result",
         build_set_nickname_no_event(new_name, device_id)),
    ]
    for label, want_event, pb in attempts:
        print(f"[*] Percobaan {label}: kirim nama {new_name!r} "
              f"(tunggu {want_event})...")
        try:
            sock.sendall(L.build_room_event_varint("profile_update", pb))
        except Exception as exc:
            print(f"[!] Percobaan {label} kirim gagal: {exc}")
            hasil["attempts"].append({"label": label, "sent": False,
                                     "error": str(exc)})
            continue
        fields, ev_names = listen_for(fr, want_event, 5)
        for k, v in ev_names.items():
            hasil["event_names"][k] = hasil["event_names"].get(k, 0) + v
        entry = {"label": label, "sent": True, "event": want_event,
                 "fields": fields}
        hasil["attempts"].append(entry)
        if fields:
            nm = fields.get(1)
            err = fields.get(2)
            print(f"[+] Percobaan {label}: Name={nm!r} Error={err!r} "
                  f"fields={fields}")
            if isinstance(nm, str) and nm:
                hasil["final_name"] = nm
                hasil["final_error"] = err if isinstance(err, str) else None
                print(f"[+] NAMA DITERIMA: {nm!r}")
                break
            if isinstance(err, str) and err:
                hasil["final_error"] = err
        else:
            print(f"[*] Percobaan {label}: tidak ada {want_event} (5 dtk).")
    try:
        sock.close()
    except Exception:
        pass
    print(f"[+] selesai. final_name={hasil['final_name']!r} "
          f"final_error={hasil['final_error']!r}")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("RENAME_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("RENAME_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("RENAME_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--name", default=os.getenv("RENAME_NAME", ""))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    if not args.name:
        print("[!] --name wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await rename_once(session, args.email, args.password,
                                  args.device_id, args.name)
    print("RENAME_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
