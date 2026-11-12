# -*- coding: utf-8 -*-
"""
outfit_clear_diag.py — Hapus outfit bot ke 0 (tanpa item).

Latar (2026-11-12, Bartes):
- Bot bernama 늑늑늑늑늑늑#994 sudah berteman (Bartes ACC di game), tapi
  bot terlihat PAKAI outfit (default server saat registrasi — script
  register tidak kirim outfit, jadi outfit bawaan server yang tampil).
- Bartes: "coba hilangkan ke 0 outfitnya" — hapus semua item outfit.
- Cara: kirim EquipRequest (RoomEvent "equip") dengan Items kosong,
  SkinColor = skin bot saat ini (dipertahankan), HairModel=0,
  HairStyle=0, BundleCode=0. Lalu GET outfit lagi untuk verifikasi.
- Alur: bot login -> alokasi biasa -> JoinRoom -> GET outfit (sebelum)
  -> kirim equip kosong -> GET outfit (sesudah) -> cetak JSON.
- GET outfit: RoomEvent "outfit", OutfitRequest EventId=0.
  Response "outfit_get_all": f1 EventId, f2 Success, f3 ErrorMessage,
  f4 repeated Outfit (f1 slot, f2 items packed, f3 skin Color).
- EquipRequest (dump.cs): f1 Items (repeated int, packed), f2 SkinColor
  (Color R/G/B), f3 HairModel, f4 HairStyle, f5 BundleCode. Proto3:
  field 0 / kosong tidak diserialisasi.
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G


def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def read_varint(buf, off):
    val, shift, i = 0, 0, off
    while i < len(buf) and shift < 35:
        b = buf[i]
        val |= (b & 0x7F) << shift
        i += 1
        if not (b & 0x80):
            return val, i - off
        shift += 7
    return None, 0


def parse_varint_stream(data):
    out, i = [], 0
    while i < len(data):
        v, n = read_varint(data, i)
        if v is None:
            break
        out.append(v)
        i += n
    return out


def parse_protobuf_fields(data):
    fields = []
    i = 0
    while i < len(data):
        tag = data[i]
        i += 1
        field, wt = tag >> 3, tag & 7
        if wt == 0:
            v, n = read_varint(data, i)
            if v is None:
                break
            i += n
            fields.append((field, v))
        elif wt == 2:
            ln, n = read_varint(data, i)
            if ln is None:
                break
            i += n
            fields.append((field, data[i:i + ln]))
            i += ln
        else:
            break
    return fields


def build_equip_protobuf(items, skin_rgb, hair_model=0, hair_style=0,
                         bundle_code=0):
    """EquipRequest: f1 Items (packed), f2 SkinColor, f3 HairModel,
    f4 HairStyle, f5 BundleCode. Field 0/kosong dihilangkan (proto3)."""
    packed = b"".join(varint(i) for i in items)
    out = b""
    if packed:
        out += b"\x0a" + varint(len(packed)) + packed
    r, g, b = skin_rgb
    skin = b""
    if r:
        skin += b"\x08" + varint(r)
    if g:
        skin += b"\x10" + varint(g)
    if b:
        skin += b"\x18" + varint(b)
    out += b"\x12" + varint(len(skin)) + skin
    if hair_model:
        out += b"\x18" + varint(hair_model)
    if hair_style:
        out += b"\x20" + varint(hair_style)
    if bundle_code:
        out += b"\x28" + varint(bundle_code)
    return out


def parse_outfit_get_all(data):
    """Parse GetOutfitResponse -> {'success': bool, 'error': str,
    'outfits': [{'slot': int, 'items': [int], 'skin': {f: v}}]}."""
    res = {"success": None, "error": None, "outfits": []}
    for f, v in parse_protobuf_fields(data):
        if f == 2:
            res["success"] = bool(v)
        elif f == 3 and isinstance(v, bytes):
            res["error"] = v.decode("utf-8", "replace")
        elif f == 4 and isinstance(v, bytes):
            slot, items, skin = None, [], None
            for of, ov in parse_protobuf_fields(v):
                if of == 1:
                    slot = ov
                elif of == 2:
                    if isinstance(ov, bytes):
                        items.extend(parse_varint_stream(ov))
                    else:
                        items.append(ov)
                elif of == 3 and isinstance(ov, bytes):
                    rgb = {}
                    for cf, cv in parse_protobuf_fields(ov):
                        rgb[cf] = cv
                    skin = rgb
            res["outfits"].append({"slot": slot, "items": items,
                                   "skin": skin})
    return res


def get_outfit(sock, fr, wait_s=5):
    """Kirim GetAllOutfits, kumpulkan semua 'outfit_get_all'."""
    try:
        sock.sendall(L.build_room_event_varint(
            "outfit", b"\x08" + varint(0)))
    except Exception as exc:
        return {"send_error": str(exc)}
    got = []
    end = time.time() + wait_s
    while time.time() < end:
        for f in fr.drain(1):
            name, d = G.decode_room_event(f)
            if name != "outfit_get_all" or not d:
                continue
            got.append(parse_outfit_get_all(bytes(d)))
        if got:
            break
    return {"responses": got}


async def outfit_clear_once(session, email, password, device_id):
    hasil = {"email": email, "server": "", "room": None,
             "joined_room": None, "outfit_before": None,
             "equip_sent": False, "outfit_after": None,
             "event_names": {}}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil
    srv, rm = None, None
    parts = (alloc.get("gameServerId", "") or "").split("::")
    if len(parts) >= 2:
        srv = parts[1]
    if len(parts) >= 3:
        try:
            rm = int(parts[2])
        except Exception:
            rm = None
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
    # GET outfit SEBELUM.
    print("[*] GET outfit (sebelum)...")
    before = get_outfit(sock, fr, 5)
    hasil["outfit_before"] = before
    print(f"[*] outfit sebelum: {json.dumps(before, ensure_ascii=False)}")
    # Skin saat ini (pertahankan); default (255,220,180) bila tak ketemu.
    skin_rgb = (255, 220, 180)
    for resp in before.get("responses", []):
        for o in resp.get("outfits", []):
            sk = o.get("skin") or {}
            if sk:
                skin_rgb = (sk.get(1, 255), sk.get(2, 220), sk.get(3, 180))
                break
        if skin_rgb != (255, 220, 180):
            break
    print(f"[*] skin dipakai: {skin_rgb}")
    # Kirim equip KOSONG (hapus semua item, tanpa rambut/bundle).
    try:
        sock.sendall(L.build_room_event_varint(
            "equip", build_equip_protobuf([], skin_rgb, 0, 0, 0)))
        hasil["equip_sent"] = True
        print("[+] EQUIP kosong terkirim (items=[], hair=0, bundle=0).")
    except Exception as exc:
        print(f"[!] kirim equip gagal: {exc}")
        return hasil
    time.sleep(1)
    # GET outfit SESUDAH (verifikasi).
    print("[*] GET outfit (sesudah)...")
    after = get_outfit(sock, fr, 5)
    hasil["outfit_after"] = after
    print(f"[*] outfit sesudah: {json.dumps(after, ensure_ascii=False)}")
    try:
        sock.close()
    except Exception:
        pass
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("OUTFIT_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("OUTFIT_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("OUTFIT_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await outfit_clear_once(
            session, args.email, args.password, args.device_id)
    print("OUTFIT_CLEAR_JSON:" + json.dumps(hasil, separators=(",", ":"),
                                            ensure_ascii=False))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
