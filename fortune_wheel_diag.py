# -*- coding: utf-8 -*-
"""
fortune_wheel_diag.py — Probe Fortune Wheel (spin harian toko) dengan
1 bot tes (pkxdshop.id@gmail.com).

Latar (2026-11-14, analisis dump — ~/workspace/fortune_wheel_analysis.md):
- Sistem bernama Fortune Wheel, mekanik server
  ToxicBirdFortuneWheelMechanic, nama RoomEvent "fw",
  EventId: GetRewards=0, Collect=1, GetInfo=2. Semua request protobuf
  lewat EmitLiveRoomEvent reliable=1 — pola yang sama dengan mekanik
  gift/quest/pet yang sudah dipakai bot.
- Byte persis (dari WriteTo):
    GetInfo   : "fw" 08 02
    GetRewards: "fw" (kosong, 0 byte — EventId=0 default proto3)
    Collect gratis: "fw" 08 01        (EventId=1, AdsWatchedCount=0)
    Collect iklan : "fw" 08 01 10 01  (EventId=1, AdsWatchedCount=1)
- Respons "fw" (protobuf):
    GetInfoResponse : f1 EventId, f2 Successful, f3 FreeCollected,
                      f4 AdsWatchedCount (status harian server per akun)
    GetRewardResponse: f1 EventId, f2 Successful,
                      f3 LootBoxConfigIds (repeated string)
    CollectResponse : f1 EventId, f2 Successful, f3 Wallet (Money),
                      f4 Rewards (repeated LootBoxItem)
    LootBoxItem: f1 Type (0 Empty, 1 MoneyReward, 2 ItemReward,
      3 PetReward, 4 VehicleReward), f2 Id, f3 Quantity, f4 Money,
      f5 Unique, f6 Presentation, f7 Pet (repeated PetData), f8 Quality
    PetData: f3 PetId, f4 DisplayName, f6 Stage
      (0 telur, 1 Incubated, 2 Baby, 3 Child, 4 Teen, 5 MergeIngredient,
       6 Mutated, 7 Shiny)
- Mekanik ini terdaftar di ToxicBirdClient (bukan room): TIDAK perlu
  destinationUserId, TIDAK ada syarat room/activity, Bartes tidak
  perlu online. TIDAK kirim JoinRoom.
- Probe ini MEMAKAI jatah spin harian bot tes (gratis + iklan hari
  ini) — memang itu yang diminta Bartes.
- Spin 2 (iklan) hanya mengirim ANGKA AdsWatchedCount=1 dari client;
  apakah server memverifikasi iklan benar ditonton tidak terlihat
  dari dump — probe inilah yang membuktikannya (Successful=true
  berarti server percaya angka client).
Alur: login -> alokasi sendiri -> TCP login -> tunggu jendela login
(tbugv, maks 3 dtk) -> SendDeviceId ('Analytics/deviceId') ->
GetInfo -> GetRewards -> Collect gratis -> Collect iklan (count=1).
Tiap request ditunggu responsnya maks 10 dtk, maks 1 kirim ulang
bila tidak ada respons.
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G

BOT_USER_ID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"

LOOTBOX_TYPE_NAMES = {0: "Empty", 1: "MoneyReward", 2: "ItemReward",
                      3: "PetReward", 4: "VehicleReward"}


def _add(out, field, val):
    """Kumpulkan field berulang jadi list; field tunggal tetap skalar."""
    if field in out:
        if isinstance(out[field], list):
            out[field].append(val)
        else:
            out[field] = [out[field], val]
    else:
        out[field] = val


def _try_str(chunk):
    """Wire-2 chunk -> str HANYA bila isinya teks printable (nama/config
    ID). Pesan nested berisi byte kontrol (tag/varint kecil) TIDAK
    boleh jadi str — itu yang membuat parse LootBoxItem crash di run 2
    (chunk 8 byte berisi 0x08/0x01/0x02 ter-decode jadi str)."""
    try:
        s = chunk.decode("utf-8")
    except Exception:
        return chunk
    if s and all(c.isprintable() for c in s):
        return s
    return chunk


def parse_protobuf(buf):
    """Parse protobuf generik. Wire 0 varint, wire 2 length-delim
    (panjang varint — BUKAN 1 byte, karena respons bisa >127 byte),
    wire 5 fixed32, wire 1 fixed64. Field berulang dikumpulkan list.
    Wire-2 yang bukan teks printable TETAP bytes (pesan nested)."""
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
            _add(out, field, v)
        elif wire == 2:
            ln, m = L.read_varint(buf, i)
            if ln is None:
                break
            i += m
            chunk = bytes(buf[i:i + ln])
            i += ln
            _add(out, field, _try_str(chunk))
        elif wire == 5:
            if i + 4 <= n:
                _add(out, field, struct.unpack("<f", buf[i:i + 4])[0])
                i += 4
            else:
                break
        elif wire == 1:
            if i + 8 <= n:
                _add(out, field, struct.unpack("<d", buf[i:i + 8])[0])
                i += 8
            else:
                break
        else:
            break
    return out


def _as_list(d, field):
    v = d.get(field)
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def _nested(v, depth=0):
    """Nilai wire-2 berupa bytes: coba parse sebagai protobuf nested,
    REKURSIF (maks 3 level — Wallet berisi daftar entri bernama
    di field berulang f4). Bila tidak bisa diparse, kembalikan hex.
    String/int diteruskan apa adanya. Catatan: chunk yang sudah
    jadi str oleh _try_str (teks printable) tidak disentuh."""
    if isinstance(v, str):
        return v
    if isinstance(v, (bytes, bytearray)):
        if depth > 3:
            return {"hex": bytes(v).hex()}
        try:
            d = parse_protobuf(bytes(v))
        except Exception:
            d = {}
        if not d:
            return {"hex": bytes(v).hex()}
        return {k: _nested(x, depth + 1) for k, x in d.items()}
    if isinstance(v, list):
        return [_nested(x, depth) for x in v]
    if isinstance(v, dict):
        return {k: _nested(x, depth) for k, x in v.items()}
    return v


def _jsonable(v):
    if isinstance(v, dict):
        return {str(k): _jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    return v


def parse_pet_data(raw):
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    return {"pet_id": d.get(3), "display_name": d.get(4),
            "stage": d.get(6)}


def parse_lootbox_item(raw):
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    item = {"type": d.get(1),
            "type_name": LOOTBOX_TYPE_NAMES.get(d.get(1),
                                                f"Unknown({d.get(1)})"),
            "id": d.get(2), "quantity": d.get(3),
            "money": _nested(d.get(4)) if d.get(4) is not None else None,
            "unique": d.get(5), "presentation": d.get(6),
            "quality": d.get(8)}
    pets = [parse_pet_data(p) for p in _as_list(d, 7)]
    if pets:
        item["pets"] = pets
    return _jsonable(item)


def parse_getinfo(data):
    # proto3: field bool/int yang tidak ditulis = False/0.
    d = parse_protobuf(data)
    return {"event_id": d.get(1) or 0, "successful": bool(d.get(2)),
            "free_collected": bool(d.get(3)),
            "ads_watched_count": d.get(4) or 0}


def parse_getrewards(data):
    d = parse_protobuf(data)
    return {"event_id": d.get(1) or 0, "successful": bool(d.get(2)),
            "lootbox_config_ids": _as_list(d, 3)}


def parse_collect(data):
    d = parse_protobuf(data)
    wallet = d.get(3)
    return {"event_id": d.get(1) or 0, "successful": bool(d.get(2)),
            "wallet": _jsonable(_nested(wallet)) if wallet is not None else None,
            "rewards": [parse_lootbox_item(r) for r in _as_list(d, 4)]}


async def fortune_wheel_once(session, email, password, device_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "server": "", "room": None, "player_id": None,
             "login_ok": False, "deviceid_sent": False,
             "getinfo": None, "rewards": None,
             "spin_free": None, "spin_ads": None,
             "error": None}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        hasil["error"] = "login/alokasi gagal"
        print("[!] login/alokasi gagal.")
        return hasil
    hasil["login_ok"] = True
    host, port = L.find_host_port(alloc)
    if not host or not port:
        hasil["error"] = "host/port tidak ketemu di allocation"
        print("[!] host/port tidak ketemu.")
        return hasil
    print(f"[+] alokasi OK: {host}:{port}")
    sock, pid = L.tcp_connect(host, port)
    if sock is None:
        hasil["error"] = "TCP connect gagal"
        print("[!] TCP connect gagal.")
        return hasil
    hasil["player_id"] = pid
    print(f"[+] Connected: playerId={pid}.")
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        hasil["error"] = f"kirim login frame gagal: {exc}"
        print(f"[!] kirim login frame gagal: {exc}")
        return hasil
    fr = G.FrameReader(sock)
    try:
        print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
        got, closed = fr.read_until(
            lambda f: G._is_tbugv(f) or G._is_joinedroom(f), 3)
        if closed:
            hasil["error"] = "koneksi tertutup saat tunggu login"
            print("[!] koneksi tertutup saat tunggu login.")
            return hasil
        if got is not None and G._is_tbugv(got):
            print("[*] tbugv diterima — observasi 1 dtk lagi.")
            time.sleep(1)
        elif got is not None:
            print("[*] JoinedRoom diterima.")
        else:
            print("[*] timeout jendela login (koneksi hidup) — lanjut kirim.")
        # SendDeviceId (pola spawn_test, TERBUKTI).
        send_dev = device_id or DEFAULT_DEVICE_ID
        try:
            sock.sendall(L.build_room_event_varint(
                "Analytics/deviceId", send_dev.encode("utf-8")))
            hasil["deviceid_sent"] = True
            print(f"[+] SendDeviceId terkirim ('Analytics/deviceId').")
        except Exception as exc:
            print(f"[!] kirim SendDeviceId gagal: {exc}")

        def wait_fw(timeout=10):
            """Baca frame sampai dapat RoomEvent 'fw' atau timeout.
            Kembalikan data protobuf (bytes) atau None."""
            end = time.time() + timeout
            while time.time() < end:
                for f in fr.drain(0.5):
                    name, d = G.decode_room_event(f)
                    if name == "fw":
                        return d
            return None

        def fw_request(label, payload, parse_fn, timeout=10):
            """Kirim event 'fw' + tunggu respons; maks 1 kirim ulang."""
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint("fw", payload))
                except Exception as exc:
                    print(f"[!] kirim {label} gagal: {exc}")
                    return None
                print(f"[*] kirim {label} ({payload.hex() if payload else '(kosong)'}), "
                      f"percobaan {attempt}/2, tunggu respons maks {timeout} dtk...")
                data = wait_fw(timeout)
                if data is not None:
                    # Cetak hex mentah SEBELUM parse — bila parse gagal,
                    # respons tidak hilang (bisa di-decode offline).
                    print(f"[+] respons 'fw' diterima untuk {label} "
                          f"({len(data)} B): {data.hex()}")
                    try:
                        return parse_fn(data)
                    except Exception as exc:
                        print(f"[!] parse respons {label} gagal: {exc!r} "
                              f"(raw hex di atas; lanjut langkah berikut)")
                        import traceback
                        traceback.print_exc()
                        return {"parse_error": repr(exc),
                                "raw_hex": data.hex()}
                print(f"[!] tidak ada respons 'fw' untuk {label} "
                      f"(percobaan {attempt}/2).")
            return None

        def _print_step(label, res, keys):
            """Cetak hasil satu langkah; tahan terhadap dict parse_error
            (tanpa 'successful') — jangan crash, langkah berikut tetap
            jalan (pelajaran run 2: KeyError 'successful' mematikan
            probe sebelum Collect iklan)."""
            if res is None:
                print(f"[!] {label}: TIDAK ADA respons.")
                return
            if isinstance(res, dict) and "parse_error" in res:
                print(f"[!] {label}: parse gagal: {res['parse_error']}")
                print(f"    raw_hex: {res.get('raw_hex')}")
                return
            print(f"[+] {label}: " + " ".join(
                f"{k}={res.get(k)}" for k in keys))

        # 1. GetInfo — status harian server.
        hasil["getinfo"] = fw_request("GetInfo", bytes.fromhex("0802"),
                                      parse_getinfo)
        if hasil["getinfo"] is not None:
            _print_step("GetInfo", hasil["getinfo"],
                        ["successful", "free_collected",
                         "ads_watched_count"])
        else:
            print("[!] GetInfo: TIDAK ADA respons — lanjut tetap (GetRewards).")

        # 2. GetRewards — daftar hadiah roda hari ini.
        hasil["rewards"] = fw_request("GetRewards", b"", parse_getrewards)
        if hasil["rewards"] is not None:
            _print_step("GetRewards", hasil["rewards"],
                        ["successful", "lootbox_config_ids"])
        else:
            print("[!] GetRewards: TIDAK ADA respons — lanjut tetap (Collect).")

        # 3. Collect gratis — LAKUKAN MESKIPUN FreeCollected sudah true
        #    (server akan menjawab Successful=false — itu tetap data).
        hasil["spin_free"] = fw_request("Collect gratis",
                                        bytes.fromhex("0801"),
                                        parse_collect)
        _print_step("Collect gratis", hasil["spin_free"],
                    ["successful", "wallet", "rewards"])

        # 4. Collect iklan (count=1) — LAKUKAN MESKIPUN Collect gratis
        #    gagal; keduanya independen.
        hasil["spin_ads"] = fw_request("Collect iklan (count=1)",
                                       bytes.fromhex("08011001"),
                                       parse_collect)
        _print_step("Collect iklan (count=1)", hasil["spin_ads"],
                    ["successful", "wallet", "rewards"])
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("FW_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("FW_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("FW_DEVICE_ID", ""))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await fortune_wheel_once(
            session, args.email, args.password, args.device_id)
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL FORTUNE WHEEL ==========")
    gi = hasil.get("getinfo") or {}
    gr = hasil.get("rewards") or {}
    sf = hasil.get("spin_free") or {}
    sa = hasil.get("spin_ads") or {}

    def _fmt_rewards(r):
        out = []
        for it in r or []:
            s = f"{it.get('type_name')} id={it.get('id')} qty={it.get('quantity')}"
            if it.get("money") is not None:
                s += f" money={it.get('money')}"
            for p in it.get("pets") or []:
                s += (f" pet={p.get('display_name')} "
                      f"(PetId={p.get('pet_id')}, Stage={p.get('stage')})")
            out.append(s)
        return out or ["(tidak ada)"]

    print(f"Login OK: {hasil.get('login_ok')}  "
          f"server: {hasil.get('server')}  player_id: {hasil.get('player_id')}")
    print(f"GetInfo: Successful={gi.get('successful')} "
          f"FreeCollected={gi.get('free_collected')} "
          f"AdsWatchedCount={gi.get('ads_watched_count')}")
    print(f"GetRewards: Successful={gr.get('successful')} "
          f"LootBoxConfigIds={gr.get('lootbox_config_ids')}")
    print(f"Spin 1 (gratis): Successful={sf.get('successful')} "
          f"Wallet={sf.get('wallet')}")
    if sf.get("parse_error"):
        print(f"  PARSE GAGAL: {sf.get('parse_error')} raw_hex={sf.get('raw_hex')}")
    for line in _fmt_rewards(sf.get("rewards")):
        print(f"  hadiah spin gratis: {line}")
    print(f"Spin 2 (iklan count=1): Successful={sa.get('successful')} "
          f"Wallet={sa.get('wallet')}")
    if sa.get("parse_error"):
        print(f"  PARSE GAGAL: {sa.get('parse_error')} raw_hex={sa.get('raw_hex')}")
    for line in _fmt_rewards(sa.get("rewards")):
        print(f"  hadiah spin iklan: {line}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("=========================================")
    print("FORTUNE_WHEEL_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
