# -*- coding: utf-8 -*-
"""
lootbox_info_diag.py — Probe LootBox ("lb") 1 bot tes untuk mengisi
tabel Supabase panel `spin_daily_prizes` dengan daftar hadiah
Fortune Wheel (Spin Daily).

Latar (2026-11-14, ~/workspace/fortune_wheel_prizes.md):
- Mekanik server ToxicBirdLootBoxMechanic, nama RoomEvent "lb",
  EventId: LootBoxInfo=0, LootBoxProbabilityInfo=1, BuyOpenLootBox=2.
- GetLootBoxInfo: kirim "lb" payload KOSONG (0 byte, EventId=0).
  Respons LootBoxInfoResponseMessage: f1 EventId, f2 Items repeated
  LootBoxInfo {f1 LootBoxId int, f2 LootBoxConfigId string,
  f3 Items repeated LootBoxItem}, f3 Successful, f4 ErrorMessage.
- LootBoxItem: f1 Type (0 Empty, 1 MoneyReward, 2 ItemReward,
  3 PetReward, 4 VehicleReward), f2 Id, f3 Quantity, f4 Money
  (nested; f1=koin, f2=gems — terbukti dari delta wallet),
  f5 Unique, f6 Presentation, f7 Pet (repeated PetData),
  f8 Quality (0 Trash, 1 Common, 2 Uncommon, 3 Rare, 4 Epic,
  5 Legendary).
- GetLootBoxProbabilityInfo: kirim "lb" payload
  08 01 10 <LootBoxId varint> 1A <len> <configId>.
  Respons LootBoxProbabilityInfoResponseMessage: f1 EventId,
  f2 Items repeated LootBoxItemProbabilityInfo {f1 LootBoxItem,
  f2 Probability float}, f3 Successful, f4 ErrorMessage.
  LootBoxId numerik didapat dari respons GetLootBoxInfo.
- Mekanik ini terdaftar di ToxicBirdClient (bukan room): TIDAK perlu
  destinationUserId, TIDAK ada syarat room/activity, Bartes tidak
  perlu online. TIDAK kirim JoinRoom.
- Probe ini TIDAK memakai jatah spin (hanya membaca daftar hadiah
  + probabilitas, tidak membuka peti).

Alur: login -> alokasi sendiri -> TCP login -> tunggu jendela login
(tbugv, maks 3 dtk) -> SendDeviceId ('Analytics/deviceId') ->
GetLootBoxInfo -> per config GetLootBoxProbabilityInfo ->
gabungkan probabilitas ke tiap hadiah -> simpan ke Supabase panel
(tabel spin_daily_prizes, upsert per config_id) -> kirim "fw"
GetRewards (payload kosong, sama seperti fortune_wheel_diag.py) dan
simpan daftar config roda AKTIF yang dikembalikan server sebagai
baris khusus config_id='_active_wheel' di tabel yang sama
(prizes = {"active_configs": [...]}). Daftar aktif inilah yang
ditampilkan halaman Spin Daily secara default (server-driven —
event berganti, daftar ikut berganti; JANGAN hardcode config roda
di halaman).
Tiap request ditunggu responsnya maks 10 dtk, maks 1 kirim ulang
bila tidak ada respons.
"""
import argparse
import asyncio
import json
import os
import struct
import time
from datetime import datetime, timezone

import lookup_uuid as L
import gift_diag as G

BOT_USER_ID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"

LOOTBOX_TYPE_NAMES = {0: "Empty", 1: "MoneyReward", 2: "ItemReward",
                      3: "PetReward", 4: "VehicleReward"}
LOOTBOX_QUALITY_NAMES = {0: "Trash", 1: "Common", 2: "Uncommon",
                         3: "Rare", 4: "Epic", 5: "Legendary"}


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
    boleh jadi str."""
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
    REKURSIF (maks 3 level). Bila tidak bisa diparse, kembalikan hex.
    String/int diteruskan apa adanya."""
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
            "quality": d.get(8),
            "quality_name": LOOTBOX_QUALITY_NAMES.get(d.get(8)),
            "probability": None}
    pets = [parse_pet_data(p) for p in _as_list(d, 7)]
    if pets:
        item["pets"] = pets
    return _jsonable(item)


def parse_lootbox_info_response(data):
    d = parse_protobuf(data)
    configs = []
    for raw in _as_list(d, 2):
        cd = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
        configs.append({
            "lootbox_id": cd.get(1),
            "config_id": cd.get(2),
            "prizes": [parse_lootbox_item(r) for r in _as_list(cd, 3)],
        })
    return {"event_id": d.get(1) or 0, "successful": bool(d.get(2)),
            "configs": configs,
            "error_message": d.get(4)}


def parse_probability_response(data):
    d = parse_protobuf(data)
    items = []
    for raw in _as_list(d, 2):
        pd = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
        item_raw = pd.get(1)
        item = (parse_lootbox_item(item_raw)
                if item_raw is not None else None)
        prob = pd.get(2)
        items.append({"item": item, "probability": prob})
    return {"event_id": d.get(1) or 0, "successful": bool(d.get(2)),
            "items": items,
            "error_message": d.get(4)}


def _varint(n):
    out = bytearray()
    n = int(n)
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def build_probability_payload(lootbox_id, config_id):
    cfg = config_id.encode("utf-8")
    return (b"\x08\x01" + b"\x10" + _varint(lootbox_id)
            + b"\x1a" + _varint(len(cfg)) + cfg)


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


async def save_config(session, config_id, lootbox_id, prizes):
    """Upsert satu config ke panel spin_daily_prizes via PostgREST.
    Kembalikan True bila HTTP 200/201. Tanpa env -> False (dicatat,
    bukan fatal). Key TIDAK pernah di-print."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] SUPABASE_URL / SUPABASE_SERVICE_KEY tidak diset — "
              "config tidak disimpan.")
        return False
    endpoint = (f"{SUPABASE_URL}/rest/v1/spin_daily_prizes"
                "?on_conflict=config_id")
    headers = {"apikey": SUPABASE_KEY,
               "Authorization": f"Bearer {SUPABASE_KEY}",
               "Content-Type": "application/json",
               "Prefer": "resolution=merge-duplicates"}
    payload = {"config_id": config_id, "lootbox_id": lootbox_id,
               "prizes": prizes,
               "fetched_at": datetime.now(timezone.utc).isoformat()}
    try:
        r = await session.post(endpoint, json=payload, headers=headers,
                               timeout=15)
        print(f"[+] upsert {config_id}: HTTP {r.status_code}")
        return r.status_code in (200, 201)
    except Exception as exc:
        print(f"[!] upsert {config_id} gagal: {type(exc).__name__}: {exc}")
        return False


async def save_active_wheel(session, active_configs):
    """Upsert baris khusus config_id='_active_wheel' ke panel
    spin_daily_prizes: prizes = {"active_configs": [...]}.
    Kembalikan True bila HTTP 200/201."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[!] SUPABASE_URL / SUPABASE_SERVICE_KEY tidak diset — "
              "daftar roda aktif tidak disimpan.")
        return False
    endpoint = (f"{SUPABASE_URL}/rest/v1/spin_daily_prizes"
                "?on_conflict=config_id")
    headers = {"apikey": SUPABASE_KEY,
               "Authorization": f"Bearer {SUPABASE_KEY}",
               "Content-Type": "application/json",
               "Prefer": "resolution=merge-duplicates"}
    payload = {"config_id": "_active_wheel", "lootbox_id": None,
               "prizes": {"active_configs": list(active_configs)},
               "fetched_at": datetime.now(timezone.utc).isoformat()}
    try:
        r = await session.post(endpoint, json=payload, headers=headers,
                               timeout=15)
        print(f"[+] upsert _active_wheel ({len(active_configs)} config aktif): "
              f"HTTP {r.status_code}")
        return r.status_code in (200, 201)
    except Exception as exc:
        print(f"[!] upsert _active_wheel gagal: {type(exc).__name__}: {exc}")
        return False


def parse_fw_getrewards(data):
    """Respons 'fw' GetRewards: f1 EventId, f2 Successful,
    f3 LootBoxConfigIds (repeated string)."""
    d = parse_protobuf(data)
    return {"event_id": d.get(1) or 0, "successful": bool(d.get(2)),
            "lootbox_config_ids": _as_list(d, 3)}


async def lootbox_info_once(session, email, password, device_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "server": "", "room": None, "player_id": None,
             "login_ok": False, "deviceid_sent": False,
             "configs": [], "saved": {},
             "active_wheel": [], "active_wheel_saved": False,
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
            print("[+] SendDeviceId terkirim ('Analytics/deviceId').")
        except Exception as exc:
            print(f"[!] kirim SendDeviceId gagal: {exc}")

        def wait_lb(timeout=10):
            """Baca frame sampai dapat RoomEvent 'lb' atau timeout.
            Kembalikan data protobuf (bytes) atau None."""
            end = time.time() + timeout
            while time.time() < end:
                for f in fr.drain(0.5):
                    name, d = G.decode_room_event(f)
                    if name == "lb":
                        return d
            return None

        def lb_request(label, payload, parse_fn, timeout=10):
            """Kirim event 'lb' + tunggu respons; maks 1 kirim ulang."""
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint("lb", payload))
                except Exception as exc:
                    print(f"[!] kirim {label} gagal: {exc}")
                    return None
                print(f"[*] kirim {label} ({payload.hex() if payload else '(kosong)'}), "
                      f"percobaan {attempt}/2, tunggu respons maks {timeout} dtk...")
                data = wait_lb(timeout)
                if data is not None:
                    # Cetak hex mentah SEBELUM parse — bila parse gagal,
                    # respons tidak hilang (bisa di-decode offline).
                    print(f"[+] respons 'lb' diterima untuk {label} "
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
                print(f"[!] tidak ada respons 'lb' untuk {label} "
                      f"(percobaan {attempt}/2).")
            return None

        # 1. GetLootBoxInfo — daftar lengkap semua config + hadiahnya.
        info = lb_request("GetLootBoxInfo", b"",
                          parse_lootbox_info_response)
        if info is None:
            hasil["error"] = "GetLootBoxInfo: TIDAK ADA respons"
            print("[!] GetLootBoxInfo: TIDAK ADA respons.")
            return hasil
        if isinstance(info, dict) and "parse_error" in info:
            hasil["error"] = f"GetLootBoxInfo parse gagal: {info['parse_error']}"
            print(f"[!] GetLootBoxInfo parse gagal: {info['parse_error']}")
            print(f"    raw_hex: {info.get('raw_hex')}")
            return hasil
        print(f"[+] GetLootBoxInfo: Successful={info.get('successful')} "
              f"configs={len(info.get('configs') or [])} "
              f"error={info.get('error_message')}")
        if not info.get("successful"):
            hasil["error"] = (f"GetLootBoxInfo Successful=false "
                              f"error={info.get('error_message')}")
            return hasil
        hasil["configs"] = info.get("configs") or []

        # 2. Per config: GetLootBoxProbabilityInfo, gabungkan probabilitas.
        for cfg in hasil["configs"]:
            config_id = cfg.get("config_id")
            lootbox_id = cfg.get("lootbox_id")
            prizes = cfg.get("prizes") or []
            print(f"[*] config {config_id} (LootBoxId={lootbox_id}): "
                  f"{len(prizes)} hadiah.")
            if lootbox_id is None or not config_id:
                print(f"[!] {config_id}: LootBoxId/config hilang — "
                      f"probabilitas dilewati.")
            else:
                payload = build_probability_payload(lootbox_id, config_id)
                prob = lb_request(
                    f"GetLootBoxProbabilityInfo {config_id}", payload,
                    parse_probability_response)
                if prob is None:
                    print(f"[!] probabilitas {config_id}: TIDAK ADA respons "
                          f"(probabilitas=null).")
                elif isinstance(prob, dict) and "parse_error" in prob:
                    print(f"[!] probabilitas {config_id} parse gagal: "
                          f"{prob['parse_error']} (probabilitas=null).")
                elif not prob.get("successful"):
                    print(f"[!] probabilitas {config_id}: Successful=false "
                          f"error={prob.get('error_message')} "
                          f"(probabilitas=null).")
                else:
                    # Cocokkan by (Type, Id, Quantity).
                    pmap = {}
                    for entry in prob.get("items") or []:
                        it = entry.get("item")
                        if it is None:
                            continue
                        key = (it.get("type"), it.get("id"),
                               it.get("quantity"))
                        pmap[key] = entry.get("probability")
                    matched = 0
                    for p in prizes:
                        key = (p.get("type"), p.get("id"),
                               p.get("quantity"))
                        if key in pmap:
                            p["probability"] = pmap[key]
                            matched += 1
                    print(f"[+] probabilitas {config_id}: "
                          f"{len(prob.get('items') or [])} entri, "
                          f"{matched}/{len(prizes)} hadiah cocok.")
            # 3. Simpan config ini ke Supabase panel.
            ok = await save_config(session, config_id, lootbox_id, prizes)
            hasil["saved"][config_id] = "ok" if ok else "gagal"

        # 4. Roda aktif: kirim "fw" GetInfo DULU lalu GetRewards (payload
        #    kosong) dan simpan config yang dikembalikan server. Urutan
        #    ini penting: client game selalu GetInfo dulu sebelum
        #    GetRewards (FortuneWheelService), dan probe tanpa GetInfo
        #    dulu mengembalikan Successful=true dengan config KOSONG
        #    (run 43574970061, respons 2 byte "10 01"). Bila GetRewards
        #    tetap kosong, coba sekali lagi setelah 1 dtk. Bila langkah
        #    ini gagal, daftar hadiah di atas tetap tersimpan; hanya
        #    daftar aktif yang tidak diperbarui (bukan fatal).
        def wait_fw(timeout=10):
            end = time.time() + timeout
            while time.time() < end:
                for f in fr.drain(0.5):
                    name, d = G.decode_room_event(f)
                    if name == "fw":
                        return d
            return None

        def fw_send(label, payload, timeout=10):
            """Kirim satu event 'fw', kembalikan data respons atau None."""
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint("fw", payload))
                except Exception as exc:
                    print(f"[!] kirim {label} gagal: {exc}")
                    return None
                print(f"[*] kirim {label} "
                      f"({payload.hex() if payload else '(kosong)'}), "
                      f"percobaan {attempt}/2, tunggu respons maks "
                      f"{timeout} dtk...")
                data = wait_fw(timeout)
                if data is not None:
                    print(f"[+] respons 'fw' diterima untuk {label} "
                          f"({len(data)} B): {data.hex()}")
                    return data
                print(f"[!] tidak ada respons 'fw' untuk {label} "
                      f"(percobaan {attempt}/2).")
            return None

        info_fw = fw_send("fw GetInfo", bytes.fromhex("0802"))
        if info_fw is not None:
            try:
                d = parse_protobuf(info_fw)
                print(f"[+] fw GetInfo: Successful={bool(d.get(2))} "
                      f"FreeCollected={bool(d.get(3))} "
                      f"AdsWatchedCount={d.get(4) or 0}")
            except Exception as exc:
                print(f"[!] parse fw GetInfo gagal: {exc!r} (lanjut)")
        fw_data = fw_send("fw GetRewards", b"")
        if fw_data is not None:
            try:
                fw = parse_fw_getrewards(fw_data)
                if (fw.get("successful") and fw.get("lootbox_config_ids")):
                    pass  # ok, lanjut simpan di bawah
                else:
                    print("[*] fw GetRewards kosong — coba sekali lagi "
                          "setelah 1 dtk (server mungkin belum siap).")
                    time.sleep(1)
                    fw_data = fw_send("fw GetRewards (ulang)", b"")
                    fw = (parse_fw_getrewards(fw_data)
                          if fw_data is not None else None)
            except Exception as exc:
                print(f"[!] parse fw GetRewards gagal: {exc!r} "
                      f"(raw hex di atas)")
                import traceback
                traceback.print_exc()
                fw = None
        else:
            fw = None
        if fw is None:
            print("[!] fw GetRewards: TIDAK ADA respons — daftar roda "
                  "aktif tidak diperbarui.")
        else:
            print(f"[+] fw GetRewards: Successful={fw.get('successful')} "
                  f"config={fw.get('lootbox_config_ids')}")
            if fw.get("successful") and fw.get("lootbox_config_ids"):
                hasil["active_wheel"] = list(fw["lootbox_config_ids"])
                hasil["active_wheel_saved"] = await save_active_wheel(
                    session, hasil["active_wheel"])
            else:
                print("[!] fw GetRewards Successful=false atau config "
                      "kosong — daftar aktif tidak disimpan.")
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("LB_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("LB_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("LB_DEVICE_ID", ""))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await lootbox_info_once(
            session, args.email, args.password, args.device_id)
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL LOOTBOX INFO ==========")
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"server: {hasil.get('server')}  player_id: {hasil.get('player_id')}")
    for cfg in hasil.get("configs") or []:
        print(f"Config: {cfg.get('config_id')} "
              f"(LootBoxId={cfg.get('lootbox_id')}) "
              f"hadiah={len(cfg.get('prizes') or [])} "
              f"saved={hasil.get('saved', {}).get(cfg.get('config_id'))}")
        for p in (cfg.get("prizes") or [])[:5]:
            s = (f"  - {p.get('type_name')} id={p.get('id')} "
                 f"qty={p.get('quantity')} quality={p.get('quality_name')} "
                 f"prob={p.get('probability')}")
            if p.get("money") is not None:
                s += f" money={p.get('money')}"
            for pet in p.get("pets") or []:
                s += (f" pet={pet.get('display_name')} "
                      f"(PetId={pet.get('pet_id')}, "
                      f"Stage={pet.get('stage')})")
            print(s)
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print(f"Roda aktif (fw GetRewards): {hasil.get('active_wheel')} "
          f"saved={hasil.get('active_wheel_saved')}")
    print("========================================")
    out = {
        "login_ok": hasil.get("login_ok"),
        "server": hasil.get("server"),
        "player_id": hasil.get("player_id"),
        "active_wheel": hasil.get("active_wheel") or [],
        "active_wheel_saved": hasil.get("active_wheel_saved"),
        "configs": [
            {"config_id": c.get("config_id"),
             "lootbox_id": c.get("lootbox_id"),
             "prize_count": len(c.get("prizes") or []),
             "prizes": c.get("prizes") or []}
            for c in hasil.get("configs") or []
        ],
        "saved": hasil.get("saved") or {},
    }
    print("LOOTBOX_INFO_JSON:" + json.dumps(out, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
