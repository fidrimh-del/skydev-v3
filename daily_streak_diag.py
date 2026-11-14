# -*- coding: utf-8 -*-
"""
daily_streak_diag.py — Probe Daily Streak (klaim harian) dengan
1 bot tes (pkxdshop.id@gmail.com).

Latar (2026-11-14, analisis dump — ~/workspace/daily_streak_analysis.md):
- Sistem Daily Streak, mekanik server ToxicBirdDailyStreakMechanic,
  RoomEvent + protobuf, reliable=1 — pola identik dengan probe "fw"
  (Fortune Wheel) yang sudah terbukti.
- Command key TERBUKTI via .rela.dyn:
    STRKS = cek status, STRKR = klaim hadiah,
    STRKF = freeze/unfreeze, STRKFR = force reset (JANGAN dipakai).
- SEMUA request payload KOSONG (0 byte): tidak ada nomor hari /
  streak count / ID hadiah yang dikirim — server menentukan hadiah
  dari akun. (DailyStreakStatusRequest + DailyStreakCollectRewardRequest
  berisi TimeZone=0, RewardId="", RewardCount=0 — semuanya default
  proto3, jadi tidak ada byte yang ditulis.)
- Respons (protobuf, TIDAK ADA field Successful — sukses dideteksi dari
  isi respons):
    DailyStreakStatusResponse:
      f1 CurrentStreak (int), f2 StreakInformations (repeated),
      f3 CollectedReward (bool), f4 LostStreak (bool),
      f5 CanFreezeStreak (bool), f6 FreezeDaysCount (int).
      StreakInformation per periode: f1 StreakPeriod, f2 StreakOffset,
      f3 StreakRangeStart, f4 LootBoxIds (repeated int — bisa packed),
      f5 PetEggId (repeated int — bisa packed).
    DailyStreakCollectRewardResponse:
      f1 CurrentStreak (int), f2 LootBoxRewards (repeated
      StreakLootBoxReward: f1 LootBoxId int, f2 LootBoxItems repeated
      LootBoxItem), f3 PetEggRewards (repeated PetData),
      f4 Wallet (Money: f1 coins, f2 gems — konvensi sama seperti "fw").
      LootBoxItem: f1 Type (0 Empty, 1 MoneyReward, 2 ItemReward,
      3 PetReward, 4 VehicleReward), f2 Id, f3 Quantity, f4 Money,
      f5 Unique, f6 Presentation, f7 Pet (repeated PetData), f8 Quality.
- Hadiah SERVER-SIDE: respons klaim membawa Wallet baru +
  LootBoxRewards + PetEggRewards. Client tidak punya tabel hadiah
  streak lokal; jadwal hadiah datang dari server lewat
  StreakInformations (LootBoxIds/PetEggId) — dicocokkan dengan data
  "lb" di tabel spin_daily_prizes (bila SUPABASE env diset).
- Freeze harga 500 KOINS/hari — TIDAK diuji probe ini (STRKF tidak
  dikirim). STRKFR juga TIDAK dikirim (force reset, konsekuensi
  tidak terbukti).
- Flag remote config `is_daily_streak_active_v2` bisa mematikan fitur
  server-side — kandidat penyebab bila probe tidak mendapat respons
  sama sekali.
Alur probe: login -> alokasi sendiri (tanpa destinationUserId) ->
TCP login -> tunggu jendela login (tbugv, maks 3 dtk) -> SendDeviceId
('Analytics/deviceId') -> STRKS (status awal) -> bila
CollectedReward=false: STRKR SEKALI -> STRKS lagi (verifikasi
CollectedReward false -> true). STRKR kedua di hari yang sama TIDAK
dikirim di probe ini; STRKF/STRKFR TIDAK dikirim.
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
    ID). Pesan nested berisi byte kontrol TIDAK boleh jadi str."""
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
    Wire-2 yang bukan teks printable TETAP bytes (pesan nested /
    packed repeated)."""
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
            "quality": d.get(8)}
    pets = [parse_pet_data(p) for p in _as_list(d, 7)]
    if pets:
        item["pets"] = pets
    return _jsonable(item)


def _decode_packed_ints(chunk):
    """Chunk bytes -> list int bila SELURUH chunk ter-decode sebagai
    varint berurutan (packed repeated int32). Kembalikan None bila
    bukan packed ints (bukan konsumsi penuh)."""
    vals = []
    i = 0
    n = len(chunk)
    while i < n:
        v, m = L.read_varint(chunk, i)
        if v is None:
            return None
        vals.append(v)
        i += m
    return vals if i == n else None


def _unpack_ints(v):
    """Field repeated int (LootBoxIds / PetEggId): bisa datang sebagai
    satu int, list int (unpacked), atau satu chunk bytes packed.
    Kembalikan list int (kosong bila tidak ada).
    HATI-HATI: chunk packed yang kebetulan semua byte-nya printable
    (mis. varint 101+102 = 0x65 0x66 = "ef") SUDAH di-decode jadi str
    oleh _try_str di parse_protobuf — encode balik ke bytes (UTF-8
    decode/encode adalah bijeksi, jadi byte asli pulih persis)."""
    if v is None:
        return []
    if isinstance(v, str):
        v = v.encode("utf-8")
    if isinstance(v, int):
        return [v]
    if isinstance(v, (bytes, bytearray)):
        packed = _decode_packed_ints(bytes(v))
        return packed if packed is not None else []
    if isinstance(v, list):
        out = []
        for x in v:
            if isinstance(x, int):
                out.append(x)
            elif isinstance(x, (bytes, bytearray)):
                packed = _decode_packed_ints(bytes(x))
                if packed is not None:
                    out.extend(packed)
        return out
    return []


def parse_streak_info(raw):
    """Satu StreakInformation: f1 StreakPeriod, f2 StreakOffset,
    f3 StreakRangeStart, f4 LootBoxIds, f5 PetEggId."""
    if isinstance(raw, str):
        return {"raw": raw}
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    return {"streak_period": d.get(1), "streak_offset": d.get(2),
            "streak_range_start": d.get(3),
            "lootbox_ids": _unpack_ints(d.get(4)),
            "pet_egg_ids": _unpack_ints(d.get(5))}


def parse_streak_lootbox_reward(raw):
    """Satu StreakLootBoxReward: f1 LootBoxId, f2 LootBoxItems."""
    if isinstance(raw, str):
        return {"raw": raw}
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    return {"lootbox_id": d.get(1),
            "items": [parse_lootbox_item(r) for r in _as_list(d, 2)]}


def parse_status(data):
    d = parse_protobuf(data)
    return {"current_streak": d.get(1) or 0,
            "streak_informations": [parse_streak_info(x)
                                    for x in _as_list(d, 2)],
            "collected_reward": bool(d.get(3)),
            "lost_streak": bool(d.get(4)),
            "can_freeze_streak": bool(d.get(5)),
            "freeze_days_count": d.get(6) or 0}


def parse_claim(data):
    d = parse_protobuf(data)
    wallet = d.get(4)
    return {"current_streak": d.get(1) or 0,
            "lootbox_rewards": [parse_streak_lootbox_reward(x)
                                for x in _as_list(d, 2)],
            "pet_egg_rewards": [parse_pet_data(x) for x in _as_list(d, 3)],
            "wallet": _jsonable(_nested(wallet))
                      if wallet is not None else None}


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


async def fetch_lootbox_by_ids(session, lootbox_ids):
    """GET spin_daily_prizes untuk LootBoxId tertentu ->
    {lootbox_id: {"config_id": ..., "prizes": [...]}}.
    Dipakai untuk memberi nama/isi hadiah streak (config asal +
    isi loot box). Tanpa env / tanpa id -> {}."""
    if not SUPABASE_URL or not SUPABASE_KEY or not lootbox_ids:
        return {}
    headers = {"apikey": SUPABASE_KEY,
               "Authorization": f"Bearer {SUPABASE_KEY}"}
    ids = ",".join(str(int(x)) for x in sorted(set(lootbox_ids)))
    endpoint = (f"{SUPABASE_URL}/rest/v1/spin_daily_prizes"
                f"?lootbox_id=in.({ids})&select=config_id,lootbox_id,prizes")
    out = {}
    try:
        r = await session.get(endpoint, headers=headers, timeout=15)
        if r.status_code == 200:
            for row in r.json() or []:
                out[row.get("lootbox_id")] = {
                    "config_id": row.get("config_id"),
                    "prizes": row.get("prizes") or []}
        else:
            print(f"[!] baca spin_daily_prizes (lootbox_id) HTTP "
                  f"{r.status_code}")
    except Exception as exc:
        print(f"[!] baca spin_daily_prizes gagal: "
              f"{type(exc).__name__}: {exc}")
    return out


async def daily_streak_once(session, email, password, device_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "server": "", "room": None, "player_id": None,
             "login_ok": False, "deviceid_sent": False,
             "status_awal": None, "claim": None, "status_akhir": None,
             "claim_terkirim": False, "claim_sukses": False,
             "sudah_klaim_hari_ini": False,
             "lootbox_info": {}, "error": None}
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
        # SendDeviceId (pola spawn_test / probe "fw", TERBUKTI).
        send_dev = device_id or DEFAULT_DEVICE_ID
        try:
            sock.sendall(L.build_room_event_varint(
                "Analytics/deviceId", send_dev.encode("utf-8")))
            hasil["deviceid_sent"] = True
            print("[+] SendDeviceId terkirim ('Analytics/deviceId').")
        except Exception as exc:
            print(f"[!] kirim SendDeviceId gagal: {exc}")

        def wait_streak(names, timeout=10):
            """Baca frame sampai dapat RoomEvent yang namanya ada di
            `names`, atau timeout. Kembalikan (nama, data bytes) atau
            (None, None). Nama event lain yang terlihat selama tunggu
            dicatat (diagnosis bila respons tak kunjung datang)."""
            end = time.time() + timeout
            seen_other = []
            while time.time() < end:
                for f in fr.drain(0.5):
                    name, d = G.decode_room_event(f)
                    if name in names:
                        return name, d
                    if name:
                        seen_other.append(name)
            if seen_other:
                print(f"    (selama tunggu {sorted(names)} terlihat event "
                      f"lain: {seen_other[:10]})")
            return None, None

        def streak_request(label, event_name, payload, parse_fn,
                           timeout=10):
            """Kirim RoomEvent streak + tunggu respons; maks 1 kirim
            ulang. Kembalikan hasil parse, atau None bila tidak ada
            respons. Raw hex selalu dicetak SEBELUM parse — bila parse
            gagal, respons tidak hilang (bisa di-decode offline)."""
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint(event_name,
                                                           payload))
                except Exception as exc:
                    print(f"[!] kirim {label} gagal: {exc}")
                    return None
                print(f"[*] kirim {label} ('{event_name}' "
                      f"{payload.hex() if payload else '(kosong)'}), "
                      f"percobaan {attempt}/2, tunggu respons maks "
                      f"{timeout} dtk...")
                name, data = wait_streak({event_name}, timeout)
                if data is not None:
                    print(f"[+] respons '{name}' diterima untuk {label} "
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
                print(f"[!] tidak ada respons '{event_name}' untuk {label} "
                      f"(percobaan {attempt}/2).")
            return None

        # 1. STRKS — status awal.
        hasil["status_awal"] = streak_request(
            "Status (STRKS)", "STRKS", b"", parse_status)
        st = hasil["status_awal"]
        if st is None:
            print("[!] STRKS: TIDAK ADA respons sama sekali — kemungkinan "
                  "fitur streak dimatikan server-side (remote config "
                  "is_daily_streak_active_v2) atau event tidak terdaftar. "
                  "Probe berhenti di sini (tidak klaim tanpa status).")
            hasil["error"] = "STRKS tanpa respons"
            return hasil
        if isinstance(st, dict) and "parse_error" in st:
            print(f"[!] STRKS: parse gagal: {st['parse_error']}")
            hasil["error"] = "STRKS parse gagal"
            return hasil
        print(f"[+] Status awal: CurrentStreak={st.get('current_streak')} "
              f"CollectedReward={st.get('collected_reward')} "
              f"LostStreak={st.get('lost_streak')} "
              f"CanFreezeStreak={st.get('can_freeze_streak')} "
              f"FreezeDaysCount={st.get('freeze_days_count')}")
        for info in st.get("streak_informations") or []:
            print(f"    periode={info.get('streak_period')} "
                  f"offset={info.get('streak_offset')} "
                  f"range_start={info.get('streak_range_start')} "
                  f"LootBoxIds={info.get('lootbox_ids')} "
                  f"PetEggId={info.get('pet_egg_ids')}")

        # Cocokkan LootBoxIds dari status dengan data "lb" (nama config
        # + isi hadiah) — best effort, tidak menghentikan probe.
        all_ids = []
        for info in st.get("streak_informations") or []:
            all_ids.extend(info.get("lootbox_ids") or [])
        if all_ids:
            hasil["lootbox_info"] = await fetch_lootbox_by_ids(
                session, all_ids)
            for lbid, info_lb in hasil["lootbox_info"].items():
                print(f"    LootBoxId {lbid} = config "
                      f"{info_lb.get('config_id')} "
                      f"({len(info_lb.get('prizes') or [])} hadiah di 'lb')")

        # 2. Bila sudah klaim hari ini: JANGAN klaim — laporkan status.
        if st.get("collected_reward"):
            hasil["sudah_klaim_hari_ini"] = True
            print("[*] CollectedReward=true — bot SUDAH klaim hari ini. "
                  "TIDAK mengirim STRKR (klaim harus menunggu reset "
                  "harian atau akun lain yang belum klaim).")
            return hasil

        # 3. STRKR — klaim SEKALI.
        hasil["claim"] = streak_request(
            "Klaim (STRKR)", "STRKR", b"", parse_claim)
        cl = hasil["claim"]
        if cl is None:
            print("[!] STRKR: TIDAK ADA respons — klaim tidak bisa "
                  "diverifikasi (server mungkin menolak diam-diam atau "
                  "fitur tidak aktif).")
            hasil["error"] = "STRKR tanpa respons"
            return hasil
        if isinstance(cl, dict) and "parse_error" in cl:
            print(f"[!] STRKR: parse gagal: {cl['parse_error']}")
            hasil["error"] = "STRKR parse gagal"
            return hasil
        hasil["claim_terkirim"] = True
        print(f"[+] Klaim: CurrentStreak={cl.get('current_streak')} "
              f"LootBoxRewards={len(cl.get('lootbox_rewards') or [])} "
              f"PetEggRewards={len(cl.get('pet_egg_rewards') or [])} "
              f"Wallet={cl.get('wallet')}")
        for rw in cl.get("lootbox_rewards") or []:
            lbid = rw.get("lootbox_id")
            cfg = (hasil["lootbox_info"].get(lbid) or {}).get("config_id")
            print(f"    LootBoxReward LootBoxId={lbid} "
                  f"(config {cfg}) berisi:")
            for it in rw.get("items") or []:
                s = (f"      {it.get('type_name')} id={it.get('id')} "
                     f"qty={it.get('quantity')}")
                if it.get("money") is not None:
                    s += f" money={it.get('money')}"
                for p in it.get("pets") or []:
                    s += (f" pet={p.get('display_name')} "
                          f"(PetId={p.get('pet_id')}, "
                          f"Stage={p.get('stage')})")
                print(s)
        for p in cl.get("pet_egg_rewards") or []:
            print(f"    PetEggReward pet={p.get('display_name')} "
                  f"(PetId={p.get('pet_id')}, Stage={p.get('stage')})")

        # 4. STRKS lagi — verifikasi CollectedReward false -> true.
        hasil["status_akhir"] = streak_request(
            "Status akhir (STRKS)", "STRKS", b"", parse_status)
        st2 = hasil["status_akhir"]
        if isinstance(st2, dict) and st2 is not None \
                and "parse_error" not in st2:
            print(f"[+] Status akhir: CurrentStreak={st2.get('current_streak')} "
                  f"CollectedReward={st2.get('collected_reward')} "
                  f"LostStreak={st2.get('lost_streak')}")
            # Deteksi sukses klaim dari isi (tidak ada flag Successful):
            # hadiah non-empty ATAU CurrentStreak naik ATAU re-STRKS
            # CollectedReward=true. (Baseline wallet tidak tersedia:
            # status tidak membawa Wallet, jadi "Wallet berubah" tidak
            # bisa dipakai di sini.)
            rewards_nonempty = bool(cl.get("lootbox_rewards")) or \
                bool(cl.get("pet_egg_rewards"))
            streak_naik = (cl.get("current_streak") or 0) > \
                (st.get("current_streak") or 0)
            collected_now = bool(st2.get("collected_reward"))
            hasil["claim_sukses"] = bool(rewards_nonempty or streak_naik
                                         or collected_now)
            print(f"[*] Deteksi sukses klaim: hadiah_nonempty="
                  f"{rewards_nonempty} streak_naik={streak_naik} "
                  f"collected_sekarang={collected_now} -> "
                  f"claim_sukses={hasil['claim_sukses']}")
        elif st2 is None:
            print("[!] STRKS akhir: TIDAK ADA respons — klaim tidak bisa "
                  "diverifikasi lewat status ulang (hadiah di respons "
                  "klaim tetap dicatat di atas).")
            hasil["claim_sukses"] = bool(cl.get("lootbox_rewards")) or \
                bool(cl.get("pet_egg_rewards"))
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("STREAK_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("STREAK_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("STREAK_DEVICE_ID", ""))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await daily_streak_once(
            session, args.email, args.password, args.device_id)
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL DAILY STREAK ==========")
    st = hasil.get("status_awal") or {}
    cl = hasil.get("claim") or {}
    st2 = hasil.get("status_akhir") or {}
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"player_id: {hasil.get('player_id')}")
    print(f"Status awal: CurrentStreak={st.get('current_streak')} "
          f"CollectedReward={st.get('collected_reward')} "
          f"LostStreak={st.get('lost_streak')} "
          f"CanFreezeStreak={st.get('can_freeze_streak')} "
          f"FreezeDaysCount={st.get('freeze_days_count')}")
    for info in st.get("streak_informations") or []:
        lbid = info.get("lootbox_ids") or []
        cfg = [ (hasil.get("lootbox_info") or {}).get(i, {}).get("config_id")
                for i in lbid ]
        print(f"  periode={info.get('streak_period')} "
              f"offset={info.get('streak_offset')} "
              f"range_start={info.get('streak_range_start')} "
              f"LootBoxIds={lbid} config={cfg} "
              f"PetEggId={info.get('pet_egg_ids')}")
    if hasil.get("sudah_klaim_hari_ini"):
        print("SUDAH KLAIM HARI INI — STRKR tidak dikirim. "
              "Klaim berikutnya setelah reset harian server "
              "(jam reset belum diketahui).")
    elif hasil.get("claim_terkirim"):
        print(f"Klaim STRKR: CurrentStreak={cl.get('current_streak')} "
              f"Wallet={cl.get('wallet')}")
        for rw in cl.get("lootbox_rewards") or []:
            lbid = rw.get("lootbox_id")
            cfg = (hasil.get("lootbox_info") or {}).get(lbid, {}).get(
                "config_id")
            print(f"  LootBoxReward LootBoxId={lbid} (config {cfg}):")
            for it in rw.get("items") or []:
                s = (f"    {it.get('type_name')} id={it.get('id')} "
                     f"qty={it.get('quantity')}")
                if it.get("money") is not None:
                    s += f" money={it.get('money')}"
                for p in it.get("pets") or []:
                    s += (f" pet={p.get('display_name')} "
                          f"(PetId={p.get('pet_id')}, "
                          f"Stage={p.get('stage')})")
                print(s)
        for p in cl.get("pet_egg_rewards") or []:
            print(f"  PetEggReward pet={p.get('display_name')} "
                  f"(PetId={p.get('pet_id')}, Stage={p.get('stage')})")
        print(f"Status akhir: CollectedReward={st2.get('collected_reward')} "
              f"CurrentStreak={st2.get('current_streak')}")
        print(f"VERDIK KLAIM: "
              f"{'BERHASIL' if hasil.get('claim_sukses') else 'TIDAK TERBUKTI'}")
    else:
        print("Klaim TIDAK terkirim (lihat error di atas).")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("=========================================")
    print("STREAK_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0 if hasil.get("login_ok") else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
