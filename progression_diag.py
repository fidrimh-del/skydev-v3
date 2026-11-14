# -*- coding: utf-8 -*-
"""
progression_diag.py — Probe Progression (daftar KPI) dengan 1 bot tes
(pkxdshop.id@gmail.com). PROBE BACA SAJA — TIDAK klaim milestone.

Latar (2026-11-14, analisis dump):
- Sistem Progression (KPI -> Milestone -> Reward) adalah jalur LEGIT
  pemberian role otomatis. Milestone reward BISA berupa Role
  (termasuk CreatorUgc=4). Bukan spoof 'tbulv' (yang sudah terbukti
  ditolak server).
- Mekanik server ToxicBirdProgressionMechanic, RoomEvent
  "progression", reliable=1. Interface IProgressionMechanic:
  GetInitializationInfo(version), AcknowledgeMilestone(kpiId,
  version), FetchKPIs(version), ValidateMissingRewards(version),
  AcknowledgeStackedLevelUp(version).
- GetInitializationInfo — payload TERVERIFIKASI via disassembly
  (ProtobufProgressionMechanic.GetInitializationInfo RVA 0x747B8B4):
  GetInitializationInfoRequest (TDI 15643) f1 EventId=0, f2 Version
  (string). EventId=0 adalah default proto3 -> dihilangkan, jadi
  payload = `12 <len> <version>` (field 2, string). Bila version
  kosong, field 2 juga default -> payload KOSONG (0 byte), sama
  seperti yang dikirim client (ToByteArray menghilangkan default).
- Respons GetInitializationInfoResponse (TDI 15655):
    f1 EventId, f2 Kpis (repeated KPI), f3 PlayerLevel (LevelInfo),
    f4 LevelUpConfigs (repeated), f5 RemotePlayerLevelMap
    (map<string,int>), f6 IsSuccess (bool), f7 ErrorMessage.
- KPI (TDI 15635): f1 Id (string), f2 Milestones (repeated),
  f3 Type (KPIType: HOUSE=0, PETS=1, LIFESTYLE=2, UGC=3),
  f4 Value (int), f5 MilestoneLevel (LevelInfo), f6 DescKey,
  f7 CompleteDescKey.
- Milestone (TDI 15633): f1 Goal (int), f2 Reward.
- ProgressionReward (TDI 15631): f1 Money, f2 Items (repeated
  ItemReward), f3 Role (RoleReward).
- RoleReward (TDI 15629): f1 Type (PlayerRole enum 0-10;
  CreatorUgc=4).
- ItemReward (TDI 15627): f1 Item (ItemQuantity: f1 ItemId,
  f2 Quantity), f2 Background (ItemPresentation).
- LevelInfo (TDI 15637): f1 CurrentLevel, f2 EligibleLevel.
- LevelUpConfig (TDI 15639): f1 RequiredMilestones,
  f2 Rewards (repeated ProgressionReward).
- Money: f1 coins, f2 gems (konvensi sama seperti "fw", terbukti
  dari delta wallet).
- "Master Builder" TIDAK ADA di client (grep 0). Sistem bintang
  builder TIDAK ADA (hanya CCU). Quest builder TIDAK memberi role.
- PERTANYAAN YANG DIJAWAB PROBE: (a) ada KPI bertipe UGC (type=3)?
  (b) ada milestone berhadiah RoleReward Role=4 (CreatorUgc)?
  (c) daftar lengkap KPI (ID, type, value, tiap milestone: goal +
  reward).
- `progression_version` TIDAK ADA di dump (dari Remote Config key
  "progression_version"). Probe mencoba version "" dulu, lalu "1"
  bila server tidak menjawab / menolak. JANGAN klaim milestone
  (AcknowledgeMilestone) — probe ini HANYA GetInitializationInfo.
Alur probe: login -> alokasi sendiri (tanpa destinationUserId) ->
TCP login -> tunggu jendela login (tbugv, maks 3 dtk) -> SendDeviceId
('Analytics/deviceId') -> GetInitializationInfo (version "" lalu
"1") -> parse daftar KPI -> HASIL + PROGRESSION_JSON.
Tiap version ditunggu responsnya maks 10 dtk, maks 1 kirim ulang
bila tidak ada respons. Bila server menjawab IsSuccess=false
(ditolak), TIDAK diulang untuk version itu — langsung coba version
lain.
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

KPI_TYPE_NAMES = {0: "HOUSE", 1: "PETS", 2: "LIFESTYLE", 3: "UGC"}
ROLE_NAMES = {0: "User", 1: "Influencer", 2: "Admin",
              3: "MinorInfluencer", 4: "CreatorUgc",
              5: "EventTeam01", 6: "EventTeam02", 7: "EventTeam03",
              8: "HighScorePlayer", 9: "EventPass", 10: "Undefined"}


def _varint(n):
    """Encode int non-negatif sebagai varint (untuk panjang field)."""
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


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
    """Wire-2 chunk -> str HANYA bila isinya teks printable (nama/ID).
    Pesan nested berisi byte kontrol TIDAK boleh jadi str."""
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


def parse_money(raw):
    """Money: f1 coins, f2 gems (konvensi "fw"). Field lain (f3+)
    dicatat apa adanya."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    out = {}
    if d.get(1) is not None:
        out["coins"] = d.get(1)
    if d.get(2) is not None:
        out["gems"] = d.get(2)
    for k, v in d.items():
        if k not in (1, 2):
            out[f"f{k}"] = _jsonable(_nested(v))
    return out


def parse_item_reward(raw):
    """ItemReward: f1 Item (ItemQuantity: f1 ItemId, f2 Quantity),
    f2 Background (ItemPresentation)."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    item = d.get(1)
    iq = parse_protobuf(item) if isinstance(item, (bytes, bytearray)) \
        else (item or {})
    return {"item_id": iq.get(1), "quantity": iq.get(2),
            "background": _jsonable(_nested(d.get(2)))
            if d.get(2) is not None else None}


def parse_role_reward(raw):
    """RoleReward: f1 Type (PlayerRole enum)."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    role = d.get(1)
    return {"role": role,
            "role_name": ROLE_NAMES.get(role, f"Unknown({role})")}


def parse_progression_reward(raw):
    """ProgressionReward: f1 Money, f2 Items (repeated ItemReward),
    f3 Role (RoleReward). Tepat satu yang terisi (tiga constructor
    terpisah di C#), tapi semua yang ada dicatat."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    out = {}
    if d.get(1) is not None:
        out["money"] = parse_money(d.get(1))
    items = _as_list(d, 2)
    if items:
        out["items"] = [parse_item_reward(x) for x in items]
    if d.get(3) is not None:
        out["role"] = parse_role_reward(d.get(3))
    if "role" in out:
        out["kind"] = "Role"
    elif "items" in out:
        out["kind"] = "Item"
    elif "money" in out:
        out["kind"] = "Money"
    else:
        out["kind"] = "Unknown"
        out["raw"] = _jsonable(_nested(d))
    return out


def parse_milestone(raw):
    """Milestone: f1 Goal (int), f2 Reward (ProgressionReward)."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    rew = d.get(2)
    return {"goal": d.get(1),
            "reward": parse_progression_reward(rew)
            if rew is not None else None}


def parse_level_info(raw):
    """LevelInfo: f1 CurrentLevel, f2 EligibleLevel."""
    if raw is None:
        return None
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    return {"current_level": d.get(1), "eligible_level": d.get(2)}


def parse_kpi(raw):
    """KPI: f1 Id, f2 Milestones, f3 Type, f4 Value,
    f5 MilestoneLevel, f6 DescKey, f7 CompleteDescKey."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    ktype = d.get(3)
    return {"id": d.get(1),
            "type": ktype,
            "type_name": KPI_TYPE_NAMES.get(ktype, f"Unknown({ktype})"),
            "value": d.get(4),
            "milestone_level": parse_level_info(d.get(5)),
            "desc_key": d.get(6),
            "complete_desc_key": d.get(7),
            "milestones": [parse_milestone(m) for m in _as_list(d, 2)]}


def parse_level_up_config(raw):
    """LevelUpConfig: f1 RequiredMilestones,
    f2 Rewards (repeated ProgressionReward)."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    return {"required_milestones": d.get(1),
            "rewards": [parse_progression_reward(r)
                        for r in _as_list(d, 2)]}


def parse_init_info(data):
    """GetInitializationInfoResponse: f1 EventId, f2 Kpis, f3
    PlayerLevel (LevelInfo), f4 LevelUpConfigs, f5 RemotePlayerLevelMap
    (map<string,int> — tiap entri nested f1 key, f2 value),
    f6 IsSuccess, f7 ErrorMessage."""
    d = parse_protobuf(data)
    remote_map = {}
    for e in _as_list(d, 5):
        if isinstance(e, (bytes, bytearray)):
            ed = parse_protobuf(bytes(e))
            if ed.get(1) is not None:
                remote_map[ed.get(1)] = ed.get(2)
        elif isinstance(e, str):
            remote_map[e] = None
    return {"event_id": d.get(1) if d.get(1) is not None else 0,
            "player_level": parse_level_info(d.get(3)),
            "level_up_configs": [parse_level_up_config(c)
                                 for c in _as_list(d, 4)],
            "remote_player_level_map": remote_map,
            "is_success": bool(d.get(6)),
            "error_message": d.get(7),
            "kpis": [parse_kpi(k) for k in _as_list(d, 2)]}


def build_init_payload(version):
    """GetInitializationInfoRequest bytes.
    f1 EventId=0 (InitializationInfo) default proto3 -> dihilangkan.
    f2 Version (string) -> 12 <len> <version>.
    Bila version kosong, field 2 juga default -> payload KOSONG."""
    if not version:
        return b""
    vb = version.encode("utf-8")
    return b"\x12" + _varint(len(vb)) + vb


async def progression_once(session, email, password, device_id, versions):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "server": "", "room": None, "player_id": None,
             "login_ok": False, "deviceid_sent": False,
             "versions_tried": [], "version_ok": None,
             "init_info": None, "ugc_kpis": [],
             "role_reward_milestones": [],
             "has_ugc_kpi": False, "has_creator_ugc_reward": False,
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
        # SendDeviceId (pola spawn_test / probe "fw" / streak, TERBUKTI).
        send_dev = device_id or DEFAULT_DEVICE_ID
        try:
            sock.sendall(L.build_room_event_varint(
                "Analytics/deviceId", send_dev.encode("utf-8")))
            hasil["deviceid_sent"] = True
            print("[+] SendDeviceId terkirim ('Analytics/deviceId').")
        except Exception as exc:
            print(f"[!] kirim SendDeviceId gagal: {exc}")

        def wait_progression_init(timeout=10):
            """Baca frame sampai dapat RoomEvent 'progression' yang
            responsnya ber-EventId=0 (InitializationInfo), atau timeout.
            Kembalikan (data bytes, notes) atau (None, notes). Event
            'progression' lain (mis. MilestoneReached) dicatat dan
            dilewati — bukan respons yang dicari."""
            end = time.time() + timeout
            notes = []
            while time.time() < end:
                for f in fr.drain(0.5):
                    name, d = G.decode_room_event(f)
                    if name == "progression" and d is not None:
                        try:
                            dd = parse_protobuf(d)
                        except Exception:
                            dd = {}
                        if dd.get(1) in (None, 0):
                            return d, notes
                        notes.append(f"progression EventId={dd.get(1)} "
                                     f"({len(d)} B, bukan init)")
                    elif name:
                        notes.append(name)
            return None, notes

        def init_request(version, timeout=10):
            """Kirim GetInitializationInfo (version) + tunggu respons
            init. Maks 1 kirim ulang HANYA bila tidak ada respons.
            Bila server menjawab IsSuccess=false (ditolak), TIDAK
            diulang untuk version itu — kembalikan hasil parse agar
            pemanggil bisa coba version lain. Raw hex selalu dicetak
            SEBELUM parse — bila parse gagal, respons tidak hilang."""
            payload = build_init_payload(version)
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint("progression",
                                                           payload))
                except Exception as exc:
                    print(f"[!] kirim GetInitializationInfo gagal: {exc}")
                    return None
                print(f"[*] kirim GetInitializationInfo (version="
                      f"{version!r}, payload "
                      f"{payload.hex() if payload else '(kosong)'}), "
                      f"percobaan {attempt}/2, tunggu respons maks "
                      f"{timeout} dtk...")
                data, notes = wait_progression_init(timeout)
                if data is not None:
                    print(f"[+] respons 'progression' (EventId=0) "
                          f"diterima ({len(data)} B): {data.hex()}")
                    try:
                        return parse_init_info(data)
                    except Exception as exc:
                        print(f"[!] parse respons GetInitializationInfo "
                              f"gagal: {exc!r} (raw hex di atas)")
                        import traceback
                        traceback.print_exc()
                        return {"parse_error": repr(exc),
                                "raw_hex": data.hex()}
                print(f"[!] tidak ada respons 'progression' (EventId=0) "
                      f"untuk version={version!r} (percobaan {attempt}/2).")
                if notes:
                    print(f"    (selama tunggu terlihat: {notes[:10]})")
            return None

        # GetInitializationInfo — coba version satu per satu.
        best_info = None
        for version in versions:
            hasil["versions_tried"].append(version)
            info = init_request(version)
            if info is None:
                print(f"[!] version={version!r}: TIDAK ADA respons.")
                continue
            if isinstance(info, dict) and "parse_error" in info:
                print(f"[!] version={version!r}: parse gagal: "
                      f"{info['parse_error']}")
                hasil["error"] = (f"parse gagal (version={version!r})")
                hasil["init_info"] = info
                best_info = info
                break
            hasil["init_info"] = info
            best_info = info
            if info.get("is_success"):
                hasil["version_ok"] = version
                print(f"[+] version={version!r}: IsSuccess=true "
                      f"({len(info.get('kpis') or [])} KPI).")
                break
            # Server menjawab tapi IsSuccess=false -> ditolak; coba
            # version lain (jangan ulang version yang sama).
            print(f"[!] version={version!r}: server menjawab "
                  f"IsSuccess=false "
                  f"(ErrorMessage={info.get('error_message')!r}) — "
                  f"coba version lain.")
            hasil["error"] = (f"IsSuccess=false (version={version!r}): "
                              f"{info.get('error_message')}")
        if hasil["version_ok"] is None and not hasil.get("error"):
            hasil["error"] = ("GetInitializationInfo tanpa respons "
                              "(semua version dicoba)")

        # Analisis init_info (yang sukses, atau terakhir diterima).
        info = best_info or hasil.get("init_info")
        if info and isinstance(info, dict) and "parse_error" not in info:
            kpis = info.get("kpis") or []
            print(f"[+] Daftar KPI diterima: {len(kpis)} KPI, "
                  f"PlayerLevel={info.get('player_level')}, "
                  f"IsSuccess={info.get('is_success')}")
            for k in kpis:
                print(f"  KPI id={k.get('id')!r} type={k.get('type')} "
                      f"({k.get('type_name')}) value={k.get('value')} "
                      f"milestone_level={k.get('milestone_level')} "
                      f"desc={k.get('desc_key')!r} "
                      f"milestones={len(k.get('milestones') or [])}")
                for idx, m in enumerate(k.get("milestones") or []):
                    rw = m.get("reward") or {}
                    detail = ""
                    if rw.get("kind") == "Role":
                        r = rw.get("role") or {}
                        detail = (f"Role={r.get('role')} "
                                  f"({r.get('role_name')})")
                    elif rw.get("kind") == "Item":
                        detail = ", ".join(
                            f"ItemId={it.get('item_id')} "
                            f"x{it.get('quantity')}"
                            for it in rw.get("items") or [])
                    elif rw.get("kind") == "Money":
                        mo = rw.get("money") or {}
                        detail = (f"coins={mo.get('coins')} "
                                  f"gems={mo.get('gems')}")
                    print(f"    milestone[{idx}] goal={m.get('goal')} "
                          f"reward={rw.get('kind')} {detail}")
            # Jawaban tegas untuk 2 pertanyaan probe.
            ugc = [k for k in kpis if k.get("type") == 3]
            hasil["ugc_kpis"] = ugc
            hasil["has_ugc_kpi"] = bool(ugc)
            role_ms = []
            for k in kpis:
                for idx, m in enumerate(k.get("milestones") or []):
                    rw = m.get("reward") or {}
                    if rw.get("kind") == "Role":
                        r = rw.get("role") or {}
                        role_ms.append({
                            "kpi_id": k.get("id"),
                            "kpi_type": k.get("type"),
                            "kpi_type_name": k.get("type_name"),
                            "milestone_index": idx,
                            "goal": m.get("goal"),
                            "role": r.get("role"),
                            "role_name": r.get("role_name")})
            hasil["role_reward_milestones"] = role_ms
            hasil["has_creator_ugc_reward"] = any(
                r.get("role") == 4 for r in role_ms)
            print(f"[*] Ada KPI UGC (type=3)? "
                  f"{'YA' if hasil['has_ugc_kpi'] else 'TIDAK'} "
                  f"({len(ugc)} KPI)")
            print(f"[*] Ada milestone berhadiah CreatorUgc (Role=4)? "
                  f"{'YA' if hasil['has_creator_ugc_reward'] else 'TIDAK'} "
                  f"({len(role_ms)} milestone Role total)")
            for r in role_ms:
                print(f"    RoleReward: KPI {r.get('kpi_id')!r} "
                      f"({r.get('kpi_type_name')}) milestone[{r.get('milestone_index')}] "
                      f"goal={r.get('goal')} -> {r.get('role_name')} "
                      f"(Role={r.get('role')})")
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return hasil


DEFAULT_VERSIONS = ["", "1"]


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("PROG_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("PROG_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("PROG_DEVICE_ID", ""))
    ap.add_argument("--versions", default="",
                    help="Daftar version dipisah koma; kosong = default "
                         "['', '1'] (coba '' dulu, lalu '1')")
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    versions = args.versions.split(",") if args.versions \
        else DEFAULT_VERSIONS
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await progression_once(
            session, args.email, args.password, args.device_id, versions)
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL PROGRESSION ==========")
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"player_id: {hasil.get('player_id')}")
    print(f"Versions dicoba: {hasil.get('versions_tried')}  "
          f"version_ok: {hasil.get('version_ok')!r}")
    info = hasil.get("init_info") or {}
    if info and "parse_error" not in info:
        print(f"IsSuccess: {info.get('is_success')}  "
              f"ErrorMessage: {info.get('error_message')!r}")
        print(f"PlayerLevel: {info.get('player_level')}  "
              f"LevelUpConfigs: {len(info.get('level_up_configs') or [])}  "
              f"RemotePlayerLevelMap: "
              f"{len(info.get('remote_player_level_map') or {})} entri")
        kpis = info.get("kpis") or []
        print(f"Total KPI: {len(kpis)}")
        for k in kpis:
            print(f"  KPI {k.get('id')!r} [{k.get('type_name')}] "
                  f"value={k.get('value')}")
            for idx, m in enumerate(k.get("milestones") or []):
                rw = m.get("reward") or {}
                if rw.get("kind") == "Role":
                    r = rw.get("role") or {}
                    det = f"{r.get('role_name')} (Role={r.get('role')})"
                elif rw.get("kind") == "Item":
                    det = ", ".join(
                        f"ItemId={it.get('item_id')} x{it.get('quantity')}"
                        for it in rw.get("items") or [])
                elif rw.get("kind") == "Money":
                    mo = rw.get("money") or {}
                    det = f"{mo.get('coins')} koin, {mo.get('gems')} gems"
                else:
                    det = str(rw.get("raw") or rw.get("kind"))
                print(f"    milestone[{idx}] goal={m.get('goal')} -> "
                      f"{rw.get('kind')}: {det}")
        print(f"Ada KPI UGC (type=3)? "
              f"{'YA' if hasil.get('has_ugc_kpi') else 'TIDAK'}")
        print(f"Ada milestone berhadiah CreatorUgc (Role=4)? "
              f"{'YA' if hasil.get('has_creator_ugc_reward') else 'TIDAK'}")
        for r in hasil.get("role_reward_milestones") or []:
            print(f"  RoleReward: KPI {r.get('kpi_id')!r} "
                  f"({r.get('kpi_type_name')}) milestone"
                  f"[{r.get('milestone_index')}] goal={r.get('goal')} -> "
                  f"{r.get('role_name')} (Role={r.get('role')})")
    elif info:
        print(f"Parse error: {info.get('parse_error')}")
    else:
        print("Tidak ada respons GetInitializationInfo.")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("=======================================")
    print("PROGRESSION_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0 if hasil.get("login_ok") else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
