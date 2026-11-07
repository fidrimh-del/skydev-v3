"""Diag Join Clan — SATU bot, SEKALI jalan.

Alur (sesuai dump_31_ijrw.cs, ClanMechanic):
  login -> TCP login -> GetUserClan (cek clan saat ini)
  -> SearchClan (cari clan by nama) -> JoinClan (by clanId)
  -> baca JoinClanResponse -> HASIL.

Protokol (dari dump):
  RoomEvent command: "clan" (ClanMechanic.COMMAND_KEY).
  ClanRequestEventId: GetClans=0, RequestJoin=2, Kick=3, GetClanInfo=4,
    LeaveClan=5, CreateClan=6, GetClanLeaderboard=7, CancelRequestJoin=8,
    RequestRewards=9, DeleteClan=10, GetPlayerInvites=11,
    AcceptPlayerRequest=12, CancelAllRequests=13, GetUserClan=14,
    AdminInfo=15, EditClan=16, SeasonInfo=17,
    CollectPendingRewardRequest=18, SearchClan=19, RemoteClan=20.
  JoinClanRequest: f1 EventId (varint, 2=RequestJoin), f2 ClanId (string).
  SearchClanRequest: f1 EventId (varint, 19=SearchClan), f2 Name (string).
  GetUserClanRequest: f1 EventId (varint, 14=GetUserClan).
  SearchClanResponse: f1 EventId, f2 Success (bool), f3 Clans (repeated
    Clan), f4 ErrorMessage (string).
  Clan (protobuf): f1 ClanId (string), f2 NumberOfMembers (int),
    f3 Score (int), f4 Tags (repeated string), f5 Type (enum),
    f6 Name (string), f7 ClanBanner (int).
  ClanResponseEventId: GetClansResponse=0, JoinClanResponse=1, ...,
    GetUserClanResponse=11.

Catatan: JoinClan memakai clanId, BUKAN nama. Jadi alurnya cari dulu
(SearchClan by nama) baru join by ID. Join bisa butuh persetujuan
admin clan (RequestJoin = permintaan bergabung, bukan langsung masuk).

Kredensial TIDAK disimpan di file ini. Bot diambil dari input
workflow / env: CLAN_EMAIL, CLAN_PASSWORD, CLAN_DEVICE_ID.
Akun ini TIDAK ada di panel DB (akun Gmail mandiri).
"""
import argparse
import asyncio
import os
import socket
import struct
import sys
import time

import lookup_uuid as L

VERBOSE = True

CLAN_CMD = "clan"
EV_GET_USER_CLAN = 14
EV_SEARCH_CLAN = 19
EV_JOIN = 2


def vlog(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs, flush=True)


# ---------- protobuf Clan (bangun manual) ----------

def build_clan_simple(event_id):
    """Request hanya EventId (GetUserClan)."""
    return b"\x08" + L.varint(event_id)


def build_search_clan(name):
    """SearchClanRequest: f1 EventId=19, f2 Name (string)."""
    n = name.encode("utf-8")
    return (b"\x08" + L.varint(EV_SEARCH_CLAN)
            + b"\x12" + L.varint(len(n)) + n)


def build_join_clan(clan_id):
    """JoinClanRequest: f1 EventId=2, f2 ClanId (string)."""
    c = clan_id.encode("utf-8")
    return (b"\x08" + L.varint(EV_JOIN)
            + b"\x12" + L.varint(len(c)) + c)


def parse_protobuf_fields(data):
    """Parser protobuf generik (best-effort): kembalikan list
    (field_no, wire_type, value)."""
    out = []
    i = 0
    n = len(data)
    while i < n:
        tag, m = L.read_varint(data, i)
        if tag is None:
            break
        i += m
        field_no = tag >> 3
        wt = tag & 7
        try:
            if wt == 0:
                v, m2 = L.read_varint(data, i)
                if v is None:
                    break
                out.append((field_no, "varint", v))
                i += m2
            elif wt == 2:
                ln, m2 = L.read_varint(data, i)
                if ln is None or i + m2 + ln > n:
                    break
                raw = data[i + m2:i + m2 + ln]
                try:
                    v = raw.decode("utf-8")
                except Exception:
                    v = raw.hex()
                out.append((field_no, "string/bytes", v))
                i += m2 + ln
            elif wt == 5:
                if i + 4 > n:
                    break
                v = struct.unpack("<f", data[i:i + 4])[0]
                out.append((field_no, "float32", v))
                i += 4
            elif wt == 1:
                if i + 8 > n:
                    break
                v = struct.unpack("<d", data[i:i + 8])[0]
                out.append((field_no, "float64", v))
                i += 8
            else:
                break
        except Exception:
            break
    return out


def parse_clan_message(raw):
    """Parse satu pesan Clan (protobuf): kembalikan dict
    {clan_id, name, members, score}."""
    d = {}
    for fno, wt, v in parse_protobuf_fields(raw):
        if fno == 1 and wt == "string/bytes" and isinstance(v, str):
            d["clan_id"] = v
        elif fno == 2 and wt == "varint":
            d["members"] = v
        elif fno == 3 and wt == "varint":
            d["score"] = v
        elif fno == 6 and wt == "string/bytes" and isinstance(v, str):
            d["name"] = v
    return d


# ---------- TCP frame (pola lookup_uuid / like_profile_probe) ----------

def _is_tbugv(frame):
    return len(frame) >= 2 and frame[0] == 0x03 and frame[1] == 0x74


def _is_joinedroom(frame):
    return len(frame) >= 6 and frame[0] == 0x06 and frame[1] == 1


class FrameReader:
    """Sama seperti like_profile_probe.py: buffer persisten + auto-pong."""

    def __init__(self, sock):
        self.s = sock
        self.buf = bytearray()

    def read_until(self, pred, seconds):
        deadline = time.time() + seconds
        self.s.settimeout(0.5)
        while time.time() < deadline:
            while len(self.buf) >= 4:
                ln = struct.unpack(">I", self.buf[:4])[0]
                if ln > 262144:
                    return None, False
                if len(self.buf) - 4 < ln:
                    break
                fr = bytes(self.buf[4:4 + ln])
                del self.buf[:4 + ln]
                if fr == b"\x01":
                    try:
                        self.s.sendall(struct.pack(">I", 1) + b"\x02")
                    except Exception:
                        pass
                    continue
                if pred(fr):
                    return fr, False
            try:
                chunk = self.s.recv(65536)
            except socket.timeout:
                continue
            except Exception:
                return None, True
            if chunk == b"":
                return None, True
            self.buf += chunk
        return None, False

    def drain(self, seconds):
        """Kumpulkan SEMUA frame selama `seconds` detik (untuk log)."""
        out = []
        deadline = time.time() + seconds
        self.s.settimeout(0.5)
        while time.time() < deadline:
            while len(self.buf) >= 4:
                ln = struct.unpack(">I", self.buf[:4])[0]
                if ln > 262144:
                    return out
                if len(self.buf) - 4 < ln:
                    break
                fr = bytes(self.buf[4:4 + ln])
                del self.buf[:4 + ln]
                if fr == b"\x01":
                    try:
                        self.s.sendall(struct.pack(">I", 1) + b"\x02")
                    except Exception:
                        pass
                    continue
                out.append(fr)
            try:
                chunk = self.s.recv(65536)
            except socket.timeout:
                continue
            except Exception:
                return out
            if chunk == b"":
                return out
            self.buf += chunk
        return out


def decode_room_event(frame):
    """Kembalikan (name, data) untuk RoomEvent, atau (None, None)."""
    try:
        if len(frame) < 3 or frame[0] != 0x03:
            return None, None
        i = 1
        ln, m = L.read_varint(frame, i)
        if ln is None:
            return None, None
        i += m
        name = frame[i:i + ln].decode("utf-8", "replace")
        i += ln
        if i >= len(frame):
            return name, b""
        dln, m2 = L.read_varint(frame, i)
        if dln is None:
            return name, b""
        i += m2
        return name, frame[i:i + dln]
    except Exception:
        return None, None


async def clan_once(session, email, password, device_id, clan_name,
                    clan_id_known=""):
    """Satu percobaan penuh. Kembalikan dict hasil (untuk HASIL)."""
    hasil = {"email": email, "clan_name": clan_name,
             "current_clan": None, "search_results": [],
             "target_clan_id": None, "join_sent": False,
             "join_response": [], "events": []}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil
    host, port = L.find_host_port(alloc)
    if not host or not port:
        print("[!] host/port tidak ketemu di allocation.")
        return hasil
    print(f"[*] alokasi OK: {host}:{port}")
    sock, _pid = L.tcp_connect(host, port)
    if sock is None:
        print("[!] TCP connect gagal.")
        return hasil
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        print(f"[!] kirim login frame gagal: {exc}")
        return hasil
    fr = FrameReader(sock)

    def _catat(frame):
        name, data = decode_room_event(frame)
        if name is None:
            return
        hasil["events"].append((name, data.hex() if data else ""))
        vlog(f"[evt] RoomEvent name={name!r} data_hex={data.hex() if data else ''}")
        if not data:
            return
        if name == "clan":
            fields = parse_protobuf_fields(data)
            vlog(f"  [clan] fields: {fields}")
            d = dict((f, v) for f, _w, v in fields)
            # Respons JoinClan (EventId=1) / GetUserClan (EventId=11).
            if d.get(1) == 1:
                hasil["join_response"].append(data.hex())
                print(f"[+] Respons JOIN diterima ({len(data)} B).")
            if d.get(1) == 11:
                print(f"[+] Respons GetUserClan diterima: {fields}")

    # --- tunggu jendela login (pola WAIT-LOGIN): tbugv atau JoinedRoom ---
    print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
    got, closed = fr.read_until(
        lambda f: _is_tbugv(f) or _is_joinedroom(f), 3)
    if closed:
        print("[!] koneksi tertutup saat tunggu login.")
        return hasil
    if got is not None and _is_tbugv(got):
        print("[*] tbugv diterima — observasi 1 dtk lagi.")
        time.sleep(1)
    elif got is not None:
        print("[*] JoinedRoom diterima.")
    else:
        print("[*] timeout jendela login (koneksi hidup) — lanjut kirim.")

    def kirim(data_pb, label):
        print(f"[*] kirim {label} via RoomEvent '{CLAN_CMD}' ...")
        try:
            sock.sendall(L.build_room_event_varint(CLAN_CMD, data_pb))
            return True
        except Exception as exc:
            print(f"[!] kirim {label} gagal: {exc}")
            return False

    def tunggu_respons(seconds, label):
        """Tunggu & catat semua RoomEvent selama `seconds` detik."""
        print(f"[*] tunggu respons {label} ({seconds} dtk)...")
        frames = fr.drain(seconds)
        for f in frames:
            _catat(f)
        return frames

    # 1) GetUserClan — cek clan saat ini.
    if not kirim(build_clan_simple(EV_GET_USER_CLAN), "GetUserClan"):
        return hasil
    tunggu_respons(4, "GetUserClan")

    # 2) SearchClan — cari clan by nama (dilewati kalau clanId sudah
    #    diketahui, mis. dari log mod ClanLog).
    target = None
    if clan_id_known:
        target = clan_id_known
        print(f"[*] clanId sudah diketahui dari log mod: {target} "
              f"(SearchClan dilewati)")
    else:
        if not kirim(build_search_clan(clan_name),
                     f"SearchClan('{clan_name}')"):
            return hasil
        frames = tunggu_respons(5, "SearchClan")
        # Parse SearchClanResponse: cari Clan yang namanya cocok
        # (case-insensitive).
        for f in frames:
            name, data = decode_room_event(f)
            if name != "clan" or not data:
                continue
            fields = parse_protobuf_fields(data)
            d = dict((f, v) for f, _w, v in fields)
            # Field 3 (repeated Clan) muncul sebagai beberapa entri
            # string/bytes berupa hex; decode tiap entri sebagai pesan Clan.
            for fno, wt, v in fields:
                if fno != 3 or wt != "string/bytes":
                    continue
                try:
                    raw = bytes.fromhex(v)
                except Exception:
                    continue
                c = parse_clan_message(raw)
                if not c.get("clan_id"):
                    continue
                hasil["search_results"].append(c)
                print(f"[+] Clan ditemukan: {c}")
                if c.get("name", "").lower() == clan_name.lower():
                    hasil["target_clan_id"] = c["clan_id"]
                    print(f"[+] Target cocok: {c['name']} "
                          f"id={c['clan_id']}")
        target = hasil["target_clan_id"]

    # 3) JoinClan — gabung by clanId (kalau target ketemu).
    if not target:
        print(f"[!] Clan '{clan_name}' TIDAK ditemukan di hasil pencarian — "
              f"join dilewati.")
    else:
        hasil["target_clan_id"] = target
        if not kirim(build_join_clan(target), f"JoinClan(id={target})"):
            return hasil
        hasil["join_sent"] = True
        tunggu_respons(5, "JoinClan")

    try:
        sock.close()
    except Exception:
        pass
    return hasil


async def main():
    global VERBOSE
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("CLAN_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("CLAN_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("CLAN_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--clan", default=os.getenv("CLAN_NAME", "biackhat"))
    ap.add_argument("--clan-id", default=os.getenv("CLAN_ID", ""),
                    help="Clan ID (dari log mod). Kalau diisi, SearchClan "
                         "dilewati dan langsung JoinClan by ID.")
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi "
              "(input workflow / env CLAN_EMAIL & CLAN_PASSWORD).")
        return 1
    print(f"[*] Diag Join Clan — bot {args.email} | clan '{args.clan}'"
          + (f" | clanId '{args.clan_id}'" if args.clan_id else ""))
    L.QUIET = False
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await clan_once(
            session, args.email, args.password, args.device_id,
            args.clan, args.clan_id)
    print("=" * 56)
    print("HASIL JOIN CLAN DIAG")
    print(f"  Bot: {hasil['email']} | clan target: {hasil['clan_name']}")
    print(f"  Hasil pencarian ({len(hasil['search_results'])} clan):")
    for c in hasil["search_results"]:
        print(f"    - {c.get('name')} id={c.get('clan_id')} "
              f"members={c.get('members')} score={c.get('score')}")
    print(f"  Target clanId: {hasil['target_clan_id']}")
    print(f"  Join terkirim: {hasil['join_sent']} | "
          f"respons JOIN diterima: {len(hasil['join_response'])}")
    for hx in hasil["join_response"]:
        print(f"    - clan join: {hx[:200]}")
    print(f"  RoomEvent diterima: {len(hasil['events'])}")
    for name, hx in hasil["events"]:
        print(f"    - {name}: {hx[:160]}")
    print(f"  Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 56)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
