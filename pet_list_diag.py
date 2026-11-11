# -*- coding: utf-8 -*-
"""
pet_list_diag.py — Diag daftar pet via TCP (tanpa mod).

Tujuan: ambil daftar pet milik akun bot (ID instance, nama, jenis) lewat
RoomEvent, supaya bot gift-pet tahu petId apa yang bisa dikirim — tanpa
buka aplikasi/mod lagi.

Temuan dump (dump_31_ijrw.cs, game 1.94.0):
  PetsController (Runtime.dll, ns Multiplayer.Scripts.Player):
    SendGetPetPageFromUser(callback, pageKey, pageSize) -> PageRequest.
  PageRequest: f1 EventId (PaginationServerEventId, 0=PaginationRequestKeyBased),
    f2 PageKey (string, "" = halaman pertama), f3 PageSize (int, 100),
    f4 GetPetsRequestMessage (oneof):
      GetPetsRequestMessage: f1 EventId (PetRequestEventId, 0=List),
        f2 StoreTreeVersion (string, "").
  PageResponse: f1 EventId, f2 NextPageKey (string),
    f3 GetPetsResponse (oneof):
      GetPetsResponse: f1 EventId (PetResponseEventId, 8=GetPetsResponse),
        f2 Successful (bool), f3 ErrorMessage (string),
        f4 Pets (repeated PetData):
          PetData: f1 OwnerId, f2 CreatedAt, f3 PetId (UUID instance),
            f4 DisplayName, f5 Color, f6 Stage (PetStage enum: 0=NotIncubated,
            1=Incubated, 2=Baby, 3=Child, 4=Teen, 5=MergeIngredient,
            6=Mutated, 7=Shiny), f7 Progress, f8 Ads,
            f9 Personality, f10 NameChange, f11 Items, f12 Type,
            f13 ExperiencePoints.

Command RoomEvent pet BELUM terkonfirmasi dari dump (diset saat runtime,
seperti CrazyRunCommand dulu). Kandidat (pola mechanic lain: "crazy-run",
"clan", "gifting"): "pet" dulu, lalu "pets" bila tak ada respons.
Script mencatat SEMUA RoomEvent mentah yang diterima, jadi kalau command
salah (server tidak merespons), log akan menunjukkannya.

Pola: login HTTP -> alokasi -> TCP connect -> login frame -> tunggu
jendela login (tbugv/JoinedRoom) -> kirim PageRequest -> catat respons.
"""
import argparse
import asyncio
import json
import os
import socket
import struct
import sys
import time

import lookup_uuid as L

VERBOSE = False


def vlog(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)


# ---------- protobuf PageRequest (daftar pet, halaman pertama) ----------
def build_pet_page_request(page_size=100, page_key=""):
    """PageRequest: f1 EventId=0, f2 PageKey, f3 PageSize,
    f4 GetPetsRequestMessage { f1 EventId=0 (List), f2 StoreTreeVersion="" }."""
    inner = b"\x08" + L.varint(0)          # GetPetsRequestMessage f1 EventId=List
    inner += b"\x12" + L.varint(0)         # f2 StoreTreeVersion=""
    pk = page_key.encode("utf-8")
    out = b"\x08" + L.varint(0)            # PageRequest f1 EventId=PaginationRequestKeyBased
    out += b"\x12" + L.varint(len(pk)) + pk  # f2 PageKey
    out += b"\x18" + L.varint(page_size)   # f3 PageSize
    out += b"\x22" + L.varint(len(inner)) + inner  # f4 GetPetsRequestMessage
    return out


# ---------- parser protobuf generik + ekstraktor pet ----------
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


def _raw_fields(data):
    """Kembalikan list (field_no, wire_type, raw_bytes_or_int) tanpa decode."""
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
                out.append((field_no, wt, v))
                i += m2
            elif wt == 2:
                ln, m2 = L.read_varint(data, i)
                if ln is None or i + m2 + ln > n:
                    break
                out.append((field_no, wt, data[i + m2:i + m2 + ln]))
                i += m2 + ln
            elif wt == 5:
                if i + 4 > n:
                    break
                out.append((field_no, wt, data[i:i + 4]))
                i += 4
            elif wt == 1:
                if i + 8 > n:
                    break
                out.append((field_no, wt, data[i:i + 8]))
                i += 8
            else:
                break
        except Exception:
            break
    return out


def _parse_petdata(data):
    """Parse satu PetData -> dict {pet_id, display_name, type, owner_id, stage}.
    PetData: f1 OwnerId, f2 CreatedAt, f3 PetId, f4 DisplayName,
    f6 Stage (PetStage enum, lihat atas), f12 Type.
    Catatan: protobuf menghilangkan field bernilai 0, jadi pet telur
    (NotIncubated=0) terbaca stage=None."""
    pet = {"pet_id": None, "display_name": None,
           "type": None, "owner_id": None, "stage": None}
    for f3, w3, v3 in _raw_fields(bytes(data)):
        if f3 == 1 and w3 == 2:
            try:
                pet["owner_id"] = bytes(v3).decode("utf-8")
            except Exception:
                pass
        elif f3 == 3 and w3 == 2:
            try:
                pet["pet_id"] = bytes(v3).decode("utf-8")
            except Exception:
                pass
        elif f3 == 4 and w3 == 2:
            try:
                pet["display_name"] = bytes(v3).decode("utf-8")
            except Exception:
                pass
        elif f3 == 6 and w3 == 0:
            pet["stage"] = v3
        elif f3 == 12 and w3 == 0:
            pet["type"] = v3
    return pet


def extract_pet_from_getpets_response(data):
    """Parse GetPetsResponse LANGSUNG (RoomEvent 'pet', bukan di dalam
    PageResponse): f1 EventId=8, f2 Successful, f3 ErrorMessage,
    f4 PetData (satu per event; server kirim satu event per pet).
    Kembalikan (pets, successful, error). Bila f1 != 8, bukan
    GetPetsResponse -> kembalikan kosong (mis. event 'pet' berisi
    pasangan UUID saja, bukan PetData lengkap)."""
    pets = []
    successful = None
    error = None
    is_getpets = False
    for fno, wt, v in _raw_fields(data):
        if fno == 1 and wt == 0:
            if v == 8:
                is_getpets = True
            else:
                return [], None, None
        elif fno == 2 and wt == 0:
            successful = bool(v)
        elif fno == 3 and wt == 2:
            try:
                error = bytes(v).decode("utf-8")
            except Exception:
                error = bytes(v).hex()
        elif fno == 4 and wt == 2:
            pet = _parse_petdata(bytes(v))
            if pet["pet_id"]:
                pets.append(pet)
    if not is_getpets:
        return [], None, None
    return pets, successful, error


def extract_pets_from_page_response(data):
    """Parse PageResponse -> GetPetsResponse (f3) -> Pets (f4, repeated
    PetData). Kembalikan (pets, successful, error, next_page_key).
    pets = list dict {pet_id, display_name, type}."""
    pets = []
    successful = None
    error = None
    next_key = None
    for fno, wt, v in _raw_fields(data):
        if fno == 2 and wt == 2 and isinstance(v, (bytes, bytearray)):
            try:
                next_key = bytes(v).decode("utf-8")
            except Exception:
                next_key = bytes(v).hex()
        elif fno == 3 and wt == 2 and isinstance(v, (bytes, bytearray)):
            # GetPetsResponse
            for f2, w2, v2 in _raw_fields(bytes(v)):
                if f2 == 2 and w2 == 0:
                    successful = bool(v2)
                elif f2 == 3 and w2 == 2:
                    try:
                        error = bytes(v2).decode("utf-8")
                    except Exception:
                        error = bytes(v2).hex()
                elif f2 == 4 and w2 == 2:
                    # Satu PetData
                    pet = _parse_petdata(bytes(v2))
                    if pet["pet_id"]:
                        pets.append(pet)
    return pets, successful, error, next_key


def decode_room_event(frame):
    """Kembalikan (nama_event, data_protobuf) untuk frame RoomEvent 0x08."""
    if len(frame) < 2 or frame[0] != 0x08:
        return None, None
    ln, m = L.read_varint(frame, 1)
    if ln is None:
        return None, None
    name = frame[1 + m:1 + m + ln].decode("utf-8", "replace")
    return name, frame[1 + m + ln:]


def _is_tbugv(frame):
    if len(frame) >= 2 and frame[0] == 0x05:
        ln, m = L.read_varint(frame, 1)
        if ln is not None and ln < 0x80 and frame[1 + m:1 + m + ln] == b"tbugv":
            return True
    return False


def _is_joinedroom(frame):
    return len(frame) >= 6 and frame[0] == 0x06 and frame[1] == 1


class FrameReader:
    """Sama seperti crazyrun_diag.py: 4-byte big-endian length prefix,
    ping b"\\x01" -> pong length 1 + b"\\x02"."""

    def __init__(self, sock):
        self.s = sock
        self.buf = bytearray()

    def _pump(self):
        """Ambil satu frame lengkap dari buffer (atau None)."""
        while len(self.buf) >= 4:
            ln = struct.unpack(">I", self.buf[:4])[0]
            if ln > 262144:
                return None
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
            return fr
        return None

    def read_until(self, pred, seconds):
        deadline = time.time() + seconds
        self.s.settimeout(0.5)
        while time.time() < deadline:
            fr = self._pump()
            if fr is not None and pred(fr):
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
            fr = self._pump()
            while fr is not None:
                out.append(fr)
                fr = self._pump()
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


async def pet_list_once(session, email, password, device_id, commands):
    """Satu percobaan penuh. Kembalikan dict hasil (untuk HASIL)."""
    hasil = {"email": email, "commands": list(commands),
             "pets": [], "successful": None, "error": None,
             "next_page_key": None, "command_ok": None,
             "events": []}
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
        fields = parse_protobuf_fields(data)
        for fno, wt, v in fields:
            vlog(f"      field {fno} ({wt}): {v!r}")
        # Tangkap daftar pet dari respons PageResponse / GetPetsResponse.
        pets, ok, err, nxt = extract_pets_from_page_response(data)
        if pets or ok is not None:
            if pets:
                hasil["pets"] = pets
                print(f"[+] {len(pets)} pet diterima (command "
                      f"'{hasil['command_ok']}'):")
                for p in pets:
                    print(f"    - id={p['pet_id']} type={p['type']} "
                          f"name={p['display_name']!r}")
            if ok is not None:
                hasil["successful"] = ok
                hasil["error"] = err
                print(f"[+] GetPetsResponse successful={ok} error={err!r}")
            if nxt is not None:
                hasil["next_page_key"] = nxt
                print(f"[+] NextPageKey={nxt!r}")
        # Respons LANGSUNG GetPetsResponse (RoomEvent 'pet'): server kirim
        # satu event per pet, masing-masing {f1=8, f2=Successful, f4=PetData}.
        # (Terbukti run 42551093555: PageResponse tak pernah tiba, tapi event
        # 'pet' berisi PetData lengkap. Parser lama menghitung 0.)
        pets2, ok2, err2 = extract_pet_from_getpets_response(data)
        if pets2:
            seen = {p["pet_id"] for p in hasil["pets"]}
            baru = [p for p in pets2 if p["pet_id"] not in seen]
            for p in baru:
                hasil["pets"].append(p)
                seen.add(p["pet_id"])
            if baru:
                print(f"[+] {len(baru)} pet diterima via GetPetsResponse "
                      f"langsung (command '{hasil['command_ok']}'):")
                for p in baru:
                    print(f"    - id={p['pet_id']} type={p['type']} "
                          f"name={p['display_name']!r}")
                if hasil.get("command_ok"):
                    hasil.setdefault("pets_by_command", {}).setdefault(
                        hasil["command_ok"], []).extend(
                            [p["pet_id"] for p in baru])
            if ok2 is not None and hasil["successful"] is None:
                hasil["successful"] = ok2
                hasil["error"] = err2

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

    req = build_pet_page_request()
    print(f"[*] PageRequest ({len(req)} B): {req.hex()}")
    for cmd in commands:
        if hasil["pets"]:
            break
        print(f"[*] kirim PageRequest via RoomEvent '{cmd}' ...")
        try:
            sock.sendall(L.build_room_event_varint(cmd, req))
        except Exception as exc:
            print(f"[!] kirim via '{cmd}' gagal: {exc}")
            continue
        hasil["command_ok"] = cmd
        frames = fr.drain(6)
        print(f"[*] {len(frames)} frame diterima setelah '{cmd}'.")
        for f in frames:
            _catat(f)
        if hasil["pets"]:
            break
        print(f"[*] tidak ada pet dari '{cmd}' — coba command berikut.")

    try:
        sock.close()
    except Exception:
        pass
    return hasil


def _kirim_hasil_cepat(kind, email, result):
    """POST hasil diag ke /api/pet-result (jalur cepat panel).
    Gagal kirim TIDAK menggagalkan diag (log tetap jadi fallback).
    Secret dari env PET_RESULT_SECRET (GitHub secret)."""
    secret = os.getenv("PET_RESULT_SECRET", "")
    if not secret:
        return
    try:
        import urllib.request
        data = json.dumps({"kind": kind, "email": email,
                           "result": result, "secret": secret}).encode()
        req = urllib.request.Request(
            "https://pkxd.shop/api/pet-result", data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=20) as resp:
            resp.read()
        print("[+] hasil cepat terkirim ke /api/pet-result")
    except Exception as exc:
        print(f"[*] kirim hasil cepat gagal (log tetap jadi fallback): {exc}")


async def main():
    # Baca versi game/protokol dari dashboard panel (app_settings).
    L.apply_panel_versions()
    global VERBOSE
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("PETLIST_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("PETLIST_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("PETLIST_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--commands",
                    default=os.getenv("PETLIST_COMMANDS", "pet,pets"))
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    VERBOSE = args.verbose
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi "
              "(input workflow / env PETLIST_EMAIL & PETLIST_PASSWORD).")
        return 1
    commands = [c.strip() for c in args.commands.split(",") if c.strip()]
    print(f"[*] Diag Pet List — bot {args.email} | commands {commands}")
    L.QUIET = False
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await pet_list_once(
            session, args.email, args.password, args.device_id, commands)
    print("=" * 56)
    print("HASIL PET LIST DIAG")
    print(f"  Bot: {hasil['email']} | commands: {hasil['commands']}")
    print(f"  Command yang merespons: {hasil['command_ok']}")
    print(f"  Successful: {hasil['successful']} | Error: {hasil['error']!r}")
    print(f"  NextPageKey: {hasil['next_page_key']!r}")
    print(f"  Jumlah pet: {len(hasil['pets'])}")
    # Ringkas: hanya 20 pet pertama (daftar lengkap ada di PETLIST_JSON
    # di bawah) agar log tetap kecil & cepat diunduh panel.
    for p in hasil["pets"][:20]:
        print(f"    - id={p['pet_id']} type={p['type']} stage={p.get('stage')} "
              f"name={p['display_name']!r} owner={p['owner_id']}")
    if len(hasil["pets"]) > 20:
        print(f"    - ... +{len(hasil['pets']) - 20} pet lagi (lihat PETLIST_JSON)")
    if hasil.get("pets_by_command"):
        print(f"  Pet per command: {hasil['pets_by_command']}")
    print(f"  RoomEvent diterima: {len(hasil['events'])}")
    # Ringkas: hitung per nama command, tanpa dump hex tiap event
    # (log besar memperlambat pengarsipan & unduhan log GitHub).
    _hitung = {}
    for _n, _hx in hasil["events"]:
        _hitung[_n] = _hitung.get(_n, 0) + 1
    for _n, _c in sorted(_hitung.items()):
        print(f"    - {_n}: {_c}")
    print(f"  Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 56)
    # Baris JSON mesin (untuk panel Pet Gift "Pet Saya"): satu baris,
    # mudah diparse dari log GitHub Actions tanpa regex rapuh.
    print("PETLIST_JSON: " + json.dumps({
        "email": hasil["email"],
        "count": len(hasil["pets"]),
        "pets": [
            {"pet_id": p["pet_id"], "type": p["type"],
             "display_name": p["display_name"], "owner_id": p["owner_id"],
             "stage": p.get("stage")}
            for p in hasil["pets"]
        ],
        "successful": hasil["successful"],
        "error": hasil["error"],
    }, ensure_ascii=False))
    # Jalur cepat: POST hasil ke /api/pet-result agar panel tidak perlu
    # menunggu arsip log GitHub (30 dtk–2 mnt). Secret dari env
    # PET_RESULT_SECRET (GitHub secret, diteruskan workflow).
    _kirim_hasil_cepat("petlist", hasil["email"], {
        "email": hasil["email"],
        "count": len(hasil["pets"]),
        "pets": [
            {"pet_id": p["pet_id"], "type": p["type"],
             "display_name": p["display_name"], "owner_id": p["owner_id"],
             "stage": p.get("stage")}
            for p in hasil["pets"]
        ],
        "successful": hasil["successful"],
        "error": hasil["error"],
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
