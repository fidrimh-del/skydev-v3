# -*- coding: utf-8 -*-
"""
pet_details_diag.py — Diag katalog pet via TCP (tanpa mod).

Tujuan: ambil KATALOG LENGKAP semua spesies pet (Type -> nama asli)
lewat RoomEvent, supaya kita tahu pet apa saja itu tanpa cek satu per
satu di game (akun utama Bartes ~2000 pet — cek manual mustahil).

Temuan dump (dump_0_ugqp.cs, game 1.94.0):
  PetSystemData menerima katalog dari server:
    SendGetPetDetails(callback) -> GetPetDetailsResponse:
      GetPetDetailsResponse: List<PetDetails> PetDetails.
  Protobuf (Multiplayer.Scripts.Serialization.Protobuf):
    PetDetailsResponse: f1 EventId (PetResponseEventId, 24=PetDetailsResponse),
      f2 PetDetails (repeated PetDetails).
    PetDetails: f1 Id (int, = Type), f2 Colors (int),
      f3 DefaultName (string, NAMA ASLI spesies — bukan kunci lokalisasi),
      f4 Personalities (int), f5 Tags (repeated string), f7 Brotchy (bool).
    PetRequestEventId: PetDetailsRequest = 19.
  Jadi: kirim RoomEvent 'pet' berisi PetDetailsRequest (f1 EventId=19),
  server balas PetDetailsResponse (f1 EventId=24) berisi semua spesies.

  CATATAN: format request PetDetailsRequest belum 100% pasti dari dump
  (hanya EventId-nya yang diketahui). Diag ini coba bentuk paling
  sederhana dulu (f1 EventId=19 saja) dan mencatat SEMUA RoomEvent
  'pet' yang diterima, jadi kalau server tidak merespons / format
  salah, log akan menunjukkannya.

  GAMBAR pet TIDAK ada di katalog ini (hanya nama). Gambar ada di asset
  game (APK) — itu tugas terpisah.

Pola: login HTTP -> alokasi -> TCP connect -> login frame -> tunggu
jendela login (tbugv/JoinedRoom) -> kirim PetDetailsRequest -> catat respons.
"""
import argparse
import asyncio
import os
import time

import lookup_uuid as L
import pet_list_diag as PL

PET_DETAILS_REQUEST_EVENT = 19   # PetRequestEventId.PetDetailsRequest
PET_DETAILS_RESPONSE_EVENT = 24  # PetResponseEventId.PetDetailsResponse


def build_pet_details_request():
    """PetDetailsRequest paling sederhana: f1 EventId=19 saja."""
    return b"\x08" + L.varint(PET_DETAILS_REQUEST_EVENT)


def _parse_pet_details(data):
    """Parse satu PetDetails -> dict {type, default_name, colors,
    personalities, tags, brotchy}.
    PetDetails: f1 Id, f2 Colors, f3 DefaultName, f4 Personalities,
    f5 Tags (repeated), f7 Brotchy."""
    d = {"type": None, "default_name": None, "colors": None,
         "personalities": None, "tags": [], "brotchy": None}
    for fno, wt, v in PL._raw_fields(bytes(data)):
        if fno == 1 and wt == 0:
            d["type"] = v
        elif fno == 2 and wt == 0:
            d["colors"] = v
        elif fno == 3 and wt == 2:
            try:
                d["default_name"] = bytes(v).decode("utf-8")
            except Exception:
                d["default_name"] = bytes(v).hex()
        elif fno == 4 and wt == 0:
            d["personalities"] = v
        elif fno == 5 and wt == 2:
            try:
                d["tags"].append(bytes(v).decode("utf-8"))
            except Exception:
                pass
        elif fno == 7 and wt == 0:
            d["brotchy"] = bool(v)
    return d


def extract_pet_details_response(data):
    """Parse PetDetailsResponse: f1 EventId=24, f2 PetDetails (repeated).
    Kembalikan list dict PetDetails (kosong bila f1 != 24)."""
    out = []
    is_details = False
    for fno, wt, v in PL._raw_fields(data):
        if fno == 1 and wt == 0:
            if v == PET_DETAILS_RESPONSE_EVENT:
                is_details = True
            else:
                return []
        elif fno == 2 and wt == 2:
            d = _parse_pet_details(bytes(v))
            if d["type"] is not None:
                out.append(d)
    if not is_details:
        return []
    return out


async def pet_details_once(session, email, password, device_id, wait_seconds):
    """Satu percobaan penuh. Kembalikan dict hasil (untuk HASIL)."""
    hasil = {"email": email, "catalog": [], "response_events": 0,
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
    fr = PL.FrameReader(sock)

    def _catat(frame):
        name, data = PL.decode_room_event(frame)
        if name is None:
            return
        hasil["events"].append((name, data.hex() if data else ""))
        if name != "pet" or not data:
            return
        details = extract_pet_details_response(data)
        if details:
            hasil["response_events"] += 1
            seen = {d["type"] for d in hasil["catalog"]}
            baru = [d for d in details if d["type"] not in seen]
            hasil["catalog"].extend(baru)
            print(f"[+] PetDetailsResponse: {len(details)} spesies "
                  f"({len(baru)} baru, total {len(hasil['catalog'])}).")
            for d in baru[:5]:
                print(f"    - type={d['type']} name={d['default_name']!r}")
            if len(baru) > 5:
                print(f"    ... dan {len(baru) - 5} lagi.")
        else:
            # Event 'pet' lain (bukan PetDetailsResponse) — catat ringkas.
            f1 = None
            for fno, wt, v in PL._raw_fields(data):
                if fno == 1 and wt == 0:
                    f1 = v
                    break
            print(f"[*] RoomEvent 'pet' lain: EventId={f1} "
                  f"({len(data)} B).")

    # --- tunggu jendela login (tbugv / JoinedRoom) ---
    print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
    got, closed = fr.read_until(
        lambda f: PL._is_tbugv(f) or PL._is_joinedroom(f), 3)
    if closed:
        print("[!] koneksi tertutup saat tunggu login.")
        return hasil
    if got is not None and PL._is_tbugv(got):
        print("[*] tbugv diterima — observasi 1 dtk lagi.")
        time.sleep(1)
    elif got is not None:
        print("[*] JoinedRoom diterima.")
    else:
        print("[*] timeout jendela login (koneksi hidup) — lanjut kirim.")

    req = build_pet_details_request()
    print(f"[*] PetDetailsRequest ({len(req)} B): {req.hex()}")
    print("[*] kirim PetDetailsRequest via RoomEvent 'pet' ...")
    try:
        sock.sendall(L.build_room_event_varint("pet", req))
    except Exception as exc:
        print(f"[!] kirim PetDetailsRequest gagal: {exc}")
        return hasil
    print(f"[*] menunggu PetDetailsResponse (maks {wait_seconds} dtk) ...")
    t_end = time.time() + wait_seconds
    while time.time() < t_end:
        chunk = min(5, max(1, int(t_end - time.time())))
        for f in fr.drain(chunk):
            _catat(f)
        # Katalog biasanya tiba dalam satu respons besar; bila sudah
        # dapat banyak dan tidak ada lagi, berhenti lebih awal.
        if hasil["catalog"] and time.time() > t_end - wait_seconds + 8:
            # Sudah menunggu 8 dtk sejak kirim dan katalog sudah ada.
            pass
    try:
        sock.close()
    except Exception:
        pass
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("PETDETAILS_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("PETDETAILS_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("PETDETAILS_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--wait", type=int,
                    default=int(os.getenv("PETDETAILS_WAIT", "15")))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    print(f"[*] Diag Pet Details (katalog) — bot {args.email} | "
          f"tunggu {args.wait} dtk")
    L.QUIET = False
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await pet_details_once(
            session, args.email, args.password, args.device_id, args.wait)
    print("=" * 56)
    print("HASIL PET DETAILS DIAG (KATALOG)")
    print(f"  Bot: {hasil['email']}")
    print(f"  PetDetailsResponse diterima: {hasil['response_events']}")
    print(f"  Jumlah spesies di katalog: {len(hasil['catalog'])}")
    for d in sorted(hasil["catalog"], key=lambda x: (x["type"] or 0)):
        print(f"    - type={d['type']} name={d['default_name']!r} "
              f"colors={d['colors']} tags={d['tags']}")
    print(f"  RoomEvent diterima: {len(hasil['events'])}")
    for name, hx in hasil["events"]:
        print(f"    - {name}: {hx[:160]}")
    print(f"  Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 56)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
