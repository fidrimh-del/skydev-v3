# -*- coding: utf-8 -*-
"""
gift_diag.py — Diag gift pet via TCP (tanpa mod), 1 bot -> 1 penerima.

Bot login -> kirim GiftingSent (RoomEvent 'gifting') berisi 1 petId ke
recipient UUID -> tunggu GiftingResult (penerima harus konfirmasi di game)
-> catat SEMUA RoomEvent 'gifting' + hasil.

Format protobuf (dump_0_ugqp.cs, game 1.94.0, terverifikasi dari dump):
  GiftingSentEvent: f1 EventId (GiftingClientEventId, 0=Sent),
    f2 Recipient (string UUID), f3 Gifts (GiftingContent).
  GiftingContent: f1 PetIds (repeated string).
  GiftingResultEvent: f1 EventId (GiftingServerEventId, 1=Result),
    f2 Successful (bool), f3 ErrorMessage (string), f4 Gifts.
  GiftingClientEventId: Sent=0, Confirmed=1, Cancelled=2.
  GiftingServerEventId: Received=0, Result=1.

Alur 2 langkah: pengirim kirim -> penerima HARUS konfirmasi di game
(SendGiftConfirmed, hanya terjadi di HP penerima) -> server kirim Result.
Jadi diag ini menunggu sampai Result tiba (default 180 dtk) supaya
penerima sempat konfirmasi. Bila Result tak kunjung tiba, gift sudah
terkirim tapi belum dikonfirmasi (atau penerima offline).

Dibuat 2026-11-09 atas permintaan Bartes (tes gift akun tes -> SkyDev).
Jangan dipakai untuk order.
"""
import argparse
import asyncio
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


# ---------- protobuf GiftingSent ----------
def build_gifting_sent(recipient, pet_ids):
    """GiftingSentEvent: f1 EventId=0 (Sent), f2 Recipient (UUID),
    f3 Gifts (GiftingContent { f1 PetIds (repeated string) })."""
    content = b""
    for pid in pet_ids:
        pb = pid.encode("utf-8")
        content += b"\x0a" + L.varint(len(pb)) + pb  # GiftingContent f1 PetIds
    rb = recipient.encode("utf-8")
    out = b"\x08" + L.varint(0)                    # f1 EventId = Sent
    out += b"\x12" + L.varint(len(rb)) + rb        # f2 Recipient
    out += b"\x1a" + L.varint(len(content)) + content  # f3 Gifts
    return out


# ---------- parser protobuf generik ----------
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
                out.append((field_no, 0, v))
                i += m2
            elif wt == 2:
                ln, m2 = L.read_varint(data, i)
                if ln is None or i + m2 + ln > n:
                    break
                out.append((field_no, 2, data[i + m2:i + m2 + ln]))
                i += m2 + ln
            elif wt == 5:
                if i + 4 > n:
                    break
                out.append((field_no, 5, data[i:i + 4]))
                i += 4
            elif wt == 1:
                if i + 8 > n:
                    break
                out.append((field_no, 1, data[i:i + 8]))
                i += 8
            else:
                break
        except Exception:
            break
    return out


def parse_gifting_result(data):
    """Parse GiftingResultEvent (atau GiftingReceivedEvent):
    f1 EventId, f2 Successful, f3 ErrorMessage, f4 Gifts.
    Kembalikan (event_id, successful, error)."""
    event_id = None
    successful = None
    error = None
    for fno, wt, v in _raw_fields(data):
        if fno == 1 and wt == 0:
            event_id = v
        elif fno == 2 and wt == 0:
            successful = bool(v)
        elif fno == 3 and wt == 2:
            try:
                error = bytes(v).decode("utf-8")
            except Exception:
                error = bytes(v).hex()
    return event_id, successful, error


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
    """4-byte big-endian length prefix, ping b"\\x01" -> pong length 1 + b"\\x02"."""

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
        """Baca sampai pred(frame) True atau timeout. Kembalikan
        (frame_ketemu_atau_None, koneksi_tertutup?)."""
        end = time.time() + seconds
        self.s.settimeout(0.5)
        while time.time() < end:
            fr = self._pump()
            if fr is not None:
                if pred(fr):
                    return fr, False
                continue
            try:
                chunk = self.s.recv(65536)
            except socket.timeout:
                continue
            except Exception:
                return None, True
            if not chunk:
                return None, True
            self.buf.extend(chunk)
        return None, False

    def drain(self, seconds):
        """Baca semua frame selama `seconds` detik. Kembalikan list frame."""
        frames = []
        end = time.time() + seconds
        self.s.settimeout(0.5)
        while time.time() < end:
            fr = self._pump()
            if fr is not None:
                frames.append(fr)
                continue
            try:
                chunk = self.s.recv(65536)
            except socket.timeout:
                continue
            except Exception:
                break
            if not chunk:
                break
            self.buf.extend(chunk)
        return frames


async def gift_once(session, email, password, device_id, pet_id,
                    recipient, wait_seconds):
    """Satu percobaan gift penuh. Kembalikan dict hasil (untuk HASIL)."""
    hasil = {"email": email, "pet_id": pet_id, "recipient": recipient,
             "sent": False, "result_event": None, "successful": None,
             "error": None, "gifting_events": [], "events": []}
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
        if name != "gifting":
            return
        hasil["gifting_events"].append(data.hex() if data else "")
        eid, ok, err = parse_gifting_result(data)
        print(f"[+] RoomEvent 'gifting' diterima: EventId={eid} "
              f"Successful={ok} Error={err!r}")
        if eid == 1:  # Result
            hasil["result_event"] = 1
            hasil["successful"] = ok
            hasil["error"] = err
        elif eid == 0:  # Received
            print("[*] server menerima gift (Received). "
                  "Menunggu penerima konfirmasi di game...")

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

    req = build_gifting_sent(recipient, [pet_id])
    print(f"[*] GiftingSent ({len(req)} B): {req.hex()}")
    print(f"[*] kirim gift pet {pet_id} ke {recipient} ...")
    try:
        sock.sendall(L.build_room_event_varint("gifting", req))
    except Exception as exc:
        print(f"[!] kirim gift gagal: {exc}")
        return hasil
    hasil["sent"] = True
    print("[+] gift terkirim. Menunggu Result "
          f"(maks {wait_seconds} dtk, penerima harus konfirmasi di game)...")
    t_end = time.time() + wait_seconds
    while time.time() < t_end:
        if hasil["result_event"] is not None:
            break
        chunk = min(10, max(1, int(t_end - time.time())))
        for f in fr.drain(chunk):
            _catat(f)
    try:
        sock.close()
    except Exception:
        pass
    return hasil


async def main():
    global VERBOSE
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("GIFT_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("GIFT_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("GIFT_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--pet-id", default=os.getenv("GIFT_PET_ID", ""))
    ap.add_argument("--recipient", default=os.getenv("GIFT_RECIPIENT", ""))
    ap.add_argument("--wait", type=int,
                    default=int(os.getenv("GIFT_WAIT", "180")))
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    VERBOSE = args.verbose
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    if not args.pet_id or not args.recipient:
        print("[!] pet-id dan recipient (UUID) wajib diisi.")
        return 1
    print(f"[*] Diag Gift Pet — bot {args.email} | pet {args.pet_id} "
          f"-> {args.recipient} | tunggu {args.wait} dtk")
    L.QUIET = False
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await gift_once(
            session, args.email, args.password, args.device_id,
            args.pet_id, args.recipient, args.wait)
    print("=" * 56)
    print("HASIL GIFT PET DIAG")
    print(f"  Bot: {hasil['email']} | pet: {hasil['pet_id']}")
    print(f"  Penerima: {hasil['recipient']}")
    print(f"  Gift terkirim: {hasil['sent']}")
    print(f"  Result EventId: {hasil['result_event']} | "
          f"Successful: {hasil['successful']} | Error: {hasil['error']!r}")
    print(f"  RoomEvent 'gifting' diterima: {len(hasil['gifting_events'])}")
    for hx in hasil["gifting_events"]:
        print(f"    - gifting: {hx[:200]}")
    print(f"  Total RoomEvent diterima: {len(hasil['events'])}")
    print(f"  Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 56)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
