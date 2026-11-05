"""lookup_uuid.py — Ambil UUID pemain PK XD dari nama (CEPAT).

Jalur yang sudah TERBUKTI: login FSTR -> allocation ->
TCP connect -> Event "login" (USER_SESSION JWT + 1.93.4) ->
LANGSUNG RoomEvent "friends" + protobuf SearchFriend (EventId=7)
-> search_friend_result -> UUID. Join TIDAK diperlukan
(skip-join test 2026-10-29: search dibalas tanpa room); join hanya
fallback bila search langsung kosong.

Versi ini DIPANGKAS untuk kecepatan dibanding probe:
  - Jalur cepat = search langsung sesudah login (tanpa join).
  - Tanpa tes UDP, tanpa Supabase, tanpa dump frame verbose.
  - Jendela early-exit (2026-10-29): connect/login/search berhenti
    SEGERA saat frame target tiba (cap tetap: connect 6, login 5,
    join 3 dtk per room, search 12 dtk); settle 2 dtk DIHAPUS
    (eksperimen skydev-test: 1,4-1,9 dtk vs baseline 27,2 dtk).
  - Join DIHAPUS dari jalur cepat (2026-10-29, skip-join test):
    search langsung sesudah login. Urutan fallback (v2):
    tunggu 6 dtk + search ulang -> join + search -> friends-kosong
    + search (join/friends-kosong 0 kasus penyelamatan di v2, hanya
    jaring pengaman terakhir; sesi lambat cukup ditunggu —
    kelambatan bersifat per-sesi, bukan per-server: 174.138.71.188
    pernah lambat 18-38 dtk dan pernah menjawab <1 dtk).
  - Satu varian query saja (server terbukti cocok case-insensitive,
    jadi kapitalisasi input tidak masalah).
  - Log ringkas + blok HASIL + waktu total + satu baris log
    ditambahkan ke uuid_lookup_log.txt.

CARA PAKAI:
  python lookup_uuid.py "Admin#000"      (satu argumen nama#kode)
  python lookup_uuid.py Admin 000        (dua argumen: nama + kode)
  python lookup_uuid.py                  (pakai env SEARCH_NAME / prompt)
  Di GitHub Actions: pakai lookup-uuid.yml (input search_name).

Kredensial: akun bot pemilik (env PROBE_* menang, kalau tidak ada
pakai nilai di script atas perintah pemilik; hapus nilai hardcode
saat masuk produksi dan pindah ke Secrets). Nilai kredensial
TIDAK PERNAH dicetak ke log.
"""

import json
import os
import re
import sys
import random
import asyncio
import hmac
import hashlib
import base64
import socket
import struct
import time
import urllib.request
import urllib.error
from collections import OrderedDict, Counter
from curl_cffi.requests import AsyncSession

try:
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    pass

# SEMENTARA UNTUK UJI COBA (repo private; nilai sama seperti script
# panel). Env selalu menang kalau sudah diisi sebagai Secrets.
APP_KEY = os.getenv("FSTR_APP_KEY") or "57a3d999-654c-4484-a20c-0f4b5512af49[span_3](start_span)"[span_3](end_span)
APP_SECRET = os.getenv("FSTR_APP_SECRET") or "jWS5aLZacNVFx6PAxpXq2g4o[span_4](start_span)"[span_4](end_span)

ACCOUNT_URL = "https://account.faster.aftvrsys.com[span_5](start_span)"[span_5](end_span)
LOBBY_URL = "https://pkxd-lobby.aftvrsys.com[span_6](start_span)"[span_6](end_span)
GAME_PROTOCOL = "1.93.0[span_7](start_span)"[span_7](end_span)
VERSION = "1.93.4[span_8](start_span)"[span_8](end_span)

# Mode senyap untuk job like massal: True = sembunyikan log operasional
# per-bot (login OK, allocation, connect, join). Hasil per-bot tetap
# dilaporkan pemanggil (run_bot cloud). Default False agar poller UUID
# dan skrip lain tidak berubah perilaku; skrip like mengeset True.
QUIET = False[span_9](start_span)[span_9](end_span)


def qlog(*args, **kwargs):
    if not QUIET:
        print(*args, **kwargs)[span_10](start_span)[span_10](end_span)


HEADERS_BASE = {
    "User-Agent": "UnityPlayer/2022.3.62f3 (UnityWebRequest/1.0, libcurl/8.10.1-DEV)",
    "Accept": "*/*",
    "Accept-Encoding": "deflate, gzip",
    "Content-Type": "application/json",
    "X-Protocol-Version": GAME_PROTOCOL,
    "X-Unity-Version": "2022.3.62f3",
    "Connection": "keep-alive",
}[span_11](start_span)[span_11](end_span)

UUID_RE = re.compile(rb"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")[span_12](start_span)[span_12](end_span)
FLAG_RE = re.compile(rb"friend|search|error|fail|denied", re.IGNORECASE)[span_13](start_span)[span_13](end_span)


def get_panel_versions():
    """(game_version, protocol_version) dari tabel app_settings di DB
    panel (diubah dari dashboard, tanpa edit GitHub). Kembalikan
    (None, None) bila env/tabel tak tersedia — pemanggil memakai nilai
    bawaan (job tetap jalan, tidak mati)."""
    url = (os.getenv("PANEL_SUPABASE_URL")
           or os.getenv("SUPABASE_URL", "")).rstrip("/")[span_14](start_span)[span_14](end_span)
    key = (os.getenv("PANEL_SUPABASE_SERVICE_KEY")
           or os.getenv("SUPABASE_SERVICE_KEY", ""))[span_15](start_span)[span_15](end_span)
    if not url or not key:
        return None, None[span_16](start_span)[span_16](end_span)
    try:
        req = urllib.request.Request(
            url + "/rest/v1/app_settings?select=key,value"
            "&key=in.(game_version,protocol_version)",
            headers={"apikey": key, "Authorization": "Bearer " + key},
            method="GET")[span_17](start_span)[span_17](end_span)
        with urllib.request.urlopen(req, timeout=10) as r:
            rows = json.loads(r.read().decode("utf-8"))[span_18](start_span)[span_18](end_span)
        gv = pv = None[span_19](start_span)[span_19](end_span)
        for row in rows or []:
            v = (row.get("value") or "").strip() or None[span_20](start_span)[span_20](end_span)
            if row.get("key") == "game_version":
                gv = v[span_21](start_span)[span_21](end_span)
            elif row.get("key") == "protocol_version":
                pv = v[span_22](start_span)[span_22](end_span)
        return gv, pv[span_23](start_span)[span_23](end_span)
    except Exception as e:
        print(f"[!] Baca app_settings gagal ({e}) -> pakai versi bawaan "
              f"({VERSION}/{GAME_PROTOCOL}).")[span_24](start_span)[span_24](end_span)
        return None, None[span_25](start_span)[span_25](end_span)


def apply_panel_versions():
    """Terapkan versi dari DB panel (bila ada) ke modul ini, termasuk
    rebuild HEADERS_BASE yang dibangun sekali saat import (override
    variabel saja TIDAK cukup). Kembalikan (VERSION, GAME_PROTOCOL)
    yang aktif. Dipanggil sekali di awal main() setiap script."""
    global GAME_PROTOCOL, VERSION, HEADERS_BASE
    gv, pv = get_panel_versions()[span_26](start_span)[span_26](end_span)
    if gv:
        VERSION = gv[span_27](start_span)[span_27](end_span)
    if pv:
        GAME_PROTOCOL = pv[span_28](start_span)[span_28](end_span)
        HEADERS_BASE = {**HEADERS_BASE, "X-Protocol-Version": pv}[span_29](start_span)[span_29](end_span)
    print(f"[*] Versi aktif: game={VERSION} protokol={GAME_PROTOCOL}")[span_30](start_span)[span_30](end_span)
    return VERSION, GAME_PROTOCOL[span_31](start_span)[span_31](end_span)


def compute_signature(method, path, body_bytes):
    secret = APP_SECRET.encode("utf-8")[span_32](start_span)[span_32](end_span)
    h = hmac.new(secret, method.encode("utf-8"), hashlib.sha256).digest()[span_33](start_span)[span_33](end_span)
    h = hmac.new(h, path.encode("utf-8"), hashlib.sha256).digest()[span_34](start_span)[span_34](end_span)
    h = hmac.new(h, b"", hashlib.sha256).digest()[span_35](start_span)[span_35](end_span)
    h = hmac.new(h, body_bytes, hashlib.sha256).digest()[span_36](start_span)[span_36](end_span)
    h = hmac.new(h, b"x-fstr-signature", hashlib.sha256).digest()[span_37](start_span)[span_37](end_span)
    return base64.b64encode(h).decode("utf-8")[span_38](start_span)[span_38](end_span)


def varint(n):
    out = bytearray()[span_39](start_span)[span_39](end_span)
    while True:
        b = n & 0x7F[span_40](start_span)[span_40](end_span)
        n >>= 7[span_41](start_span)[span_41](end_span)
        if n:
            out.append(b | 0x80)[span_42](start_span)[span_42](end_span)
        else:
            out.append(b)[span_43](start_span)[span_43](end_span)
            return bytes(out)[span_44](start_span)[span_44](end_span)


def read_varint(buf, off):
    val = 0[span_45](start_span)[span_45](end_span)
    shift = 0[span_46](start_span)[span_46](end_span)
    i = off[span_47](start_span)[span_47](end_span)
    while i < len(buf) and shift < 35:
        b = buf[i][span_48](start_span)[span_48](end_span)
        val |= (b & 0x7F) << shift[span_49](start_span)[span_49](end_span)
        i += 1[span_50](start_span)[span_50](end_span)
        if not (b & 0x80):
            return val, i - off[span_51](start_span)[span_51](end_span)
        shift += 7[span_52](start_span)[span_52](end_span)
    return None, 0[span_53](start_span)[span_53](end_span)


def build_search_protobuf(name):
    nb = name.encode("utf-8")[span_54](start_span)[span_54](end_span)
    return b"\x08\x07\x12" + varint(len(nb)) + nb[span_55](start_span)[span_55](end_span)


def build_room_event_payload(event_name, data):
    nb = event_name.encode("utf-8")[span_56](start_span)[span_56](end_span)
    return bytes([0x08]) + varint(len(nb)) + nb + data[span_57](start_span)[span_57](end_span)


def build_event_varint(event_name, data):
    """Event (0x05) dengan nama varint — format yang sama dengan
    RoomEvent (0x08), hanya tipenya 0x05. Dipakai event 'login'."""
    nb = event_name.encode("utf-8")[span_58](start_span)[span_58](end_span)
    return bytes([0x05]) + varint(len(nb)) + nb + data[span_59](start_span)[span_59](end_span)


def build_login_protobuf(user_token):
    """Protobuf event login persis game: field 1 (0x0a) =
    USER_SESSION JWT, field 2 (0x12) = versi game '1.93.4'."""
    tb = user_token.encode("utf-8")[span_60](start_span)[span_60](end_span)
    vb = VERSION.encode("utf-8")[span_61](start_span)[span_61](end_span)
    return b"\x0a" + varint(len(tb)) + tb + b"\x12" + varint(len(vb)) + vb[span_62](start_span)[span_62](end_span)


def build_login_frame(user_token):
    payload = build_event_varint("login", build_login_protobuf(user_token))[span_63](start_span)[span_63](end_span)
    return struct.pack(">I", len(payload)) + payload[span_64](start_span)[span_64](end_span)


def tcp_connect(host, port):
    """Connect saja (tanpa join). Kembalikan (socket, player_id)
    atau (None, None). Auto-pong aktif selama jendela baca."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)[span_65](start_span)[span_65](end_span)
        s.settimeout(5)[span_66](start_span)[span_66](end_span)
        s.connect((host, port))[span_67](start_span)[span_67](end_span)
    except Exception as e:
        qlog(f"[-] Gagal konek TCP: {e}")[span_68](start_span)[span_68](end_span)
        return None, None[span_69](start_span)[span_69](end_span)
    try:
        s.sendall(struct.pack(">I", 1) + b"\x03")[span_70](start_span)[span_70](end_span)
    except Exception as e:
        qlog(f"[-] Gagal kirim Connect: {e}")[span_71](start_span)[span_71](end_span)
        s.close()[span_72](start_span)[span_72](end_span)
        return None, None[span_73](start_span)[span_73](end_span)
    resp, closed, pongs, _ = read_until(
        s, lambda fr: any(len(p) >= 5 and p[0] == 0x03 for p in fr), 6)[span_74](start_span)[span_74](end_span)
    pid = None[span_75](start_span)[span_75](end_span)
    for p in resp:
        if len(p) >= 5 and p[0] == 0x03:
            pid = int.from_bytes(p[1:5], "big")[span_76](start_span)[span_76](end_span)
    qlog(f"[*] Jendela Connect: {len(resp)} frame, pong: {pongs}")[span_77](start_span)[span_77](end_span)
    if pid is not None:
        qlog(f"[+] Connected: playerId={pid}")[span_78](start_span)[span_78](end_span)
        return s, pid[span_79](start_span)[span_79](end_span)
    qlog("[-] Tidak ada frame Connected.")[span_80](start_span)[span_80](end_span)
    s.close()[span_81](start_span)[span_81](end_span)
    return None, None[span_82](start_span)[span_82](end_span)


def tcp_join_only(s, gsid):
    """JoinRoom saja (dipakai kalau login tidak auto-join).
    Kembalikan roomId atau None."""
    room_candidates = [][span_83](start_span)[span_83](end_span)
    suffix = None[span_84](start_span)[span_84](end_span)
    if "::" in gsid:
        try:
            suffix = int(gsid.rsplit("::", 1)[1])[span_85](start_span)[span_85](end_span)
        except Exception:
            suffix = None[span_86](start_span)[span_86](end_span)
    if suffix is not None:
        room_candidates.append(suffix)[span_87](start_span)[span_87](end_span)
    for r in range(0, 4):
        if r not in room_candidates:
            room_candidates.append(r)[span_88](start_span)[span_88](end_span)
    for rid in room_candidates:
        try:
            s.sendall(build_join_frame(rid))[span_89](start_span)[span_89](end_span)
        except Exception as e:
            qlog(f"[-] Gagal kirim JoinRoom: {e}")[span_90](start_span)[span_90](end_span)
            break
        resp, closed, pongs = read_frames(s, 3)[span_91](start_span)[span_91](end_span)
        joined = None[span_92](start_span)[span_92](end_span)
        for p in resp:
            if len(p) >= 6 and p[0] == 0x06 and p[1] == 1:
                joined = int.from_bytes(p[2:6], "big")[span_93](start_span)[span_93](end_span)
        qlog(f"[*] JoinRoom({rid}): {len(resp)} frame, pong: {pongs}, "
             f"hasil={'SUKSES room ' + str(joined) if joined is not None else 'gagal'}")[span_94](start_span)[span_94](end_span)
        if closed:
            qlog("[!] Koneksi ditutup saat join.")[span_95](start_span)[span_95](end_span)
            break
        if joined is not None:
            return joined[span_96](start_span)[span_96](end_span)
    qlog("[!] Semua JoinRoom gagal.")[span_97](start_span)[span_97](end_span)
    return None[span_98](start_span)[span_98](end_span)


def scan_login_window(resp):
    """Cari balasan login di jendela baca: Event 'tbugv' dan
    JoinedRoom (0x06 success=1). Kembalikan (tbugv, room)."""
    tbugv = False[span_99](start_span)[span_99](end_span)
    room = None[span_100](start_span)[span_100](end_span)
    for p in resp:
        if len(p) >= 6 and p[0] == 0x06 and p[1] == 1:
            room = int.from_bytes(p[2:6], "big")[span_101](start_span)[span_101](end_span)
        if len(p) >= 2 and p[0] == 0x05:
            try:
                l = p[1][span_102](start_span)[span_102](end_span)
                if l < 0x80 and p[2:2 + l] == b"tbugv":
                    tbugv = True[span_103](start_span)[span_103](end_span)
            except Exception:
                pass
    return tbugv, room[span_104](start_span)[span_104](end_span)


def parse_protobuf_fields(data):
    """Parse protobuf generik -> list (field, wiretype, value).
    wiretype 0 = varint, 2 = length-delimited."""
    fields = [][span_105](start_span)[span_105](end_span)
    i = 0[span_106](start_span)[span_106](end_span)
    while i < len(data):
        tag = data[i][span_107](start_span)[span_107](end_span)
        i += 1[span_108](start_span)[span_108](end_span)
        field, wt = tag >> 3, tag & 7[span_109](start_span)[span_109](end_span)
        if wt == 0:
            v, n = read_varint(data, i)[span_110](start_span)[span_110](end_span)
            if v is None:
                break
            i += n[span_111](start_span)[span_111](end_span)
            fields.append((field, wt, v))[span_112](start_span)[span_112](end_span)
        elif wt == 2:
            ln, n = read_varint(data, i)[span_113](start_span)[span_113](end_span)
            if ln is None:
                break
            i += n[span_114](start_span)[span_114](end_span)
            fields.append((field, wt, data[i:i + ln]))[span_115](start_span)[span_115](end_span)
            i += ln[span_116](start_span)[span_116](end_span)
        else:
            break
    return fields[span_117](start_span)[span_117](end_span)


def decode_search_result(frame):
    """Decode satu frame RoomEvent 'search_friend_result' ->
    dict(success, uuid, name, field2) atau None kalau bukan
    frame hasil search."""
    if len(frame) < 2 or frame[0] != 0x08:
        return None[span_118](start_span)[span_118](end_span)
    ln, n = read_varint(frame, 1)[span_119](start_span)[span_119](end_span)
    if ln is None:
        return None[span_120](start_span)[span_120](end_span)
    if frame[1 + n:1 + n + ln] != b"search_friend_result":
        return None[span_121](start_span)[span_121](end_span)
    data = frame[1 + n + ln:][span_122](start_span)[span_122](end_span)
    out = {"success": None, "uuid": None, "name": None, "field2": None}[span_123](start_span)[span_123](end_span)
    for f, wt, v in parse_protobuf_fields(data):
        if f == 1 and wt == 0:
            out["success"] = (v == 1)[span_124](start_span)[span_124](end_span)
        elif f == 2 and wt == 2:
            try:
                out["field2"] = v.decode("utf-8", "replace")[span_125](start_span)[span_125](end_span)
            except Exception:
                pass
        elif f == 3 and wt == 2:
            for f2, wt2, v2 in parse_protobuf_fields(v):
                if f2 == 1 and wt2 == 2:
                    try:
                        out["uuid"] = v2.decode("utf-8", "replace")[span_126](start_span)[span_126](end_span)
                    except Exception:
                        pass
                elif f2 == 2 and wt2 == 2:
                    try:
                        out["name"] = v2.decode("utf-8", "replace")[span_127](start_span)[span_127](end_span)
                    except Exception:
                        pass
    return out[span_128](start_span)[span_128](end_span)


def build_room_event_varint(event_name, data):
    payload = build_room_event_payload(event_name, data)[span_129](start_span)[span_129](end_span)
    return struct.pack(">I", len(payload)) + payload[span_130](start_span)[span_130](end_span)


def build_join_frame(room_id):
    payload = bytes([0x06]) + struct.pack(">I", room_id)[span_131](start_span)[span_131](end_span)
    return struct.pack(">I", len(payload)) + payload[span_132](start_span)[span_132](end_span)


def read_frames(sock, seconds):
    deadline = time.time() + seconds[span_133](start_span)[span_133](end_span)
    buf = bytearray()[span_134](start_span)[span_134](end_span)
    frames = [][span_135](start_span)[span_135](end_span)
    closed = False[span_136](start_span)[span_136](end_span)
    pongs = 0[span_137](start_span)[span_137](end_span)
    sock.settimeout(0.5)[span_138](start_span)[span_138](end_span)
    while time.time() < deadline:
        try:
            chunk = sock.recv(65536)[span_139](start_span)[span_139](end_span)
        except socket.timeout:
            continue
        except Exception:
            break
        if chunk == b"":
            closed = True[span_140](start_span)[span_140](end_span)
            break
        buf += chunk[span_141](start_span)[span_141](end_span)
        while len(buf) >= 4:
            ln = struct.unpack(">I", buf[:4])[0][span_142](start_span)[span_142](end_span)
            if ln > 262144:
                return frames, closed, pongs[span_143](start_span)[span_143](end_span)
            if len(buf) - 4 < ln:
                break
            fr = bytes(buf[4:4 + ln])[span_144](start_span)[span_144](end_span)
            del buf[:4 + ln][span_145](start_span)[span_145](end_span)
            frames.append(fr)[span_146](start_span)[span_146](end_span)
            if fr == b"\x01":
                try:
                    sock.sendall(struct.pack(">I", 1) + b"\x02")[span_147](start_span)[span_147](end_span)
                    pongs += 1[span_148](start_span)[span_148](end_span)
                except Exception:
                    pass
    return frames, closed, pongs[span_149](start_span)[span_149](end_span)


def read_until(sock, pred, cap, slice_s=0.25):
    """Baca sampai pred(frames) True atau cap detik habis."""
    deadline = time.time() + cap[span_150](start_span)[span_150](end_span)
    buf = bytearray()[span_151](start_span)[span_151](end_span)
    frames = [][span_152](start_span)[span_152](end_span)
    closed = False[span_153](start_span)[span_153](end_span)
    pongs = 0[span_154](start_span)[span_154](end_span)
    sock.settimeout(slice_s)[span_155](start_span)[span_155](end_span)
    while time.time() < deadline:
        try:
            chunk = sock.recv(65536)[span_156](start_span)[span_156](end_span)
        except socket.timeout:
            if pred(frames):
                break
            continue
        except Exception:
            break
        if chunk == b"":
            closed = True[span_157](start_span)[span_157](end_span)
            break
        buf += chunk[span_158](start_span)[span_158](end_span)
        while len(buf) >= 4:
            ln = struct.unpack(">I", buf[:4])[0][span_159](start_span)[span_159](end_span)
            if ln > 262144:
                return frames, closed, pongs, pred(frames)[span_160](start_span)[span_160](end_span)
            if len(buf) - 4 < ln:
                break
            fr = bytes(buf[4:4 + ln])[span_161](start_span)[span_161](end_span)
            del buf[:4 + ln][span_162](start_span)[span_162](end_span)
            frames.append(fr)[span_163](start_span)[span_163](end_span)
            if fr == b"\x01":
                try:
                    sock.sendall(struct.pack(">I", 1) + b"\x02")[span_164](start_span)[span_164](end_span)
                    pongs += 1[span_165](start_span)[span_165](end_span)
                except Exception:
                    pass
        if pred(frames):
            break
    return frames, closed, pongs, pred(frames)[span_166](start_span)[span_166](end_span)


async def login_and_allocate(email, password, device_id, session):
    device_info = OrderedDict([
        ("userAgent", ""), ("platform", "DESKTOP"), ("system", "WindowsPlayer"),
        ("systemVersion", "Windows 10  (10.0.19045) 64bit"),
        ("manufacturer", "Unknown"), ("deviceModel", ""), ("appVersion", VERSION),
        ("sdkVersion", ""), ("vendorId", ""), ("advertisingId", ""),
        ("pushToken", ""), ("cloudId", ""), ("carrierId", ""),
        ("timezone", "SE Asia Standard Time"), ("language", "EN"),
        ("country", "Unknown"), ("deviceId", device_id),
    ])[span_167](start_span)[span_167](end_span)
    p1 = OrderedDict([("identityProvider", "FASTER"),
                      ("credentials", {"type": "EMAIL", "email": email, "password": password}),
                      ("deviceInfo", device_info)])[span_168](start_span)[span_168](end_span)
    b_p1 = json.dumps(p1, separators=(",", ":"), ensure_ascii=False).encode("utf-8")[span_169](start_span)[span_169](end_span)
    h1 = HEADERS_BASE.copy()[span_170](start_span)[span_170](end_span)
    h1["x-fstr-application-key"] = APP_KEY[span_171](start_span)[span_171](end_span)
    h1["x-fstr-signature"] = compute_signature("POST", "/v1/auth/sessions", b_p1)[span_172](start_span)[span_172](end_span)
    r1 = await session.post(ACCOUNT_URL + "/v1/auth/sessions", data=b_p1, headers=h1, timeout=20)[span_173](start_span)[span_173](end_span)
    if r1.status_code != 200:
        qlog(f"[-] auth/sessions gagal: {r1.status_code}")[span_174](start_span)[span_174](end_span)
        return None, None[span_175](start_span)[span_175](end_span)
    auth_token = r1.json().get("authToken")[span_176](start_span)[span_176](end_span)
    await asyncio.sleep(random.uniform(0.1, 0.3))[span_177](start_span)[span_177](end_span)
    p2 = OrderedDict([("authToken", auth_token), ("deviceInfo", device_info)])[span_178](start_span)[span_178](end_span)
    b_p2 = json.dumps(p2, separators=(",", ":"), ensure_ascii=False).encode("utf-8")[span_179](start_span)[span_179](end_span)
    h2 = HEADERS_BASE.copy()[span_180](start_span)[span_180](end_span)
    h2["x-fstr-application-key"] = APP_KEY[span_181](start_span)[span_181](end_span)
    h2["x-fstr-signature"] = compute_signature("POST", "/v1/users/sessions", b_p2)[span_182](start_span)[span_182](end_span)
    r2 = await session.post(ACCOUNT_URL + "/v1/users/sessions", data=b_p2, headers=h2, timeout=20)[span_183](start_span)[span_183](end_span)
    user_jwt = r2.json().get("userToken")[span_184](start_span)[span_184](end_span)
    if not user_jwt:
        qlog("[-] users/sessions tidak mengembalikan userToken")[span_185](start_span)[span_185](end_span)
        return None, None[span_186](start_span)[span_186](end_span)
    qlog("[+] Login OK, JWT didapat.")[span_187](start_span)[span_187](end_span)
    await asyncio.sleep(random.uniform(0.1, 0.3))[span_188](start_span)[span_188](end_span)

    # --- Bagian Alokasi dengan Retry & Backoff saat Rate Limit ---
    p3 = {"userToken": user_jwt, "gameServerProtocolVersion": GAME_PROTOCOL, "isPrivate": False}[span_189](start_span)[span_189](end_span)
    b_p3 = json.dumps(p3, separators=(",", ":"), ensure_ascii=False).encode("utf-8")[span_190](start_span)[span_190](end_span)

    body = None
    for attempt in range(1, 4):
        try:
            r3 = await session.post(LOBBY_URL + "/lobby/allocation", data=b_p3,
                                    headers=HEADERS_BASE.copy(), timeout=20)[span_191](start_span)[span_191](end_span)
            qlog(f"[*] allocation status (attempt {attempt}): {r3.status_code}")
            if r3.status_code == 200:
                try:
                    body = r3.json()[span_192](start_span)[span_192](end_span)
                except Exception:
                    body = {}[span_193](start_span)[span_193](end_span)
                break
            elif r3.status_code == 429:
                # Lobby rate-limit: tunggu backoff 2.5 - 4.5 detik
                qlog(f"[!] Lobby 429 Rate Limit (attempt {attempt}/3), jeda sejenak...")
                await asyncio.sleep(random.uniform(2.5, 4.5))
            else:
                # Error server lainnya (misal 502/503/504)
                await asyncio.sleep(1.0)
        except Exception as e:
            qlog(f"[-] Request allocation error: {e}")
            await asyncio.sleep(1.0)

    return user_jwt, body[span_194](start_span)[span_194](end_span)


def find_host_port(body):
    host = port = None[span_195](start_span)[span_195](end_span)

    def ok_host(v):
        return isinstance(v, str) and v and "::" not in v and " " not in v[span_196](start_span)[span_196](end_span)

    def scan(d):
        nonlocal host, port
        if not isinstance(d, dict):
            return[span_197](start_span)[span_197](end_span)
        for k, v in d.items():
            lk = k.lower()[span_198](start_span)[span_198](end_span)
            if host is None and lk in ("gameserveraddress", "address",
                                       "host", "hostname", "ip") and ok_host(v):[span_199](start_span)[span_199](end_span)
                host = v[span_200](start_span)[span_200](end_span)
            if port is None and lk in ("gameserverport", "port",
                                       "tcpport") and isinstance(v, int):[span_201](start_span)[span_201](end_span)
                port = v[span_202](start_span)[span_202](end_span)
        for v in d.values():
            if isinstance(v, dict):
                scan(v)[span_203](start_span)[span_203](end_span)
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        scan(item)[span_204](start_span)[span_204](end_span)

    scan(body)[span_205](start_span)[span_205](end_span)
    return host, port[span_206](start_span)[span_206](end_span)


def resolve_name(argv):
    """Nama dari argumen: 'Nama#Kode' atau 'Nama Kode' (digabung
    '#'). Kalau tidak ada argumen: env SEARCH_NAME, kalau tidak
    ada juga: prompt input()."""
    args = argv[1:][span_207](start_span)[span_207](end_span)
    if len(args) >= 2:
        return args[0] + "#" + args[1][span_208](start_span)[span_208](end_span)
    if len(args) == 1:
        return args[0][span_209](start_span)[span_209](end_span)
    env = os.getenv("SEARCH_NAME", "")[span_210](start_span)[span_210](end_span)
    if env:
        return env[span_211](start_span)[span_211](end_span)
    return input("Nama pemain (format Nama#Kode, mis. Admin#000): ").strip()[span_212](start_span)[span_212](end_span)


async def lookup_once(session, bot, name):
    """Satu percobaan penuh (login s.d. search). Kembalikan dict
    hasil decode atau None."""
    user_jwt, alloc = await login_and_allocate(
        bot["email"], bot["password"], bot["device_id"], session)[span_213](start_span)[span_213](end_span)
    if not alloc:
        print("[-] Allocation gagal.")[span_214](start_span)[span_214](end_span)
        return None[span_215](start_span)[span_215](end_span)
    host, port = find_host_port(alloc)[span_216](start_span)[span_216](end_span)
    gsid = alloc.get("gameServerId", "")[span_217](start_span)[span_217](end_span)
    suffix = gsid.rsplit("::", 1)[1] if "::" in gsid else "?[span_218](start_span)"[span_218](end_span)
    print(f"[*] Server: {host}:{port} (room suffix {suffix})")[span_219](start_span)[span_219](end_span)
    if not host or not port:
        print("[-] Host/port tidak ketemu di allocation.")[span_220](start_span)[span_220](end_span)
        return None[span_221](start_span)[span_221](end_span)
    s, pid = tcp_connect(host, port)[span_222](start_span)[span_222](end_span)
    if s is None:
        return None[span_223](start_span)[span_223](end_span)
    try:
        s.sendall(build_login_frame(user_jwt))[span_224](start_span)[span_224](end_span)
        print("[+] Event login terkirim.")[span_225](start_span)[span_225](end_span)
    except Exception as e:
        print(f"[-] Gagal kirim event login: {e}")[span_226](start_span)[span_226](end_span)
        s.close()[span_227](start_span)[span_227](end_span)
        return None[span_228](start_span)[span_228](end_span)

    def do_search():
        frame = build_room_event_varint("friends", build_search_protobuf(name))[span_229](start_span)[span_229](end_span)
        try:
            s.sendall(frame)[span_230](start_span)[span_230](end_span)
            s.sendall(frame)[span_231](start_span)[span_231](end_span)
        except Exception:
            return None, True[span_232](start_span)[span_232](end_span)

        def _pred(fr):
            for p in fr:
                dec = decode_search_result(p)[span_233](start_span)[span_233](end_span)
                if dec is not None and dec["success"] and dec["uuid"]:[span_234](start_span)[span_234](end_span)
                    return True[span_235](start_span)[span_235](end_span)
            return False[span_236](start_span)[span_236](end_span)

        resp, closed, _, _ = read_until(s, _pred, 12)[span_237](start_span)[span_237](end_span)
        for p in resp:
            dec = decode_search_result(p)[span_238](start_span)[span_238](end_span)
            if dec is not None:
                print(f"    decode: success={dec['success']} uuid={dec['uuid']} "
                      f"nama={dec['name']!r}")[span_239](start_span)[span_239](end_span)
                if dec["success"] and dec["uuid"]:[span_240](start_span)[span_240](end_span)
                    return dec, closed[span_241](start_span)[span_241](end_span)
        return None, closed[span_242](start_span)[span_242](end_span)

    print(f"[*] Cari: {name} ...")[span_243](start_span)[span_243](end_span)
    hit, closed = do_search()[span_244](start_span)[span_244](end_span)
    if hit is None and not closed:
        print("[*] Search langsung kosong — fallback: tunggu 6 dtk lalu search ulang.")[span_245](start_span)[span_245](end_span)
        resp, closed, _ = read_frames(s, 6)[span_246](start_span)[span_246](end_span)
        for p in resp:
            dec = decode_search_result(p)[span_247](start_span)[span_247](end_span)
            if dec is not None and dec["success"] and dec["uuid"]:[span_248](start_span)[span_248](end_span)
                hit = dec[span_249](start_span)[span_249](end_span)
                break
        if hit is None and not closed:
            hit, closed = do_search()[span_250](start_span)[span_250](end_span)
    if hit is None and not closed:
        print("[*] Search masih kosong — fallback: join lalu search lagi.")[span_251](start_span)[span_251](end_span)
        resp, closed, pongs, _ = read_until(
            s, lambda fr: scan_login_window(fr)[1] is not None, 5)[span_252](start_span)[span_252](end_span)
        tbugv, room = scan_login_window(resp)[span_253](start_span)[span_253](end_span)
        print(f"[*] Login: tbugv={'YA' if tbugv else 'tidak'}, "
              f"auto-join={'room ' + str(room) if room is not None else 'tidak'}")[span_254](start_span)[span_254](end_span)
        if not closed:
            for p in resp:
                dec = decode_search_result(p)[span_255](start_span)[span_255](end_span)
                if dec is not None and dec["success"] and dec["uuid"]:[span_256](start_span)[span_256](end_span)
                    hit = dec[span_257](start_span)[span_257](end_span)
                    break
            if hit is None:
                if room is None:
                    room = tcp_join_only(s, gsid)[span_258](start_span)[span_258](end_span)
                if room is not None:
                    print(f"[+] Bergabung room {room}.")[span_259](start_span)[span_259](end_span)
                    hit, closed = do_search()[span_260](start_span)[span_260](end_span)
                else:
                    print("[!] Join gagal di instance ini.")[span_261](start_span)[span_261](end_span)
    if hit is None and not closed:
        print("[*] Search masih kosong — fallback: friends-kosong lalu search lagi.")[span_262](start_span)[span_262](end_span)
        try:
            s.sendall(build_room_event_varint("friends", b""))[span_263](start_span)[span_263](end_span)
            read_frames(s, 6)[span_264](start_span)[span_264](end_span)
            hit, closed = do_search()[span_265](start_span)[span_265](end_span)
        except Exception:
            pass
    s.close()[span_266](start_span)[span_266](end_span)
    return hit[span_267](start_span)[span_267](end_span)


def probe_bot():
    return {
        "email": os.getenv("PROBE_EMAIL", "") or "pkxdshop.id@gmail.com",
        "password": os.getenv("PROBE_PASSWORD", "") or "Kontol@161",
        "device_id": os.getenv("PROBE_DEVICE_ID", "") or "9f31c2d4-probe-device",
    }[span_268](start_span)[span_268](end_span)


async def main():
    name = resolve_name(sys.argv)[span_269](start_span)[span_269](end_span)
    if not name:
        print("[!] Nama pemain wajib diisi.")[span_270](start_span)[span_270](end_span)
        sys.exit(1)[span_271](start_span)[span_271](end_span)
    bot = probe_bot()[span_272](start_span)[span_272](end_span)
    print(f"[*] Cari UUID untuk: {name}")[span_273](start_span)[span_273](end_span)
    t0 = time.time()[span_274](start_span)[span_274](end_span)
    async with AsyncSession(impersonate="chrome110", verify=False) as session:[span_275](start_span)[span_275](end_span)
        for attempt in range(1, 4):[span_276](start_span)[span_276](end_span)
            print(f"[*] --- Percobaan ke-{attempt} ---")[span_277](start_span)[span_277](end_span)
            hit = await lookup_once(session, bot, name)[span_278](start_span)[span_278](end_span)
            if hit:
                dt = time.time() - t0[span_279](start_span)[span_279](end_span)
                print("=" * 48)[span_280](start_span)[span_280](end_span)
                print(f"HASIL: {name} -> {hit['uuid']}")[span_281](start_span)[span_281](end_span)
                print(f"Nama kanonis: {hit['name']}")[span_282](start_span)[span_282](end_span)
                print(f"Waktu total: {dt:.0f} detik")[span_283](start_span)[span_283](end_span)
                print("=" * 48)[span_284](start_span)[span_284](end_span)
                try:
                    with open("uuid_lookup_log.txt", "a", encoding="utf-8") as f:[span_285](start_span)[span_285](end_span)
                        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | "
                                f"{name} -> {hit['uuid']} | {hit['name']}\n")[span_286](start_span)[span_286](end_span)
                    print("[*] Dicatat ke uuid_lookup_log.txt")[span_287](start_span)[span_287](end_span)
                except Exception as e:
                    print(f"[-] Gagal tulis log file: {e}")[span_288](start_span)[span_288](end_span)
                return
            print(f"[-] Percobaan ke-{attempt} belum berhasil.")[span_289](start_span)[span_289](end_span)
    print(f"[-] GAGAL: UUID untuk {name} tidak ditemukan "
          f"({time.time() - t0:.0f} detik, 3 percobaan).")[span_290](start_span)[span_290](end_span)


if __name__ == "__main__":
    asyncio.run(main())[span_291](start_span)[span_291](end_span)
