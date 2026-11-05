"""
like_profile_probe.py v2 — Uji coba like profil PK XD via TCP (OPTIMAL).
Diperbarui dengan pelaporan error diagnostik eksplisit pada kegagalan tahap awal.
"""

import sys
import os
import json
import time
import socket
import struct
import asyncio
import urllib.request
import urllib.parse
import urllib.error

import lookup_uuid as L

TARGET_DEFAULT = "aae2efe1-1abb-48c2-8799-ea192692a49c"
LIKE_TYPE_PLAYER = 1

VERBOSE = os.getenv("LIKE_VERBOSE", "0") == "1"


def vlog(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)


HISTORY_OK = None


def build_like_protobuf(event_id, liked_uuid, like_type=LIKE_TYPE_PLAYER):
    ub = liked_uuid.encode("utf-8")
    return (b"\x08" + L.varint(event_id)
            + b"\x12" + L.varint(len(ub)) + ub
            + b"\x18" + L.varint(like_type))


def decode_room_event_data(frame, expected_name):
    if len(frame) < 2 or frame[0] != 0x08:
        return None
    ln, n = L.read_varint(frame, 1)
    if ln is None:
        return None
    if frame[1 + n:1 + n + ln] != expected_name.encode("utf-8"):
        return None
    return frame[1 + n + ln:]


def decode_create_like_result(frame):
    data = decode_room_event_data(frame, "create_like_result")
    if data is None:
        return None
    out = {"liked_user": None, "error": None, "like_type": None}
    for f, wt, v in L.parse_protobuf_fields(data):
        if f == 1 and wt == 2:
            out["liked_user"] = v.decode("utf-8", "replace")
        elif f == 2 and wt == 2:
            out["error"] = v.decode("utf-8", "replace")
        elif f == 3 and wt == 0:
            out["like_type"] = v
    return out


def decode_is_there_result(frame):
    data = decode_room_event_data(frame, "is_there_like_result")
    if data is None:
        return None
    out = {"exists": None, "liked_user": None, "error": None, "like_type": None}
    for f, wt, v in L.parse_protobuf_fields(data):
        if f == 1 and wt == 0:
            out["exists"] = (v == 1)
        elif f == 2 and wt == 2:
            out["liked_user"] = v.decode("utf-8", "replace")
        elif f == 3 and wt == 2:
            out["error"] = v.decode("utf-8", "replace")
        elif f == 4 and wt == 0:
            out["like_type"] = v
    if out["exists"] is None:
        out["exists"] = False
    return out


class FrameReader:
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


def _supabase():
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    return url.rstrip("/"), key


def get_bots(target_uuid, limit=8):
    url, key = _supabase()
    if not url or not key:
        print("[-] SUPABASE_URL / SUPABASE_SERVICE_KEY tidak ada di env.")
        return []
    _rpc = "get_fresh_named_bots" if os.getenv("BOT_TYPE", "accounts") == "named_bots" else "get_fresh_bots"
    req = urllib.request.Request(
        url + f"/rest/v1/rpc/{_rpc}",
        data=json.dumps({"p_target_uuid": target_uuid, "p_limit": limit}).encode("utf-8"),
        headers={"apikey": key, "Authorization": "Bearer " + key, "Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            rows = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[-] get_fresh_bots HTTP {e.code}")
        return []
    except Exception as e:
        print(f"[-] get_fresh_bots gagal: {e}")
        return []
    bots = [b for b in rows if b.get("email") and b.get("password") and b.get("device_id")]
    print(f"[*] {len(bots)} bot didapat dari Supabase.")
    return bots


def get_profile_bots(target_uuid, limit=8):
    url, key = _supabase()
    if not url or not key:
        print("[-] SUPABASE_URL / SUPABASE_SERVICE_KEY tidak ada di env.")
        return []
    _rpc = ("get_fresh_named_profile_like_bots"
            if os.getenv("BOT_TYPE", "accounts") == "named_bots"
            else "get_fresh_profile_like_bots")
    req = urllib.request.Request(
        url + f"/rest/v1/rpc/{_rpc}",
        data=json.dumps({"p_target_uuid": target_uuid, "p_limit": limit}).encode("utf-8"),
        headers={"apikey": key, "Authorization": "Bearer " + key, "Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            rows = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[-] {_rpc} HTTP {e.code}")
        return []
    except Exception as e:
        print(f"[-] {_rpc} gagal: {e}")
        return []
    bots = [b for b in rows if b.get("email") and b.get("password") and b.get("device_id")]
    print(f"[*] {len(bots)} bot fresh profil didapat dari Supabase (RPC {_rpc}, limit {limit}).")
    return bots


def get_bots_page(target_uuid, limit=5000, offset=0):
    if limit <= 0:
        return []
    if offset < 0:
        offset = 0
    rows = get_bots(target_uuid, limit=offset + limit)
    return rows[offset:offset + limit]


def history_get(bot_email, target_uuid):
    global HISTORY_OK
    if HISTORY_OK is False:
        return None
    url, key = _supabase()
    if not url or not key:
        return None
    q = ("/rest/v1/profile_like_history?select=bot_email"
         "&bot_email=eq." + urllib.parse.quote(bot_email, safe="")
         + "&target_uuid=eq." + urllib.parse.quote(target_uuid, safe=""))
    req = urllib.request.Request(url + q, headers={"apikey": key, "Authorization": "Bearer " + key}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            rows = json.loads(r.read().decode("utf-8"))
        HISTORY_OK = True
        return len(rows) > 0
    except urllib.error.HTTPError as e:
        if e.code == 404:
            HISTORY_OK = False
        return None
    except Exception:
        return None


def history_put(bot_email, target_uuid):
    global HISTORY_OK
    if HISTORY_OK is False:
        return
    url, key = _supabase()
    if not url or not key:
        return
    req = urllib.request.Request(
        url + "/rest/v1/profile_like_history",
        data=json.dumps({"bot_email": bot_email, "target_uuid": target_uuid, "source": "probe"}).encode("utf-8"),
        headers={"apikey": key, "Authorization": "Bearer " + key, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15):
            pass
        HISTORY_OK = True
    except Exception:
        pass


async def like_once(session, bot, target_uuid):
    user_jwt, alloc = await L.login_and_allocate(
        bot["email"], bot["password"], bot["device_id"], session
    )
    if not user_jwt:
        return {"err": "auth_failed"}  # 401 Unauthorized / 429 Rate Limit HTTP[span_5](start_span)[span_5](end_span)
    if not alloc:
        return {"err": "alloc_failed"} # Gagal alokasi lobby[span_6](start_span)[span_6](end_span)

    host, port = L.find_host_port(alloc)
    if not host or not port:
        return {"err": "no_host_port"}

    s, pid = L.tcp_connect(host, port)
    if s is None:
        return {"err": "tcp_connect_failed"} # Socket TCP ditolak/RST oleh server[span_7](start_span)[span_7](end_span)

    reader = FrameReader(s)
    try:
        s.sendall(L.build_login_frame(user_jwt))
        vlog("[+] Event login terkirim.")
    except Exception as e:
        vlog(f"[-] Gagal kirim event login: {e}")
        s.close()
        return {"err": "send_login_failed"}

    # Event-Driven Barrier: Tunggu respons login awal sebelum Create[span_8](start_span)[span_8](end_span)
    reader.read_until(lambda f: len(f) >= 2 and f[0] == 0x05, 3.0)

    def do_is_there():
        frame = L.build_room_event_varint("like", build_like_protobuf(2, target_uuid))
        try:
            s.sendall(frame)
            s.sendall(frame)
        except Exception:
            return None
        fr, closed = reader.read_until(lambda f: decode_is_there_result(f) is not None, 8)
        if fr is None:
            return None
        dec = decode_is_there_result(fr)
        vlog(f"    is_there: exists={dec['exists']} liked_user={dec['liked_user']} error={dec['error']!r}")
        return dec

    def do_create_verify():
        vlog("[*] Kirim Create like (SEKALI) ...")
        try:
            s.sendall(L.build_room_event_varint("like", build_like_protobuf(0, target_uuid)))
        except Exception:
            return None, False, False

        # Non-blocking read pengganti time.sleep(2) agar pong tetap dijawab[span_9](start_span)[span_9](end_span)
        reader.read_until(lambda f: False, 2.0)

        vlog("[*] Cek IsThere sesudah Create ...")
        after = do_is_there()
        answered = after is not None
        if not (after and after["exists"] is True):
            vlog("[*] IsThere-sesudah belum True — ulang sekali (3 dtk) ...")
            reader.read_until(lambda f: False, 3.0)
            after = do_is_there()
            answered = answered or after is not None
        return after, True, answered

    after, _terkirim, answered = do_create_verify()
    s.close()
    if after and after["exists"] is True:
        history_put(bot["email"], target_uuid)
    return {"already": False, "before": None, "after": after, "answered": answered}
