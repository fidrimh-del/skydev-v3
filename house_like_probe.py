"""
house_like_probe.py — Uji coba like RUMAH PK XD via TCP (sekali jalan).

Revisi Stabilitas:
  - Non-blocking asyncio.sleep (mencegah event loop membeku).
  - Soket di-close secara agresif dengan shutdown(SHUT_RDWR) di blok finally.
  - Timeout IsThere dipangkas (4 detik) agar bot gagal cepat dan tidak menahan thread (anti-22d).
  - Mode cepat tanpa IsThere ulang jika server tidak menjawab query pertama.
"""

import asyncio
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import like_profile_probe as P
import lookup_uuid as L

TARGET_DEFAULT = "aae2efe1-1abb-48c2-8799-ea192692a49c"
LIKE_TYPE_HOUSE = 0
HOUSE_HISTORY_OK = None


def history_get_house(bot_email, target_uuid):
    global HOUSE_HISTORY_OK
    if HOUSE_HISTORY_OK is False:
        return None
    url, key = P._supabase()
    if not url or not key:
        return None
    q = (
        "/rest/v1/house_like_history?select=bot_email"
        "&bot_email=eq."
        + urllib.parse.quote(bot_email, safe="")
        + "&target_uuid=eq."
        + urllib.parse.quote(target_uuid, safe="")
    )
    req = urllib.request.Request(
        url + q,
        headers={"apikey": key, "Authorization": "Bearer " + key},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            rows = json.loads(r.read().decode("utf-8"))
        HOUSE_HISTORY_OK = True
        return len(rows) > 0
    except urllib.error.HTTPError as e:
        if e.code == 404:
            HOUSE_HISTORY_OK = False
        return None
    except Exception:
        return None


def history_put_house(bot_email, target_uuid, source="probe"):
    global HOUSE_HISTORY_OK
    if HOUSE_HISTORY_OK is False:
        return
    url, key = P._supabase()
    if not url or not key:
        return
    req = urllib.request.Request(
        url + "/rest/v1/house_like_history",
        data=json.dumps({
            "bot_email": bot_email,
            "target_uuid": target_uuid,
            "source": source,
        }).encode("utf-8"),
        headers={
            "apikey": key,
            "Authorization": "Bearer " + key,
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15):
            pass
        HOUSE_HISTORY_OK = True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            HOUSE_HISTORY_OK = False
    except Exception:
        pass


def get_house_bots(target_uuid, limit=8, partisi=None, jumlah_partisi=None):
    url, key = P._supabase()
    if not url or not key:
        print("[-] SUPABASE_URL / SUPABASE_SERVICE_KEY tidak ada di env.")
        return []
    _base = (
        "get_fresh_named_house_like_bots"
        if os.getenv("BOT_TYPE", "accounts") == "named_bots"
        else "get_fresh_house_like_bots"
    )
    _rpc = _base + ("_part" if partisi is not None else "")
    payload = {"p_target_uuid": target_uuid, "p_limit": limit}
    if partisi is not None:
        payload["p_partisi"] = partisi
        payload["p_jumlah_partisi"] = jumlah_partisi
    req = urllib.request.Request(
        url + f"/rest/v1/rpc/{_rpc}",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "apikey": key,
            "Authorization": "Bearer " + key,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            rows = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[-] {_rpc} gagal: {e}")
        return []
    bots = [
        b
        for b in rows
        if b.get("email") and b.get("password") and b.get("device_id")
    ]
    print(
        f"[*] {len(bots)} bot fresh rumah didapat dari Supabase (RPC {_rpc},"
        f" limit {limit})."
    )
    return bots


async def like_house_once(session, bot, target_uuid):
    user_jwt, alloc = await L.login_and_allocate(
        bot["email"], bot["password"], bot["device_id"], session
    )
    if not alloc:
        return None

    host, port = L.find_host_port(alloc)
    gsid = alloc.get("gameServerId", "")
    if not host or not port:
        return None

    s, pid = L.tcp_connect(host, port)
    if s is None:
        return None

    reader = P.FrameReader(s)
    try:
        s.sendall(L.build_login_frame(user_jwt))
    except Exception:
        _safe_close(s)
        return None

    _jstate = {"room": None}

    def _login_pred(fr):
        t, r = L.scan_login_window([fr])
        if r is not None:
            _jstate["room"] = r
        return _jstate["room"] is not None

    try:
        fr, closed = reader.read_until(_login_pred, 4)
        if closed:
            _safe_close(s)
            return None

        room = _jstate["room"]
        if room is None:
            room = L.tcp_join_only(s, gsid)
        if room is None:
            _safe_close(s)
            return None

        def do_is_there_house():
            frame = L.build_room_event_varint(
                "like", P.build_like_protobuf(2, target_uuid, LIKE_TYPE_HOUSE)
            )
            try:
                s.sendall(frame)
                s.sendall(frame)
            except Exception:
                return None
            fr, closed = reader.read_until(
                lambda f: P.decode_is_there_result(f) is not None, 4
            )
            if fr is None:
                return None
            return P.decode_is_there_result(fr)

        # 1. Kirim event create like rumah
        try:
            s.sendall(
                L.build_room_event_varint(
                    "like",
                    P.build_like_protobuf(0, target_uuid, LIKE_TYPE_HOUSE),
                )
            )
        except Exception:
            _safe_close(s)
            return None

        # Jeda asinkron agar server memproses like tanpa membekukan event loop
        await asyncio.sleep(1.2)

        # 2. Cek verifikasi IsThere
        after = do_is_there_house()
        answered = after is not None

        if not (after and after.get("exists") is True):
            await asyncio.sleep(1.0)
            after = do_is_there_house()
            answered = answered or (after is not None)

        if after and after.get("exists") is True:
            history_put_house(bot["email"], target_uuid)

        return {
            "already": False,
            "before": None,
            "after": after,
            "answered": answered,
        }

    finally:
        _safe_close(s)


def _safe_close(sock):
    """Menutup soket TCP secara agresif agar tidak meninggalkan zombie connection."""
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except Exception:
        pass
    try:
        sock.close()
    except Exception:
        pass


async def main():
    P.VERBOSE = True
    L.QUIET = False
    L.apply_panel_versions()
    target = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.getenv("TARGET_UUID", TARGET_DEFAULT)
    )
    print(f"[*] Target like RUMAH (UUID pemilik): {target}")
    bots = P.get_bots(target, limit=8)
    if not bots:
        print("[!] Tidak ada bot — berhenti.")
        return 1
    t0 = time.time()
    sent = 0
    skipped = 0
    async with L.AsyncSession(impersonate="chrome110", verify=False) as session:
        for bi, bot in enumerate(bots, 1):
            if history_get_house(bot["email"], target) is True:
                skipped += 1
                continue
            tb = time.time()
            for attempt in range(1, 3):
                res = await like_house_once(session, bot, target)
                if res is None:
                    continue
                if res.get("already"):
                    skipped += 1
                    break
                after = res.get("after") or {}
                if after.get("exists") is True:
                    sent += 1
                    print(
                        f"[+] LIKE RUMAH TERKIRIM (bot {bi},"
                        f" {time.time() - tb:.1f} detik)."
                    )
                break
            if sent:
                print(
                    f"HASIL: Sukses 1 like dalam {time.time() - t0:.1f} detik."
                )
                return 0
    return 0 if skipped > 0 and sent == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
