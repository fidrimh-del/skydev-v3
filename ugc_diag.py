"""UGC Diagnostic Run — rekam SEMUA frame pasca-join + tiru kiriman klien game.

Dua mode:
  - Activity (default): `python -u ugc_diag.py [activity_id]`
    Login -> alokasi ACTIVITY -> TCP -> auto-join -> Analytics ->
    activity/get -> amati.
  - Kontrol NORMAL: `python -u ugc_diag.py normal`
    Login -> alokasi NORMAL (default) -> TCP -> auto-join -> Analytics ->
    amati 25 dtk TANPA activity/get. Membandingkan apakah tutup ~4,5 dtk
    juga terjadi di room normal (berarti soal perilaku klien) atau hanya
    di activity room (berarti spesifik activity).

Tujuan: melihat apa yang dikirim server sebelum menutup koneksi bot di
activity room (v1: tutup sesudah auto-join; v2: tutup ~3,5 dtk sesudah
activity/get tanpa balasan; diag activity: tutup T+4,49 dtk di fase B).

Ping (0x01) hanya dihitung, tidak dicetak per frame, agar log terbaca.
Token/JWT TIDAK PERNAH dicetak ke log.
"""

import asyncio
import json
import socket
import struct
import sys
import time
import uuid

import lookup_uuid as LU
from curl_cffi.requests import AsyncSession

ACTIVITY_ID_DEFAULT = "3e7cec3f-1ae9-4ba2-a19b-a6339e0f9b23"  # UGC Bartes
MODE_NORMAL = len(sys.argv) > 1 and sys.argv[1] == "normal"


def describe(fr):
    if fr == b"\x01":
        return "PING"
    if fr == b"\x02":
        return "PONG"
    if not fr:
        return "KOSONG"
    t = fr[0]
    if t == 0x03:
        pid = int.from_bytes(fr[1:5], "big") if len(fr) >= 5 else None
        return f"CONNECTED playerId={pid}"
    if t == 0x06:
        return f"ROOMSYNC len={len(fr)} head={fr[:12].hex()}"
    if t in (0x05, 0x08):
        ln, n = LU.read_varint(fr, 1)
        if ln is not None and 1 + n + ln <= len(fr):
            try:
                name = fr[1 + n:1 + n + ln].decode("utf-8")
            except Exception:
                name = "?"
            kind = "EVENT" if t == 0x05 else "ROOMEVENT"
            extra = ""
            if name in ("tbugv", "tbulv"):
                extra = f" payload={fr[1 + n + ln:][:24].hex()}"
            return f"{kind} name={name} len={len(fr)}{extra}"
        return f"TYPE{t:02x} len={len(fr)}"
    return f"TYPE{t:02x} len={len(fr)} head={fr[:16].hex()}"


def diag_read(s, seconds, t0, tag):
    """Baca frame selama `seconds`, cetak tiap frame non-ping + hitung ping.
    Kembalikan (frames, closed, ping_count). Auto-pong aktif."""
    deadline = time.time() + seconds
    buf = bytearray()
    frames = []
    closed = False
    pings = 0
    s.settimeout(0.5)
    while time.time() < deadline:
        try:
            chunk = s.recv(65536)
        except socket.timeout:
            continue
        except Exception:
            break
        if chunk == b"":
            closed = True
            break
        buf += chunk
        while len(buf) >= 4:
            ln = struct.unpack(">I", buf[:4])[0]
            if ln > 262144:
                return frames, True, pings
            if len(buf) - 4 < ln:
                break
            fr = bytes(buf[4:4 + ln])
            del buf[:4 + ln]
            frames.append(fr)
            if fr == b"\x01":
                pings += 1
                try:
                    s.sendall(struct.pack(">I", 1) + b"\x02")
                except Exception:
                    pass
            else:
                print(f"DIAG T+{time.time() - t0:6.2f}s [{tag}] {describe(fr)}",
                      flush=True)
    if pings:
        print(f"DIAG T+{time.time() - t0:6.2f}s [{tag}] (ping x{pings})", flush=True)
    return frames, closed, pings


async def main():
    activity_id = None if MODE_NORMAL else (
        sys.argv[1] if len(sys.argv) > 1 else ACTIVITY_ID_DEFAULT)
    bot = LU.probe_bot()
    print(f"[*] Mode: {'NORMAL (kontrol)' if MODE_NORMAL else 'ACTIVITY'}"
          + ("" if MODE_NORMAL else f", target={activity_id}"))
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        user_jwt, alloc = await LU.login_and_allocate(
            bot["email"], bot["password"], bot["device_id"], session)
        if not alloc:
            print("HASIL DIAG: GAGAL login/alokasi normal.")
            return 1
        if MODE_NORMAL:
            host, port = LU.find_host_port(alloc)
            gsid = alloc.get("gameServerId", "")
            print(f"[*] Server normal: {host}:{port} ({gsid})")
        else:
            dc = alloc.get("gameServerDatacenterId")
            body_act = {"userToken": user_jwt, "activityId": activity_id,
                        "datacenterId": dc,
                        "gameServerProtocolVersion": LU.GAME_PROTOCOL}
            r = await session.post(
                LU.LOBBY_URL + "/lobby/allocation",
                data=json.dumps(body_act, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
                headers=LU.HEADERS_BASE.copy(), timeout=20)
            try:
                act = r.json()
            except Exception:
                print("HASIL DIAG: GAGAL alokasi activity (bukan JSON).")
                return 1
            if not act.get("successful"):
                print(f"HASIL DIAG: GAGAL alokasi activity: {act.get('failureDescription')}")
                return 1
            host, port = LU.find_host_port(act)
            gsid = act.get("gameServerId", "")
            print(f"[*] Server activity: {host}:{port} ({gsid})")

    s, pid = LU.tcp_connect(host, port)
    if s is None:
        print("HASIL DIAG: GAGAL konek TCP.")
        return 1
    t0 = time.time()
    t_close = None
    try:
        s.sendall(LU.build_login_frame(user_jwt))
        print("[+] Event login terkirim.")
    except Exception as e:
        print(f"[-] Gagal kirim event login: {e}")
        s.close()
        return 1

    # Fase A: sampai tbugv+room (maks 8 dtk).
    tbugv, room, closed = False, None, False
    for _ in range(8):
        resp, closed, _ = diag_read(s, 1, t0, "A-login")
        tb, rm = LU.scan_login_window(resp)
        tbugv = tbugv or tb
        if rm is not None:
            room = rm
        if tbugv and room is not None:
            break
        if closed:
            break
    if closed and t_close is None:
        t_close = time.time()
    print(f"[*] Fase A: tbugv={tbugv}, room={room}, closed={closed}")
    if room is None and not closed:
        room = LU.tcp_join_only(s, gsid)
        print(f"[*] Fallback JoinRoom: room={room}")
    if room is None:
        print("HASIL DIAG: GAGAL join.")
        s.close()
        return 1

    # Fase B: tiru klien game (Analytics), amati 5 dtk.
    session_id = str(uuid.uuid4())
    device_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, bot["email"]))
    try:
        s.sendall(LU.build_room_event_varint("Analytics/sessionId",
                                             session_id.encode()))
        s.sendall(LU.build_room_event_varint("Analytics/deviceId",
                                             device_id.encode()))
        print("[+] Analytics sessionId+deviceId terkirim.")
    except Exception as e:
        print(f"[-] Gagal kirim Analytics: {e}")
    _, closed, _ = diag_read(s, 5, t0, "B-analytics")
    if closed and t_close is None:
        t_close = time.time()
    print(f"[*] Fase B selesai, closed={closed}")

    # Fase C: activity -> activity/get + amati 25 dtk;
    #         normal -> amati 25 dtk saja (kontrol).
    got = False
    if MODE_NORMAL:
        print("[*] Fase C (kontrol): amati 25 dtk tanpa activity/get.")
        for _ in range(13):
            _, closed, _ = diag_read(s, 2, t0, "C-kontrol")
            if closed:
                if t_close is None:
                    t_close = time.time()
                break
    else:
        if not closed:
            try:
                s.sendall(LU.build_room_event_varint("activity/get", b""))
                print("[+] RoomEvent activity/get terkirim.")
            except Exception as e:
                print(f"[-] Gagal kirim activity/get: {e}")
                closed = True
        if not closed:
            for _ in range(13):
                resp, closed, _ = diag_read(s, 2, t0, "C-activity")
                for p in resp:
                    if (len(p) >= 14 and p[0] == 0x08 and p[1] == 0x0C
                            and p[2:14] == b"get-activity"):
                        got = True
                        print(f"[+] Balasan get-activity diterima ({len(p)} byte).")
                if got or closed:
                    if closed and t_close is None:
                        t_close = time.time()
                    break
    s.close()
    survival = (t_close - t0) if t_close is not None else (time.time() - t0)
    print(f"[*] Fase C selesai: get-activity={got}, closed={closed}, "
          f"survival={survival:.2f} dtk.")
    print(f"HASIL DIAG: mode={'NORMAL' if MODE_NORMAL else 'ACTIVITY'}, "
          f"room={room}, get-activity={got}, closed_akhir={closed}, "
          f"survival={survival:.2f}dtk.")
    if MODE_NORMAL:
        return 0 if not closed else 1
    return 0 if got else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
