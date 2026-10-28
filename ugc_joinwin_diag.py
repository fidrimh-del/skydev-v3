"""Join-Window Diagnostic — rekam SEMUA frame jendela join activity room.

Latar: 4 dari 9 run activity gagal di room sync (0x06): tbugv datang tapi
room sync tidak kunjung datang; kadang koneksi ditutup ~4,5 dtk, kadang
tetap hidup tanpa room sync; JoinRoom tidak pernah berhasil di konteks
activity (0 sukses dari 9 percobaan). Diag ini menjawab satu pertanyaan:
apakah frame 0x06 BENAR-BENAR tidak dikirim server, atau dikirim tapi
formatnya tidak dikenali scan_login_window?

Alur: login FSTR + alokasi NORMAL + alokasi ACTIVITY -> TCP -> login ->
baca 15 dtk sambil mencatat SEMUA frame non-ping (waktu, tipe, panjang,
head hex; frame 0x06 dicetak LENGKAP) + Analytics + potion sesudah tbugv
(seperti v5.1, agar koneksi tetap hidup). TIDAK ada burst/activity/get —
fokus murni jendela join.
Token/JWT TIDAK PERNAH dicetak ke log.
"""

import asyncio
import json
import sys
import time
import uuid

import lookup_uuid as LU
from curl_cffi.requests import AsyncSession

ACTIVITY_ID_DEFAULT = "3e7cec3f-1ae9-4ba2-a19b-a6339e0f9b23"  # UGC Bartes
POTION_HEX = "080c6d616769635f706f74696f6e080110ffffffffffffffffff01"


def frame_raw(hex_payload):
    import struct
    raw = bytes.fromhex(hex_payload)
    return struct.pack(">I", len(raw)) + raw


def describe(fr):
    if not fr:
        return "KOSONG"
    t = fr[0]
    if t in (0x05, 0x08):
        ln, n = LU.read_varint(fr, 1)
        try:
            nm = fr[1 + n:1 + n + ln].decode("utf-8") if ln is not None else "?"
        except Exception:
            nm = "?"
        return f"{'EVENT' if t == 0x05 else 'ROOMEVENT'} {nm}"
    return f"TYPE0x{t:02x}"


async def main():
    activity_id = sys.argv[1] if len(sys.argv) > 1 else ACTIVITY_ID_DEFAULT
    bot = LU.probe_bot()
    print(f"[*] Target activity: {activity_id} (join-window diag)")
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        user_jwt, alloc = await LU.login_and_allocate(
            bot["email"], bot["password"], bot["device_id"], session)
        if not alloc:
            print("HASIL DIAG: GAGAL login/alokasi normal.")
            return 1
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
    potion = frame_raw(POTION_HEX)
    try:
        s.sendall(LU.build_login_frame(user_jwt))
        print("[+] Event login terkirim.")
    except Exception as e:
        print(f"[-] Gagal kirim event login: {e}")
        s.close()
        return 1

    tbugv, room, closed = False, None, False
    analytics_sent = False
    sync_frames = []
    total = 0
    for _ in range(15):
        resp, closed, _ = LU.read_frames(s, 1)
        for fr in resp:
            total += 1
            if fr == b"\x01":
                continue
            d = describe(fr)
            print(f"JOINWIN T+{time.time() - t0:6.2f}s {d} len={len(fr)} "
                  f"head={fr[:32].hex()}", flush=True)
            if fr and fr[0] == 0x06:
                sync_frames.append(fr)
                print(f"JOINWIN *** ROOMSYNC PENUH: {fr.hex()}", flush=True)
        tb, rm = LU.scan_login_window(resp)
        tbugv = tbugv or tb
        if rm is not None:
            room = rm
        if tbugv and not analytics_sent:
            try:
                s.sendall(LU.build_room_event_varint(
                    "Analytics/sessionId", str(uuid.uuid4()).encode()))
                s.sendall(LU.build_room_event_varint(
                    "Analytics/deviceId",
                    str(uuid.uuid5(uuid.NAMESPACE_DNS, bot["email"])).encode()))
                for _ in range(3):
                    s.sendall(potion)
                analytics_sent = True
                print("[+] Analytics + POTION awal terkirim (sesudah tbugv).")
            except Exception:
                closed = True
                break
        elif analytics_sent and not closed:
            try:
                s.sendall(potion)
            except Exception:
                closed = True
                break
        if closed:
            break
    s.close()
    print(f"[*] Jendela join selesai: total_frame={total}, tbugv={tbugv}, "
          f"room_terparse={room}, frame_0x06_diterima={len(sync_frames)}, "
          f"closed={closed}.")
    if sync_frames:
        for fr in sync_frames:
            print(f"[*] 0x06: p1={fr[1] if len(fr) > 1 else '?'}, "
                  f"p[2:6]={fr[2:6].hex() if len(fr) >= 6 else '?'} "
                  f"(parsed_room={room}).")
    print(f"HASIL DIAG: tbugv={tbugv}, room={room}, "
          f"sync_frames={len(sync_frames)}, closed={closed}.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
