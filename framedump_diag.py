"""Diagnostik dump frame: bot join pasif ke server teman, lalu mencetak
SEMUA frame top-level (bukan RoomEvent) dari jendela connect + settle +
20 dtk bacaan: tag, panjang, hex (cap 200 char), dan ASCII digit-run.
RoomEvent hanya dihitung per nama. Tujuannya: mencari pengumuman
port UDP di frame top-level / dalam Batch 0x0d (yang selama ini
tidak di-decode bot). Jalan di runner."""
import asyncio
import json
import re
import sys
import time
import uuid

import assistant_bot as AB
import lookup_uuid as LU
from curl_cffi.requests import AsyncSession

ROOMEVENTS = {}


def dump_frame(fr, label):
    if not fr:
        return
    tag = fr[0]
    hx = fr.hex()
    if len(hx) > 200:
        hx = hx[:200] + "..."
    print(f"HASIL: FRAME {label} tag=0x{tag:02x} len={len(fr)} hex={hx}")
    for run in re.findall(rb"[ -~]{4,}", fr):
        s = run.decode("ascii")
        if any(c.isdigit() for c in s) and len(s) <= 64:
            print(f"HASIL: FRAME {label} ascii={s}")


def handle(fr, label):
    if not fr:
        return
    if fr[0] == 0x08:
        ev = None
        if len(fr) >= 2:
            ln, n = LU.read_varint(fr, 1)
            if ln is not None:
                ev = fr[1 + n:1 + n + ln].decode("utf-8", "replace")
        if ev:
            ROOMEVENTS[ev] = ROOMEVENTS.get(ev, 0) + 1
        return
    if fr[0] == 0x0d:
        msgs, ok = AB.unpack_batch(fr)
        print(f"HASIL: FRAME {label} BATCH isi={len(msgs)} ok={ok}")
        for m in msgs:
            dump_frame(m, label + "/batch")
        return
    dump_frame(fr, label)


async def main():
    bot = LU.probe_bot()
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        user_jwt, _ = await AB.login_android(
            bot["email"], bot["password"], bot["device_id"], session)
        if not user_jwt:
            print("HASIL: GAGAL login.")
            return 1
        body = {"userToken": user_jwt,
                "gameServerProtocolVersion": LU.GAME_PROTOCOL,
                "isPrivate": False,
                "destinationUserId": AB.FRIEND_USER_ID}
        r = await session.post(
            LU.LOBBY_URL + "/lobby/allocation",
            data=json.dumps(body).encode(), headers=LU.HEADERS_BASE.copy(), timeout=20)
        act = r.json()
        print(f"HASIL: alokasi teman {r.status_code} successful={act.get('successful')} "
              f"type={act.get('gameServerType')} "
              f"alasan={act.get('failureReason') or act.get('failureDescription') or '-'}")
        if not act.get("successful"):
            print("HASIL: alokasi GAGAL.")
            return 1
        host, port = LU.find_host_port(act)
        print(f"HASIL: server TCP {host}:{port} ({act.get('gameServerId')})")
    s, pid, conn_frames = AB.tcp_connect_fast(host, port)
    if s is None:
        print("HASIL: GAGAL konek TCP.")
        return 1
    print(f"HASIL: TCP playerId={pid} frame_connect={len(conn_frames)}")
    for fr in conn_frames:
        handle(fr, "connect")
    s.sendall(LU.build_login_frame(user_jwt))
    print("[+] framedump login terkirim (TANPA kiriman apa pun sesudahnya).")
    room, method, closed, bot_uuid = None, None, False, None
    t0 = time.time()
    for _ in range(25):
        resp, closed, _ = LU.read_frames(s, 1)
        rm, md = AB.find_joined_in(resp)
        if rm is not None:
            room, method = rm, md
        for fr in resp:
            u = AB.uuid_from_tbugv(fr)
            if u:
                bot_uuid = u
            handle(fr, "join")
        if room is not None:
            break
        if closed:
            break
    print(f"HASIL: join pasif room={room} metode={method} "
          f"waktu={round(time.time() - t0, 2)} dtk closed={closed}")
    if room is None:
        try:
            s.close()
        except Exception:
            pass
        print("HASIL: join pasif GAGAL.")
        return 1
    potion = AB.frame_raw(AB.MAGIC_POTION_HEX)
    try:
        s.sendall(LU.build_room_event_varint(
            "Analytics/sessionId", str(uuid.uuid4()).encode()))
        s.sendall(LU.build_room_event_varint(
            "Analytics/deviceId",
            str(uuid.uuid5(uuid.NAMESPACE_DNS, bot["email"])).encode()))
        for _ in range(5):
            s.sendall(potion)
    except Exception:
        pass
    burst = []
    if bot_uuid:
        burst.append(LU.build_room_event_varint(
            "slot_interaction", b"\x08\x02\x1a\x24" + bot_uuid.encode("ascii")))
    burst += [AB.frame_raw(AB.FETCH_EVENTS_HEX), AB.frame_raw(AB.INIT_POS_HEX),
              AB.frame_raw(AB.INIT_DAY_NIGHT_HEX), AB.frame_raw(AB.PET_6_HEX)]
    if bot_uuid:
        burst.append(LU.build_room_event_varint(
            "slot_interaction", b"\x08\x02\x1a\x24" + bot_uuid.encode("ascii")))
    burst += [AB.frame_raw(AB.OUTFIT_HEX), AB.frame_raw(AB.VHC_HEX), AB.frame_raw(AB.STRKS_HEX)]
    try:
        for fr in burst:
            s.sendall(fr)
        print(f"[+] framedump burst terkirim ({len(burst)} frame).")
    except Exception as e:
        print(f"[-] framedump burst gagal: {e}")
    for i in range(25):
        resp, closed, _ = LU.read_frames(s, 1)
        for fr in resp:
            handle(fr, "settle" if i < 5 else "baca")
        if i % 5 == 4:
            try:
                s.sendall(potion)
            except Exception:
                break
        if closed:
            break
    try:
        s.close()
    except Exception:
        pass
    print(f"HASIL: RoomEvent terlihat: {ROOMEVENTS}")
    print("HASIL: diagnostik framedump selesai.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
