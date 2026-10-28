"""Diagnostik GetRoom v2 (sesudah burst): bot join pasif ke server teman,
BURST + settle seperti klien asli, baru kirim RoomEvent
'friends' berisi GetFriendRoomRequest (EventId=6, FriendId=UUID Bartes),
lalu baca respons 'get_friend_room_result' dan laporkan Room-nya.
Pola kirim/baca = lookup_uuid.py yang terbukti (SearchFriend=7).
Jalan di runner (sandbox tidak bisa TCP)."""
import asyncio
import json
import uuid
import sys
import time

import assistant_bot as AB
import lookup_uuid as LU
from curl_cffi.requests import AsyncSession

FRIEND_ID = AB.FRIEND_USER_ID  # UUID DesertEagle#431


def build_getroom_protobuf(friend_id):
    fb = friend_id.encode("utf-8")
    return b"\x08\x06\x12" + LU.varint(len(fb)) + fb


def parse_room_event(fr):
    if not fr or fr[0] != 0x08 or len(fr) < 2:
        return None
    ln, n = LU.read_varint(fr, 1)
    if ln is None:
        return None
    return fr[1 + n:1 + n + ln].decode("utf-8", "replace"), fr[1 + n + ln:]


def decode_result(data):
    out = {}
    for f, wt, v in LU.parse_protobuf_fields(data):
        if wt == 0:
            if isinstance(v, int):
                out[f"field{f}"] = v
            elif isinstance(v, (bytes, bytearray)) and len(v) == 1:
                out[f"field{f}"] = v[0]
            else:
                out[f"field{f}"] = v.hex() if isinstance(v, (bytes, bytearray)) else v
        elif wt == 2:
            try:
                out[f"field{f}"] = v.decode("utf-8", "replace")
            except Exception:
                out[f"field{f}"] = v.hex()
        else:
            out[f"field{f}_wt{wt}"] = v.hex() if isinstance(v, (bytes, bytearray)) else v
    return out


async def main():
    bot = LU.probe_bot()
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        user_jwt, _alloc = await AB.login_android(
            bot["email"], bot["password"], bot["device_id"], session)
        if not user_jwt:
            print("HASIL: GAGAL login.")
            return 1
        body = {"userToken": user_jwt,
                "gameServerProtocolVersion": LU.GAME_PROTOCOL,
                "isPrivate": False,
                "destinationUserId": FRIEND_ID}
        r = await session.post(
            LU.LOBBY_URL + "/lobby/allocation",
            data=json.dumps(body).encode(), headers=LU.HEADERS_BASE.copy(), timeout=20)
        act = r.json()
        print(f"HASIL: alokasi teman {r.status_code} successful={act.get('successful')} "
              f"type={act.get('gameServerType')} "
              f"alasan={act.get('failureReason') or act.get('failureDescription') or '-'}")
        if not act.get("successful"):
            print("HASIL: alokasi GAGAL, tidak ada server.")
            return 1
        host, port = LU.find_host_port(act)
        print(f"HASIL: server {host}:{port} ({act.get('gameServerId')})")
    s, pid, conn_frames = AB.tcp_connect_fast(host, port)
    if s is None:
        print("HASIL: GAGAL konek TCP.")
        return 1
    s.sendall(LU.build_login_frame(user_jwt))
    print("[+] GetRoom login terkirim (TANPA kiriman apa pun sesudahnya).")
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
        print("HASIL: join pasif GAGAL (tidak ada JoinedRoom 25 dtk).")
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
        print(f"[+] GetRoom burst terkirim ({len(burst)} frame).")
    except Exception as e:
        print(f"[-] GetRoom burst gagal: {e}")
    n_settle = 0
    for _ in range(5):
        resp, closed, _ = LU.read_frames(s, 1)
        n_settle += len(resp)
        try:
            s.sendall(potion)
        except Exception:
            break
    print(f"[*] GetRoom settle: {n_settle} frame, closed={closed}")
    try:
        s.sendall(LU.build_room_event_varint("friends", build_getroom_protobuf(FRIEND_ID)))
        print("HASIL: GetRoom terkirim SESUDAH burst (RoomEvent 'friends', EventId=6, "
              "FriendId=UUID DesertEagle).")
    except Exception as e:
        print(f"HASIL: GetRoom GAGAL dikirim: {e}")
        return 1
    seen, answer = {}, None
    for _ in range(30):
        resp, closed, _ = LU.read_frames(s, 1)
        for fr in resp:
            ev = parse_room_event(fr)
            if ev is None:
                # Di dalam Batch 0x0d?
                if fr and fr[0] == 0x0d:
                    msgs, _ok = AB.unpack_batch(fr)
                    for m in msgs:
                        ev2 = parse_room_event(m)
                        if ev2 is not None:
                            seen[ev2[0]] = seen.get(ev2[0], 0) + 1
                            if ev2[0] == "get_friend_room_result":
                                answer = ev2[1]
                continue
            seen[ev[0]] = seen.get(ev[0], 0) + 1
            if ev[0] == "get_friend_room_result":
                answer = ev[1]
        if answer is not None or closed:
            break
    try:
        s.close()
    except Exception:
        pass
    print(f"HASIL: event terlihat 30 dtk: {seen}")
    if answer is None:
        print("HASIL: GetRoom TIDAK ADA JAWABAN dalam 30 dtk.")
        return 0
    fields = decode_result(answer)
    print(f"HASIL: get_friend_room_result field={fields}")
    print(f"HASIL: GetRoom Room={fields.get('field2')} Success={fields.get('field1')} "
          f"Error={fields.get('field3')}")
    print("HASIL: diagnostik GetRoom v2 selesai.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
