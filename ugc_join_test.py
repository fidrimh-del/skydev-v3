"""UGC Join Test v2 — bot masuk ke UGC via alokasi activity + activity/get.

Perbedaan dari v1: sesudah auto-join, bot SEGERA mengirim RoomEvent
"activity/get" (bukti dari TBLog klien game: OUT 14B = 08 0c +
"activity/get", tanpa payload), lalu membaca balasan RoomEvent
"get-activity" (payload terrain) dan memeriksa apakah activityId target
ada di payload itu. v1 hanya sampai auto-join dan koneksinya ditutup
server; history UGC tetap 6 (tidak tercatat).

Alur:
  1. Login FSTR + alokasi NORMAL (baseline + datacenterId).
  2. Alokasi ACTIVITY -> server activity.
  3. TCP connect -> event login -> auto-join (tbugv); fallback
     JoinRoom hanya kalau auto-join gagal, dan DILAPORKAN jalurnya.
  4. Segera kirim "activity/get", baca balasan "get-activity" (maks 20 dtk),
     cek activityId target di payload.
  5. Tinggal di room 60 detik (pong jalan), lalu tutup.

Kriteria sukses menurut pemilik: history join UGC target 6 -> 7.
Exit 0 hanya bila balasan "get-activity" diterima DAN activityId target
ditemukan di payload; selain itu exit 1.
Token/JWT TIDAK PERNAH dicetak ke log.
"""

import asyncio
import json
import sys
import time

import lookup_uuid as LU
from curl_cffi.requests import AsyncSession

ACTIVITY_ID_DEFAULT = "3e7cec3f-1ae9-4ba2-a19b-a6339e0f9b23"  # UGC Bartes
STAY_SECONDS = 60


def find_get_activity(frames):
    """Cari frame RoomEvent 'get-activity'. Kembalikan (ketemu, ukuran)."""
    for p in frames:
        if (len(p) >= 14 and p[0] == 0x08 and p[1] == 0x0C
                and p[2:14] == b"get-activity"):
            return True, len(p)
    return False, 0


async def main():
    activity_id = sys.argv[1] if len(sys.argv) > 1 else ACTIVITY_ID_DEFAULT
    bot = LU.probe_bot()
    print(f"[*] Target activity: {activity_id}")
    async with AsyncSession(impersonate="chrome110", verify=False) as session:
        user_jwt, alloc = await LU.login_and_allocate(
            bot["email"], bot["password"], bot["device_id"], session)
        if not alloc:
            print("HASIL: GAGAL login/alokasi normal.")
            return 1
        dc = alloc.get("gameServerDatacenterId")
        print(f"[*] Normal: {alloc.get('gameServerId')} (dc={dc})")

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
            print(f"HASIL: GAGAL alokasi activity (status {r.status_code}, bukan JSON).")
            return 1
        print(f"[*] Alokasi activity status: {r.status_code}, "
              f"successful={act.get('successful')}, "
              f"reason={act.get('failureReason')}")
        if not act.get("successful"):
            print(f"HASIL: GAGAL alokasi activity: {act.get('failureDescription')}")
            return 1
        host, port = LU.find_host_port(act)
        gsid = act.get("gameServerId", "")
        print(f"[*] Server activity: {host}:{port} ({gsid})")

    s, pid = LU.tcp_connect(host, port)
    if s is None:
        print("HASIL: GAGAL konek TCP ke server activity.")
        return 1
    try:
        s.sendall(LU.build_login_frame(user_jwt))
        print("[+] Event login terkirim.")
    except Exception as e:
        print(f"[-] Gagal kirim event login: {e}")
        s.close()
        print("HASIL: GAGAL kirim login.")
        return 1

    # Baca bertahap sampai tbugv terlihat (maks ~8 dtk), agar activity/get
    # bisa dikirim SEGERA sesudah auto-join (v1 menunggu jendela penuh dan
    # koneksi keburu ditutup).
    tbugv, room, closed = False, None, False
    for _ in range(8):
        resp, closed, _ = LU.read_frames(s, 1)
        tb, rm = LU.scan_login_window(resp)
        tbugv = tbugv or tb
        if rm is not None:
            room = rm
        if tbugv and room is not None:
            break
        if closed:
            break
    print(f"[*] Login: tbugv={'YA' if tbugv else 'tidak'}, "
          f"auto-join={'room ' + str(room) if room is not None else 'tidak'}, "
          f"koneksi_tertutup={closed}")
    path = "auto-join"
    if room is None and not closed:
        room = LU.tcp_join_only(s, gsid)
        path = "JoinRoom-fallback" if room is not None else "gagal"
    if room is None:
        print("HASIL: GAGAL join (auto-join maupun JoinRoom).")
        s.close()
        return 1
    print(f"[+] Bot di room {room} via {path}. Kirim activity/get...")
    try:
        s.sendall(LU.build_room_event_varint("activity/get", b""))
        print("[+] RoomEvent activity/get terkirim.")
    except Exception as e:
        print(f"[-] Gagal kirim activity/get: {e}")
        s.close()
        print("HASIL: GAGAL kirim activity/get.")
        return 1

    # Baca balasan get-activity (maks ~20 dtk).
    got, gsize, found_id, closed = False, 0, False, False
    all_frames = []
    for _ in range(10):
        resp, closed, _ = LU.read_frames(s, 2)
        all_frames.extend(resp)
        g, gs = find_get_activity(resp)
        if g:
            got, gsize = True, gs
            break
        if closed:
            break
    if got:
        blob = b"".join(all_frames)
        found_id = activity_id.encode("utf-8") in blob
        print(f"[+] Balasan get-activity diterima ({gsize} byte payload), "
              f"activityId target di payload={'YA' if found_id else 'TIDAK'}.")
    else:
        print(f"[-] Tidak ada balasan get-activity (koneksi_tertutup={closed}).")

    # Tinggal di room (pong jalan).
    frames_stay = 0
    t_end = time.time() + STAY_SECONDS
    while time.time() < t_end and not closed:
        resp, closed, _ = LU.read_frames(s, 5)
        frames_stay += len(resp)
    s.close()
    print(f"[*] Selesai tinggal: {frames_stay} frame selama stay, "
          f"koneksi_tertutup_akhir={closed}.")
    if got and found_id:
        print("HASIL: BOT SAMPAI DI ROOM + TERRAIN TARGET DITERIMA "
              f"(activity={activity_id}, server={host}:{port}, room={room}, "
              f"jalur={path}, terrain={gsize}B).")
        print("CEK HISTORY UGC: 6 -> ? (7 = berfungsi).")
        return 0
    print("HASIL: GAGAL verifikasi terrain target "
          f"(get-activity={got}, activityId_di_payload={found_id}).")
    print("CEK HISTORY UGC: 6 -> ? (7 = berfungsi).")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
