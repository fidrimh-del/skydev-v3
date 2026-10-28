"""UGC Join Test — bot masuk ke UGC via alokasi activity (sekali jalan).

Alur (semua langkah sudah terbukti kecuali verifikasi room activity):
  1. Login FSTR + alokasi NORMAL (baseline + datacenterId).
  2. Alokasi ACTIVITY (body dump: userToken, activityId, datacenterId,
     gameServerProtocolVersion) -> server activity.
  3. TCP connect -> event login (JWT + 1.93.3) -> auto-join (tbugv).
     Fallback JoinRoom hanya kalau auto-join gagal, dan DILAPORKAN
     jalurnya agar hasil tidak ambigu.
  4. Tinggal di room 60 detik (pong jalan) agar server sempat
     mencatat kunjungan, lalu tutup.

Kriteria sukses menurut pemilik: history join UGC target 6 -> 7.
Exit 0 hanya bila bot sampai di sebuah room; selain itu exit 1.
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
        print(f"[*] Normal: {alloc.get('gameServerId')} "
              f"(dc={dc})")

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
    resp, closed, pongs = LU.read_frames(s, 5)
    tbugv, room = LU.scan_login_window(resp)
    print(f"[*] Login: tbugv={'YA' if tbugv else 'tidak'}, "
          f"auto-join={'room ' + str(room) if room is not None else 'tidak'}")
    path = "auto-join"
    if room is None:
        room = LU.tcp_join_only(s, gsid)
        path = "JoinRoom-fallback" if room is not None else "gagal"
    if room is None:
        print("HASIL: GAGAL join (auto-join maupun JoinRoom).")
        s.close()
        return 1
    print(f"[+] Bot di room {room} via {path}. Tinggal {STAY_SECONDS} dtk...")
    frames_stay = 0
    t_end = time.time() + STAY_SECONDS
    while time.time() < t_end and not closed:
        resp, closed, pongs = LU.read_frames(s, 5)
        frames_stay += len(resp)
    s.close()
    print(f"[*] Selesai tinggal: {frames_stay} frame selama stay, "
          f"koneksi_tertutup={closed}.")
    print("HASIL: BOT SAMPAI DI ROOM "
          f"(activity={activity_id}, server={host}:{port}, room={room}, "
          f"jalur={path}).")
    print("CEK HISTORY UGC: 6 -> ? (7 = berfungsi).")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
