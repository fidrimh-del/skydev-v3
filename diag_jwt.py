"""Diagnostik: ambil frame Connected (0x03) dari koneksi TCP bot, decode
payload JWT-nya, dan cetak klaim-klaimnya (mencari field port UDP).
TIDAK mencetak token utuh — hanya kunci klaim + nilai pendek.
Jalan di runner (sandbox tidak bisa TCP)."""
import asyncio
import base64
import json
import sys

import assistant_bot as AB
import lookup_uuid as LU
from curl_cffi.requests import AsyncSession


def b64url_json(seg):
    seg += "=" * (-len(seg) % 4)
    return json.loads(base64.urlsafe_b64decode(seg).decode("utf-8", "replace"))


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
                "destinationUserId": AB.FRIEND_USER_ID}
        r = await session.post(
            LU.LOBBY_URL + "/lobby/allocation",
            data=json.dumps(body).encode(), headers=LU.HEADERS_BASE.copy(), timeout=20)
        act = r.json()
        print(f"HASIL: alokasi teman {r.status_code} successful={act.get('successful')} "
              f"type={act.get('gameServerType')} "
              f"alasan={act.get('failureReason') or act.get('failureDescription') or '-'}")
        host, port = LU.find_host_port(act)
        print(f"HASIL: server {host}:{port} ({act.get('gameServerId')})")
    s, pid, conn_frames = AB.tcp_connect_fast(host, port)
    print(f"HASIL: TCP playerId={pid} frame_connect={len(conn_frames)}")
    if s is None:
        print("HASIL: GAGAL konek TCP.")
        return 1
    token = None
    for fr in conn_frames:
        raw = None
        if isinstance(fr, (bytes, bytearray)):
            raw = bytes(fr)
        elif isinstance(fr, (tuple, list)) and len(fr) >= 2:
            raw = bytes(fr[1]) if isinstance(fr[1], (bytes, bytearray)) else None
        if raw and len(raw) > 20 and raw[0] == 0x03:
            token = raw[1:].decode("ascii", "replace")
            break
    try:
        s.close()
    except Exception:
        pass
    if not token:
        print("HASIL: frame 0x03 TIDAK ditemukan di jendela connect.")
        return 1
    print(f"HASIL: token 0x03 panjang={len(token)} awal={token[:12]}")
    parts = token.split(".")
    print(f"HASIL: token segmen={len(parts)}")
    if len(parts) >= 2:
        try:
            claims = b64url_json(parts[1])
            print("HASIL: JWT-CLAIM kunci=" + ",".join(sorted(claims.keys())))
            for k in sorted(claims.keys()):
                v = claims[k]
                sv = v if isinstance(v, str) else json.dumps(v)
                if len(sv) > 100:
                    sv = sv[:100] + "..."
                print(f"HASIL: JWT-CLAIM {k} = {sv}")
        except Exception as e:
            print(f"HASIL: JWT-CLAIM decode gagal: {e}")
    print("HASIL: diagnostik JWT selesai.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
