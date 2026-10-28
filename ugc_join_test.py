"""UGC Join Test v4 — bot masuk ke UGC via alokasi activity + BURST INIT + SPAM magic_potion.

Perbedaan dari v3: v3 mengirim burst init lengkap dan koneksi TETAP
ditutup 4,41 dtk tanpa balasan (run 40670372745). Analisis ulang TBLog
lengkap (2 join sukses ke dunia yang sama) membuktikan satu-satunya
selisih wire yang tersisa adalah SPAM magic_potion: frame 27B yang
dikirim klien game berulang-ulang sejak sesudah Analytics sampai
activity/get dan seterusnya. v4 = v3 + spam magic_potion (sebelum burst,
di antara burst dan activity/get, dan berkala selama observasi).
Koreksi dari analisis ulang: preset x8 DIHAPUS (join sukses kedua tidak
mengirim preset sama sekali); pet = 6, lalu 19, 24 (v3 pakai 6, 6).

Urutan burst (byte persis dari TBLog_0_u4jk.txt, baris 2461-2498):
  slot_interaction (58B, bawa UUID pemain -> dipakai UUID BOT dari frame
    tbugv, BUKAN UUID pemilik log) -> preset x8 (14B) -> fetch_events
  (14B) -> init_pos (10B) -> init_day_night (16B) -> pet (7B) ->
  slot_interaction lagi -> outfit (8B) -> vhc (7B) -> STRKS (7B) ->
  pet lagi -> activity/get (14B).
magic_potion SENGAJA tidak ikut (spam aksi pemain, bukan sinyal entry;
jadi variabel berikutnya bila v3 gagal).

Alur:
  1. Login FSTR + alokasi NORMAL (baseline + datacenterId).
  2. Alokasi ACTIVITY -> server activity.
  3. TCP connect -> event login -> auto-join (tbugv); tangkap UUID bot
     dari frame tbugv; fallback JoinRoom hanya kalau auto-join gagal.
  4. Kirim Analytics x2, lalu burst init, lalu activity/get.
  5. Baca balasan get-activity (maks ~25 dtk), catat survival (waktu
     dari login sampai tutup; zona maut v1/v2/diag = ~4,5 dtk).
  6. Bila masih hidup, tinggal di room 60 detik (pong jalan).

Kriteria sukses menurut pemilik: history join UGC target 6 -> 7.
Exit 0 hanya bila balasan "get-activity" diterima DAN activityId target
ditemukan di payload; selain itu exit 1.
Token/JWT TIDAK PERNAH dicetak ke log.
"""

import asyncio
import json
import re
import struct
import sys
import time
import uuid

import lookup_uuid as LU
from curl_cffi.requests import AsyncSession

ACTIVITY_ID_DEFAULT = "3e7cec3f-1ae9-4ba2-a19b-a6339e0f9b23"  # UGC Bartes
STAY_SECONDS = 60
UUID_RE = re.compile(
    rb"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

# Burst init: payload RoomEvent lengkap (hex) dari TBLog, KECUALI
# slot_interaction (dibangun dinamis dengan UUID bot).
# Preset x8 DIHAPUS (join sukses kedua tidak mengirim preset).
# Pet: 6, lalu 19, 24 (nilai dari 2 join sukses).
# magic_potion: frame 27B klien game, dikirim berulang (spam).
FETCH_EVENTS_HEX = "080c66657463685f6576656e7473"
INIT_POS_HEX = "0808696e69745f706f73"
INIT_DAY_NIGHT_HEX = "080e696e69745f6461795f6e69676874"
PET_6_HEX = "08037065740806"
PET_19_HEX = "08037065740813"
PET_24_HEX = "08037065740818"
OUTFIT_HEX = "08066f7574666974"
VHC_HEX = "08037668630805"
STRKS_HEX = "08055354524b53"
MAGIC_POTION_HEX = "080c6d616769635f706f74696f6e080110ffffffffffffffffff01"


def frame_raw(hex_payload):
    raw = bytes.fromhex(hex_payload)
    return struct.pack(">I", len(raw)) + raw


def uuid_from_tbugv(frame):
    """Ambil UUID pemain dari frame EVENT 'tbugv' (atau None)."""
    if len(frame) < 8 or frame[0] != 0x05:
        return None
    ln, n = LU.read_varint(frame, 1)
    if ln is None or frame[1 + n:1 + n + ln] != b"tbugv":
        return None
    m = UUID_RE.search(frame[1 + n + ln:])
    return m.group(0).decode("ascii") if m else None


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
    print(f"[*] Target activity: {activity_id} (v4: burst init + spam magic_potion + activity/get)")
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
    t0 = time.time()
    t_close = None
    try:
        s.sendall(LU.build_login_frame(user_jwt))
        print("[+] Event login terkirim.")
    except Exception as e:
        print(f"[-] Gagal kirim event login: {e}")
        s.close()
        print("HASIL: GAGAL kirim login.")
        return 1

    # Baca bertahap sampai tbugv terlihat (maks ~8 dtk); tangkap UUID bot.
    tbugv, room, closed, bot_uuid = False, None, False, None
    for _ in range(8):
        resp, closed, _ = LU.read_frames(s, 1)
        tb, rm = LU.scan_login_window(resp)
        tbugv = tbugv or tb
        if rm is not None:
            room = rm
        for fr in resp:
            u = uuid_from_tbugv(fr)
            if u:
                bot_uuid = u
        if tbugv and room is not None:
            break
        if closed:
            break
    if closed and t_close is None:
        t_close = time.time()
    print(f"[*] Login: tbugv={'YA' if tbugv else 'tidak'}, "
          f"auto-join={'room ' + str(room) if room is not None else 'tidak'}, "
          f"UUID bot={'terbaca' if bot_uuid else 'TIDAK terbaca'}, "
          f"koneksi_tertutup={closed}")
    path = "auto-join"
    if room is None and not closed:
        room = LU.tcp_join_only(s, gsid)
        path = "JoinRoom-fallback" if room is not None else "gagal"
    if room is None:
        print("HASIL: GAGAL join (auto-join maupun JoinRoom).")
        s.close()
        return 1

    # Analytics (seperti klien game + kontrol normal), lalu burst init.
    try:
        s.sendall(LU.build_room_event_varint(
            "Analytics/sessionId", str(uuid.uuid4()).encode()))
        s.sendall(LU.build_room_event_varint(
            "Analytics/deviceId",
            str(uuid.uuid5(uuid.NAMESPACE_DNS, bot["email"])).encode()))
        print("[+] Analytics sessionId+deviceId terkirim.")
    except Exception as e:
        print(f"[-] Gagal kirim Analytics: {e}")

    potion = frame_raw(MAGIC_POTION_HEX)
    burst = []
    if bot_uuid:
        slot = LU.build_room_event_varint(
            "slot_interaction",
            b"\x08\x02\x1a\x24" + bot_uuid.encode("ascii"))
        burst.append(("slot_interaction", slot))
    else:
        print("[!] UUID bot tidak terbaca: slot_interaction DILEWATI.")
    burst += [
        ("fetch_events", frame_raw(FETCH_EVENTS_HEX)),
        ("init_pos", frame_raw(INIT_POS_HEX)),
        ("init_day_night", frame_raw(INIT_DAY_NIGHT_HEX)),
        ("pet6", frame_raw(PET_6_HEX)),
    ]
    if bot_uuid:
        burst.append(("slot_interaction", LU.build_room_event_varint(
            "slot_interaction",
            b"\x08\x02\x1a\x24" + bot_uuid.encode("ascii"))))
    burst += [
        ("outfit", frame_raw(OUTFIT_HEX)),
        ("vhc", frame_raw(VHC_HEX)),
        ("STRKS", frame_raw(STRKS_HEX)),
    ]
    try:
        # Spam magic_potion dulu (seperti klien game: langsung sesudah
        # Analytics, berulang), lalu burst init, lalu potion lagi.
        for _ in range(5):
            s.sendall(potion)
            time.sleep(0.1)
        print("[+] POTION spam awal terkirim (5 frame).")
        for name, fr in burst:
            s.sendall(fr)
            time.sleep(0.05)
        print(f"[+] BURST init terkirim ({len(burst)} frame).")
        for _ in range(2):
            s.sendall(potion)
            time.sleep(0.1)
        s.sendall(frame_raw(PET_19_HEX))
        s.sendall(frame_raw(PET_24_HEX))
        print("[+] POTION + pet19 + pet24 terkirim.")
    except Exception as e:
        print(f"[-] Gagal kirim burst init: {e}")
        s.close()
        print("HASIL: GAGAL kirim burst init.")
        return 1
    try:
        s.sendall(LU.build_room_event_varint("activity/get", b""))
        print("[+] RoomEvent activity/get terkirim (sesudah burst).")
    except Exception as e:
        print(f"[-] Gagal kirim activity/get: {e}")
        s.close()
        print("HASIL: GAGAL kirim activity/get.")
        return 1

    # Baca balasan get-activity (maks ~25 dtk), selingi spam potion
    # tiap putaran agar klien tetap "aktif" seperti klien game.
    got, gsize, found_id, closed = False, 0, False, False
    all_frames = []
    for _ in range(25):
        resp, closed, _ = LU.read_frames(s, 1)
        all_frames.extend(resp)
        g, gs = find_get_activity(resp)
        if g:
            got, gsize = True, gs
            break
        if closed:
            if t_close is None:
                t_close = time.time()
            break
        try:
            s.sendall(potion)
        except Exception:
            closed = True
            if t_close is None:
                t_close = time.time()
            break
    survival = (t_close - t0) if t_close is not None else (time.time() - t0)
    if got:
        blob = b"".join(all_frames)
        found_id = activity_id.encode("utf-8") in blob
        print(f"[+] Balasan get-activity diterima ({gsize} byte payload), "
              f"activityId target di payload={'YA' if found_id else 'TIDAK'}.")
    else:
        print(f"[-] Tidak ada balasan get-activity (koneksi_tertutup={closed}, "
              f"survival={survival:.2f} dtk).")

    # Tinggal di room bila masih hidup (pong jalan).
    frames_stay = 0
    t_end = time.time() + STAY_SECONDS
    while time.time() < t_end and not closed:
        resp, closed, _ = LU.read_frames(s, 5)
        frames_stay += len(resp)
    if closed and t_close is None:
        t_close = time.time()
    s.close()
    survival = (t_close - t0) if t_close is not None else (time.time() - t0)
    print(f"[*] Selesai tinggal: {frames_stay} frame selama stay, "
          f"koneksi_tertutup_akhir={closed}, survival={survival:.2f} dtk.")
    if got and found_id:
        print("HASIL: BOT SAMPAI DI ROOM + TERRAIN TARGET DITERIMA "
              f"(activity={activity_id}, server={host}:{port}, room={room}, "
              f"jalur={path}, terrain={gsize}B, survival={survival:.2f}dtk).")
        print("CEK HISTORY UGC: 6 -> ? (7 = berfungsi).")
        return 0
    print("HASIL: GAGAL verifikasi terrain target "
          f"(get-activity={got}, activityId_di_payload={found_id}, "
          f"survival={survival:.2f}dtk).")
    print("CEK HISTORY UGC: 6 -> ? (7 = berfungsi).")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
