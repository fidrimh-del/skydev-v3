"""house_diag_verbose.py — Diag like RUMAH verbose (1 leg).

Tujuan: bedakan gagal JOIN vs gagal IsThere pada like rumah.
- L.QUIET=False, P.VERBOSE=True supaya qlog (hasil JoinRoom) tampil.
- Tiap bot dicatat tahapnya + waktunya:
  alloc -> tcp -> login-window -> JOIN(room?) -> Create -> IsThere.
- Klasifikasi per bot:
  sent / fail-join / fail-isthere-noanswer / fail-isthere-false
  / fail-login / fail-alloc / fail-tcp.
- TIDAK mengubah file produksi; hanya membaca + memakai fungsi live
  (login_and_allocate, tcp_connect, tcp_join_only, FrameReader, dll).

Pakai: python -u house_diag_verbose.py <TARGET_UUID> <JUMLAH> [PARALEL]
Default: target aae2efe1 (akun Bartes), jumlah 25, paralel 12.
"""
import asyncio
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import house_like_probe as H
import like_profile_probe as P
import lookup_uuid as L

TARGET_DEFAULT = "aae2efe1-1abb-48c2-8799-ea192692a49c"
LIKE_TYPE_HOUSE = 0


def _safe_close(s):
    try:
        import socket
        s.shutdown(socket.SHUT_RDWR)
    except Exception:
        pass
    try:
        s.close()
    except Exception:
        pass


async def diag_like_once(session, bot, target_uuid, idx):
    """Replika like_house_once dengan log tiap tahap + klasifikasi.
    Kembalikan (klasifikasi, detail)."""
    t0 = time.time()
    log = []

    def dt():
        return f"{time.time() - t0:.1f}d"

    user_jwt, alloc = await L.login_and_allocate(
        bot["email"], bot["password"], bot["device_id"], session
    )
    if not alloc:
        log.append(f"[{dt()}] alloc GAGAL")
        return "fail-alloc", " | ".join(log)
    log.append(f"[{dt()}] alloc OK")

    host, port = L.find_host_port(alloc)
    gsid = alloc.get("gameServerId", "")
    if not host or not port:
        return "fail-alloc", " | ".join(log) + f" [{dt()}] host/port kosong"

    s, pid = L.tcp_connect(host, port)
    if s is None:
        return "fail-tcp", " | ".join(log) + f" [{dt()}] tcp GAGAL"
    log.append(f"[{dt()}] tcp OK pid={pid}")

    reader = P.FrameReader(s)
    try:
        s.sendall(L.build_login_frame(user_jwt))
    except Exception as e:
        _safe_close(s)
        return "fail-login", " | ".join(log) + f" [{dt()}] kirim login GAGAL: {e}"

    _jstate = {"room": None, "tbugv": False}

    def _login_pred(fr):
        t, r = L.scan_login_window([fr])
        if t:
            _jstate["tbugv"] = True
        if r is not None:
            _jstate["room"] = r
        return _jstate["room"] is not None

    try:
        fr, closed = reader.read_until(_login_pred, 4)
        if closed:
            _safe_close(s)
            return "fail-login", " | ".join(log) + f" [{dt()}] koneksi tertutup saat login-window"
        room = _jstate["room"]
        if room is not None:
            log.append(f"[{dt()}] auto-join room={room} tbugv={_jstate['tbugv']}")
        else:
            log.append(f"[{dt()}] login-window timeout (tbugv={_jstate['tbugv']}), JOIN manual...")
            room = L.tcp_join_only(s, gsid)
            if room is None:
                _safe_close(s)
                return "fail-join", " | ".join(log) + f" [{dt()}] JOIN GAGAL (5 ruangan, tak ada JoinedRoom)"
            log.append(f"[{dt()}] JOIN OK room={room}")

        def do_is_there():
            frame = L.build_room_event_varint(
                "like", P.build_like_protobuf(2, target_uuid, LIKE_TYPE_HOUSE)
            )
            try:
                s.sendall(frame)
                s.sendall(frame)
            except Exception:
                return None
            fr2, closed2 = reader.read_until(
                lambda f: P.decode_is_there_result(f) is not None, 4
            )
            if fr2 is None:
                return None
            return P.decode_is_there_result(fr2)

        try:
            s.sendall(
                L.build_room_event_varint(
                    "like",
                    P.build_like_protobuf(0, target_uuid, LIKE_TYPE_HOUSE),
                )
            )
        except Exception:
            _safe_close(s)
            return "fail-login", " | ".join(log) + f" [{dt()}] kirim Create GAGAL"
        log.append(f"[{dt()}] Create terkirim")

        await asyncio.sleep(1.2)

        after = do_is_there()
        answered = after is not None
        log.append(
            f"[{dt()}] IsThere#1: "
            + ("TIDAK DIJAWAB" if after is None else f"exists={after.get('exists')} err={after.get('error')!r}")
        )
        if not (after and after.get("exists") is True):
            await asyncio.sleep(1.0)
            after = do_is_there()
            answered = answered or (after is not None)
            log.append(
                f"[{dt()}] IsThere#2: "
                + ("TIDAK DIJAWAB" if after is None else f"exists={after.get('exists')} err={after.get('error')!r}")
            )

        if after and after.get("exists") is True:
            H.history_put_house(bot["email"], target_uuid)
            return "sent", " | ".join(log)
        if not answered:
            return "fail-isthere-noanswer", " | ".join(log)
        return "fail-isthere-false", " | ".join(log)
    finally:
        _safe_close(s)


async def bot_coro(idx, bot, target_uuid):
    prof = random.choice(("chrome110", "chrome116", "safari15_5"))
    async with L.AsyncSession(impersonate=prof, verify=False) as session:
        try:
            return await asyncio.wait_for(
                diag_like_once(session, bot, target_uuid, idx), timeout=30.0
            )
        except asyncio.TimeoutError:
            return "fail-timeout30", f"hard timeout 30d"
        except Exception as e:
            return "fail-exc", f"exception {e!r}"


def run_bot(idx, bot, target_uuid, state):
    with state["lock"]:
        if state["sent"] >= state["jumlah"]:
            return "skip"
        state["tried"].add(bot["email"])
    time.sleep(random.uniform(0.6, 2.0))
    t0 = time.time()
    try:
        klas, detail = asyncio.run(bot_coro(idx, bot, target_uuid))
    except Exception as e:
        klas, detail = "fail-exc", f"exception {e!r}"
    dt = time.time() - t0
    with state["lock"]:
        state["hasil"][klas] = state["hasil"].get(klas, 0) + 1
        if klas == "sent":
            state["sent"] += 1
        print(f"[DIAG] Bot {idx}: {klas} ({dt:.0f}d) :: {detail}", flush=True)
    return klas


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else TARGET_DEFAULT
    jumlah = int(sys.argv[2]) if len(sys.argv) > 2 else 25
    paralel = int(sys.argv[3]) if len(sys.argv) > 3 else 12
    paralel = max(1, min(12, paralel))

    L.apply_panel_versions()
    L.QUIET = False
    P.VERBOSE = True
    print(f"[*] DIAG LIKE RUMAH verbose: target={target} jumlah={jumlah} paralel={paralel}")
    print(f"[*] Versi aktif: game={L.VERSION} protokol={L.GAME_PROTOCOL}")

    bots = H.get_house_bots(target, limit=max(jumlah + 4, 30))
    if not bots:
        print("[!] Tidak ada bot fresh — berhenti.")
        return 1
    print(f"[*] {len(bots)} bot fresh didapat.")

    state = {
        "lock": threading.Lock(),
        "jumlah": jumlah,
        "sent": 0,
        "tried": set(),
        "hasil": {},
    }
    t0 = time.time()
    nomor = 0
    with ThreadPoolExecutor(max_workers=paralel) as pool:
        futs = []
        for bot in bots[: jumlah + 10]:
            nomor += 1
            futs.append(pool.submit(run_bot, nomor, bot, target, state))
            time.sleep(0.1)
            with state["lock"]:
                if state["sent"] >= jumlah:
                    break
        for f in futs:
            try:
                f.result()
            except Exception as e:
                print(f"[!] worker exception: {e}")

    dt = time.time() - t0
    print("=" * 60)
    print("HASIL DIAG LIKE RUMAH")
    print(f"  Target: {target}")
    print(f"  Diminta: {jumlah} | Terkirim: {state['sent']} | Waktu: {dt:.0f} detik")
    for k in sorted(state["hasil"]):
        print(f"  {k}: {state['hasil'][k]}")
    print("=" * 60)
    return 0 if state["sent"] >= jumlah else 1


if __name__ == "__main__":
    sys.exit(main())
