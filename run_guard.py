"""run_guard.py — Pengaman anti-double-kirim untuk semua produk bot.

Cara kerja: setiap leg mengklaim (target, workflow) dengan run_id-nya
via RPC claim_run di Supabase panel. RPC hanya menerima klaim bila run_id
lebih besar dari yang tersimpan (run_id GitHub selalu naik). Bila ada run
yang LEBIH BARU sudah klaim target ini, run ini dianggap basi — mis. run
lama yang nyangkut di antrean GitHub lalu jalan belakangan — dan harus
berhenti sebelum kirim apa pun.

Dipakai oleh: follow_cloud.py, like_cloud.py, like_profile_cloud.py,
like_house_cloud.py, visit_order.py. Panggil guard_or_exit() di awal main(),
setelah target diketahui dan sebelum bot apa pun disentuh.
"""
import json
import os
import sys
import urllib.request


def _supabase():
    url = os.getenv("SUPABASE_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    return url, key


def claim_run(target_uuid, workflow, run_id):
    """Klaim (target, workflow) untuk run_id ini. True bila run ini yang
    terbaru (boleh jalan), False bila ada run lebih baru (basi, harus
    berhenti). Bila RPC tidak bisa dihubungi, kembalikan True (fail-open)
    agar order normal tidak macet karena gangguan sesaat."""
    url, key = _supabase()
    if not url or not key:
        print("[run_guard] SUPABASE_URL/KEY tidak ada — lewati pengaman (fail-open).")
        return True
    body = json.dumps({
        "p_target": str(target_uuid),
        "p_workflow": str(workflow),
        "p_run_id": int(run_id),
    }).encode("utf-8")
    req = urllib.request.Request(
        url + "/rest/v1/rpc/claim_run",
        data=body,
        headers={
            "apikey": key,
            "Authorization": "Bearer " + key,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            result = json.loads(resp.read().decode() or "true")
            return bool(result)
    except Exception as e:
        print(f"[run_guard] RPC claim_run gagal ({e}) — lewati pengaman (fail-open).")
        return True


def guard_or_exit(target_uuid, workflow):
    """Panggil di awal main(). Bila run ini basi, cetak pesan dan keluar
    dengan kode 0 (bukan error) agar job tidak ditandai gagal."""
    run_id = os.getenv("GITHUB_RUN_ID", "")
    if not run_id:
        return
    try:
        run_id_int = int(run_id)
    except ValueError:
        return
    ok = claim_run(target_uuid, workflow, run_id_int)
    if ok:
        print(f"[run_guard] OK: run {run_id_int} adalah yang terbaru untuk {workflow} target {target_uuid}.")
        return
    print(
        f"[run_guard] BERHENTI: run {run_id_int} BASI untuk {workflow} target {target_uuid} — "
        "sudah ada run yang lebih baru. Tidak kirim apa pun."
    )
    sys.exit(0)
