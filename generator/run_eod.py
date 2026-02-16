#!/usr/bin/env python3.11
# -*- coding: utf-8 -*-
"""Run end-of-day snapshot job (scheduled at 17:00 daily).

Flow:
1) Check SSE trade_cal for today
2) If open day: generate_daily.py --mysql
3) Export latest/index from MySQL to site/data
4) Commit & push to GitHub (optional; default on)

Hardening:
- lock file (avoid concurrent runs)
- subprocess timeouts
"""

import datetime as dt
import json
import os
import pathlib
import subprocess
import sys
import fcntl

import requests

ROOT = pathlib.Path(__file__).resolve().parents[1]
GEN = ROOT / "generator"
ENV = pathlib.Path("/home/admin/.openclaw/workspace/.env")
LOCK = pathlib.Path("/tmp/qxy_eod.lock")


def load_env(path: pathlib.Path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def run(cmd, timeout=900, cwd=None):
    p = subprocess.run(cmd, cwd=cwd, timeout=timeout, text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(f"cmd failed: {' '.join(cmd)}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}")
    return p.stdout.strip()


def is_trade_day(date_ymd: str, token: str) -> bool:
    r = requests.post(
        "https://api.tushare.pro",
        json={
            "api_name": "trade_cal",
            "token": token,
            "params": {"exchange": "SSE", "start_date": date_ymd, "end_date": date_ymd},
            "fields": "cal_date,is_open",
        },
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()
    if j.get("code") != 0:
        raise RuntimeError(f"tushare trade_cal error: {j}")
    items = (j.get("data") or {}).get("items") or []
    if not items:
        return False
    is_open = int(items[0][1]) if len(items[0]) > 1 else 0
    return is_open == 1


def main():
    load_env(ENV)
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        raise SystemExit("Missing TUSHARE_TOKEN")

    with LOCK.open("w") as lf:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

        today = dt.datetime.now().strftime("%Y%m%d")
        if not is_trade_day(today, token):
            print(json.dumps({"ok": True, "skipped": True, "reason": "non-trading-day", "date": today}, ensure_ascii=False))
            return 0

        # 1) generate + persist MySQL
        run(["python3.11", str(GEN / "generate_daily.py"), "--date", today, "--sleep", "0.08", "--mysql"], timeout=1800, cwd=str(ROOT))

        # 2) export latest/index
        run(["python3.11", str(GEN / "export_from_mysql.py"), "--latest", "--rebuild-index", "--limit", "500"], timeout=300, cwd=str(ROOT))

        # 3) git push if changed
        st = run(["git", "status", "--porcelain"], timeout=30, cwd=str(ROOT))
        if st.strip():
            run(["git", "add", "site/data"], timeout=30, cwd=str(ROOT))
            run(["git", "commit", "-m", f"EOD snapshot {today}"], timeout=60, cwd=str(ROOT))
            run(["git", "push", "origin", "main"], timeout=120, cwd=str(ROOT))
            print(json.dumps({"ok": True, "date": today, "pushed": True}, ensure_ascii=False))
        else:
            print(json.dumps({"ok": True, "date": today, "pushed": False, "reason": "no-data-change"}, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
