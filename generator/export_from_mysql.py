#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Export snapshots from MySQL to site/data/*.json for GitHub Pages.

Usage:
  python3.11 export_from_mysql.py --date 2026-02-13
  python3.11 export_from_mysql.py --latest
  python3.11 export_from_mysql.py --rebuild-index --limit 500

Notes:
- This keeps GitHub Pages as the viewer, while MySQL is the durable store.
"""

import argparse
import datetime as dt
import json
import os

import requests

from db import load_daily_snapshot, list_snapshot_dates, get_market_history


def _ensure_dir(p):
    if not os.path.isdir(p):
        os.makedirs(p)


def _build_trade_calendar(days_back=365, days_forward=30):
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        return {"generated_at": dt.datetime.now().isoformat(), "open_days": []}
    end = dt.date.today() + dt.timedelta(days=days_forward)
    start = dt.date.today() - dt.timedelta(days=days_back)
    try:
        r = requests.post(
            "https://api.tushare.pro",
            json={
                "api_name": "trade_cal",
                "token": token,
                "params": {
                    "exchange": "SSE",
                    "start_date": start.strftime("%Y%m%d"),
                    "end_date": end.strftime("%Y%m%d"),
                },
                "fields": "cal_date,is_open",
            },
            timeout=30,
        )
        r.raise_for_status()
        j = r.json()
        if j.get("code") != 0:
            return {"generated_at": dt.datetime.now().isoformat(), "open_days": []}
        items = (j.get("data") or {}).get("items") or []
        open_days = sorted([f"{str(x[0])[:4]}-{str(x[0])[4:6]}-{str(x[0])[6:8]}" for x in items if int(x[1]) == 1])
        return {"generated_at": dt.datetime.now().isoformat(), "open_days": open_days}
    except Exception:
        return {"generated_at": dt.datetime.now().isoformat(), "open_days": []}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="export a specific date YYYY-MM-DD")
    ap.add_argument("--latest", action="store_true", help="export latest as latest.json")
    ap.add_argument("--rebuild-index", action="store_true", help="rebuild index.json from DB")
    ap.add_argument("--limit", type=int, default=500)
    args = ap.parse_args()

    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "site", "data"))
    _ensure_dir(data_dir)

    if args.date:
        payload = load_daily_snapshot(args.date)
        if not payload:
            raise SystemExit(f"No snapshot in DB for {args.date}")
        payload["market_history"] = get_market_history(end_date_ymd=args.date, limit=60)
        with open(os.path.join(data_dir, f"{args.date}.json"), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    if args.latest:
        dates = list_snapshot_dates(limit=1)
        if not dates:
            raise SystemExit("No snapshots in DB")
        payload = load_daily_snapshot(dates[0])
        payload["market_history"] = get_market_history(end_date_ymd=dates[0], limit=60)
        with open(os.path.join(data_dir, "latest.json"), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    if args.rebuild_index:
        # Keep union: DB dates + existing site/data/*.json (backfill runs before MySQL)
        dates = list_snapshot_dates(limit=args.limit)
        try:
            existing = []
            for fn in os.listdir(data_dir):
                if fn.startswith("20") and fn.endswith(".json") and fn != "latest.json":
                    existing.append(fn[:-5])
            dates = sorted(set(dates) | set(existing), reverse=True)
        except Exception:
            pass

        idx = {"dates": dates}
        with open(os.path.join(data_dir, "index.json"), "w", encoding="utf-8") as f:
            json.dump(idx, f, ensure_ascii=False, indent=2)

        db_dates = list_snapshot_dates(limit=args.limit)
        with open(os.path.join(data_dir, "db_dates.json"), "w", encoding="utf-8") as f:
            json.dump({"dates": db_dates}, f, ensure_ascii=False, indent=2)

        cal = _build_trade_calendar(days_back=365, days_forward=45)
        with open(os.path.join(data_dir, "trade_calendar.json"), "w", encoding="utf-8") as f:
            json.dump(cal, f, ensure_ascii=False, indent=2)

        with open(os.path.join(data_dir, "data_status.json"), "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": dt.datetime.now().isoformat(),
                "mysql_snapshot_dates": db_dates,
                "trade_calendar_open_days": cal.get("open_days", []),
            }, f, ensure_ascii=False, indent=2)

    print("OK")


if __name__ == "__main__":
    main()
