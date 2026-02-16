#!/usr/bin/env python3.11
# -*- coding: utf-8 -*-
"""Import ch-stock datas/market_data.csv into MySQL market_history_daily."""

import argparse
import csv
import os
import re
from datetime import datetime

from db import upsert_market_history_rows


def _to_float(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    s = s.replace(",", "")
    if s.endswith("%"):
        s = s[:-1]
    try:
        return float(s)
    except Exception:
        return None


def _to_int(v):
    f = _to_float(v)
    return int(f) if f is not None else None


def _to_date(s):
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="/home/admin/.openclaw/workspace/projects/ch-stock/datas/market_data.csv")
    args = ap.parse_args()

    if not os.path.exists(args.csv):
        raise SystemExit(f"CSV not found: {args.csv}")

    rows = []
    with open(args.csv, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            d = _to_date(r.get("日期"))
            if not d:
                continue
            rows.append({
                "trade_date": d,
                "up_count": _to_int(r.get("上涨")),
                "down_count": _to_int(r.get("下跌")),
                "limit_up_count": _to_int(r.get("涨停")),
                "limit_down_count": _to_int(r.get("跌停")),
                "activity_pct": _to_float(r.get("活跃度")),
                # keep original csv unit as 万元 (same as ch-stock source)
                "turnover_wan": _to_float(r.get("成交额")),
                "financing_net_buy_wan": _to_float(r.get("融资净买入")),
                "source": "ch_stock_csv",
            })

    n = upsert_market_history_rows(rows)
    print(f"OK imported rows={len(rows)} affected={n}")


if __name__ == "__main__":
    main()
