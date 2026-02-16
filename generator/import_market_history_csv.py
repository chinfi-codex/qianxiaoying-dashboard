#!/usr/bin/env python3.11
# -*- coding: utf-8 -*-
"""Import ch-stock datas/market_data.csv into MySQL market_history_daily."""

import argparse
import csv
import os
from datetime import datetime

import requests

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


def _fetch_financing_from_tushare(token, start_ymd, end_ymd):
    """Return map: YYYY-MM-DD -> net_buy_wan (rzmre-rzche)."""
    if not token:
        return {}
    api = "https://api.tushare.pro"
    try:
        r = requests.post(
            api,
            json={
                "api_name": "margin",
                "token": token,
                "params": {"start_date": start_ymd, "end_date": end_ymd},
                "fields": "trade_date,rzmre,rzche",
            },
            timeout=60,
        )
        r.raise_for_status()
        j = r.json()
        if j.get("code") != 0:
            return {}
        data = j.get("data") or {}
        cols = data.get("fields") or []
        items = data.get("items") or []
        idx = {c: i for i, c in enumerate(cols)}
        if "trade_date" not in idx:
            return {}
        m = {}
        for it in items:
            td = str(it[idx["trade_date"]])
            d = _to_date(td)
            if not d:
                continue
            rzmre = _to_float(it[idx.get("rzmre", -1)] if idx.get("rzmre") is not None else None) or 0.0
            rzche = _to_float(it[idx.get("rzche", -1)] if idx.get("rzche") is not None else None) or 0.0
            m[d] = m.get(d, 0.0) + (rzmre - rzche)
        return m
    except Exception:
        return {}


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

    if rows:
        start_ymd = min(r["trade_date"] for r in rows).replace("-", "")
        end_ymd = max(r["trade_date"] for r in rows).replace("-", "")
        fin_map = _fetch_financing_from_tushare(os.environ.get("TUSHARE_TOKEN"), start_ymd, end_ymd)
        for r in rows:
            if r["trade_date"] in fin_map:
                r["financing_net_buy_wan"] = fin_map[r["trade_date"]]

    n = upsert_market_history_rows(rows)
    print(f"OK imported rows={len(rows)} affected={n}")


if __name__ == "__main__":
    main()
