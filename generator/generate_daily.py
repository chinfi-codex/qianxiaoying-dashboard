#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate daily 赚钱效应 snapshot JSON for GitHub Pages.

Data source: Tushare Pro (HTTP JSON).

Env:
  TUSHARE_TOKEN

Output:
  site/data/YYYY-MM-DD.json
  site/data/latest.json
  site/data/index.json (append date)

Notes:
- Top200 is based on 前复权涨跌幅 (computed from close*adj_factor).
- Filters out ST / *ST / 退市 stocks (by name) and BJ exchange (.BJ).
- Turnover buckets: >10亿, 3-10亿, <3亿 (in CNY).
"""

import argparse
import datetime as dt
import json
import os
import sys
import time

try:
    import requests
except ImportError:
    print("Missing dependency: requests. Install: pip3 install requests", file=sys.stderr)
    raise

API_URL = "https://api.tushare.pro"


def _post(api_name, token, params=None, fields=None, timeout=30):
    payload = {
        "api_name": api_name,
        "token": token,
        "params": params or {},
    }
    if fields:
        payload["fields"] = fields
    r = requests.post(API_URL, json=payload, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    if j.get("code") != 0:
        raise RuntimeError("Tushare error: %s" % j)
    data = j.get("data") or {}
    cols = data.get("fields") or []
    items = data.get("items") or []
    out = []
    for it in items:
        out.append(dict(zip(cols, it)))
    return out


def _to_date_ymd(trade_date):
    # trade_date: YYYYMMDD
    s = str(trade_date)
    return "%s-%s-%s" % (s[:4], s[4:6], s[6:8])


def _board(ts_code):
    if ts_code.endswith(".BJ"):
        return "北交所"
    code = ts_code.split(".")[0]
    if ts_code.endswith(".SH") and code.startswith("688"):
        return "科创板"
    if ts_code.endswith(".SZ") and code.startswith("300"):
        return "创业板"
    if ts_code.endswith(".SH"):
        return "上证"
    # default: treat SZ as 创业/深主板; prd wants three boards, we map remaining SZ to 创业? better: 深市并入创业板会误导
    return "上证"


def _cap_bucket_yi(mktcap_yi):
    if mktcap_yi is None:
        return None
    if mktcap_yi < 50:
        return "微盘"
    if mktcap_yi <= 200:
        return "中盘"
    return "大盘"


def _turn_bucket_yi(turnover_yi):
    if turnover_yi is None:
        return None
    if turnover_yi > 10:
        return ">10亿"
    if turnover_yi >= 3:
        return "3-10亿"
    return "<3亿"


def _median(nums):
    xs = [x for x in nums if x is not None]
    xs.sort()
    if not xs:
        return None
    n = len(xs)
    mid = n // 2
    if n % 2 == 1:
        return xs[mid]
    return (xs[mid - 1] + xs[mid]) / 2.0


def _ensure_dir(p):
    if not os.path.isdir(p):
        os.makedirs(p)


def _load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="target date YYYYMMDD (defaults: today)")
    ap.add_argument("--sleep", type=float, default=0.12, help="sleep between api calls")
    args = ap.parse_args()

    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        print("Missing env: TUSHARE_TOKEN", file=sys.stderr)
        return 2

    today = dt.datetime.now().strftime("%Y%m%d")
    target = args.date or today

    # resolve to an open trading day <= target
    cal = _post("trade_cal", token, {"exchange": "SSE", "start_date": "20000101", "end_date": target},
                fields="cal_date,is_open")
    open_days = [x["cal_date"] for x in cal if int(x.get("is_open") or 0) == 1]
    if not open_days:
        raise RuntimeError("No open trading days found up to %s" % target)
    # API ordering is not guaranteed; normalize
    open_days = sorted(open_days)
    trade_date = open_days[-1]

    # universe
    sb = _post("stock_basic", token, {"list_status": "L"}, fields="ts_code,name")
    universe = []
    st_set = set()
    for x in sb:
        ts_code = x.get("ts_code")
        name = (x.get("name") or "").upper()
        if not ts_code:
            continue
        if ts_code.endswith(".BJ"):
            continue
        # crude ST filter
        if "ST" in name or "退" in name:
            st_set.add(ts_code)
            continue
        universe.append(ts_code)

    # fetch daily + adj_factor + daily_basic in chunks
    chunk = 500
    daily_rows = []
    adj_rows = []
    basic_rows = []

    for i in range(0, len(universe), chunk):
        codes = universe[i:i+chunk]
        code_str = ",".join(codes)
        daily_rows.extend(_post("daily", token, {"trade_date": trade_date, "ts_code": code_str},
                               fields="ts_code,trade_date,close"))
        time.sleep(args.sleep)
        adj_rows.extend(_post("adj_factor", token, {"trade_date": trade_date, "ts_code": code_str},
                             fields="ts_code,trade_date,adj_factor"))
        time.sleep(args.sleep)
        basic_rows.extend(_post("daily_basic", token, {"trade_date": trade_date, "ts_code": code_str},
                               fields="ts_code,trade_date,amount,total_mv"))
        time.sleep(args.sleep)

    # previous open day for pct
    prev_date = open_days[-2] if len(open_days) >= 2 else None
    prev_daily = {}
    prev_adj = {}
    if prev_date:
        for i in range(0, len(universe), chunk):
            codes = universe[i:i+chunk]
            code_str = ",".join(codes)
            for r in _post("daily", token, {"trade_date": prev_date, "ts_code": code_str}, fields="ts_code,close"):
                prev_daily[r["ts_code"]] = r.get("close")
            time.sleep(args.sleep)
            for r in _post("adj_factor", token, {"trade_date": prev_date, "ts_code": code_str}, fields="ts_code,adj_factor"):
                prev_adj[r["ts_code"]] = r.get("adj_factor")
            time.sleep(args.sleep)

    daily = {r["ts_code"]: r for r in daily_rows if r.get("ts_code")}
    adj = {r["ts_code"]: r for r in adj_rows if r.get("ts_code")}
    basic = {r["ts_code"]: r for r in basic_rows if r.get("ts_code")}

    rows = []
    for ts_code, d in daily.items():
        if ts_code in st_set:
            continue
        af = (adj.get(ts_code) or {}).get("adj_factor")
        close = d.get("close")
        if close is None or af is None:
            continue
        adj_close = float(close) * float(af)

        p_close = prev_daily.get(ts_code)
        p_af = prev_adj.get(ts_code)
        pct = None
        if p_close is not None and p_af is not None:
            p_adj_close = float(p_close) * float(p_af)
            if p_adj_close != 0:
                pct = (adj_close / p_adj_close - 1.0) * 100.0

        b = basic.get(ts_code) or {}
        # amount: 千元 -> 元
        amount = b.get("amount")
        turnover_cny = float(amount) * 1000.0 if amount is not None else None
        turnover_yi = turnover_cny / 1e8 if turnover_cny is not None else None

        # total_mv: 万元 -> 元
        total_mv = b.get("total_mv")
        mktcap_cny = float(total_mv) * 10000.0 if total_mv is not None else None
        mktcap_yi = mktcap_cny / 1e8 if mktcap_cny is not None else None

        rows.append({
            "ts_code": ts_code,
            "name": "",
            "pct_chg": round(pct, 2) if pct is not None else None,
            "mktcap_yi": round(mktcap_yi, 2) if mktcap_yi is not None else None,
            "turnover_yi": round(turnover_yi, 2) if turnover_yi is not None else None,
            "board": _board(ts_code),
            "pattern": "—",
            "cap_bucket": _cap_bucket_yi(mktcap_yi) if mktcap_yi is not None else None,
            "turn_bucket": _turn_bucket_yi(turnover_yi) if turnover_yi is not None else None,
        })

    # fill names from stock_basic
    name_map = {x.get("ts_code"): x.get("name") for x in sb if x.get("ts_code")}
    for r in rows:
        r["name"] = name_map.get(r["ts_code"], "")

    # sort by pct desc; take top200
    rows = [r for r in rows if r.get("pct_chg") is not None]
    rows.sort(key=lambda x: x.get("pct_chg"), reverse=True)
    top200 = rows[:200]

    med_mktcap_yi = _median([r.get("mktcap_yi") for r in top200])
    med_turn_yi = _median([r.get("turnover_yi") for r in top200])

    # dominant board
    board_cnt = {}
    for r in top200:
        board_cnt[r.get("board")] = board_cnt.get(r.get("board"), 0) + 1
    dominant_board = None
    if board_cnt:
        dominant_board = sorted(board_cnt.items(), key=lambda kv: kv[1], reverse=True)[0][0]

    out = {
        "date": _to_date_ymd(trade_date),
        "conclusion": "（自动生成占位）今日赚钱效应：待规则化结论。",
        "kpi": {
            "median_mktcap_cny": int(round(med_mktcap_yi * 1e8)) if med_mktcap_yi is not None else None,
            "median_turnover_cny": int(round(med_turn_yi * 1e8)) if med_turn_yi is not None else None,
            "dominant_board": dominant_board,
            "dominant_pattern": "待定",
        },
        "top200": top200,
    }

    data_dir = os.path.join(os.path.dirname(__file__), "..", "site", "data")
    data_dir = os.path.abspath(data_dir)
    _ensure_dir(data_dir)

    date_key = out["date"]
    with open(os.path.join(data_dir, "%s.json" % date_key), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    with open(os.path.join(data_dir, "latest.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    index_path = os.path.join(data_dir, "index.json")
    idx = _load_json(index_path, {"dates": []})
    if date_key not in idx.get("dates", []):
        idx["dates"].append(date_key)
        idx["dates"].sort(reverse=True)
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(idx, f, ensure_ascii=False, indent=2)

    print("OK", date_key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
