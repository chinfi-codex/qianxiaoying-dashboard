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
    """Map to 3 buckets per boss decision: 主板(沪深主板合并) / 创业板 / 科创板.
    北交所已在上游剔除。
    """
    code = ts_code.split(".")[0]
    if ts_code.endswith(".SH") and code.startswith("688"):
        return "科创板"
    if ts_code.endswith(".SZ") and code.startswith("300"):
        return "创业板"
    return "主板"


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
    # NOTE: daily_basic does NOT support comma-separated ts_code lists reliably.
    # We'll fetch total_mv later for Top200 only (200 calls).

    for i in range(0, len(universe), chunk):
        codes = universe[i:i+chunk]
        code_str = ",".join(codes)
        daily_rows.extend(_post("daily", token, {"trade_date": trade_date, "ts_code": code_str},
                               fields="ts_code,trade_date,close,amount"))
        time.sleep(args.sleep)
        adj_rows.extend(_post("adj_factor", token, {"trade_date": trade_date, "ts_code": code_str},
                             fields="ts_code,trade_date,adj_factor"))
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
    basic = {}

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
        # turnover from daily.amount: 千元 -> 元
        amount = d.get("amount")
        turnover_cny = float(amount) * 1000.0 if amount is not None else None
        turnover_yi = turnover_cny / 1e8 if turnover_cny is not None else None

        # total_mv from daily_basic.total_mv: 万元 -> 元
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

    # fetch total_mv for all stocks on trade_date (paged), then fill Top200
    mv_map = {}
    offset = 0
    limit = 5000
    while True:
        page = _post(
            "daily_basic",
            token,
            {"trade_date": trade_date, "limit": limit, "offset": offset},
            fields="ts_code,total_mv",
        )
        if not page:
            break
        for it in page:
            if it.get("ts_code") and it.get("total_mv") is not None:
                mv_map[it["ts_code"]] = float(it["total_mv"])
        if len(page) < limit:
            break
        offset += limit
        time.sleep(args.sleep)

    for r in top200:
        c = r["ts_code"]
        total_mv = mv_map.get(c)
        if total_mv is None:
            continue
        mktcap_cny = float(total_mv) * 10000.0
        mktcap_yi = mktcap_cny / 1e8
        r["mktcap_yi"] = round(mktcap_yi, 2)
        r["cap_bucket"] = _cap_bucket_yi(mktcap_yi)

    med_mktcap_yi = _median([r.get("mktcap_yi") for r in top200])
    med_turn_yi = _median([r.get("turnover_yi") for r in top200])

    # dominant board
    board_cnt = {}
    for r in top200:
        board_cnt[r.get("board")] = board_cnt.get(r.get("board"), 0) + 1
    dominant_board = None
    if board_cnt:
        dominant_board = sorted(board_cnt.items(), key=lambda kv: kv[1], reverse=True)[0][0]

    # -----------------
    # Pattern rules (v1): per PRD quantitative definitions
    # - wide box (120d) with 30% constraint
    # - pos_120 low
    # - 120d box breakout
    # - 250d new high
    # - high-position limit (sentiment high) via ret20/ret60
    # - bottom lift-off
    # - limit-up / streak
    # -----------------

    top_codes = [r["ts_code"] for r in top200]

    # history window: last 260 open days up to trade_date (for 250d features)
    hist_days = [d for d in open_days if d <= trade_date][-260:]
    hist_start = hist_days[0] if hist_days else trade_date

    def limit_up_threshold(code):
        b = _board(code)
        if b in ("创业板", "科创板"):
            return 19.8
        return 9.8

    def to_ymd(d):
        return _to_date_ymd(d)

    # Fetch qfq OHLC series for each code once; compute derived metrics
    series_map = {}  # code -> list of dict(date, o,h,l,c)
    close_map = {}   # code -> list of (date, close)

    for idx, code in enumerate(top_codes):
        drows = _post(
            "daily",
            token,
            {"ts_code": code, "start_date": hist_start, "end_date": trade_date},
            fields="trade_date,open,high,low,close",
        )
        time.sleep(args.sleep)
        arows = _post(
            "adj_factor",
            token,
            {"ts_code": code, "start_date": hist_start, "end_date": trade_date},
            fields="trade_date,adj_factor",
        )
        time.sleep(args.sleep)
        af = {x["trade_date"]: x.get("adj_factor") for x in arows if x.get("trade_date")}
        arr = []
        for x in drows:
            td = x.get("trade_date")
            if not td or td not in af:
                continue
            f = float(af[td])
            arr.append({
                "date": td,
                "o": float(x.get("open")) * f,
                "h": float(x.get("high")) * f,
                "l": float(x.get("low")) * f,
                "c": float(x.get("close")) * f,
            })
        arr.sort(key=lambda z: z["date"])
        if not arr:
            continue
        series_map[code] = arr
        close_map[code] = [(z["date"], z["c"]) for z in arr]

    def _slice_last(arr, n):
        return arr[-n:] if len(arr) >= n else arr[:]

    def _hhv(vals):
        return max(vals) if vals else None

    def _llv(vals):
        return min(vals) if vals else None

    def _ma(vals, n):
        if len(vals) < n:
            return None
        return sum(vals[-n:]) / float(n)

    def calc_pos_120(code):
        arr = series_map.get(code) or []
        arr120 = _slice_last(arr, 120)
        hs = [z["h"] for z in arr120]
        ls = [z["l"] for z in arr120]
        hi = _hhv(hs)
        lo = _llv(ls)
        if hi is None or lo is None or hi == lo:
            return None
        c = arr120[-1]["c"]
        return (c - lo) / (hi - lo)

    def wide_box_120(code):
        arr = series_map.get(code) or []
        arr120 = _slice_last(arr, 120)
        if len(arr120) < 60:
            return False
        upper = _hhv([z["h"] for z in arr120])
        lower = _llv([z["l"] for z in arr120])
        if upper is None or lower is None or upper == lower:
            return False
        height = upper - lower
        mid = (upper + lower) / 2.0
        mean_abs_dev = sum([abs(z["c"] - mid) for z in arr120]) / float(len(arr120)) / height
        closes = [z["c"] for z in arr120]
        band_width = (_hhv(closes) - _llv(closes)) / height
        return (mean_abs_dev <= 0.30) and (band_width <= 0.30)

    def box_breakout_120(code):
        arr = series_map.get(code) or []
        arr120 = _slice_last(arr, 120)
        if len(arr120) < 60:
            return False
        upper = _hhv([z["h"] for z in arr120[:-1]])
        if upper is None:
            return False
        return arr120[-1]["c"] > upper * 1.005

    def new_high_250(code):
        arr = series_map.get(code) or []
        arr250 = _slice_last(arr, 250)
        if len(arr250) < 120:
            return False
        prev_high = _hhv([z["h"] for z in arr250[:-1]])
        if prev_high is None:
            return False
        return arr250[-1]["c"] > prev_high * 1.003

    def ret_nd(code, n):
        arr = series_map.get(code) or []
        if len(arr) < n + 1:
            return None
        p0 = arr[-n-1]["c"]
        p1 = arr[-1]["c"]
        if p0 == 0:
            return None
        return (p1 / p0) - 1.0

    def limit_streak(code):
        arr = series_map.get(code) or []
        if len(arr) < 2:
            return 0
        th = limit_up_threshold(code)
        cnt = 0
        # compute qfq pct day by day backwards
        for i in range(len(arr)-1, 0, -1):
            p1 = arr[i]["c"]
            p0 = arr[i-1]["c"]
            if p0 == 0:
                break
            pct = (p1 / p0 - 1.0) * 100.0
            if pct >= th:
                cnt += 1
            else:
                break
        return cnt

    def bottom_liftoff(code):
        if not wide_box_120(code):
            return False
        pos = calc_pos_120(code)
        if pos is None or pos > 0.25:
            return False
        arr = series_map.get(code) or []
        closes = [z["c"] for z in arr]
        ma20 = _ma(closes, 20)
        if ma20 is None:
            return False
        # slope approx: ma20 today - ma20 10 days ago
        if len(closes) < 30:
            return False
        ma20_prev = sum(closes[-30:-10]) / 20.0
        slope = ma20 - ma20_prev
        return (closes[-1] > ma20) and (slope > 0)

    # Assign patterns with priority
    priority = [
        "连板",
        "低位首板",
        "首板",
        "120日箱体突破",
        "历史新高",
        "低位突破",
        "箱体底部启动",
        "高位板",
    ]

    def pick_primary(labels):
        for p in priority:
            if p in labels:
                return p
        return "—"

    pattern_cnt = {}
    for r in top200:
        c = r["ts_code"]
        labels = []
        st = limit_streak(c)
        r["streak"] = st
        if st >= 2:
            labels.append("连板")
        elif st == 1:
            labels.append("首板")

        pos = calc_pos_120(c)
        is_low = (pos is not None and pos <= 0.30)

        # low breakout
        arr = series_map.get(c) or []
        arr20 = _slice_last(arr, 20)
        if len(arr20) >= 10:
            prev_hhv = _hhv([z["c"] for z in arr20[:-1]])
            if prev_hhv is not None and arr20[-1]["c"] > prev_hhv * 1.005 and is_low:
                labels.append("低位突破")
                if st == 1:
                    labels.append("低位首板")

        if wide_box_120(c) and box_breakout_120(c):
            labels.append("120日箱体突破")

        if new_high_250(c):
            labels.append("历史新高")

        # sentiment high-limit
        r20 = ret_nd(c, 20)
        r60 = ret_nd(c, 60)
        if st >= 1 and ((r20 is not None and r20 >= 0.50) or (r60 is not None and r60 >= 1.00)):
            labels.append("高位板")

        if bottom_liftoff(c):
            labels.append("箱体底部启动")

        primary = pick_primary(labels)
        r["pattern"] = primary
        r["pattern_labels"] = labels
        pattern_cnt[primary] = pattern_cnt.get(primary, 0) + 1

    dominant_pattern = None
    if pattern_cnt:
        items = [(k, v) for k, v in pattern_cnt.items() if k != "—"]
        if not items:
            items = list(pattern_cnt.items())
        dominant_pattern = sorted(items, key=lambda kv: kv[1], reverse=True)[0][0]

    concl = "今日赚钱效应："
    if dominant_board:
        concl += "%s占优" % dominant_board
    if dominant_pattern and dominant_pattern != "—":
        concl += "，形态以%s为主" % dominant_pattern
    else:
        concl += "，形态分散"

    # Build pattern gallery groups (top8 by pct per category), include 120-day qfq Kline.
    # Candles will be colored on frontend: red=up, green=down.

    def kline_120(code):
        arr = series_map.get(code) or []
        arr = arr[-120:]
        outk = []
        for z in arr:
            outk.append([
                _to_date_ymd(z["date"]),
                round(z["o"], 4),
                round(z["h"], 4),
                round(z["l"], 4),
                round(z["c"], 4),
            ])
        return outk

    pattern_groups = {}
    pats = {}
    for r in top200:
        p = r.get("pattern")
        if not p or p == "—":
            continue
        pats.setdefault(p, []).append(r)

    for p, items in pats.items():
        items = sorted(items, key=lambda x: x.get("pct_chg") or -999, reverse=True)[:8]
        grp = []
        for it in items:
            code = it.get("ts_code")
            grp.append({
                "ts_code": code,
                "name": it.get("name"),
                "pct_chg": it.get("pct_chg"),
                "kline_120": kline_120(code),
            })
        pattern_groups[p] = grp

    out = {
        "date": _to_date_ymd(trade_date),
        "conclusion": concl,
        "kpi": {
            "median_mktcap_cny": int(round(med_mktcap_yi * 1e8)) if med_mktcap_yi is not None else None,
            "median_turnover_cny": int(round(med_turn_yi * 1e8)) if med_turn_yi is not None else None,
            "dominant_board": dominant_board,
            "dominant_pattern": dominant_pattern,
        },
        "top200": top200,
        "pattern_groups": pattern_groups,
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
