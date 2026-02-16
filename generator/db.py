#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MySQL persistence for qianxiaoying-dashboard.

Env (recommended in workspace .env):
  MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD

Tables:
  daily_snapshot(trade_date, json_data)
  job_runs(...)
"""

import json
import os
import datetime as dt


def _env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def get_conn():
    try:
        import mysql.connector  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "Missing dependency mysql-connector-python. Install with: python3.11 -m pip install mysql-connector-python"
        ) from e

    host = _env("MYSQL_HOST", "127.0.0.1")
    port = int(_env("MYSQL_PORT", "3306"))
    db = _env("MYSQL_DB", "qxy")
    user = _env("MYSQL_USER", "qxy_user")
    pwd = _env("MYSQL_PASSWORD")
    if not pwd:
        raise RuntimeError("Missing env MYSQL_PASSWORD")

    return mysql.connector.connect(host=host, port=port, user=user, password=pwd, database=db)


def upsert_daily_snapshot(trade_date_ymd: str, payload: dict):
    """trade_date_ymd: YYYY-MM-DD"""
    d = dt.date.fromisoformat(trade_date_ymd)
    j = json.dumps(payload, ensure_ascii=False)
    cn = get_conn()
    try:
        cur = cn.cursor()
        cur.execute(
            """
            INSERT INTO daily_snapshot(trade_date, json_data)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE json_data=VALUES(json_data)
            """,
            (d, j),
        )
        cn.commit()
        cur.close()
    finally:
        cn.close()


def load_daily_snapshot(trade_date_ymd: str):
    d = dt.date.fromisoformat(trade_date_ymd)
    cn = get_conn()
    try:
        cur = cn.cursor()
        cur.execute("SELECT json_data FROM daily_snapshot WHERE trade_date=%s", (d,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return json.loads(row[0])
    finally:
        cn.close()


def list_snapshot_dates(limit=500):
    cn = get_conn()
    try:
        cur = cn.cursor()
        cur.execute(
            "SELECT trade_date FROM daily_snapshot ORDER BY trade_date DESC LIMIT %s",
            (int(limit),),
        )
        rows = cur.fetchall() or []
        cur.close()
        return [r[0].isoformat() for r in rows]
    finally:
        cn.close()


def upsert_market_history_rows(rows):
    """rows: list[dict] with keys:
    trade_date (YYYY-MM-DD), up_count, down_count, limit_up_count, limit_down_count,
    activity_pct, turnover_wan, financing_net_buy_wan, source
    """
    if not rows:
        return 0
    cn = get_conn()
    try:
        cur = cn.cursor()
        sql = (
            "INSERT INTO market_history_daily("
            "trade_date,up_count,down_count,limit_up_count,limit_down_count,activity_pct,turnover_wan,financing_net_buy_wan,source"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE "
            "up_count=VALUES(up_count),down_count=VALUES(down_count),limit_up_count=VALUES(limit_up_count),limit_down_count=VALUES(limit_down_count),"
            "activity_pct=VALUES(activity_pct),turnover_wan=VALUES(turnover_wan),financing_net_buy_wan=VALUES(financing_net_buy_wan),source=VALUES(source)"
        )
        params = []
        for r in rows:
            params.append((
                dt.date.fromisoformat(r["trade_date"]),
                r.get("up_count"),
                r.get("down_count"),
                r.get("limit_up_count"),
                r.get("limit_down_count"),
                r.get("activity_pct"),
                r.get("turnover_wan"),
                r.get("financing_net_buy_wan"),
                r.get("source") or "ch_stock_csv",
            ))
        cur.executemany(sql, params)
        cn.commit()
        n = cur.rowcount
        cur.close()
        return n
    finally:
        cn.close()


def get_market_history(end_date_ymd=None, limit=60):
    cn = get_conn()
    try:
        cur = cn.cursor(dictionary=True)
        if end_date_ymd:
            d = dt.date.fromisoformat(end_date_ymd)
            cur.execute(
                """
                SELECT trade_date,up_count,down_count,limit_up_count,limit_down_count,activity_pct,turnover_wan,financing_net_buy_wan
                FROM market_history_daily
                WHERE trade_date <= %s
                ORDER BY trade_date DESC
                LIMIT %s
                """,
                (d, int(limit)),
            )
        else:
            cur.execute(
                """
                SELECT trade_date,up_count,down_count,limit_up_count,limit_down_count,activity_pct,turnover_wan,financing_net_buy_wan
                FROM market_history_daily
                ORDER BY trade_date DESC
                LIMIT %s
                """,
                (int(limit),),
            )
        rows = cur.fetchall() or []
        cur.close()
        rows = list(reversed(rows))
        out = []
        for r in rows:
            out.append({
                "date": r["trade_date"].isoformat() if r.get("trade_date") else None,
                "上涨": r.get("up_count"),
                "下跌": r.get("down_count"),
                "涨停": r.get("limit_up_count"),
                "跌停": r.get("limit_down_count"),
                "活跃度": float(r["activity_pct"]) if r.get("activity_pct") is not None else None,
                "成交额": float(r["turnover_wan"]) if r.get("turnover_wan") is not None else None,
                "融资净买入": float(r["financing_net_buy_wan"]) if r.get("financing_net_buy_wan") is not None else None,
            })
        return out
    finally:
        cn.close()


def log_job_run(job_name: str, trade_date_ymd: str | None, status: str, error_text: str | None = None, meta: dict | None = None):
    """Append a job_runs row for observability."""
    cn = get_conn()
    try:
        cur = cn.cursor()
        td = dt.date.fromisoformat(trade_date_ymd) if trade_date_ymd else None
        cur.execute(
            """
            INSERT INTO job_runs(job_name, trade_date, status, finished_at, error_text, meta_json)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s, %s)
            """,
            (job_name, td, status, error_text, json.dumps(meta, ensure_ascii=False) if meta is not None else None),
        )
        cn.commit()
        cur.close()
    finally:
        cn.close()
