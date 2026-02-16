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
