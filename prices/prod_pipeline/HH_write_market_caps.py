#!/usr/bin/env python3
from __future__ import annotations

import os
from collections import deque
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

from cassandra.query import SimpleStatement

from common import (
    Heartbeat,
    PipelineHealthTracker,
    TABLE_10M,
    TABLE_DAILY,
    TABLE_HOURLY,
    connect_astra,
    drain_async,
    enqueue_async,
    floor_10m,
    floor_hour,
    now_str,
    now_utc,
    should_log_progress,
    to_cassandra_ts,
    to_utc,
)


TABLE_LIVE = os.getenv("PP_TABLE_LIVE", "pp_prices_live")
TABLE_MCAP_10M = os.getenv("PP_TABLE_MCAP_10M", "pp_market_cap_10m_7d")
TABLE_MCAP_HOURLY = os.getenv("PP_TABLE_MCAP_HOURLY", "pp_market_cap_hourly_30d")
TABLE_MCAP_DAILY = os.getenv("PP_TABLE_MCAP_DAILY", "pp_market_cap_daily_contin")

PP_TOP_N = int(os.getenv("PP_TOP_N", "1000"))
MCAP_10M_SLOTS = max(1, int(os.getenv("PP_MCAP_10M_SLOTS", "12")))
MCAP_HOURS = max(1, int(os.getenv("PP_MCAP_HOURS", "24")))
MCAP_DAYS = max(1, int(os.getenv("PP_MCAP_DAYS", "7")))
MCAP_10M_CARRY_HOURS = max(0, int(os.getenv("PP_MCAP_10M_CARRY_HOURS", "8")))
MCAP_HOURLY_CARRY_HOURS = max(0, int(os.getenv("PP_MCAP_HOURLY_CARRY_HOURS", "12")))
MCAP_DAILY_CARRY_DAYS = max(0, int(os.getenv("PP_MCAP_DAILY_CARRY_DAYS", "3")))
REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
ASTRA_MAX_IN_FLIGHT = int(os.getenv("PP_ASTRA_MAX_IN_FLIGHT", "64"))


def _f(x):
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def _to_date_key(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    year = getattr(value, "year", None)
    month = getattr(value, "month", None)
    day_num = getattr(value, "day", None)
    if isinstance(year, int) and isinstance(month, int) and isinstance(day_num, int):
        try:
            return date(year, month, day_num)
        except Exception:
            return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if "T" in text:
            text = text.split("T", 1)[0]
        try:
            return date.fromisoformat(text)
        except Exception:
            return None

    return None


def rank_map(cat_to_mcap: dict[str, float]) -> dict[str, int]:
    items = [(c, m) for c, m in cat_to_mcap.items() if c != "ALL"]
    items.sort(key=lambda t: t[1], reverse=True)
    out = {c: i + 1 for i, (c, _m) in enumerate(items)}
    out["ALL"] = 0
    return out


def main() -> None:
    hb = Heartbeat("HH_write_market_caps")
    session, cluster = connect_astra()
    tracker = PipelineHealthTracker(session, "HH_write_market_caps")
    tracker.set_metric("top_n", PP_TOP_N)
    tracker.set_metric("mcap_10m_slots", MCAP_10M_SLOTS)
    tracker.set_metric("mcap_hours", MCAP_HOURS)
    tracker.set_metric("mcap_days", MCAP_DAYS)
    tracker.set_metric("mcap_10m_carry_hours", MCAP_10M_CARRY_HOURS)
    tracker.set_metric("mcap_hourly_carry_hours", MCAP_HOURLY_CARRY_HOURS)
    tracker.set_metric("mcap_daily_carry_days", MCAP_DAILY_CARRY_DAYS)
    tracker.start()
    try:
        sel_live = SimpleStatement(
            f"SELECT id, category, market_cap_rank FROM {TABLE_LIVE}",
            fetch_size=2000,
        )
        live_rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC))
        live_rows = [r for r in live_rows if isinstance(getattr(r, "market_cap_rank", None), int) and r.market_cap_rank > 0]
        live_rows.sort(key=lambda r: r.market_cap_rank)
        coins = live_rows[:PP_TOP_N]
        if not coins:
            print(f"[{now_str()}] No ranked rows in {TABLE_LIVE}.")
            tracker.mark_noop()
            tracker.set_metric("coins_scoped", 0)
            tracker.finish("noop")
            return
        tracker.set_metric("coins_scoped", len(coins))

        print(f"[{now_str()}] Recomputing market caps for {len(coins)} coin(s), TOP_N={PP_TOP_N}")

        now_ts = now_utc()
        end_10m = floor_10m(now_ts) + timedelta(minutes=10)
        start_10m = end_10m - timedelta(minutes=10 * MCAP_10M_SLOTS)
        carry_10m = timedelta(hours=MCAP_10M_CARRY_HOURS) if MCAP_10M_CARRY_HOURS > 0 else None
        query_start_10m = start_10m - carry_10m if carry_10m is not None else start_10m
        slots_10m = [start_10m + timedelta(minutes=10 * i) for i in range(MCAP_10M_SLOTS)]

        end_hour = floor_hour(now_ts) + timedelta(hours=1)
        start_hour = end_hour - timedelta(hours=MCAP_HOURS)
        carry_hour = timedelta(hours=MCAP_HOURLY_CARRY_HOURS) if MCAP_HOURLY_CARRY_HOURS > 0 else None
        query_start_hour = start_hour - carry_hour if carry_hour is not None else start_hour
        slots_hour = [start_hour + timedelta(hours=i) for i in range(MCAP_HOURS)]

        end_day = now_ts.date()
        start_day = end_day - timedelta(days=MCAP_DAYS - 1)
        carry_day = timedelta(days=MCAP_DAILY_CARRY_DAYS) if MCAP_DAILY_CARRY_DAYS > 0 else None
        query_start_day = start_day - carry_day if carry_day is not None else start_day
        slots_day = [start_day + timedelta(days=i) for i in range(MCAP_DAYS)]

        sel_10m = session.prepare(
            f"""
            SELECT ts, market_cap, volume_24h, last_updated
            FROM {TABLE_10M}
            WHERE id=? AND ts>=? AND ts<?
            """
        )
        sel_hourly = session.prepare(
            f"""
            SELECT ts, market_cap, volume_24h, last_updated
            FROM {TABLE_HOURLY}
            WHERE id=? AND ts>=? AND ts<?
            """
        )
        sel_daily = session.prepare(
            f"""
            SELECT date, market_cap, volume_24h, last_updated
            FROM {TABLE_DAILY}
            WHERE id=? AND date>=? AND date<=?
            """
        )

        ins_10m = session.prepare(
            f"""
            INSERT INTO {TABLE_MCAP_10M}
              (category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )
        ins_hourly = session.prepare(
            f"""
            INSERT INTO {TABLE_MCAP_HOURLY}
              (category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )
        ins_daily = session.prepare(
            f"""
            INSERT INTO {TABLE_MCAP_DAILY}
              (category, date, last_updated, market_cap, market_cap_rank, volume_24h)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )

        # ts -> category -> {mcap, vol, lu}
        agg10: dict[Any, dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(lambda: {"mcap": 0.0, "vol": 0.0, "lu": None}))
        aggH: dict[Any, dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(lambda: {"mcap": 0.0, "vol": 0.0, "lu": None}))
        aggD: dict[Any, dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(lambda: {"mcap": 0.0, "vol": 0.0, "lu": None}))

        def bump(target, key, cat, mcap, vol, lu):
            e = target[key][cat]
            e["mcap"] += _f(mcap)
            e["vol"] += _f(vol)
            lu = to_utc(lu)
            if lu is not None and (e["lu"] is None or lu > e["lu"]):
                e["lu"] = lu

        def sort_ts_rows(rows):
            out: list[tuple[Any, Any]] = []
            for r in rows:
                ts = to_utc(getattr(r, "ts", None))
                if ts is None:
                    continue
                out.append((ts, r))
            out.sort(key=lambda t: t[0])
            return out

        def sort_day_rows(rows):
            out: list[tuple[Any, Any]] = []
            for r in rows:
                d = _to_date_key(getattr(r, "date", None))
                if d is None:
                    continue
                out.append((d, r))
            out.sort(key=lambda t: t[0])
            return out

        def bump_ts_with_carry(target, bucket_ts, cat, sorted_rows, max_age):
            pos = 0
            last: tuple[Any, Any] | None = None
            for ts in bucket_ts:
                while pos < len(sorted_rows) and sorted_rows[pos][0] <= ts:
                    last = sorted_rows[pos]
                    pos += 1
                if last is None:
                    continue
                src_ts, src_row = last
                if max_age is not None and (ts - src_ts) > max_age:
                    continue
                mcap = getattr(src_row, "market_cap", None)
                vol = getattr(src_row, "volume_24h", None)
                lu = getattr(src_row, "last_updated", None)
                bump(target, ts, cat, mcap, vol, lu)
                bump(target, ts, "ALL", mcap, vol, lu)

        def bump_day_with_carry(target, bucket_days, cat, sorted_rows, max_age):
            pos = 0
            last: tuple[Any, Any] | None = None
            for d in bucket_days:
                while pos < len(sorted_rows) and sorted_rows[pos][0] <= d:
                    last = sorted_rows[pos]
                    pos += 1
                if last is None:
                    continue
                src_day, src_row = last
                if max_age is not None and (d - src_day) > max_age:
                    continue
                mcap = getattr(src_row, "market_cap", None)
                vol = getattr(src_row, "volume_24h", None)
                lu = getattr(src_row, "last_updated", None)
                bump(target, d, cat, mcap, vol, lu)
                bump(target, d, "ALL", mcap, vol, lu)

        for idx, coin in enumerate(coins, 1):
            cid = coin.id
            cat = (getattr(coin, "category", None) or "Other").strip() or "Other"
            if should_log_progress(idx, len(coins), default_every=100):
                print(f"[{now_str()}] coin {idx}/{len(coins)} -> {cid}")
            hb.maybe(extra=f"coin={idx}/{len(coins)}")

            rows10 = session.execute(
                sel_10m,
                [cid, to_cassandra_ts(query_start_10m), to_cassandra_ts(end_10m)],
                timeout=REQUEST_TIMEOUT_SEC,
            )
            bump_ts_with_carry(agg10, slots_10m, cat, sort_ts_rows(rows10), carry_10m)

            rowsH = session.execute(
                sel_hourly,
                [cid, to_cassandra_ts(query_start_hour), to_cassandra_ts(end_hour)],
                timeout=REQUEST_TIMEOUT_SEC,
            )
            bump_ts_with_carry(aggH, slots_hour, cat, sort_ts_rows(rowsH), carry_hour)

            rowsD = session.execute(
                sel_daily,
                [cid, query_start_day, end_day],
                timeout=REQUEST_TIMEOUT_SEC,
            )
            bump_day_with_carry(aggD, slots_day, cat, sort_day_rows(rowsD), carry_day)

        wrote10 = wroteH = wroteD = 0

        p10 = deque()
        for ts, cats in sorted(agg10.items()):
            ranks = rank_map({c: vals["mcap"] for c, vals in cats.items()})
            for c, vals in cats.items():
                lu = vals["lu"] or ts
                enqueue_async(
                    session,
                    p10,
                    ins_10m,
                    [c, to_cassandra_ts(ts), to_cassandra_ts(lu), float(vals["mcap"]), ranks.get(c), float(vals["vol"])],
                    timeout=REQUEST_TIMEOUT_SEC,
                    max_in_flight=ASTRA_MAX_IN_FLIGHT,
                )
                wrote10 += 1
        drain_async(p10)
        hb.maybe(extra="flush_10m=done", force=True)

        pH = deque()
        for ts, cats in sorted(aggH.items()):
            ranks = rank_map({c: vals["mcap"] for c, vals in cats.items()})
            for c, vals in cats.items():
                lu = vals["lu"] or ts
                enqueue_async(
                    session,
                    pH,
                    ins_hourly,
                    [c, to_cassandra_ts(ts), to_cassandra_ts(lu), float(vals["mcap"]), ranks.get(c), float(vals["vol"])],
                    timeout=REQUEST_TIMEOUT_SEC,
                    max_in_flight=ASTRA_MAX_IN_FLIGHT,
                )
                wroteH += 1
        drain_async(pH)
        hb.maybe(extra="flush_hourly=done", force=True)

        pD = deque()
        for d, cats in sorted(aggD.items()):
            ranks = rank_map({c: vals["mcap"] for c, vals in cats.items()})
            for c, vals in cats.items():
                lu = vals["lu"] or now_ts
                enqueue_async(
                    session,
                    pD,
                    ins_daily,
                    [c, d, to_cassandra_ts(lu), float(vals["mcap"]), ranks.get(c), float(vals["vol"])],
                    timeout=REQUEST_TIMEOUT_SEC,
                    max_in_flight=ASTRA_MAX_IN_FLIGHT,
                )
                wroteD += 1
        drain_async(pD)
        hb.maybe(extra="flush_daily=done", force=True)

        print(f"[{now_str()}] mcap writes: 10m={wrote10} hourly={wroteH} daily={wroteD}")
        tracker.set_metric("rows_mcap_10m", wrote10)
        tracker.set_metric("rows_mcap_hourly", wroteH)
        tracker.set_metric("rows_mcap_daily", wroteD)
        tracker.finish("success")
    except Exception as exc:
        tracker.finish("failed", f"{type(exc).__name__}: {exc}")
        raise
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
