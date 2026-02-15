#!/usr/bin/env python3
from __future__ import annotations

import os
from collections import deque
from collections import defaultdict
from datetime import timedelta
from typing import Any

from cassandra.query import SimpleStatement

from common import (
    Heartbeat,
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
MCAP_10M_SLOTS = int(os.getenv("PP_MCAP_10M_SLOTS", "12"))
MCAP_HOURS = int(os.getenv("PP_MCAP_HOURS", "24"))
MCAP_DAYS = int(os.getenv("PP_MCAP_DAYS", "7"))
REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
ASTRA_MAX_IN_FLIGHT = int(os.getenv("PP_ASTRA_MAX_IN_FLIGHT", "64"))


def _f(x):
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def rank_map(cat_to_mcap: dict[str, float]) -> dict[str, int]:
    items = [(c, m) for c, m in cat_to_mcap.items() if c != "ALL"]
    items.sort(key=lambda t: t[1], reverse=True)
    out = {c: i + 1 for i, (c, _m) in enumerate(items)}
    out["ALL"] = 0
    return out


def main() -> None:
    hb = Heartbeat("HH_write_market_caps")
    session, cluster = connect_astra()
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
            return

        print(f"[{now_str()}] Recomputing market caps for {len(coins)} coin(s), TOP_N={PP_TOP_N}")

        now_ts = now_utc()
        end_10m = floor_10m(now_ts) + timedelta(minutes=10)
        start_10m = end_10m - timedelta(minutes=10 * MCAP_10M_SLOTS)

        end_hour = floor_hour(now_ts) + timedelta(hours=1)
        start_hour = end_hour - timedelta(hours=MCAP_HOURS)

        end_day = now_ts.date()
        start_day = end_day - timedelta(days=MCAP_DAYS - 1)

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

        for idx, coin in enumerate(coins, 1):
            cid = coin.id
            cat = (getattr(coin, "category", None) or "Other").strip() or "Other"
            if should_log_progress(idx, len(coins), default_every=100):
                print(f"[{now_str()}] coin {idx}/{len(coins)} -> {cid}")
            hb.maybe(extra=f"coin={idx}/{len(coins)}")

            rows10 = session.execute(
                sel_10m,
                [cid, to_cassandra_ts(start_10m), to_cassandra_ts(end_10m)],
                timeout=REQUEST_TIMEOUT_SEC,
            )
            for r in rows10:
                ts = to_utc(getattr(r, "ts", None))
                if ts is None:
                    continue
                bump(agg10, ts, cat, r.market_cap, r.volume_24h, r.last_updated)
                bump(agg10, ts, "ALL", r.market_cap, r.volume_24h, r.last_updated)

            rowsH = session.execute(
                sel_hourly,
                [cid, to_cassandra_ts(start_hour), to_cassandra_ts(end_hour)],
                timeout=REQUEST_TIMEOUT_SEC,
            )
            for r in rowsH:
                ts = to_utc(getattr(r, "ts", None))
                if ts is None:
                    continue
                bump(aggH, ts, cat, r.market_cap, r.volume_24h, r.last_updated)
                bump(aggH, ts, "ALL", r.market_cap, r.volume_24h, r.last_updated)

            rowsD = session.execute(
                sel_daily,
                [cid, start_day, end_day],
                timeout=REQUEST_TIMEOUT_SEC,
            )
            for r in rowsD:
                d = getattr(r, "date", None)
                if d is None:
                    continue
                bump(aggD, d, cat, r.market_cap, r.volume_24h, r.last_updated)
                bump(aggD, d, "ALL", r.market_cap, r.volume_24h, r.last_updated)

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
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
