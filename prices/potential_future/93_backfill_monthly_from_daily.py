#!/usr/bin/env python3
from __future__ import annotations

import os
from collections import deque
from datetime import date, datetime, timedelta

from cassandra.query import SimpleStatement

from prices.potential_future.common import (
    Heartbeat,
    TABLE_DAILY,
    TABLE_LIVE,
    TABLE_MONTHLY,
    connect_astra,
    drain_async,
    enqueue_async,
    now_str,
    now_utc,
    should_log_progress,
    scope_label,
    select_coins_from_live_rows,
    to_cassandra_ts,
    to_utc,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
ASTRA_MAX_IN_FLIGHT = int(os.getenv("PP_ASTRA_MAX_IN_FLIGHT", "64"))
INCLUDE_ALL_DAILY_IDS = os.getenv("PP_BACKFILL_INCLUDE_ALL_DAILY_IDS", "1") == "1"
CLEAR_PARTITION_FIRST = os.getenv("PP_MONTHLY_BACKFILL_CLEAR", "0") == "1"
LOG_EVERY = int(os.getenv("PP_MONTHLY_BACKFILL_LOG_EVERY", "50"))


def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _ym(d) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _to_date(value):
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        try:
            return date(int(value.year), int(value.month), int(value.day))
        except Exception:
            pass
    text = str(value)
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _aggregate_month(rows):
    ordered = sorted(rows, key=lambda r: _to_date(getattr(r, "date", None)) or date.min)
    if not ordered:
        return None
    out = {
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "volume": 0.0,
        "market_cap": None,
        "market_cap_rank": None,
        "circulating_supply": None,
        "total_supply": None,
        "last_updated": None,
    }
    for r in ordered:
        op = _f(getattr(r, "open", None))
        hi = _f(getattr(r, "high", None))
        lo = _f(getattr(r, "low", None))
        cl = _f(getattr(r, "close", None))
        px = _f(getattr(r, "price_usd", None))
        open_candidate = op if op is not None else (cl if cl is not None else px)
        close_candidate = cl if cl is not None else (px if px is not None else open_candidate)
        high_candidate = hi if hi is not None else (close_candidate if close_candidate is not None else open_candidate)
        low_candidate = lo if lo is not None else (close_candidate if close_candidate is not None else open_candidate)

        if out["open"] is None and open_candidate is not None:
            out["open"] = open_candidate
        if high_candidate is not None:
            out["high"] = high_candidate if out["high"] is None else max(out["high"], high_candidate)
        if low_candidate is not None:
            out["low"] = low_candidate if out["low"] is None else min(out["low"], low_candidate)
        if close_candidate is not None:
            out["close"] = close_candidate

        out["volume"] += _f(getattr(r, "volume_24h", None)) or 0.0
        mcap = _f(getattr(r, "market_cap", None))
        if mcap is not None:
            out["market_cap"] = mcap
        rank = getattr(r, "market_cap_rank", None)
        if isinstance(rank, int):
            out["market_cap_rank"] = rank
        circ = _f(getattr(r, "circulating_supply", None))
        if circ is not None:
            out["circulating_supply"] = circ
        total = _f(getattr(r, "total_supply", None))
        if total is not None:
            out["total_supply"] = total
        lu = to_utc(getattr(r, "last_updated", None))
        if lu is not None and (out["last_updated"] is None or lu > out["last_updated"]):
            out["last_updated"] = lu

    if out["open"] is None or out["close"] is None:
        return None
    return out


def main() -> None:
    session, cluster = connect_astra()
    try:
        sel_live = SimpleStatement(
            f"SELECT id, symbol, name, market_cap_rank FROM {TABLE_LIVE}",
            fetch_size=2000,
        )
        live_rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC))
        scoped = select_coins_from_live_rows(live_rows)
        meta = {c.id: c for c in scoped}

        if INCLUDE_ALL_DAILY_IDS:
            sel_ids_daily = SimpleStatement(
                f"SELECT DISTINCT id FROM {TABLE_DAILY}",
                fetch_size=2000,
            )
            for r in session.execute(sel_ids_daily, timeout=REQUEST_TIMEOUT_SEC):
                cid = getattr(r, "id", None)
                if cid and cid not in meta:
                    meta[cid] = type("Coin", (), {"id": cid, "symbol": cid.upper(), "name": cid, "market_cap_rank": None})()

        if not meta:
            print(f"[{now_str()}] No coins for monthly backfill (scope={scope_label()}).")
            return

        print(
            f"[{now_str()}] Monthly backfill start: coins={len(meta)} "
            f"scope={scope_label()} include_all_daily_ids={INCLUDE_ALL_DAILY_IDS}"
        )

        sel_daily_all = session.prepare(
            f"""
            SELECT date, open, high, low, close, price_usd, volume_24h,
                   market_cap, market_cap_rank, circulating_supply, total_supply, last_updated
            FROM {TABLE_DAILY}
            WHERE id=?
            """
        )
        del_monthly_partition = session.prepare(f"DELETE FROM {TABLE_MONTHLY} WHERE id=?")
        ins_monthly = session.prepare(
            f"""
            INSERT INTO {TABLE_MONTHLY}
              (id, year_month, symbol, name, open, high, low, close, volume,
               market_cap, market_cap_rank, circulating_supply, total_supply,
               candle_source, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        wrote = 0
        hb = Heartbeat("93_backfill_monthly_from_daily")
        for idx, cid in enumerate(sorted(meta.keys()), 1):
            coin = meta[cid]
            if should_log_progress(idx, len(meta), default_every=LOG_EVERY):
                print(f"[{now_str()}] coin {idx}/{len(meta)} -> {cid}")
            hb.maybe(extra=f"coin={idx}/{len(meta)}")

            rows = list(session.execute(sel_daily_all, [cid], timeout=REQUEST_TIMEOUT_SEC))
            if not rows:
                continue

            if CLEAR_PARTITION_FIRST:
                session.execute(del_monthly_partition, [cid], timeout=REQUEST_TIMEOUT_SEC)

            by_month = {}
            for r in rows:
                d = _to_date(getattr(r, "date", None))
                if d is None:
                    continue
                key = _ym(d)
                by_month.setdefault(key, []).append(r)

            pending = deque()
            for ym, month_rows in by_month.items():
                agg = _aggregate_month(month_rows)
                if agg is None:
                    continue
                last_upd = agg["last_updated"] or (now_utc() - timedelta(seconds=1))
                enqueue_async(
                    session,
                    pending,
                    ins_monthly,
                    [
                        cid,
                        ym,
                        getattr(coin, "symbol", None),
                        getattr(coin, "name", None),
                        agg["open"],
                        agg["high"],
                        agg["low"],
                        agg["close"],
                        float(agg["volume"] or 0.0),
                        agg["market_cap"],
                        agg["market_cap_rank"],
                        agg["circulating_supply"],
                        agg["total_supply"],
                        "daily_backfill",
                        to_cassandra_ts(last_upd),
                    ],
                    timeout=REQUEST_TIMEOUT_SEC,
                    max_in_flight=ASTRA_MAX_IN_FLIGHT,
                )
                wrote += 1
            drain_async(pending)
            hb.maybe(extra=f"coin={idx}/{len(meta)} flush=done", force=True)

        print(f"[{now_str()}] Monthly backfill done. wrote={wrote}")
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
