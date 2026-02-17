#!/usr/bin/env python3
from __future__ import annotations

import os
from datetime import date, timedelta

from cassandra.query import SimpleStatement

from prices.potential_future.common import (
    Heartbeat,
    PipelineHealthTracker,
    TABLE_DAILY,
    TABLE_LIVE,
    TABLE_MONTHLY,
    connect_astra,
    now_str,
    now_utc,
    should_log_progress,
    scope_label,
    select_coins_from_live_rows,
    to_cassandra_ts,
    to_utc,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
MONTHLY_FINALIZE_LOOKBACK = int(os.getenv("PP_MONTHLY_FINALIZE_LOOKBACK", "2"))


def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def ym_tag(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def next_month_start(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def month_start_offset(curr_month_start: date, offset_back: int) -> date:
    serial = curr_month_start.year * 12 + (curr_month_start.month - 1) - offset_back
    y = serial // 12
    m = (serial % 12) + 1
    return date(y, m, 1)


def aggregate_month(rows):
    ordered = sorted(rows, key=lambda r: getattr(r, "date", date.min))
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


def apply_live_partial(agg, live_row):
    out = dict(agg)
    live_price = _f(getattr(live_row, "price_usd", None))
    live_mcap = _f(getattr(live_row, "market_cap", None))
    live_rank = getattr(live_row, "market_cap_rank", None)
    live_circ = _f(getattr(live_row, "circulating_supply", None))
    live_total = _f(getattr(live_row, "total_supply", None))
    live_lu = to_utc(getattr(live_row, "last_updated", None))

    if live_price is not None:
        if out["open"] is None:
            out["open"] = live_price
        out["close"] = live_price
        out["high"] = live_price if out["high"] is None else max(out["high"], live_price)
        out["low"] = live_price if out["low"] is None else min(out["low"], live_price)

    if live_mcap is not None:
        out["market_cap"] = live_mcap
    if isinstance(live_rank, int):
        out["market_cap_rank"] = live_rank
    if live_circ is not None:
        out["circulating_supply"] = live_circ
    if live_total is not None:
        out["total_supply"] = live_total
    if live_lu is not None and (out["last_updated"] is None or live_lu > out["last_updated"]):
        out["last_updated"] = live_lu

    return out


def main() -> None:
    hb = Heartbeat("EG_build_monthly_from_daily")
    session, cluster = connect_astra()
    tracker = PipelineHealthTracker(session, "EG_build_monthly_from_daily")
    tracker.set_metric("monthly_finalize_lookback", MONTHLY_FINALIZE_LOOKBACK)
    tracker.start()
    try:
        sel_live = SimpleStatement(
            f"""
            SELECT id, symbol, name, market_cap_rank, price_usd, market_cap, volume_24h,
                   circulating_supply, total_supply, last_updated
            FROM {TABLE_LIVE}
            """,
            fetch_size=2000,
        )
        live_rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC))
        coins = select_coins_from_live_rows(live_rows)
        if not coins:
            print(f"[{now_str()}] No scoped coins in {TABLE_LIVE} for {scope_label()}; run AA_load_live_selected.py first.")
            tracker.mark_noop()
            tracker.set_metric("coins_scoped", 0)
            tracker.finish("noop")
            return
        tracker.set_metric("coins_scoped", len(coins))

        sel_daily = session.prepare(
            f"""
            SELECT date, open, high, low, close, price_usd, volume_24h, market_cap,
                   market_cap_rank, circulating_supply, total_supply, last_updated
            FROM {TABLE_DAILY}
            WHERE id=? AND date>=? AND date<?
            """
        )
        sel_monthly_one = session.prepare(
            f"""
            SELECT candle_source
            FROM {TABLE_MONTHLY}
            WHERE id=? AND year_month=? LIMIT 1
            """
        )
        ins_monthly = session.prepare(
            f"""
            INSERT INTO {TABLE_MONTHLY}
              (id, year_month, symbol, name, open, high, low, close, volume,
               market_cap, market_cap_rank, circulating_supply, total_supply,
               candle_source, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        today = now_utc().date()
        curr_start = date(today.year, today.month, 1)
        curr_end = next_month_start(curr_start)

        print(f"[{now_str()}] Monthly run for scope={scope_label()} coins={len(coins)}")

        wrote = 0
        for idx, coin in enumerate(coins, 1):
            if should_log_progress(idx, len(coins), default_every=25):
                print(f"[{now_str()}] coin {idx}/{len(coins)} -> {coin.id}")
            hb.maybe(extra=f"coin={idx}/{len(coins)}")
            # Closed months: finalize from daily data only (stable once finalized).
            for k in range(1, MONTHLY_FINALIZE_LOOKBACK + 1):
                month_start = month_start_offset(curr_start, k)
                month_end = next_month_start(month_start)
                month_key = ym_tag(month_start)

                existing = session.execute(
                    sel_monthly_one,
                    [coin.id, month_key],
                    timeout=REQUEST_TIMEOUT_SEC,
                ).one()
                if existing and getattr(existing, "candle_source", None) == "daily_final":
                    continue

                rows = list(
                    session.execute(
                        sel_daily,
                        [coin.id, month_start, month_end],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )
                )
                agg = aggregate_month(rows)
                if not agg:
                    continue

                last_upd = agg["last_updated"]
                if last_upd is None:
                    last_upd = now_utc() - timedelta(seconds=1)

                session.execute(
                    ins_monthly,
                    [
                        coin.id,
                        month_key,
                        coin.symbol,
                        coin.name,
                        agg["open"],
                        agg["high"],
                        agg["low"],
                        agg["close"],
                        float(agg["volume"] or 0.0),
                        agg["market_cap"],
                        agg["market_cap_rank"],
                        agg["circulating_supply"],
                        agg["total_supply"],
                        "daily_final",
                        to_cassandra_ts(last_upd),
                    ],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
                wrote += 1

            # Current month: partial from daily + live close so month line tracks latest price.
            current_rows = list(
                session.execute(
                    sel_daily,
                    [coin.id, curr_start, curr_end],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
            )
            agg = aggregate_month(current_rows)
            if agg is None:
                agg = {
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
            agg = apply_live_partial(agg, coin)
            if agg["open"] is None or agg["close"] is None:
                continue

            last_upd = agg["last_updated"] or now_utc()
            session.execute(
                ins_monthly,
                [
                    coin.id,
                    ym_tag(curr_start),
                    coin.symbol,
                    coin.name,
                    agg["open"],
                    agg["high"],
                    agg["low"],
                    agg["close"],
                    float(agg["volume"] or 0.0),
                    agg["market_cap"],
                    agg["market_cap_rank"],
                    agg["circulating_supply"],
                    agg["total_supply"],
                    "daily_partial_live",
                    to_cassandra_ts(last_upd),
                ],
                timeout=REQUEST_TIMEOUT_SEC,
            )
            wrote += 1

        print(f"[{now_str()}] Done. monthly_writes={wrote}")
        tracker.set_metric("rows_monthly", wrote)
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
