#!/usr/bin/env python3
from __future__ import annotations

import os
from datetime import timedelta

from cassandra.query import SimpleStatement

from common import (
    TABLE_10M,
    TABLE_HOURLY,
    TABLE_LIVE,
    cg_market_chart_range,
    connect_astra,
    extract_series_in_window,
    floor_10m,
    floor_hour,
    last_value_in_window,
    now_str,
    now_utc,
    scope_label,
    select_coins_from_live_rows,
    to_cassandra_ts,
    to_utc,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
SLOT_DELAY_SEC = int(os.getenv("PP_SLOT_DELAY_SEC", "90"))
HOURLY_FINALIZE_LOOKBACK = int(os.getenv("PP_HOURLY_FINALIZE_LOOKBACK", "2"))


def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def build_hour_from_10m(rows, slot_start, slot_end):
    if not rows:
        return None
    ordered = sorted(rows, key=lambda r: to_utc(getattr(r, "ts", None)) or slot_start)
    first = ordered[0]
    last = ordered[-1]

    open_price = _f(getattr(first, "open", None))
    close = _f(getattr(last, "close", None))
    if open_price is None:
        open_price = _f(getattr(first, "close", None))
    if close is None:
        return None

    highs = []
    lows = []
    points = 0
    last_updated = None
    for r in ordered:
        h = _f(getattr(r, "high", None))
        l = _f(getattr(r, "low", None))
        c = _f(getattr(r, "close", None))
        if h is None and c is not None:
            h = c
        if l is None and c is not None:
            l = c
        if h is not None:
            highs.append(h)
        if l is not None:
            lows.append(l)
        pc = getattr(r, "point_count", None)
        if isinstance(pc, int):
            points += pc
        lu = to_utc(getattr(r, "last_updated", None))
        if lu is not None and (last_updated is None or lu > last_updated):
            last_updated = lu

    if not highs or not lows:
        highs = [open_price, close]
        lows = [open_price, close]

    high = max([open_price] + highs + [close])
    low = min([open_price] + lows + [close])
    if last_updated is None:
        last_updated = slot_end - timedelta(seconds=1)

    return {
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "price_usd": close,
        "market_cap": _f(getattr(last, "market_cap", None)),
        "volume_24h": _f(getattr(last, "volume_24h", None)),
        "market_cap_rank": int(last.market_cap_rank) if getattr(last, "market_cap_rank", None) is not None else None,
        "circulating_supply": _f(getattr(last, "circulating_supply", None)),
        "total_supply": _f(getattr(last, "total_supply", None)),
        "point_count": points,
        "last_updated": last_updated,
    }


def main() -> None:
    session, cluster = connect_astra()
    try:
        sel_live = SimpleStatement(
            f"SELECT id, symbol, name, market_cap_rank FROM {TABLE_LIVE}",
            fetch_size=2000,
        )
        live_rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC))
        coins = select_coins_from_live_rows(live_rows)
        if not coins:
            print(f"[{now_str()}] No scoped coins in {TABLE_LIVE} for {scope_label()}; run AA_load_live_selected.py first.")
            return

        sel_10m = session.prepare(
            f"""
            SELECT ts, open, high, low, close,
                   market_cap, volume_24h, market_cap_rank,
                   circulating_supply, total_supply,
                   point_count, last_updated
            FROM {TABLE_10M}
            WHERE id=? AND ts>=? AND ts<?
            """
        )
        sel_hourly_one = session.prepare(
            f"""
            SELECT candle_source
            FROM {TABLE_HOURLY}
            WHERE id=? AND ts=? LIMIT 1
            """
        )
        ins_hourly = session.prepare(
            f"""
            INSERT INTO {TABLE_HOURLY}
              (id, ts, symbol, name,
               open, high, low, close, price_usd,
               market_cap, volume_24h, market_cap_rank, circulating_supply, total_supply,
               candle_source, point_count, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        now_guarded = now_utc() - timedelta(seconds=SLOT_DELAY_SEC)
        curr_hour_start = floor_hour(now_guarded)
        curr_hour_end = curr_hour_start + timedelta(hours=1)
        partial_end = min(curr_hour_end, floor_10m(now_guarded))

        print(f"[{now_str()}] Hourly run for scope={scope_label()} coins={len(coins)}")

        for coin in coins:
            # 1) Build/update current partial hour from 10m
            if partial_end > curr_hour_start:
                rows = list(
                    session.execute(
                        sel_10m,
                        [coin.id, to_cassandra_ts(curr_hour_start), to_cassandra_ts(partial_end)],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )
                )
                candle = build_hour_from_10m(rows, curr_hour_start, partial_end)
                if candle:
                    session.execute(
                        ins_hourly,
                        [
                            coin.id, to_cassandra_ts(curr_hour_start), coin.symbol, coin.name,
                            candle["open"], candle["high"], candle["low"], candle["close"], candle["price_usd"],
                            candle["market_cap"], candle["volume_24h"], candle["market_cap_rank"],
                            candle["circulating_supply"], candle["total_supply"],
                            "10m_partial", candle["point_count"], to_cassandra_ts(candle["last_updated"]),
                        ],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )
                    print(f"[{now_str()}] {coin.id} upserted partial hour {curr_hour_start}")

            # 2) Closed hour(s): provisional from 10m + API finalization
            for k in range(1, HOURLY_FINALIZE_LOOKBACK + 1):
                hour_start = curr_hour_start - timedelta(hours=k)
                hour_end = hour_start + timedelta(hours=1)

                existing = session.execute(
                    sel_hourly_one,
                    [coin.id, to_cassandra_ts(hour_start)],
                    timeout=REQUEST_TIMEOUT_SEC,
                ).one()
                if existing and getattr(existing, "candle_source", None) == "cg_hourly_final":
                    # Keep finalized candle stable: never overwrite with 10m reconstruction.
                    continue

                rows = list(
                    session.execute(
                        sel_10m,
                        [coin.id, to_cassandra_ts(hour_start), to_cassandra_ts(hour_end)],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )
                )
                candle = build_hour_from_10m(rows, hour_start, hour_end)
                if candle:
                    session.execute(
                        ins_hourly,
                        [
                            coin.id, to_cassandra_ts(hour_start), coin.symbol, coin.name,
                            candle["open"], candle["high"], candle["low"], candle["close"], candle["price_usd"],
                            candle["market_cap"], candle["volume_24h"], candle["market_cap_rank"],
                            candle["circulating_supply"], candle["total_supply"],
                            "10m_final", candle["point_count"], to_cassandra_ts(candle["last_updated"]),
                        ],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )

                try:
                    data = cg_market_chart_range(coin.id, hour_start, hour_end, vs_currency="usd")
                except Exception as exc:
                    print(f"[{now_str()}] [warn] API finalize failed for {coin.id} {hour_start}: {exc}")
                    continue

                prices = extract_series_in_window(data.get("prices", []) or [], hour_start, hour_end)
                if not prices:
                    continue
                price_values = [v for _, v in prices]
                open_price = price_values[0]
                close = price_values[-1]
                high = max(price_values)
                low = min(price_values)
                last_price_ts = prices[-1][0]
                mcap, _ = last_value_in_window(data.get("market_caps", []) or [], hour_start, hour_end)
                vol, _ = last_value_in_window(data.get("total_volumes", []) or [], hour_start, hour_end)

                session.execute(
                    ins_hourly,
                    [
                        coin.id, to_cassandra_ts(hour_start), coin.symbol, coin.name,
                        open_price, high, low, close, close,
                        mcap, vol,
                        coin.market_cap_rank if isinstance(coin.market_cap_rank, int) else None,
                        None, None,
                        "cg_hourly_final", len(price_values), to_cassandra_ts(last_price_ts),
                    ],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
                print(f"[{now_str()}] {coin.id} finalized hour via API: {hour_start}")
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

    print(f"[{now_str()}] Done.")


if __name__ == "__main__":
    main()
