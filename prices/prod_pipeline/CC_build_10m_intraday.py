#!/usr/bin/env python3
from __future__ import annotations

import os
from datetime import timedelta

from cassandra.query import SimpleStatement

from common import (
    TABLE_10M,
    TABLE_LIVE,
    TABLE_ROLLING,
    connect_astra,
    floor_10m,
    now_str,
    now_utc,
    scope_label,
    select_coins_from_live_rows,
    to_cassandra_ts,
    to_utc,
)


SLOT_MINUTES = int(os.getenv("PP_SLOT_MINUTES", "10"))
SLOT_DELAY_SEC = int(os.getenv("PP_SLOT_DELAY_SEC", "90"))
SLOTS_BACKFILL = int(os.getenv("PP_SLOTS_BACKFILL", "4"))
REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))


def slot_start_now():
    return floor_10m(now_utc() - timedelta(seconds=SLOT_DELAY_SEC))


def last_n_slots_oldest_first(n: int):
    end = slot_start_now() + timedelta(minutes=SLOT_MINUTES)
    out = []
    for _ in range(n):
        start = end - timedelta(minutes=SLOT_MINUTES)
        out.append((start, end))
        end = start
    out.reverse()
    return out


def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def main() -> None:
    session, cluster = connect_astra()

    sel_live = SimpleStatement(
        f"SELECT id, symbol, name, market_cap_rank FROM {TABLE_LIVE}",
        fetch_size=2000,
    )
    live_rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC))
    coins = select_coins_from_live_rows(live_rows)
    if not coins:
        print(f"[{now_str()}] No scoped coins in {TABLE_LIVE} for {scope_label()}; run AA_load_live_selected.py first.")
        return

    print(f"[{now_str()}] Building 10m intraday for scope={scope_label()} coins={len(coins)}")
    slots = last_n_slots_oldest_first(SLOTS_BACKFILL)
    print(f"[{now_str()}] Slots: {slots[0][0]} .. {slots[-1][1]} (count={len(slots)})")

    sel_in_slot = session.prepare(
        f"""
        SELECT last_updated, symbol, name, price_usd, market_cap, volume_24h,
               market_cap_rank, circulating_supply, total_supply
        FROM {TABLE_ROLLING}
        WHERE id=? AND last_updated>=? AND last_updated<?
        """
    )
    sel_prev_rolling = session.prepare(
        f"""
        SELECT last_updated, symbol, name, price_usd, market_cap, volume_24h,
               market_cap_rank, circulating_supply, total_supply
        FROM {TABLE_ROLLING}
        WHERE id=? AND last_updated<? LIMIT 1
        """
    )
    sel_prev_close = session.prepare(
        f"""
        SELECT close
        FROM {TABLE_10M}
        WHERE id=? AND ts<? ORDER BY ts DESC LIMIT 1
        """
    )
    ins_10m = session.prepare(
        f"""
        INSERT INTO {TABLE_10M}
          (id, ts, symbol, name,
           open, high, low, close, price_usd,
           market_cap, volume_24h,
           market_cap_rank, circulating_supply, total_supply,
           last_updated, candle_source, point_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    wrote = 0
    skipped = 0
    try:
        for coin in coins:
            for slot_start, slot_end in slots:
                in_slot_rows = list(
                    session.execute(
                        sel_in_slot,
                        [coin.id, to_cassandra_ts(slot_start), to_cassandra_ts(slot_end)],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )
                )
                in_slot_rows.sort(key=lambda r: to_utc(getattr(r, "last_updated", None)) or slot_start)

                prev_candle = session.execute(
                    sel_prev_close,
                    [coin.id, to_cassandra_ts(slot_start)],
                    timeout=REQUEST_TIMEOUT_SEC,
                ).one()
                prev_close = _f(getattr(prev_candle, "close", None)) if prev_candle else None

                if in_slot_rows:
                    price_points = [_f(r.price_usd) for r in in_slot_rows if r.price_usd is not None]
                    if not price_points:
                        skipped += 1
                        continue

                    first_price = price_points[0]
                    close = price_points[-1]
                    open_price = prev_close if prev_close is not None else first_price
                    high = max([open_price] + price_points)
                    low = min([open_price] + price_points)
                    last_row = in_slot_rows[-1]
                    last_updated = to_utc(last_row.last_updated) or (slot_end - timedelta(seconds=1))

                    vals = [
                        coin.id,
                        to_cassandra_ts(slot_start),
                        (coin.symbol or "").upper(),
                        coin.name,
                        open_price,
                        high,
                        low,
                        close,
                        close,
                        _f(last_row.market_cap),
                        _f(last_row.volume_24h),
                        int(last_row.market_cap_rank) if last_row.market_cap_rank is not None else None,
                        _f(last_row.circulating_supply),
                        _f(last_row.total_supply),
                        to_cassandra_ts(last_updated),
                        "live_points",
                        len(price_points),
                    ]
                    session.execute(ins_10m, vals, timeout=REQUEST_TIMEOUT_SEC)
                    wrote += 1
                    continue

                prev_row = session.execute(
                    sel_prev_rolling,
                    [coin.id, to_cassandra_ts(slot_start)],
                    timeout=REQUEST_TIMEOUT_SEC,
                ).one()
                if not prev_row:
                    skipped += 1
                    continue

                carry_price = prev_close if prev_close is not None else _f(prev_row.price_usd)
                if carry_price is None:
                    skipped += 1
                    continue

                last_updated = to_utc(prev_row.last_updated) or (slot_end - timedelta(seconds=1))
                if last_updated > slot_end:
                    last_updated = slot_end - timedelta(seconds=1)

                vals = [
                    coin.id,
                    to_cassandra_ts(slot_start),
                    (coin.symbol or "").upper(),
                    coin.name,
                    carry_price,
                    carry_price,
                    carry_price,
                    carry_price,
                    carry_price,
                    _f(prev_row.market_cap),
                    _f(prev_row.volume_24h),
                    int(prev_row.market_cap_rank) if prev_row.market_cap_rank is not None else None,
                    _f(prev_row.circulating_supply),
                    _f(prev_row.total_supply),
                    to_cassandra_ts(last_updated),
                    "carry_prev",
                    0,
                ]
                session.execute(ins_10m, vals, timeout=REQUEST_TIMEOUT_SEC)
                wrote += 1
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

    print(f"[{now_str()}] Done. 10m wrote={wrote} skipped={skipped}.")


if __name__ == "__main__":
    main()
