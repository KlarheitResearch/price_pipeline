#!/usr/bin/env python3
"""Read-only semantic quality audit for daily CoinGecko candles."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any, cast

from cassandra.cluster import Cluster, Session
from cassandra.query import SimpleStatement

from astra_connect.connect import AstraConfig, get_session


UTC = timezone.utc


def as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def parse_day(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def fnum(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


def inum(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rank-start", type=int, default=1)
    parser.add_argument("--rank-end", type=int, default=100)
    parser.add_argument("--lookback-days", type=int, default=365)
    parser.add_argument("--start-day")
    parser.add_argument("--end-day")
    parser.add_argument("--max-details", type=int, default=60)
    parser.add_argument("--fail-on-definite", action="store_true")
    args = parser.parse_args()

    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    end_day = parse_day(args.end_day) or yesterday
    start_day = parse_day(args.start_day) or (end_day - timedelta(days=max(1, args.lookback_days) - 1))
    if start_day > end_day:
        raise SystemExit("start-day must be <= end-day")

    AstraConfig.from_env()
    session, cluster = cast(tuple[Session, Cluster], get_session(return_cluster=True))
    try:
        live = list(
            session.execute(
                SimpleStatement(
                    "SELECT id, symbol, market_cap_rank FROM gecko_prices_live",
                    fetch_size=2000,
                ),
                timeout=60,
            )
        )
        coins = sorted(
            (
                int(row.market_cap_rank),
                str(row.id),
                str(getattr(row, "symbol", None) or row.id),
            )
            for row in live
            if isinstance(getattr(row, "market_cap_rank", None), int)
            and args.rank_start <= int(row.market_cap_rank) <= args.rank_end
        )
        select_range = session.prepare(
            """
            SELECT date, open, high, low, close, candle_source, point_count, last_updated
            FROM gecko_candles_daily_contin
            WHERE id=? AND date>=? AND date<=?
            """
        )

        totals = Counter()
        sources = Counter()
        definite_details: list[tuple] = []
        unverified_details: list[tuple] = []
        affected_coins: set[str] = set()
        affected_dates = Counter()

        for rank, coin_id, symbol in coins:
            rows = list(session.execute(select_range, [coin_id, start_day, end_day], timeout=60))
            totals["rows"] += len(rows)
            for row in rows:
                day_key = as_date(row.date)
                src = (getattr(row, "candle_source", None) or "").strip().lower()
                sources[src or "<null>"] += 1
                values = [fnum(getattr(row, key, None)) for key in ("open", "high", "low", "close")]
                point_count = inum(getattr(row, "point_count", None))
                last_updated = getattr(row, "last_updated", None)

                null_ohlc = any(value is None for value in values)
                invalid = False
                flat = False
                if not null_ohlc:
                    open_, high, low, close = cast(list[float], values)
                    invalid = high < max(open_, close) or low > min(open_, close) or high < low
                    flat = max(open_, high, low, close) == min(open_, high, low, close)

                stale = last_updated is not None and last_updated.date() < day_key
                weak_source = any(token in src for token in ("carry", "interp"))
                definite = null_ohlc or invalid or stale or weak_source
                unverified_flat = flat and (point_count is None or point_count <= 1) and not definite

                totals["flat"] += int(flat)
                totals["flat_verified_multi_point"] += int(flat and point_count is not None and point_count >= 2)
                totals["unverified_flat"] += int(unverified_flat)
                totals["stale"] += int(stale)
                totals["weak_source"] += int(weak_source)
                totals["invalid_or_null"] += int(null_ohlc or invalid)
                totals["missing_point_count"] += int(point_count is None)

                if definite:
                    affected_coins.add(coin_id)
                    affected_dates[day_key] += 1
                    definite_details.append(
                        (day_key, rank, coin_id, symbol, src or "<null>", point_count, last_updated, values)
                    )
                elif unverified_flat:
                    unverified_details.append((day_key, rank, coin_id, src or "<null>", point_count))

        print(
            f"daily_ohlc_audit ranks={args.rank_start}-{args.rank_end} coins={len(coins)} "
            f"window={start_day}..{end_day} rows={totals['rows']}"
        )
        print(
            "quality "
            f"definite_bad={len(definite_details)} affected_coins={len(affected_coins)} "
            f"stale={totals['stale']} weak_source={totals['weak_source']} "
            f"invalid_or_null={totals['invalid_or_null']} flat={totals['flat']} "
            f"flat_verified_multi_point={totals['flat_verified_multi_point']} "
            f"flat_unverified={totals['unverified_flat']} missing_point_count={totals['missing_point_count']}"
        )
        print(f"sources={sources.most_common()}")
        print(f"worst_dates={affected_dates.most_common(20)}")

        for detail in sorted(definite_details)[: max(0, args.max_details)]:
            day_key, rank, coin_id, symbol, src, points, last_updated, values = detail
            print(
                f"BAD day={day_key} rank={rank} id={coin_id} symbol={symbol} "
                f"source={src} points={points} last_updated={last_updated} ohlc={values}"
            )
        if len(definite_details) > args.max_details:
            print(f"... {len(definite_details) - args.max_details} additional definite-bad rows omitted")
        if unverified_details:
            print(
                f"WARN {len(unverified_details)} flat rows lack multi-point proof; "
                "these need API comparison, but are not automatically failures because stable/RWA assets can be truly flat."
            )

        if args.fail_on_definite and definite_details:
            return 1
        return 0
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
