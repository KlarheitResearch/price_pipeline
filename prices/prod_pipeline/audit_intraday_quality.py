#!/usr/bin/env python3
"""Read-only slot, provenance, and flatline audit for 10m/hourly price data."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, cast

from cassandra.cluster import Cluster, Session
from cassandra.query import SimpleStatement

from astra_connect.connect import AstraConfig, get_session


UTC = timezone.utc


def to_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def floor_10m(value: datetime) -> datetime:
    value = to_utc(value) or datetime.now(UTC)
    return value.replace(minute=(value.minute // 10) * 10, second=0, microsecond=0)


def floor_hour(value: datetime) -> datetime:
    value = to_utc(value) or datetime.now(UTC)
    return value.replace(minute=0, second=0, microsecond=0)


def fnum(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


def max_equal_run(values: Iterable[float]) -> int:
    best = current = 0
    previous: float | None = None
    for value in values:
        if previous is not None and value == previous:
            current += 1
        else:
            current = 1
            previous = value
        best = max(best, current)
    return best


def close_enough(left: float, right: float) -> bool:
    scale = max(abs(left), abs(right), 1e-12)
    return abs(left - right) / scale <= 1e-10


@dataclass
class QualityResult:
    rank: int
    symbol: str
    coin_id: str
    granularity: str
    rows: int
    expected: int
    unique_values: int
    max_equal_run: int
    null_source_ratio: float
    rolling_overlap: int
    rolling_mismatch_ratio: float
    flags: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rank-start", type=int, default=1)
    parser.add_argument("--rank-end", type=int, default=100)
    parser.add_argument("--coin-ids", default="")
    parser.add_argument("--granularity", choices=["10m", "hourly", "both"], default="both")
    parser.add_argument("--hours-10m", type=float, default=24.0)
    parser.add_argument("--hours-hourly", type=float, default=168.0)
    parser.add_argument("--max-missing-ratio", type=float, default=0.05)
    parser.add_argument("--max-null-source-ratio", type=float, default=0.10)
    parser.add_argument(
        "--max-rolling-mismatch-ratio",
        type=float,
        default=1.0,
        help=(
            "Optional exact-value diagnostic threshold. Disabled by default because the regular "
            "and rolling tables sample different instants within a slot."
        ),
    )
    parser.add_argument("--min-rolling-overlap", type=int, default=6)
    parser.add_argument("--fail-on-issues", action="store_true")
    return parser.parse_args()


def audit_one(
    *,
    session: Session,
    select_series,
    select_rolling,
    coin: Any,
    granularity: str,
    start: datetime,
    end: datetime,
    floor_fn: Callable[[datetime], datetime],
    interval_seconds: int,
    args: argparse.Namespace,
) -> QualityResult:
    rows = list(
        session.execute(
            select_series,
            [coin.id, start.replace(tzinfo=None), end.replace(tzinfo=None)],
            timeout=60,
        )
    )
    rows.sort(key=lambda row: getattr(row, "ts"))

    stored: dict[datetime, float] = {}
    sources: list[str | None] = []
    for row in rows:
        ts = to_utc(getattr(row, "ts", None))
        price = fnum(getattr(row, "close", None))
        if price is None:
            price = fnum(getattr(row, "price_usd", None))
        if ts is None or price is None:
            continue
        stored[floor_fn(ts)] = price
        sources.append(getattr(row, "candle_source", None))

    rolling_rows = list(
        session.execute(
            select_rolling,
            [coin.id, start.replace(tzinfo=None), end.replace(tzinfo=None)],
            timeout=60,
        )
    )
    rolling: dict[datetime, tuple[float, datetime]] = {}
    for row in rolling_rows:
        observed_at = to_utc(getattr(row, "last_updated", None))
        price = fnum(getattr(row, "price_usd", None))
        if observed_at is None or price is None:
            continue
        slot = floor_fn(observed_at)
        previous = rolling.get(slot)
        if previous is None or observed_at >= previous[1]:
            rolling[slot] = (price, observed_at)

    overlap = sorted(set(stored) & set(rolling))
    mismatches = sum(1 for slot in overlap if not close_enough(stored[slot], rolling[slot][0]))
    mismatch_ratio = mismatches / len(overlap) if overlap else 0.0
    expected = max(0, int((end - start).total_seconds() // interval_seconds))
    unique_values = len(set(stored.values()))
    equal_run = max_equal_run(stored[slot] for slot in sorted(stored))
    null_source_ratio = (
        sum(1 for source in sources if not str(source or "").strip()) / len(sources)
        if sources
        else 1.0
    )

    flags: list[str] = []
    missing_ratio = max(0.0, (expected - len(stored)) / expected) if expected else 0.0
    if missing_ratio > args.max_missing_ratio:
        flags.append(f"missing:{missing_ratio:.1%}")
    if null_source_ratio > args.max_null_source_ratio:
        flags.append(f"null_source:{null_source_ratio:.1%}")
    if len(overlap) >= args.min_rolling_overlap and mismatch_ratio > args.max_rolling_mismatch_ratio:
        flags.append(f"rolling_mismatch:{mismatch_ratio:.1%}")
    rolling_unique = len({value for value, _ in rolling.values()})
    if len(overlap) >= args.min_rolling_overlap and rolling_unique >= max(6, unique_values * 2):
        flags.append(f"flat_vs_rolling:{unique_values}<{rolling_unique}")

    return QualityResult(
        rank=int(coin.market_cap_rank),
        symbol=str(coin.symbol or "").upper(),
        coin_id=str(coin.id),
        granularity=granularity,
        rows=len(stored),
        expected=expected,
        unique_values=unique_values,
        max_equal_run=equal_run,
        null_source_ratio=null_source_ratio,
        rolling_overlap=len(overlap),
        rolling_mismatch_ratio=mismatch_ratio,
        flags=flags,
    )


def main() -> int:
    args = parse_args()
    if args.rank_start < 1 or args.rank_end < args.rank_start:
        raise SystemExit("Invalid rank range")

    wanted_ids = {item.strip().lower() for item in args.coin_ids.split(",") if item.strip()}
    AstraConfig.from_env()
    session, cluster = cast(tuple[Session, Cluster], get_session(return_cluster=True))
    try:
        live_rows = list(
            session.execute(
                SimpleStatement(
                    "SELECT id, symbol, market_cap_rank FROM gecko_prices_live",
                    fetch_size=500,
                ),
                timeout=60,
            )
        )
        coins = sorted(
            [
                row
                for row in live_rows
                if isinstance(getattr(row, "market_cap_rank", None), int)
                and args.rank_start <= row.market_cap_rank <= args.rank_end
                and (not wanted_ids or str(row.id).lower() in wanted_ids)
            ],
            key=lambda row: row.market_cap_rank,
        )

        select_10m = session.prepare(
            "SELECT ts, close, price_usd, candle_source, last_updated "
            "FROM gecko_prices_10m_7d WHERE id=? AND ts>=? AND ts<?"
        )
        select_hourly = session.prepare(
            "SELECT ts, close, price_usd, candle_source, last_updated "
            "FROM gecko_candles_hourly_30d WHERE id=? AND ts>=? AND ts<?"
        )
        select_rolling = session.prepare(
            "SELECT last_updated, price_usd FROM gecko_prices_live_rolling "
            "WHERE id=? AND last_updated>=? AND last_updated<?"
        )

        now = datetime.now(UTC)
        results: list[QualityResult] = []
        if args.granularity in ("10m", "both"):
            end = floor_10m(now)
            start = end - timedelta(hours=args.hours_10m)
            for coin in coins:
                results.append(
                    audit_one(
                        session=session,
                        select_series=select_10m,
                        select_rolling=select_rolling,
                        coin=coin,
                        granularity="10m",
                        start=start,
                        end=end,
                        floor_fn=floor_10m,
                        interval_seconds=600,
                        args=args,
                    )
                )
        if args.granularity in ("hourly", "both"):
            end = floor_hour(now)
            start = end - timedelta(hours=args.hours_hourly)
            for coin in coins:
                results.append(
                    audit_one(
                        session=session,
                        select_series=select_hourly,
                        select_rolling=select_rolling,
                        coin=coin,
                        granularity="hourly",
                        start=start,
                        end=end,
                        floor_fn=floor_hour,
                        interval_seconds=3600,
                        args=args,
                    )
                )

        issues = [result for result in results if result.flags]
        for result in issues:
            print(
                f"[{result.granularity:6}] rank={result.rank:>4} {result.symbol:<12} "
                f"({result.coin_id}) rows={result.rows}/{result.expected} unique={result.unique_values} "
                f"max_run={result.max_equal_run} null_source={result.null_source_ratio:.1%} "
                f"rolling_overlap={result.rolling_overlap} mismatch={result.rolling_mismatch_ratio:.1%} "
                f"flags={','.join(result.flags)}"
            )

        by_granularity = {
            granularity: sum(1 for result in issues if result.granularity == granularity)
            for granularity in ("10m", "hourly")
        }
        print(
            f"Audited {len(coins)} coin(s), {len(results)} series; "
            f"issues={len(issues)} (10m={by_granularity['10m']}, hourly={by_granularity['hourly']})."
        )
        if issues:
            ids = sorted({result.coin_id for result in issues})
            print("Suggested --coin-ids: " + ",".join(ids))
        return 2 if args.fail_on_issues and issues else 0
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
