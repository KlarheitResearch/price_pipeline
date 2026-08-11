#!/usr/bin/env python3
"""
Probe and backfill raw Gecko datasets without touching aggregate tables.

Strategy:
- Fill missing 10m rows from existing hourly rows, else daily rows, else carry-forward.
- Fill missing hourly rows from 10m rows, else daily rows, else carry-forward.
- Fill missing daily rows from observed 10m rows, else observed hourly rows.
- Daily carry-forward is disabled unless explicitly requested.
- Refresh touched monthly rows from daily rows for the affected months.

The goal is continuity and explicit provenance, not authoritative API reconstruction.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable, Optional, cast

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session
from cassandra.query import BatchStatement, BatchType, SimpleStatement

from astra_connect.connect import AstraConfig, get_session

AstraConfig.from_env()

UTC = timezone.utc
TEN_MINUTES = timedelta(minutes=10)
ONE_HOUR = timedelta(hours=1)
ONE_DAY = timedelta(days=1)


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")


def to_utc(dt_: datetime | None) -> datetime | None:
    if dt_ is None:
        return None
    if dt_.tzinfo is None:
        return dt_.replace(tzinfo=UTC)
    return dt_.astimezone(UTC)


def to_cassandra_ts(dt_: datetime) -> datetime:
    return (to_utc(dt_) or now_utc()).replace(tzinfo=None)


def parse_utc(value: str, *, end_if_date: bool) -> datetime:
    value = value.strip()
    if len(value) == 10:
        d = datetime.strptime(value, "%Y-%m-%d").date()
        base = datetime(d.year, d.month, d.day, tzinfo=UTC)
        return base + ONE_DAY if end_if_date else base
    dt_ = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt_.tzinfo is None:
        dt_ = dt_.replace(tzinfo=UTC)
    else:
        dt_ = dt_.astimezone(UTC)
    return dt_


def floor_10m(dt_: datetime) -> datetime:
    dt_ = to_utc(dt_) or now_utc()
    return dt_.replace(minute=(dt_.minute // 10) * 10, second=0, microsecond=0)


def floor_hour(dt_: datetime) -> datetime:
    dt_ = to_utc(dt_) or now_utc()
    return dt_.replace(minute=0, second=0, microsecond=0)


def day_start(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def next_month_start(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def ym_tag(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def iter_10m(start_utc: datetime, end_utc_excl: datetime) -> Iterable[datetime]:
    cur = floor_10m(start_utc)
    while cur < end_utc_excl:
        yield cur
        cur += TEN_MINUTES


def iter_hours(start_utc: datetime, end_utc_excl: datetime) -> Iterable[datetime]:
    cur = floor_hour(start_utc)
    while cur < end_utc_excl:
        yield cur
        cur += ONE_HOUR


def iter_days(start_day: date, end_day_incl: date) -> Iterable[date]:
    cur = start_day
    while cur <= end_day_incl:
        yield cur
        cur += ONE_DAY


def fnum(x: Any, fallback: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return fallback
        return float(x)
    except Exception:
        return fallback


def int_or_none(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def pick_source_text(prefix: str, src: str | None) -> str:
    suffix = (src or "unknown").strip().lower().replace(" ", "_")
    return f"{prefix}:{suffix}"[:96]


def safe_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.encode("ascii", "backslashreplace").decode("ascii")


def source_leaf(src: str | None) -> str | None:
    text = (src or "").strip().lower()
    if not text:
        return None
    if ":" in text:
        text = text.split(":")[-1]
    return text.replace(" ", "_")


def row_is_observed_for_day(row: dict[str, Any], expected_day: date) -> bool:
    source = (row.get("candle_source") or "").strip().lower()
    if any(token in source for token in ("carry", "from_daily", "from_hourly", "interp")):
        return False
    point_count = int_or_none(row.get("point_count"))
    if point_count is not None and point_count <= 0:
        return False
    last_updated = to_utc(cast(Optional[datetime], row.get("last_updated")))
    if last_updated is not None and last_updated.date() < expected_day:
        return False
    return row_close(row) is not None


def row_close(row: dict[str, Any]) -> Optional[float]:
    return (
        fnum(row.get("close"))
        if row.get("close") is not None
        else fnum(row.get("price_usd"))
    )


def derive_interval_est_from_source(source_granularity: str, volume_24h: Optional[float]) -> Optional[float]:
    vol = fnum(volume_24h)
    if vol is None:
        return None
    if source_granularity == "hourly":
        return vol / 144.0
    if source_granularity == "daily":
        return vol / 144.0
    return None


@dataclass
class CoinMeta:
    coin_id: str
    symbol: str
    name: str
    rank: Optional[int]
    circ: Optional[float]
    totl: Optional[float]


class BatchWriter:
    def __init__(self, session: Session, prepared_stmt, batch_size: int, label: str):
        self.session = session
        self.prepared_stmt = prepared_stmt
        self.batch_size = batch_size
        self.label = label
        self.total = 0
        self._batch = BatchStatement(
            batch_type=BatchType.UNLOGGED,
            consistency_level=ConsistencyLevel.QUORUM,
        )

    def add(self, values: list[Any]) -> None:
        self._batch.add(self.prepared_stmt, values)
        self.total += 1
        if self.total % self.batch_size == 0:
            self.flush()

    def flush(self) -> None:
        if len(self._batch):
            self.session.execute(self._batch)
            self._batch.clear()


def collect_row_dicts(rows: Iterable[Any], key_name: str) -> dict[Any, dict[str, Any]]:
    out: dict[Any, dict[str, Any]] = {}
    for row in rows:
        key = getattr(row, key_name, None)
        if isinstance(key, datetime):
            key = to_utc(key)
        elif key_name == "date" and key is not None and not isinstance(key, date):
            try:
                key = key.date()
            except Exception:
                key = date.fromisoformat(str(key)[:10])
        out[key] = {
            "symbol": getattr(row, "symbol", None),
            "name": getattr(row, "name", None),
            "open": fnum(getattr(row, "open", None)),
            "high": fnum(getattr(row, "high", None)),
            "low": fnum(getattr(row, "low", None)),
            "close": fnum(getattr(row, "close", None)),
            "price_usd": fnum(getattr(row, "price_usd", None)),
            "market_cap": fnum(getattr(row, "market_cap", None)),
            "volume_24h": fnum(getattr(row, "volume_24h", None)),
            "market_cap_rank": int_or_none(getattr(row, "market_cap_rank", None)),
            "circulating_supply": fnum(getattr(row, "circulating_supply", None)),
            "total_supply": fnum(getattr(row, "total_supply", None)),
            "last_updated": to_utc(getattr(row, "last_updated", None)),
            "candle_source": getattr(row, "candle_source", None),
            "point_count": int_or_none(getattr(row, "point_count", None)),
            "volume_interval_est": fnum(getattr(row, "volume_interval_est", None)),
        }
    return out


def find_prev_key(sorted_keys: list[Any], target) -> Any | None:
    prev = None
    for key in sorted_keys:
        if key >= target:
            break
        prev = key
    return prev


def derive_ohlc_from_rows(rows: list[dict[str, Any]]) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if not rows:
        return None, None, None, None
    opens = [fnum(r.get("open"), fnum(r.get("price_usd"))) for r in rows]
    closes = [fnum(r.get("close"), fnum(r.get("price_usd"))) for r in rows]
    highs = [fnum(r.get("high"), fnum(r.get("price_usd"))) for r in rows]
    lows = [fnum(r.get("low"), fnum(r.get("price_usd"))) for r in rows]
    opens = [x for x in opens if x is not None]
    closes = [x for x in closes if x is not None]
    highs = [x for x in highs if x is not None]
    lows = [x for x in lows if x is not None]
    if not closes:
        return None, None, None, None
    return (
        opens[0] if opens else closes[0],
        max(highs) if highs else max(closes),
        min(lows) if lows else min(closes),
        closes[-1],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe and flat-fill raw Gecko datasets.")
    parser.add_argument("--rank-start", type=int, default=1)
    parser.add_argument("--rank-end", type=int, default=1000)
    parser.add_argument("--from-utc", type=str, required=True)
    parser.add_argument("--to-utc", type=str, required=True)
    parser.add_argument("--probe-only", action="store_true")
    parser.add_argument(
        "--allow-daily-carry",
        action="store_true",
        help="Allow synthetic open=high=low=close daily rows when no observed intraday data exists.",
    )
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--progress-every", type=int, default=25)
    args = parser.parse_args()

    start_utc = parse_utc(args.from_utc, end_if_date=False)
    end_utc = parse_utc(args.to_utc, end_if_date=True)
    current_utc = now_utc()
    if end_utc > current_utc:
        print(
            f"[{now_str()}] requested to_utc={end_utc.isoformat()} exceeds now={current_utc.isoformat()}, clamping"
        )
        end_utc = current_utc
    if end_utc <= start_utc:
        raise SystemExit("Invalid range: to_utc must be after from_utc.")

    print(
        f"[{now_str()}] raw-backfill config "
        f"ranks={args.rank_start}-{args.rank_end} "
        f"range={start_utc.isoformat()} -> {end_utc.isoformat()} "
        f"probe_only={args.probe_only} allow_daily_carry={args.allow_daily_carry}"
    )

    session, cluster = cast(tuple[Session, Cluster], get_session(return_cluster=True))
    try:
        sel_live = SimpleStatement(
            """
            SELECT id, symbol, name, market_cap_rank, circulating_supply, total_supply
            FROM gecko_prices_live
            """,
            fetch_size=1000,
        )
        ps_10m = session.prepare(
            """
            SELECT ts, symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
                   market_cap_rank, circulating_supply, total_supply, last_updated,
                   candle_source, point_count, volume_interval_est
            FROM gecko_prices_10m_7d
            WHERE id=? AND ts>=? AND ts<?
            """
        )
        ps_hourly = session.prepare(
            """
            SELECT ts, symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
                   market_cap_rank, circulating_supply, total_supply, last_updated,
                   candle_source, point_count
            FROM gecko_candles_hourly_30d
            WHERE id=? AND ts>=? AND ts<?
            """
        )
        ps_daily = session.prepare(
            """
            SELECT date, symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
                   market_cap_rank, circulating_supply, total_supply, last_updated,
                   candle_source, point_count
            FROM gecko_candles_daily_contin
            WHERE id=? AND date>=? AND date<=?
            """
        )
        ps_daily_month = session.prepare(
            """
            SELECT date, symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
                   market_cap_rank, circulating_supply, total_supply, last_updated,
                   candle_source, point_count
            FROM gecko_candles_daily_contin
            WHERE id=? AND date>=? AND date<?
            """
        )
        ins_10m = session.prepare(
            """
            INSERT INTO gecko_prices_10m_7d
              (id, ts, symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
               market_cap_rank, circulating_supply, total_supply, last_updated,
               candle_source, point_count, volume_interval_est)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        ins_hourly = session.prepare(
            """
            INSERT INTO gecko_candles_hourly_30d
              (id, ts, symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
               market_cap_rank, circulating_supply, total_supply, candle_source, last_updated, point_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        ins_daily = session.prepare(
            """
            INSERT INTO gecko_candles_daily_contin
              (id, date, symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
               market_cap_rank, circulating_supply, total_supply, candle_source, last_updated, point_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        ins_monthly = session.prepare(
            """
            INSERT INTO gecko_candles_monthly
              (id, year_month, symbol, name, open, high, low, close, volume, market_cap, market_cap_rank,
               circulating_supply, total_supply, candle_source, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        live_rows = list(session.execute(sel_live, timeout=60))
        ranked = [r for r in live_rows if isinstance(getattr(r, "market_cap_rank", None), int) and r.market_cap_rank > 0]
        ranked.sort(key=lambda r: r.market_cap_rank)
        selected = [
            CoinMeta(
                coin_id=r.id,
                symbol=(getattr(r, "symbol", None) or r.id or "?").upper(),
                name=getattr(r, "name", None) or r.id,
                rank=int_or_none(getattr(r, "market_cap_rank", None)),
                circ=fnum(getattr(r, "circulating_supply", None)),
                totl=fnum(getattr(r, "total_supply", None)),
            )
            for r in ranked
            if args.rank_start <= int(getattr(r, "market_cap_rank", 0)) <= args.rank_end
        ]
        print(f"[{now_str()}] selected coins={len(selected)}")

        writer_10m = BatchWriter(session, ins_10m, args.batch_size, "10m")
        writer_hourly = BatchWriter(session, ins_hourly, args.batch_size, "hourly")
        writer_daily = BatchWriter(session, ins_daily, args.batch_size, "daily")
        writer_monthly = BatchWriter(session, ins_monthly, args.batch_size, "monthly")

        strategy_counts = {
            "10m": Counter(),
            "hourly": Counter(),
            "daily": Counter(),
            "monthly": Counter(),
        }
        insert_counts = Counter()
        touched_months: dict[str, set[str]] = defaultdict(set)

        day_lo = start_utc.date()
        day_hi = (end_utc - timedelta(seconds=1)).date()

        for idx, coin in enumerate(selected, 1):
            if idx == 1 or idx % args.progress_every == 0 or idx == len(selected):
                print(
                    f"[{now_str()}] coin {idx}/{len(selected)} "
                    f"{safe_text(coin.symbol)} ({safe_text(coin.coin_id)})"
                )

            rows_10m = session.execute(
                ps_10m,
                [coin.coin_id, to_cassandra_ts(start_utc), to_cassandra_ts(end_utc)],
                timeout=60,
            )
            rows_hourly = session.execute(
                ps_hourly,
                [coin.coin_id, to_cassandra_ts(start_utc), to_cassandra_ts(end_utc)],
                timeout=60,
            )
            rows_daily = session.execute(
                ps_daily,
                [coin.coin_id, day_lo, day_hi],
                timeout=60,
            )

            ten_map = collect_row_dicts(rows_10m, "ts")
            hourly_map = collect_row_dicts(rows_hourly, "ts")
            daily_map = collect_row_dicts(rows_daily, "date")

            ten_keys_sorted = sorted(k for k in ten_map.keys() if isinstance(k, datetime))
            hourly_keys_sorted = sorted(k for k in hourly_map.keys() if isinstance(k, datetime))
            daily_keys_sorted = sorted(daily_map.keys())

            # 10m backfill
            for slot in iter_10m(start_utc, end_utc):
                if slot in ten_map:
                    continue

                chosen: dict[str, Any] | None = None
                src_label = ""
                src_granularity = ""

                hour_row = hourly_map.get(floor_hour(slot))
                if hour_row and row_close(hour_row) is not None:
                    chosen = hour_row
                    src_label = pick_source_text("bf_10m_from_hourly", source_leaf(hour_row.get("candle_source")))
                    src_granularity = "hourly"
                else:
                    day_row = daily_map.get(slot.date())
                    if day_row and row_close(day_row) is not None:
                        chosen = day_row
                        src_label = pick_source_text("bf_10m_from_daily", source_leaf(day_row.get("candle_source")))
                        src_granularity = "daily"
                    else:
                        prev_10m_key = find_prev_key(ten_keys_sorted, slot)
                        if prev_10m_key is not None:
                            prev_row = ten_map[prev_10m_key]
                            if row_close(prev_row) is not None:
                                chosen = prev_row
                                src_label = "bf_10m_carry_10m"
                                src_granularity = "10m"
                        if chosen is None:
                            prev_hour_key = find_prev_key(hourly_keys_sorted, slot)
                            if prev_hour_key is not None:
                                prev_row = hourly_map[prev_hour_key]
                                if row_close(prev_row) is not None:
                                    chosen = prev_row
                                    src_label = "bf_10m_carry_hourly"
                                    src_granularity = "hourly"
                        if chosen is None:
                            prev_day_key = find_prev_key(daily_keys_sorted, slot.date())
                            if prev_day_key is not None:
                                prev_row = daily_map[prev_day_key]
                                if row_close(prev_row) is not None:
                                    chosen = prev_row
                                    src_label = "bf_10m_carry_daily"
                                    src_granularity = "daily"

                if chosen is None:
                    continue

                close_px = row_close(chosen)
                if close_px is None:
                    continue

                rank = int_or_none(chosen.get("market_cap_rank")) or coin.rank
                circ = fnum(chosen.get("circulating_supply"), coin.circ)
                totl = fnum(chosen.get("total_supply"), coin.totl)
                last_upd = to_utc(cast(Optional[datetime], chosen.get("last_updated"))) or slot
                new_row = {
                    "symbol": chosen.get("symbol") or coin.symbol,
                    "name": chosen.get("name") or coin.name,
                    "open": close_px,
                    "high": close_px,
                    "low": close_px,
                    "close": close_px,
                    "price_usd": close_px,
                    "market_cap": fnum(chosen.get("market_cap")),
                    "volume_24h": fnum(chosen.get("volume_24h")),
                    "market_cap_rank": rank,
                    "circulating_supply": circ,
                    "total_supply": totl,
                    "last_updated": last_upd,
                    "candle_source": src_label,
                    "point_count": 1,
                    "volume_interval_est": derive_interval_est_from_source(src_granularity, chosen.get("volume_24h")),
                }
                ten_map[slot] = new_row
                ten_keys_sorted.append(slot)
                ten_keys_sorted.sort()
                strategy_counts["10m"][src_label] += 1
                insert_counts["10m"] += 1
                if not args.probe_only:
                    writer_10m.add(
                        [
                            coin.coin_id,
                            to_cassandra_ts(slot),
                            new_row["symbol"],
                            new_row["name"],
                            new_row["open"],
                            new_row["high"],
                            new_row["low"],
                            new_row["close"],
                            new_row["price_usd"],
                            new_row["market_cap"],
                            new_row["volume_24h"],
                            new_row["market_cap_rank"],
                            new_row["circulating_supply"],
                            new_row["total_supply"],
                            to_cassandra_ts(last_upd),
                            new_row["candle_source"],
                            new_row["point_count"],
                            new_row["volume_interval_est"],
                        ]
                    )

            # hourly backfill
            for hour in iter_hours(start_utc, end_utc):
                if hour in hourly_map:
                    continue

                ten_rows = [ten_map[slot] for slot in iter_10m(hour, min(end_utc, hour + ONE_HOUR)) if slot in ten_map]
                if ten_rows:
                    o, h, l, c = derive_ohlc_from_rows(ten_rows)
                    last_row = ten_rows[-1]
                    sources = {str(source_leaf(cast(Optional[str], r.get("candle_source"))) or "") for r in ten_rows if r.get("candle_source")}
                    src_label = (
                        pick_source_text("bf_hourly_from_10m", next(iter(sources)))
                        if len(sources) == 1 else "bf_hourly_from_10m:mixed"
                    )
                    new_row = {
                        "symbol": ten_rows[0].get("symbol") or coin.symbol,
                        "name": ten_rows[0].get("name") or coin.name,
                        "open": o,
                        "high": h,
                        "low": l,
                        "close": c,
                        "price_usd": c,
                        "market_cap": fnum(last_row.get("market_cap")),
                        "volume_24h": fnum(last_row.get("volume_24h")),
                        "market_cap_rank": int_or_none(last_row.get("market_cap_rank")) or coin.rank,
                        "circulating_supply": fnum(last_row.get("circulating_supply"), coin.circ),
                        "total_supply": fnum(last_row.get("total_supply"), coin.totl),
                        "last_updated": to_utc(cast(Optional[datetime], last_row.get("last_updated"))) or hour,
                        "candle_source": src_label,
                        "point_count": len(ten_rows),
                    }
                else:
                    day_row = daily_map.get(hour.date())
                    if day_row and row_close(day_row) is not None:
                        close_px = row_close(day_row)
                        src_label = pick_source_text("bf_hourly_from_daily", source_leaf(day_row.get("candle_source")))
                        new_row = {
                            "symbol": day_row.get("symbol") or coin.symbol,
                            "name": day_row.get("name") or coin.name,
                            "open": close_px,
                            "high": close_px,
                            "low": close_px,
                            "close": close_px,
                            "price_usd": close_px,
                            "market_cap": fnum(day_row.get("market_cap")),
                            "volume_24h": fnum(day_row.get("volume_24h")),
                            "market_cap_rank": int_or_none(day_row.get("market_cap_rank")) or coin.rank,
                            "circulating_supply": fnum(day_row.get("circulating_supply"), coin.circ),
                            "total_supply": fnum(day_row.get("total_supply"), coin.totl),
                            "last_updated": to_utc(cast(Optional[datetime], day_row.get("last_updated"))) or hour,
                            "candle_source": src_label,
                            "point_count": 1,
                        }
                    else:
                        prev_hour_key = find_prev_key(hourly_keys_sorted, hour)
                        if prev_hour_key is None:
                            continue
                        prev_row = hourly_map[prev_hour_key]
                        close_px = row_close(prev_row)
                        if close_px is None:
                            continue
                        src_label = "bf_hourly_carry_hourly"
                        new_row = {
                            "symbol": prev_row.get("symbol") or coin.symbol,
                            "name": prev_row.get("name") or coin.name,
                            "open": close_px,
                            "high": close_px,
                            "low": close_px,
                            "close": close_px,
                            "price_usd": close_px,
                            "market_cap": fnum(prev_row.get("market_cap")),
                            "volume_24h": fnum(prev_row.get("volume_24h")),
                            "market_cap_rank": int_or_none(prev_row.get("market_cap_rank")) or coin.rank,
                            "circulating_supply": fnum(prev_row.get("circulating_supply"), coin.circ),
                            "total_supply": fnum(prev_row.get("total_supply"), coin.totl),
                            "last_updated": to_utc(cast(Optional[datetime], prev_row.get("last_updated"))) or hour,
                            "candle_source": src_label,
                            "point_count": 1,
                        }

                hourly_map[hour] = new_row
                hourly_keys_sorted.append(hour)
                hourly_keys_sorted.sort()
                strategy_counts["hourly"][new_row["candle_source"]] += 1
                insert_counts["hourly"] += 1
                if not args.probe_only:
                    writer_hourly.add(
                        [
                            coin.coin_id,
                            to_cassandra_ts(hour),
                            new_row["symbol"],
                            new_row["name"],
                            new_row["open"],
                            new_row["high"],
                            new_row["low"],
                            new_row["close"],
                            new_row["price_usd"],
                            new_row["market_cap"],
                            new_row["volume_24h"],
                            new_row["market_cap_rank"],
                            new_row["circulating_supply"],
                            new_row["total_supply"],
                            new_row["candle_source"],
                            to_cassandra_ts(cast(datetime, new_row["last_updated"])),
                            new_row["point_count"],
                        ]
                    )

            # daily backfill
            for d in iter_days(day_lo, day_hi):
                if d in daily_map:
                    continue

                day_10m = [
                    ten_map[slot]
                    for slot in iter_10m(day_start(d), min(end_utc, day_start(d) + ONE_DAY))
                    if slot in ten_map and row_is_observed_for_day(ten_map[slot], d)
                ]
                if day_10m:
                    o, h, l, c = derive_ohlc_from_rows(day_10m)
                    last_row = day_10m[-1]
                    sources = {str(source_leaf(cast(Optional[str], r.get("candle_source"))) or "") for r in day_10m if r.get("candle_source")}
                    src_label = (
                        pick_source_text("bf_daily_from_10m", next(iter(sources)))
                        if len(sources) == 1 else "bf_daily_from_10m:mixed"
                    )
                    new_row = {
                        "symbol": day_10m[0].get("symbol") or coin.symbol,
                        "name": day_10m[0].get("name") or coin.name,
                        "open": o,
                        "high": h,
                        "low": l,
                        "close": c,
                        "price_usd": c,
                        "market_cap": fnum(last_row.get("market_cap")),
                        "volume_24h": fnum(last_row.get("volume_24h")),
                        "market_cap_rank": int_or_none(last_row.get("market_cap_rank")) or coin.rank,
                        "circulating_supply": fnum(last_row.get("circulating_supply"), coin.circ),
                        "total_supply": fnum(last_row.get("total_supply"), coin.totl),
                        "last_updated": to_utc(cast(Optional[datetime], last_row.get("last_updated"))) or (day_start(d) + ONE_DAY - timedelta(seconds=1)),
                        "candle_source": src_label,
                        "point_count": len(day_10m),
                    }
                else:
                    day_hours = [
                        hourly_map[hour]
                        for hour in iter_hours(day_start(d), min(end_utc, day_start(d) + ONE_DAY))
                        if hour in hourly_map and row_is_observed_for_day(hourly_map[hour], d)
                    ]
                    if day_hours:
                        o, h, l, c = derive_ohlc_from_rows(day_hours)
                        last_row = day_hours[-1]
                        sources = {str(source_leaf(cast(Optional[str], r.get("candle_source"))) or "") for r in day_hours if r.get("candle_source")}
                        src_label = (
                            pick_source_text("bf_daily_from_hourly", next(iter(sources)))
                            if len(sources) == 1 else "bf_daily_from_hourly:mixed"
                        )
                        new_row = {
                            "symbol": day_hours[0].get("symbol") or coin.symbol,
                            "name": day_hours[0].get("name") or coin.name,
                            "open": o,
                            "high": h,
                            "low": l,
                            "close": c,
                            "price_usd": c,
                            "market_cap": fnum(last_row.get("market_cap")),
                            "volume_24h": fnum(last_row.get("volume_24h")),
                            "market_cap_rank": int_or_none(last_row.get("market_cap_rank")) or coin.rank,
                            "circulating_supply": fnum(last_row.get("circulating_supply"), coin.circ),
                            "total_supply": fnum(last_row.get("total_supply"), coin.totl),
                            "last_updated": to_utc(cast(Optional[datetime], last_row.get("last_updated"))) or (day_start(d) + ONE_DAY - timedelta(seconds=1)),
                            "candle_source": src_label,
                            "point_count": len(day_hours),
                        }
                    elif args.allow_daily_carry:
                        prev_day_key = find_prev_key(daily_keys_sorted, d)
                        if prev_day_key is None:
                            continue
                        prev_row = daily_map[prev_day_key]
                        close_px = row_close(prev_row)
                        if close_px is None:
                            continue
                        src_label = "bf_daily_carry_daily"
                        new_row = {
                            "symbol": prev_row.get("symbol") or coin.symbol,
                            "name": prev_row.get("name") or coin.name,
                            "open": close_px,
                            "high": close_px,
                            "low": close_px,
                            "close": close_px,
                            "price_usd": close_px,
                            "market_cap": fnum(prev_row.get("market_cap")),
                            "volume_24h": fnum(prev_row.get("volume_24h")),
                            "market_cap_rank": int_or_none(prev_row.get("market_cap_rank")) or coin.rank,
                            "circulating_supply": fnum(prev_row.get("circulating_supply"), coin.circ),
                            "total_supply": fnum(prev_row.get("total_supply"), coin.totl),
                            "last_updated": to_utc(cast(Optional[datetime], prev_row.get("last_updated"))) or (day_start(d) + ONE_DAY - timedelta(seconds=1)),
                            "candle_source": src_label,
                            "point_count": 1,
                        }
                    else:
                        continue

                daily_map[d] = new_row
                daily_keys_sorted.append(d)
                daily_keys_sorted.sort()
                strategy_counts["daily"][new_row["candle_source"]] += 1
                insert_counts["daily"] += 1
                touched_months[coin.coin_id].add(ym_tag(d))
                if not args.probe_only:
                    writer_daily.add(
                        [
                            coin.coin_id,
                            d,
                            new_row["symbol"],
                            new_row["name"],
                            new_row["open"],
                            new_row["high"],
                            new_row["low"],
                            new_row["close"],
                            new_row["price_usd"],
                            new_row["market_cap"],
                            new_row["volume_24h"],
                            new_row["market_cap_rank"],
                            new_row["circulating_supply"],
                            new_row["total_supply"],
                            new_row["candle_source"],
                            to_cassandra_ts(cast(datetime, new_row["last_updated"])),
                            new_row["point_count"],
                        ]
                    )

            # monthly refresh for touched months in the range
            months_in_scope = {ym_tag(d) for d in iter_days(day_lo, day_hi)}
            for ym in sorted(months_in_scope):
                y = int(ym[:4])
                m = int(ym[5:7])
                month_d = date(y, m, 1)
                month_end_excl = next_month_start(month_d)
                month_rows = session.execute(
                    ps_daily_month,
                    [coin.coin_id, month_d, month_end_excl],
                    timeout=60,
                )
                month_daily = collect_row_dicts(month_rows, "date")
                if not month_daily:
                    continue
                ordered_days = sorted(month_daily.keys())
                ordered_rows = [month_daily[d] for d in ordered_days]
                o, h, l, c = derive_ohlc_from_rows(ordered_rows)
                if c is None:
                    continue
                last_row = ordered_rows[-1]
                last_upd = to_utc(cast(Optional[datetime], last_row.get("last_updated"))) or (
                    datetime(month_end_excl.year, month_end_excl.month, month_end_excl.day, tzinfo=UTC) - timedelta(seconds=1)
                )
                month_volume = sum(float(r.get("volume_24h") or 0.0) for r in ordered_rows)
                is_current_month = ym == ym_tag((end_utc - timedelta(seconds=1)).date())
                src_label = "bf_monthly_from_daily_partial" if is_current_month else "bf_monthly_from_daily_final"
                strategy_counts["monthly"][src_label] += 1
                insert_counts["monthly"] += 1
                if not args.probe_only:
                    writer_monthly.add(
                        [
                            coin.coin_id,
                            ym,
                            ordered_rows[0].get("symbol") or coin.symbol,
                            ordered_rows[0].get("name") or coin.name,
                            o,
                            h,
                            l,
                            c,
                            month_volume,
                            fnum(last_row.get("market_cap")),
                            int_or_none(last_row.get("market_cap_rank")) or coin.rank,
                            fnum(last_row.get("circulating_supply"), coin.circ),
                            fnum(last_row.get("total_supply"), coin.totl),
                            src_label,
                            to_cassandra_ts(last_upd),
                        ]
                    )

        if not args.probe_only:
            writer_10m.flush()
            writer_hourly.flush()
            writer_daily.flush()
            writer_monthly.flush()

        print(f"[{now_str()}] probe/backfill summary")
        for granularity in ("10m", "hourly", "daily", "monthly"):
            print(f"  {granularity}: inserted={insert_counts[granularity]}")
            for label, count in strategy_counts[granularity].most_common(12):
                print(f"    {label}: {count}")

    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
