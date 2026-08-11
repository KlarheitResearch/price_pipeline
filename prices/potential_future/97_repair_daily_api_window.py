#!/usr/bin/env python3
from __future__ import annotations

import os
from collections import deque
from datetime import date, datetime, timedelta

from cassandra.query import SimpleStatement

from prices.potential_future.common import (
    API_TIER,
    Heartbeat,
    PipelineHealthTracker,
    TABLE_DAILY,
    TABLE_LIVE,
    UTC,
    cg_market_chart_range,
    connect_astra,
    drain_async,
    enqueue_async,
    extract_series_in_window,
    now_str,
    now_utc,
    should_log_progress,
    scope_label,
    select_coins_from_live_rows,
    to_cassandra_ts,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
ASTRA_MAX_IN_FLIGHT = int(os.getenv("PP_ASTRA_MAX_IN_FLIGHT", "64"))
LOG_EVERY = int(os.getenv("PP_DAILY_REPAIR_LOG_EVERY", "25"))

ONLY_NON_API = os.getenv("PP_DAILY_REPAIR_ONLY_NON_API", "1") == "1"
REWRITE_API = os.getenv("PP_DAILY_REPAIR_REWRITE_API", "0") == "1"
TAG_OLD_NON_API = os.getenv("PP_DAILY_REPAIR_TAG_OLD_NON_API", "1") == "1"
CANONICALIZE_API_SOURCE = os.getenv("PP_DAILY_REPAIR_CANONICALIZE_API_SOURCE", "1") == "1"
CANONICALIZE_ALL_WINDOW = os.getenv("PP_DAILY_REPAIR_CANONICALIZE_ALL_WINDOW", "1") == "1"
LABELS_ONLY = os.getenv("PP_DAILY_REPAIR_LABELS_ONLY", "0") == "1"
OLD_NON_API_SOURCE = (os.getenv("PP_DAILY_REPAIR_OLD_NON_API_SOURCE", "legacy_non_api") or "legacy_non_api").strip()
FINAL_API_SOURCE = (os.getenv("PP_DAILY_REPAIR_FINAL_SOURCE", "cg_daily_final") or "cg_daily_final").strip()
REWRITE_DEGENERATE_API = os.getenv("PP_DAILY_REPAIR_REWRITE_DEGENERATE_API", "1") == "1"
CONSOLIDATE_BUCKETS = os.getenv("PP_DAILY_REPAIR_CONSOLIDATE_BUCKETS", "0") == "1"
API_BUCKET_SOURCE = (os.getenv("PP_DAILY_REPAIR_API_BUCKET_SOURCE", "legacy_api") or "legacy_api").strip()
NON_API_BUCKET_SOURCE = (os.getenv("PP_DAILY_REPAIR_NON_API_BUCKET_SOURCE", "legacy_non_api") or "legacy_non_api").strip()
CONSOLIDATE_SCAN_ALL_ROWS = os.getenv("PP_DAILY_REPAIR_CONSOLIDATE_SCAN_ALL_ROWS", "0") == "1"
MARKET_CHART_CHUNK_DAYS = max(
    1,
    int(os.getenv("PP_DAILY_REPAIR_MARKET_CHART_CHUNK_DAYS", "90")),
)

MAX_API_DAYS_DEMO = int(os.getenv("PP_DAILY_REPAIR_MAX_API_DAYS_DEMO", "365"))
MAX_COINS = int(os.getenv("PP_DAILY_REPAIR_MAX_COINS", "0"))


def _parse_source_set(raw: str) -> set[str]:
    out = set()
    for part in (raw or "").split(","):
        src = part.strip().lower()
        if src:
            out.add(src)
    return out


API_SOURCE_SET = _parse_source_set(
    os.getenv(
        "PP_DAILY_REPAIR_API_SOURCES",
        "cg_daily_final,cg_daily_bootstrap,cg_pro_range_daily,cg_pro_range_daily+fix_ohlc_prevclose_v1,extra_daily_repair_api",
    )
)


def _parse_day(raw: str | None) -> date | None:
    text = (raw or "").strip()
    if not text:
        return None
    return datetime.strptime(text, "%Y-%m-%d").date()


def _day_start(day_key: date) -> datetime:
    return datetime(day_key.year, day_key.month, day_key.day, tzinfo=UTC)


def _iter_days(start_day: date, end_day: date):
    cur = start_day
    while cur <= end_day:
        yield cur
        cur += timedelta(days=1)


def _source(row) -> str:
    return (getattr(row, "candle_source", None) or "").strip().lower()


def _as_py_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        # cassandra.util.Date exposes .date()
        if hasattr(value, "date"):
            d = value.date()
            if isinstance(d, date):
                return d
    except Exception:
        pass
    try:
        # fallback: parse YYYY-MM-DD string
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except Exception:
        return None


def _is_api_source_label(src: str) -> bool:
    s = (src or "").strip().lower()
    if not s:
        return False
    if s in API_SOURCE_SET:
        return True
    if s.startswith("cg_pro"):
        return True
    if s.startswith("cg_daily"):
        return True
    if "repair_api" in s:
        return True
    return False


def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _i(x, default=None):
    try:
        return int(x) if x is not None else default
    except Exception:
        return default


def _build_daily_price_map(
    prices: list[tuple[datetime, float]],
    start_day: date,
    end_day: date,
) -> dict[date, list[tuple[datetime, float]]]:
    out: dict[date, list[tuple[datetime, float]]] = {}
    for ts, value in prices:
        d = ts.date()
        if start_day <= d <= end_day:
            out.setdefault(d, []).append((ts, float(value)))
    for d in list(out.keys()):
        out[d].sort(key=lambda x: x[0])
    return out


def _build_last_value_map(
    points: list[tuple[datetime, float]],
    start_day: date,
    end_day: date,
) -> dict[date, tuple[float, datetime]]:
    out: dict[date, tuple[float, datetime]] = {}
    for ts, value in points:
        d = ts.date()
        if start_day <= d <= end_day:
            out[d] = (float(value), ts)
    return out


def _fetch_market_chart_chunked(
    coin_id: str,
    start_ts: datetime,
    end_ts_exclusive: datetime,
    chunk_days: int,
) -> tuple[list[tuple[datetime, float]], list[tuple[datetime, float]], list[tuple[datetime, float]], int]:
    prices_all: list[tuple[datetime, float]] = []
    mcap_all: list[tuple[datetime, float]] = []
    vol_all: list[tuple[datetime, float]] = []
    calls = 0

    cur = start_ts
    step = timedelta(days=max(1, int(chunk_days)))
    while cur < end_ts_exclusive:
        nxt = min(end_ts_exclusive, cur + step)
        data = cg_market_chart_range(coin_id, cur, nxt, vs_currency="usd", interval="hourly")
        calls += 1
        prices_all.extend(extract_series_in_window(data.get("prices", []) or [], cur, nxt))
        mcap_all.extend(extract_series_in_window(data.get("market_caps", []) or [], cur, nxt))
        vol_all.extend(extract_series_in_window(data.get("total_volumes", []) or [], cur, nxt))
        cur = nxt

    return prices_all, mcap_all, vol_all, calls


def main() -> None:
    now_day = now_utc().date()
    end_day = _parse_day(os.getenv("PP_DAILY_REPAIR_END_DAY")) or (now_day - timedelta(days=1))
    start_day = _parse_day(os.getenv("PP_DAILY_REPAIR_START_DAY"))
    if start_day is None:
        start_day = end_day - timedelta(days=MAX_API_DAYS_DEMO - 1)
    if start_day > end_day:
        raise RuntimeError(f"Invalid window: start_day={start_day} > end_day={end_day}")

    demo_floor = now_day - timedelta(days=MAX_API_DAYS_DEMO)
    api_start_day = start_day
    api_end_day = end_day
    api_window_clamped = False
    if API_TIER == "demo" and api_start_day < demo_floor:
        api_start_day = demo_floor
        api_window_clamped = True

    if api_start_day > api_end_day:
        print(
            f"[{now_str()}] Window {start_day}..{end_day} is fully older than demo API horizon "
            f"({MAX_API_DAYS_DEMO} days). Only source-tagging can run."
        )

    print(
        f"[{now_str()}] Daily one-time repair start scope={scope_label()} "
        f"window={start_day}..{end_day} api_window={api_start_day}..{api_end_day} "
        f"api_tier={API_TIER} only_non_api={ONLY_NON_API} rewrite_api={REWRITE_API} "
        f"tag_old_non_api={TAG_OLD_NON_API} canonicalize_api_source={CANONICALIZE_API_SOURCE} "
        f"canonicalize_all_window={CANONICALIZE_ALL_WINDOW} labels_only={LABELS_ONLY} "
        f"chunk_days={MARKET_CHART_CHUNK_DAYS} consolidate_buckets={CONSOLIDATE_BUCKETS} "
        f"scan_all_rows={CONSOLIDATE_SCAN_ALL_ROWS}"
    )
    print(
        f"[{now_str()}] Source mapping: api_sources={sorted(API_SOURCE_SET)} "
        f"final_api_source={FINAL_API_SOURCE} old_non_api_source={OLD_NON_API_SOURCE} "
        f"api_bucket_source={API_BUCKET_SOURCE} non_api_bucket_source={NON_API_BUCKET_SOURCE}"
    )
    if api_window_clamped:
        print(
            f"[{now_str()}] API window clamped to demo horizon. "
            f"Set PP_DAILY_REPAIR_START_DAY >= {demo_floor} or use pro keys."
        )

    session, cluster = connect_astra()
    tracker = PipelineHealthTracker(session, "97_repair_daily_api_window")
    tracker.set_metric("start_day", str(start_day))
    tracker.set_metric("end_day", str(end_day))
    tracker.set_metric("api_start_day", str(api_start_day))
    tracker.set_metric("api_end_day", str(api_end_day))
    tracker.set_metric("api_tier", API_TIER)
    tracker.set_metric("only_non_api", 1 if ONLY_NON_API else 0)
    tracker.set_metric("rewrite_api", 1 if REWRITE_API else 0)
    tracker.set_metric("tag_old_non_api", 1 if TAG_OLD_NON_API else 0)
    tracker.set_metric("canonicalize_api_source", 1 if CANONICALIZE_API_SOURCE else 0)
    tracker.set_metric("canonicalize_all_window", 1 if CANONICALIZE_ALL_WINDOW else 0)
    tracker.set_metric("labels_only", 1 if LABELS_ONLY else 0)
    tracker.set_metric("consolidate_buckets", 1 if CONSOLIDATE_BUCKETS else 0)
    tracker.set_metric("consolidate_scan_all_rows", 1 if CONSOLIDATE_SCAN_ALL_ROWS else 0)
    tracker.set_metric("chunk_days", MARKET_CHART_CHUNK_DAYS)
    tracker.set_metric("api_source_set", ",".join(sorted(API_SOURCE_SET)))
    tracker.start()

    coins_processed = 0
    api_calls = 0
    api_days_written = 0
    api_days_missing_points = 0
    old_rows_tagged = 0
    api_rows_canonicalized = 0
    bucket_rows_api = 0
    bucket_rows_non_api = 0
    try:
        update_source = session.prepare(
            f"""
            UPDATE {TABLE_DAILY}
            SET candle_source=?
            WHERE id=? AND date=?
            """
        )

        # Deterministic full-table source consolidation for historical cleanup.
        if CONSOLIDATE_BUCKETS and LABELS_ONLY and CONSOLIDATE_SCAN_ALL_ROWS:
            print(f"[{now_str()}] Consolidation mode: scanning all rows from {TABLE_DAILY} in {start_day}..{end_day}")
            scan_stmt = SimpleStatement(
                f"SELECT id, date, candle_source FROM {TABLE_DAILY}",
                fetch_size=5000,
            )
            pending = deque()
            scanned = 0
            for row in session.execute(scan_stmt, timeout=REQUEST_TIMEOUT_SEC):
                scanned += 1
                if scanned % max(10000, LOG_EVERY * 1000) == 0:
                    print(f"[{now_str()}] scanned_rows={scanned} bucket_rows_api={bucket_rows_api} bucket_rows_non_api={bucket_rows_non_api}")
                row_id = getattr(row, "id", None)
                day_key = _as_py_date(getattr(row, "date", None))
                if not row_id or day_key is None:
                    continue
                if day_key < start_day or day_key > end_day:
                    continue
                src = (getattr(row, "candle_source", None) or "").strip().lower()
                is_api = _is_api_source_label(src)
                target = API_BUCKET_SOURCE if is_api else NON_API_BUCKET_SOURCE
                target_lc = target.lower()
                if src == target_lc:
                    continue
                enqueue_async(
                    session,
                    pending,
                    update_source,
                    [target, row_id, day_key],
                    timeout=REQUEST_TIMEOUT_SEC,
                    max_in_flight=ASTRA_MAX_IN_FLIGHT,
                )
                if is_api:
                    bucket_rows_api += 1
                else:
                    bucket_rows_non_api += 1
            drain_async(pending)
            print(
                f"[{now_str()}] Consolidation scan done. scanned_rows={scanned} "
                f"bucket_rows_api={bucket_rows_api} bucket_rows_non_api={bucket_rows_non_api}"
            )
            tracker.set_metric("scanned_rows", scanned)
            tracker.set_metric("coins_processed", 0)
            tracker.set_metric("api_calls", 0)
            tracker.set_metric("api_days_written", 0)
            tracker.set_metric("api_days_missing_points", 0)
            tracker.set_metric("old_rows_tagged", 0)
            tracker.set_metric("api_rows_canonicalized", 0)
            tracker.set_metric("bucket_rows_api", bucket_rows_api)
            tracker.set_metric("bucket_rows_non_api", bucket_rows_non_api)
            tracker.finish("success")
            return

        sel_live = SimpleStatement(
            f"SELECT id, symbol, name, market_cap_rank, circulating_supply, total_supply FROM {TABLE_LIVE}",
            fetch_size=2000,
        )
        live_rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC))
        coins = select_coins_from_live_rows(live_rows)
        if not coins:
            print(f"[{now_str()}] No scoped coins in {TABLE_LIVE} for {scope_label()}.")
            tracker.mark_noop()
            tracker.set_metric("coins_scoped", 0)
            tracker.finish("noop")
            return
        if MAX_COINS > 0:
            coins = coins[:MAX_COINS]
        tracker.set_metric("coins_scoped", len(coins))

        sel_daily_range = session.prepare(
            f"""
            SELECT date, symbol, name,
                   open, high, low, close, price_usd,
                   market_cap, market_cap_rank, volume_24h,
                   circulating_supply, total_supply,
                   candle_source, point_count, last_updated
            FROM {TABLE_DAILY}
            WHERE id=? AND date>=? AND date<=?
            """
        )
        ins_daily = session.prepare(
            f"""
            INSERT INTO {TABLE_DAILY}
              (id, date, symbol, name,
               open, high, low, close, price_usd,
               market_cap, volume_24h,
               market_cap_rank, circulating_supply, total_supply,
               candle_source, point_count, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        hb = Heartbeat("97_repair_daily_api_window")
        for idx, coin in enumerate(coins, 1):
            if should_log_progress(idx, len(coins), default_every=LOG_EVERY):
                print(f"[{now_str()}] coin {idx}/{len(coins)} -> {coin.id}")
            hb.maybe(extra=f"coin={idx}/{len(coins)}")
            coins_processed += 1
            pending = deque()

            rows = list(
                session.execute(
                    sel_daily_range,
                    [coin.id, start_day, end_day],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
            )
            existing_by_day = {}
            for r in rows:
                d = _as_py_date(getattr(r, "date", None))
                if d is not None:
                    existing_by_day[d] = r

            if CONSOLIDATE_BUCKETS:
                api_bucket_lc = API_BUCKET_SOURCE.lower()
                non_api_bucket_lc = NON_API_BUCKET_SOURCE.lower()
                for d in _iter_days(start_day, end_day):
                    ex = existing_by_day.get(d)
                    if ex is None:
                        continue
                    src = _source(ex)
                    is_api = _is_api_source_label(src)
                    target = API_BUCKET_SOURCE if is_api else NON_API_BUCKET_SOURCE
                    target_lc = api_bucket_lc if is_api else non_api_bucket_lc
                    if src == target_lc:
                        continue
                    enqueue_async(
                        session,
                        pending,
                        update_source,
                        [target, coin.id, d],
                        timeout=REQUEST_TIMEOUT_SEC,
                        max_in_flight=ASTRA_MAX_IN_FLIGHT,
                    )
                    if is_api:
                        bucket_rows_api += 1
                    else:
                        bucket_rows_non_api += 1
            else:
                # Old region: optional re-tag as explicitly non-API.
                if TAG_OLD_NON_API:
                    old_region_end = min(end_day, api_start_day - timedelta(days=1))
                    if start_day <= old_region_end:
                        for d in _iter_days(start_day, old_region_end):
                            ex = existing_by_day.get(d)
                            if ex is None:
                                continue
                            src = _source(ex)
                            if src in API_SOURCE_SET or src == OLD_NON_API_SOURCE.lower():
                                continue
                            enqueue_async(
                                session,
                                pending,
                                update_source,
                                [OLD_NON_API_SOURCE, coin.id, d],
                                timeout=REQUEST_TIMEOUT_SEC,
                                max_in_flight=ASTRA_MAX_IN_FLIGHT,
                            )
                            old_rows_tagged += 1

                # Optional source canonicalization for already API-backed rows.
                canon_start_day = start_day if CANONICALIZE_ALL_WINDOW else api_start_day
                canon_end_day = end_day if CANONICALIZE_ALL_WINDOW else api_end_day
                if CANONICALIZE_API_SOURCE and canon_start_day <= canon_end_day:
                    for d in _iter_days(canon_start_day, canon_end_day):
                        ex = existing_by_day.get(d)
                        if ex is None:
                            continue
                        src = _source(ex)
                        if src in API_SOURCE_SET and src != FINAL_API_SOURCE.lower():
                            enqueue_async(
                                session,
                                pending,
                                update_source,
                                [FINAL_API_SOURCE, coin.id, d],
                                timeout=REQUEST_TIMEOUT_SEC,
                                max_in_flight=ASTRA_MAX_IN_FLIGHT,
                            )
                            api_rows_canonicalized += 1

            target_days: list[date] = []
            if not LABELS_ONLY and api_start_day <= api_end_day:
                for d in _iter_days(api_start_day, api_end_day):
                    ex = existing_by_day.get(d)
                    if ex is None:
                        target_days.append(d)
                        continue
                    src = _source(ex)
                    if REWRITE_API:
                        target_days.append(d)
                    elif ONLY_NON_API and src not in API_SOURCE_SET:
                        target_days.append(d)
                    elif (
                        REWRITE_DEGENERATE_API
                        and src == FINAL_API_SOURCE.lower()
                        and _i(getattr(ex, "point_count", None), 0) <= 1
                    ):
                        # Safety valve to recover rows previously written with degenerate OHLC.
                        target_days.append(d)

            if target_days:
                api_from = _day_start(api_start_day)
                api_to_exclusive = _day_start(api_end_day + timedelta(days=1))
                try:
                    prices, market_caps, volumes, calls = _fetch_market_chart_chunked(
                        coin.id,
                        api_from,
                        api_to_exclusive,
                        MARKET_CHART_CHUNK_DAYS,
                    )
                    api_calls += calls
                except Exception as exc:
                    print(f"[{now_str()}] [warn] API fetch failed for {coin.id}: {exc}")
                    drain_async(pending)
                    continue
                by_day_prices = _build_daily_price_map(prices, api_start_day, api_end_day)
                by_day_mcap = _build_last_value_map(market_caps, api_start_day, api_end_day)
                by_day_vol = _build_last_value_map(volumes, api_start_day, api_end_day)

                for d in sorted(target_days):
                    day_prices = by_day_prices.get(d) or []
                    # Require at least 2 intraday points to avoid writing flat/degenerate OHLC.
                    if len(day_prices) < 2:
                        api_days_missing_points += 1
                        continue
                    values = [v for _ts, v in day_prices]
                    open_price = float(values[0])
                    close_price = float(values[-1])
                    high_price = float(max(values))
                    low_price = float(min(values))
                    last_price_ts = day_prices[-1][0]
                    point_count = int(len(values))

                    ex = existing_by_day.get(d)
                    symbol = ((getattr(ex, "symbol", None) if ex is not None else None) or coin.symbol or "").upper()
                    name = (getattr(ex, "name", None) if ex is not None else None) or coin.name
                    rank = _i(getattr(ex, "market_cap_rank", None) if ex is not None else None)
                    if rank is None:
                        rank = _i(getattr(coin, "market_cap_rank", None))

                    mcap_pair = by_day_mcap.get(d)
                    vol_pair = by_day_vol.get(d)
                    mcap = float(mcap_pair[0]) if mcap_pair is not None else _f(getattr(ex, "market_cap", None))
                    vol = float(vol_pair[0]) if vol_pair is not None else _f(getattr(ex, "volume_24h", None))

                    carry_circ = _f(getattr(ex, "circulating_supply", None))
                    carry_total = _f(getattr(ex, "total_supply", None))
                    if carry_circ is None:
                        carry_circ = _f(getattr(coin, "circulating_supply", None))
                    if carry_total is None:
                        carry_total = _f(getattr(coin, "total_supply", None))

                    enqueue_async(
                        session,
                        pending,
                        ins_daily,
                        [
                            coin.id,
                            d,
                            symbol,
                            name,
                            open_price,
                            high_price,
                            low_price,
                            close_price,
                            close_price,
                            mcap,
                            vol,
                            rank,
                            carry_circ,
                            carry_total,
                            FINAL_API_SOURCE,
                            point_count,
                            to_cassandra_ts(last_price_ts),
                        ],
                        timeout=REQUEST_TIMEOUT_SEC,
                        max_in_flight=ASTRA_MAX_IN_FLIGHT,
                    )
                    api_days_written += 1

            drain_async(pending)

        print(
            f"[{now_str()}] Daily one-time repair done. coins={coins_processed} api_calls={api_calls} "
            f"api_days_written={api_days_written} "
            f"api_days_missing_points={api_days_missing_points} "
            f"old_rows_tagged={old_rows_tagged} api_rows_canonicalized={api_rows_canonicalized} "
            f"bucket_rows_api={bucket_rows_api} bucket_rows_non_api={bucket_rows_non_api}"
        )
        tracker.set_metric("coins_processed", coins_processed)
        tracker.set_metric("api_calls", api_calls)
        tracker.set_metric("api_days_written", api_days_written)
        tracker.set_metric("api_days_missing_points", api_days_missing_points)
        tracker.set_metric("old_rows_tagged", old_rows_tagged)
        tracker.set_metric("api_rows_canonicalized", api_rows_canonicalized)
        tracker.set_metric("bucket_rows_api", bucket_rows_api)
        tracker.set_metric("bucket_rows_non_api", bucket_rows_non_api)
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
