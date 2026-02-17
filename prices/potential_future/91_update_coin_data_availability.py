#!/usr/bin/env python3
from __future__ import annotations

import os
from collections import deque
from datetime import datetime, date, timedelta

from cassandra.query import SimpleStatement

from prices.potential_future.common import (
    Heartbeat,
    PipelineHealthTracker,
    TABLE_10M,
    TABLE_DAILY,
    TABLE_HOURLY,
    TABLE_LIVE,
    UTC,
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
RUN_DAILY = os.getenv("PP_AVAIL_RUN_DAILY", "1") == "1"
RUN_INTRADAY = os.getenv("PP_AVAIL_RUN_INTRADAY", "1") == "1"
DAILY_WINDOW_DAYS = int(os.getenv("PP_AVAIL_DAILY_WINDOW_DAYS", "365"))
INTRADAY_10M_DAYS = int(os.getenv("PP_AVAIL_10M_DAYS", "7"))
INTRADAY_HOURLY_DAYS = int(os.getenv("PP_AVAIL_HOURLY_DAYS", "30"))
LOG_EVERY = int(os.getenv("PP_AVAIL_LOG_EVERY", "100"))
TTL_10M = int(os.getenv("PP_INTRADAY_COVERAGE_TTL_10M_SEC", str(8 * 24 * 3600)))
TTL_1H = int(os.getenv("PP_INTRADAY_COVERAGE_TTL_1H_SEC", str(33 * 24 * 3600)))

TABLE_DAILY_RANGES = os.getenv("PP_TABLE_DAILY_RANGES", "pp_coin_daily_coverage_ranges")
TABLE_DAILY_AVAIL = os.getenv("PP_TABLE_DAILY_AVAIL", "pp_coin_daily_availability")
TABLE_INTRADAY_COV = os.getenv("PP_TABLE_INTRADAY_COV", "pp_coin_intraday_coverage")

G_10M = 1
G_1H = 2
SLOTS_10M = 24 * 6
SLOTS_1H = 24


def _parse_source_set(env_name: str, default_csv: str) -> set[str]:
    raw = (os.getenv(env_name, default_csv) or "").strip()
    out = set()
    for part in raw.split(","):
        src = part.strip().lower()
        if src:
            out.add(src)
    return out


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


TRUSTED_DAILY_SOURCES = _parse_source_set(
    "PP_AVAIL_TRUSTED_DAILY_SOURCES",
    "cg_daily_final,cg_daily_bootstrap",
)
TRUSTED_HOURLY_SOURCES = _parse_source_set(
    "PP_AVAIL_TRUSTED_HOURLY_SOURCES",
    "cg_hourly_final",
)
TRUSTED_10M_SOURCES = _parse_source_set(
    "PP_AVAIL_TRUSTED_10M_SOURCES",
    "repair_api_points",
)
TRUSTED_DAILY_MIN_POINTS = max(0, _int_env("PP_AVAIL_TRUSTED_DAILY_MIN_POINTS", 1))
TRUSTED_HOURLY_MIN_POINTS = max(0, _int_env("PP_AVAIL_TRUSTED_HOURLY_MIN_POINTS", 1))
TRUSTED_10M_MIN_POINTS = max(0, _int_env("PP_AVAIL_TRUSTED_10M_MIN_POINTS", 1))


def _target_day():
    raw = (os.getenv("PP_AVAIL_TARGET_DAY") or "").strip()
    if raw:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    return (now_utc() - timedelta(days=1)).date()


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


def _build_ranges(days_sorted):
    if not days_sorted:
        return []
    out = []
    s = days_sorted[0]
    prev = s
    for d in days_sorted[1:]:
        if d == prev + timedelta(days=1):
            prev = d
            continue
        out.append((s, prev))
        s = d
        prev = d
    out.append((s, prev))
    return out


def _open_range_start(have_set, day_key):
    if day_key not in have_set:
        return None
    cur = day_key
    while (cur - timedelta(days=1)) in have_set:
        cur -= timedelta(days=1)
    return cur


def _bitset_len(slot_count: int) -> int:
    return (slot_count + 7) // 8


def _set_bit(bitmap: bytearray, slot_idx: int) -> None:
    if slot_idx < 0:
        return
    bi = slot_idx // 8
    if bi >= len(bitmap):
        return
    bitmap[bi] |= (1 << (slot_idx % 8))


def _popcount(buf: bytes) -> int:
    return sum(bin(b).count("1") for b in buf)


def _scan_intraday(rows, slot_count: int, slot_fn, row_filter):
    by_day = {}
    skipped_untrusted = 0
    for r in rows:
        if not row_filter(r):
            skipped_untrusted += 1
            continue
        ts = to_utc(getattr(r, "ts", None))
        if ts is None:
            continue
        day_key = ts.date()
        e = by_day.get(day_key)
        if e is None:
            e = {
                "bitmap": bytearray(_bitset_len(slot_count)),
                "first_seen": ts,
                "last_seen": ts,
            }
            by_day[day_key] = e
        if ts < e["first_seen"]:
            e["first_seen"] = ts
        if ts > e["last_seen"]:
            e["last_seen"] = ts
        _set_bit(e["bitmap"], slot_fn(ts))
    return by_day, skipped_untrusted


def _slot_idx_10m(ts):
    return ts.hour * 6 + (ts.minute // 10)


def _slot_idx_1h(ts):
    return ts.hour


def _row_source(row) -> str:
    return (getattr(row, "candle_source", None) or "").strip().lower()


def _row_point_count(row) -> int:
    raw = getattr(row, "point_count", None)
    try:
        return int(raw) if raw is not None else 0
    except Exception:
        return 0


def _is_trusted_row(row, trusted_sources: set[str], min_points: int) -> bool:
    if not trusted_sources:
        return False
    src = _row_source(row)
    if src not in trusted_sources:
        return False
    return _row_point_count(row) >= max(0, int(min_points))


def _is_trusted_daily_row(row) -> bool:
    return _is_trusted_row(row, TRUSTED_DAILY_SOURCES, TRUSTED_DAILY_MIN_POINTS)


def _is_trusted_10m_row(row) -> bool:
    return _is_trusted_row(row, TRUSTED_10M_SOURCES, TRUSTED_10M_MIN_POINTS)


def _is_trusted_1h_row(row) -> bool:
    return _is_trusted_row(row, TRUSTED_HOURLY_SOURCES, TRUSTED_HOURLY_MIN_POINTS)


def main() -> None:
    if DAILY_WINDOW_DAYS <= 0:
        raise RuntimeError("PP_AVAIL_DAILY_WINDOW_DAYS must be > 0")
    if INTRADAY_10M_DAYS <= 0:
        raise RuntimeError("PP_AVAIL_10M_DAYS must be > 0")
    if INTRADAY_HOURLY_DAYS <= 0:
        raise RuntimeError("PP_AVAIL_HOURLY_DAYS must be > 0")

    target_day = _target_day()
    daily_first = target_day - timedelta(days=DAILY_WINDOW_DAYS - 1)

    session, cluster = connect_astra()
    tracker = PipelineHealthTracker(session, "91_update_coin_data_availability")
    tracker.set_metric("daily_window_days", DAILY_WINDOW_DAYS)
    tracker.set_metric("intraday_10m_days", INTRADAY_10M_DAYS)
    tracker.set_metric("intraday_hourly_days", INTRADAY_HOURLY_DAYS)
    tracker.set_metric("trusted_daily_sources", ",".join(sorted(TRUSTED_DAILY_SOURCES)))
    tracker.set_metric("trusted_hourly_sources", ",".join(sorted(TRUSTED_HOURLY_SOURCES)))
    tracker.set_metric("trusted_10m_sources", ",".join(sorted(TRUSTED_10M_SOURCES)))
    tracker.set_metric("trusted_daily_min_points", TRUSTED_DAILY_MIN_POINTS)
    tracker.set_metric("trusted_hourly_min_points", TRUSTED_HOURLY_MIN_POINTS)
    tracker.set_metric("trusted_10m_min_points", TRUSTED_10M_MIN_POINTS)
    tracker.start()
    try:
        sel_live = SimpleStatement(
            f"SELECT id, symbol, name, market_cap_rank FROM {TABLE_LIVE}",
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
        tracker.set_metric("coins_scoped", len(coins))

        print(
            f"[{now_str()}] Availability update: scope={scope_label()} coins={len(coins)} "
            f"target_day={target_day} run_daily={RUN_DAILY} run_intraday={RUN_INTRADAY} "
            f"trusted_daily={sorted(TRUSTED_DAILY_SOURCES)} trusted_hourly={sorted(TRUSTED_HOURLY_SOURCES)} "
            f"trusted_10m={sorted(TRUSTED_10M_SOURCES)}"
        )

        sel_daily = session.prepare(
            f"""
            SELECT date, price_usd, close, market_cap, volume_24h, candle_source, point_count
            FROM {TABLE_DAILY}
            WHERE id=? AND date>=? AND date<=?
            """
        )
        del_daily_ranges = session.prepare(f"DELETE FROM {TABLE_DAILY_RANGES} WHERE id=?")
        ins_daily_range = session.prepare(
            f"""
            INSERT INTO {TABLE_DAILY_RANGES}
              (id, start_date, end_date, detected_at)
            VALUES (?, ?, ?, ?)
            """
        )
        ins_daily_avail = session.prepare(
            f"""
            INSERT INTO {TABLE_DAILY_AVAIL}
              (id, first_day, last_day, expected_days,
               have_any, missing_any,
               have_price, missing_price,
               have_volume, missing_volume,
               have_mcap, missing_mcap,
               symbol, name, updated_at, open_range_start)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        sel_10m_range = session.prepare(
            f"""
            SELECT ts, candle_source, point_count
            FROM {TABLE_10M}
            WHERE id=? AND ts>=? AND ts<?
            """
        )
        sel_hourly_range = session.prepare(
            f"""
            SELECT ts, candle_source, point_count
            FROM {TABLE_HOURLY}
            WHERE id=? AND ts>=? AND ts<?
            """
        )
        del_intraday = session.prepare(
            f"""
            DELETE FROM {TABLE_INTRADAY_COV}
            WHERE id=? AND granularity=?
            """
        )
        if TTL_10M > 0:
            ins_intraday_10m = session.prepare(
                f"""
                INSERT INTO {TABLE_INTRADAY_COV}
                  (id, granularity, day, bitmap, set_count, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                USING TTL {TTL_10M}
                """
            )
        else:
            ins_intraday_10m = session.prepare(
                f"""
                INSERT INTO {TABLE_INTRADAY_COV}
                  (id, granularity, day, bitmap, set_count, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
            )
        if TTL_1H > 0:
            ins_intraday_1h = session.prepare(
                f"""
                INSERT INTO {TABLE_INTRADAY_COV}
                  (id, granularity, day, bitmap, set_count, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                USING TTL {TTL_1H}
                """
            )
        else:
            ins_intraday_1h = session.prepare(
                f"""
                INSERT INTO {TABLE_INTRADAY_COV}
                  (id, granularity, day, bitmap, set_count, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
            )

        wrote_ranges = 0
        wrote_daily = 0
        wrote_intraday = 0
        skipped_untrusted_daily = 0
        skipped_untrusted_10m = 0
        skipped_untrusted_1h = 0
        hb = Heartbeat("91_update_coin_data_availability")

        now_ts = now_utc()
        start_10m = (now_ts - timedelta(days=INTRADAY_10M_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_10m = now_ts + timedelta(minutes=10)
        start_1h = (now_ts - timedelta(days=INTRADAY_HOURLY_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_1h = now_ts + timedelta(hours=1)

        for idx, coin in enumerate(coins, 1):
            if should_log_progress(idx, len(coins), default_every=LOG_EVERY):
                print(f"[{now_str()}] coin {idx}/{len(coins)} -> {coin.id}")
            hb.maybe(extra=f"coin={idx}/{len(coins)}")
            pending = deque()

            if RUN_DAILY:
                rows = list(
                    session.execute(
                        sel_daily,
                        [coin.id, daily_first, target_day],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )
                )
                have_any = set()
                have_price = set()
                have_vol = set()
                have_mcap = set()

                for r in rows:
                    if not _is_trusted_daily_row(r):
                        skipped_untrusted_daily += 1
                        continue
                    d = _to_date(getattr(r, "date", None))
                    if d is None:
                        continue
                    have_any.add(d)
                    px = getattr(r, "close", None)
                    if px is None:
                        px = getattr(r, "price_usd", None)
                    if px is not None:
                        have_price.add(d)
                    if getattr(r, "volume_24h", None) is not None:
                        have_vol.add(d)
                    if getattr(r, "market_cap", None) is not None:
                        have_mcap.add(d)

                have_any_sorted = sorted(have_any)
                ranges = _build_ranges(have_any_sorted)
                first_day = have_any_sorted[0] if have_any_sorted else None
                last_day = have_any_sorted[-1] if have_any_sorted else None
                expected_days = DAILY_WINDOW_DAYS

                session.execute(del_daily_ranges, [coin.id], timeout=REQUEST_TIMEOUT_SEC)
                for sday, eday in ranges:
                    enqueue_async(
                        session,
                        pending,
                        ins_daily_range,
                        [coin.id, sday, eday, to_cassandra_ts(now_ts)],
                        timeout=REQUEST_TIMEOUT_SEC,
                        max_in_flight=ASTRA_MAX_IN_FLIGHT,
                    )
                    wrote_ranges += 1

                enqueue_async(
                    session,
                    pending,
                    ins_daily_avail,
                    [
                        coin.id,
                        first_day,
                        last_day,
                        int(expected_days),
                        int(len(have_any)),
                        int(expected_days - len(have_any)),
                        int(len(have_price)),
                        int(expected_days - len(have_price)),
                        int(len(have_vol)),
                        int(expected_days - len(have_vol)),
                        int(len(have_mcap)),
                        int(expected_days - len(have_mcap)),
                        coin.symbol,
                        coin.name,
                        to_cassandra_ts(now_ts),
                        _open_range_start(have_any, target_day),
                    ],
                    timeout=REQUEST_TIMEOUT_SEC,
                    max_in_flight=ASTRA_MAX_IN_FLIGHT,
                )
                wrote_daily += 1

            if RUN_INTRADAY:
                # 10m bitmap coverage
                rows_10m = list(
                    session.execute(
                        sel_10m_range,
                        [coin.id, to_cassandra_ts(start_10m), to_cassandra_ts(end_10m)],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )
                )
                by_day_10m, skipped_10m = _scan_intraday(rows_10m, SLOTS_10M, _slot_idx_10m, _is_trusted_10m_row)
                skipped_untrusted_10m += skipped_10m
                session.execute(del_intraday, [coin.id, G_10M], timeout=REQUEST_TIMEOUT_SEC)
                for d, entry in by_day_10m.items():
                    bm = bytes(entry["bitmap"])
                    set_count = _popcount(bm)
                    if set_count <= 0:
                        continue
                    enqueue_async(
                        session,
                        pending,
                        ins_intraday_10m,
                        [coin.id, G_10M, d, bm, int(set_count), to_cassandra_ts(entry["first_seen"]), to_cassandra_ts(entry["last_seen"])],
                        timeout=REQUEST_TIMEOUT_SEC,
                        max_in_flight=ASTRA_MAX_IN_FLIGHT,
                    )
                    wrote_intraday += 1

                # hourly bitmap coverage
                rows_1h = list(
                    session.execute(
                        sel_hourly_range,
                        [coin.id, to_cassandra_ts(start_1h), to_cassandra_ts(end_1h)],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )
                )
                by_day_1h, skipped_1h = _scan_intraday(rows_1h, SLOTS_1H, _slot_idx_1h, _is_trusted_1h_row)
                skipped_untrusted_1h += skipped_1h
                session.execute(del_intraday, [coin.id, G_1H], timeout=REQUEST_TIMEOUT_SEC)
                for d, entry in by_day_1h.items():
                    bm = bytes(entry["bitmap"])
                    set_count = _popcount(bm)
                    if set_count <= 0:
                        continue
                    enqueue_async(
                        session,
                        pending,
                        ins_intraday_1h,
                        [coin.id, G_1H, d, bm, int(set_count), to_cassandra_ts(entry["first_seen"]), to_cassandra_ts(entry["last_seen"])],
                        timeout=REQUEST_TIMEOUT_SEC,
                        max_in_flight=ASTRA_MAX_IN_FLIGHT,
                    )
                    wrote_intraday += 1

            drain_async(pending)
            hb.maybe(extra=f"coin={idx}/{len(coins)} flush=done", force=True)

        print(
            f"[{now_str()}] Availability done. wrote_daily_rows={wrote_daily} "
            f"wrote_daily_ranges={wrote_ranges} wrote_intraday_rows={wrote_intraday} "
            f"skipped_untrusted_daily={skipped_untrusted_daily} "
            f"skipped_untrusted_10m={skipped_untrusted_10m} skipped_untrusted_1h={skipped_untrusted_1h}"
        )
        tracker.set_metric("rows_daily", wrote_daily)
        tracker.set_metric("rows_daily_ranges", wrote_ranges)
        tracker.set_metric("rows_intraday", wrote_intraday)
        tracker.set_metric("skipped_untrusted_daily", skipped_untrusted_daily)
        tracker.set_metric("skipped_untrusted_10m", skipped_untrusted_10m)
        tracker.set_metric("skipped_untrusted_1h", skipped_untrusted_1h)
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
