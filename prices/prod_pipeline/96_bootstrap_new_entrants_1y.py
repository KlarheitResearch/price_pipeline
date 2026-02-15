#!/usr/bin/env python3
from __future__ import annotations

import os
import pathlib
import subprocess
import sys
from collections import deque
from datetime import date, datetime, timedelta

from cassandra.query import SimpleStatement

from common import (
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
BOOTSTRAP_DAYS = int(os.getenv("PP_BOOTSTRAP_DAYS", "365"))
BOOTSTRAP_MAX_COINS = int(os.getenv("PP_BOOTSTRAP_MAX_COINS", "20"))
BOOTSTRAP_OVERWRITE = os.getenv("PP_BOOTSTRAP_OVERWRITE", "0") == "1"
BOOTSTRAP_RUN_MONTHLY_BACKFILL = os.getenv("PP_BOOTSTRAP_RUN_MONTHLY_BACKFILL", "0") == "1"
LOG_EVERY = int(os.getenv("PP_BOOTSTRAP_LOG_EVERY", "50"))


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


def _midnight_utc(day_key: date) -> datetime:
    return datetime(day_key.year, day_key.month, day_key.day, tzinfo=UTC)


def _build_daily_price_map(prices: list[tuple[datetime, float]], start_day: date, end_day: date) -> dict[date, list[tuple[datetime, float]]]:
    out: dict[date, list[tuple[datetime, float]]] = {}
    for ts, value in prices:
        day_key = ts.date()
        if start_day <= day_key <= end_day:
            out.setdefault(day_key, []).append((ts, float(value)))
    for day_key in list(out.keys()):
        out[day_key].sort(key=lambda x: x[0])
    return out


def _build_last_value_map(points: list[tuple[datetime, float]], start_day: date, end_day: date) -> dict[date, tuple[float, datetime]]:
    out: dict[date, tuple[float, datetime]] = {}
    for ts, value in points:
        day_key = ts.date()
        if start_day <= day_key <= end_day:
            out[day_key] = (float(value), ts)
    return out


def main() -> None:
    if BOOTSTRAP_DAYS <= 0:
        raise RuntimeError("PP_BOOTSTRAP_DAYS must be > 0")
    if BOOTSTRAP_MAX_COINS <= 0:
        raise RuntimeError("PP_BOOTSTRAP_MAX_COINS must be > 0")

    end_day = now_utc().date() - timedelta(days=1)  # closed days only
    start_day = end_day - timedelta(days=BOOTSTRAP_DAYS - 1)
    start_ts = _midnight_utc(start_day)
    end_ts_exclusive = _midnight_utc(end_day + timedelta(days=1))

    print(
        f"[{now_str()}] Entrant bootstrap start: scope={scope_label()} "
        f"window={start_day}..{end_day} max_coins={BOOTSTRAP_MAX_COINS} overwrite={BOOTSTRAP_OVERWRITE}"
    )

    session, cluster = connect_astra()
    tracker = PipelineHealthTracker(session, "96_bootstrap_new_entrants_1y")
    tracker.set_metric("bootstrap_days", BOOTSTRAP_DAYS)
    tracker.set_metric("bootstrap_max_coins", BOOTSTRAP_MAX_COINS)
    tracker.set_metric("bootstrap_overwrite", 1 if BOOTSTRAP_OVERWRITE else 0)
    tracker.start()
    total_inserted = 0
    bootstrapped_coins = 0
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

        sel_first = session.prepare(
            f"""
            SELECT date
            FROM {TABLE_DAILY}
            WHERE id=?
            LIMIT 1
            """
        )
        sel_last = session.prepare(
            f"""
            SELECT date
            FROM {TABLE_DAILY}
            WHERE id=?
            ORDER BY date DESC
            LIMIT 1
            """
        )
        sel_existing_range = session.prepare(
            f"""
            SELECT date
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

        candidates = []
        for coin in coins:
            first_row = session.execute(sel_first, [coin.id], timeout=REQUEST_TIMEOUT_SEC).one()
            first_day = _to_date(getattr(first_row, "date", None)) if first_row else None
            if first_day is None or first_day > start_day:
                candidates.append(coin)

        if not candidates:
            print(f"[{now_str()}] No entrant bootstrap candidates found.")
            tracker.mark_noop()
            tracker.set_metric("candidates_total", 0)
            tracker.finish("noop")
            return

        candidates.sort(key=lambda c: (c.market_cap_rank if isinstance(c.market_cap_rank, int) else 10**9, c.id))
        to_process = candidates[:BOOTSTRAP_MAX_COINS]
        tracker.set_metric("candidates_total", len(candidates))
        tracker.set_metric("candidates_processed", len(to_process))

        print(f"[{now_str()}] Candidates={len(candidates)} processing={len(to_process)}")
        hb = Heartbeat("96_bootstrap_new_entrants_1y")
        for idx, coin in enumerate(to_process, 1):
            if should_log_progress(idx, len(to_process), default_every=LOG_EVERY):
                print(f"[{now_str()}] coin {idx}/{len(to_process)} -> {coin.id}")
            hb.maybe(extra=f"coin={idx}/{len(to_process)}")

            existing_rows = list(
                session.execute(
                    sel_existing_range,
                    [coin.id, start_day, end_day],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
            )
            existing_days = {_to_date(getattr(r, "date", None)) for r in existing_rows}
            existing_days.discard(None)

            first_row = session.execute(sel_first, [coin.id], timeout=REQUEST_TIMEOUT_SEC).one()
            last_row = session.execute(sel_last, [coin.id], timeout=REQUEST_TIMEOUT_SEC).one()
            first_day = _to_date(getattr(first_row, "date", None)) if first_row else None
            last_day = _to_date(getattr(last_row, "date", None)) if last_row else None
            print(
                f"[{now_str()}] {coin.id} coverage first={first_day} last={last_day} "
                f"have_in_window={len(existing_days)}"
            )

            try:
                data = cg_market_chart_range(coin.id, start_ts, end_ts_exclusive, vs_currency="usd")
            except Exception as exc:
                print(f"[{now_str()}] [warn] bootstrap API failed for {coin.id}: {exc}")
                continue

            prices = extract_series_in_window(data.get("prices", []) or [], start_ts, end_ts_exclusive)
            if not prices:
                print(f"[{now_str()}] [warn] no API prices for {coin.id} in bootstrap window.")
                continue

            market_caps = extract_series_in_window(data.get("market_caps", []) or [], start_ts, end_ts_exclusive)
            volumes = extract_series_in_window(data.get("total_volumes", []) or [], start_ts, end_ts_exclusive)

            by_day_prices = _build_daily_price_map(prices, start_day, end_day)
            by_day_mcap = _build_last_value_map(market_caps, start_day, end_day)
            by_day_vol = _build_last_value_map(volumes, start_day, end_day)

            coin_inserted = 0
            pending = deque()
            day_key = start_day
            while day_key <= end_day:
                if not BOOTSTRAP_OVERWRITE and day_key in existing_days:
                    day_key += timedelta(days=1)
                    continue

                day_prices = by_day_prices.get(day_key) or []
                if not day_prices:
                    day_key += timedelta(days=1)
                    continue

                vals = [v for _, v in day_prices]
                open_price = float(vals[0])
                close_price = float(vals[-1])
                high_price = float(max(vals))
                low_price = float(min(vals))
                last_price_ts = day_prices[-1][0]

                mcap_pair = by_day_mcap.get(day_key)
                vol_pair = by_day_vol.get(day_key)
                mcap = float(mcap_pair[0]) if mcap_pair is not None else None
                vol = float(vol_pair[0]) if vol_pair is not None else None

                enqueue_async(
                    session,
                    pending,
                    ins_daily,
                    [
                        coin.id,
                        day_key,
                        (coin.symbol or "").upper(),
                        coin.name,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        close_price,
                        mcap,
                        vol,
                        int(coin.market_cap_rank) if isinstance(coin.market_cap_rank, int) else None,
                        None,
                        None,
                        "cg_daily_bootstrap",
                        int(len(vals)),
                        to_cassandra_ts(last_price_ts),
                    ],
                    timeout=REQUEST_TIMEOUT_SEC,
                    max_in_flight=ASTRA_MAX_IN_FLIGHT,
                )
                coin_inserted += 1
                total_inserted += 1
                day_key += timedelta(days=1)
            drain_async(pending)
            hb.maybe(extra=f"coin={idx}/{len(to_process)} flush=done", force=True)

            if coin_inserted > 0:
                bootstrapped_coins += 1
                print(f"[{now_str()}] {coin.id} bootstrap inserted days={coin_inserted}")

        print(
            f"[{now_str()}] Entrant bootstrap done. "
            f"bootstrapped_coins={bootstrapped_coins} inserted_days={total_inserted}"
        )
        tracker.set_metric("bootstrapped_coins", bootstrapped_coins)
        tracker.set_metric("inserted_days", total_inserted)
        tracker.set_metric("monthly_followup_planned", 1 if (total_inserted > 0 and BOOTSTRAP_RUN_MONTHLY_BACKFILL) else 0)
        tracker.finish("success")
    except Exception as exc:
        tracker.finish("failed", f"{type(exc).__name__}: {exc}")
        raise
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

    if total_inserted > 0 and BOOTSTRAP_RUN_MONTHLY_BACKFILL:
        base_dir = pathlib.Path(__file__).resolve().parent
        script = base_dir / "93_backfill_monthly_from_daily.py"
        os.environ.setdefault("PP_BACKFILL_INCLUDE_ALL_DAILY_IDS", "0")
        print(f"[{now_str()}] follow-up -> {script.name}")
        subprocess.run([sys.executable, str(script)], check=True)


if __name__ == "__main__":
    main()
