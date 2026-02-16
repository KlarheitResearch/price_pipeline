#!/usr/bin/env python3
from __future__ import annotations

import os
from datetime import timedelta
from types import SimpleNamespace

from cassandra.query import SimpleStatement

from common import (
    Heartbeat,
    PipelineHealthTracker,
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
    should_log_progress,
    scope_label,
    select_coins_from_live_rows,
    to_cassandra_ts,
    to_utc,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
SLOT_DELAY_SEC = int(os.getenv("PP_SLOT_DELAY_SEC", "90"))
HOURLY_FINALIZE_LOOKBACK = int(os.getenv("PP_HOURLY_FINALIZE_LOOKBACK", "2"))
DD_API_MODE = (os.getenv("PP_DD_API_MODE", "missing_only") or "missing_only").strip().lower()
if DD_API_MODE not in {"off", "missing_only", "always"}:
    DD_API_MODE = "missing_only"
try:
    DD_MIN_POINTS_FOR_FINAL = max(1, int(os.getenv("PP_DD_MIN_POINTS_FOR_FINAL", "4")))
except Exception:
    DD_MIN_POINTS_FOR_FINAL = 4

_HOURLY_WEAK_SOURCES = {
    "",
    "carry_prev",
    "repair_carry",
    "repair_api_interp",
    "live_partial",
    "10m_partial",
}


def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _i(x, default=0):
    try:
        return int(x) if x is not None else default
    except Exception:
        return default


def _source(existing_row) -> str:
    return (getattr(existing_row, "candle_source", None) or "").strip().lower() if existing_row is not None else ""


def _needs_provisional_rebuild(existing_row) -> bool:
    if existing_row is None:
        return True
    src = _source(existing_row)
    if src == "cg_hourly_final":
        return False
    if src in _HOURLY_WEAK_SOURCES:
        return True
    pts = _i(getattr(existing_row, "point_count", None), 0)
    return pts < DD_MIN_POINTS_FOR_FINAL


def _needs_api_finalize(existing_row) -> bool:
    if DD_API_MODE == "off":
        return False
    if existing_row is None:
        return True
    src = _source(existing_row)
    if src == "cg_hourly_final":
        return False
    if DD_API_MODE == "always":
        return True
    if src in _HOURLY_WEAK_SOURCES:
        return True
    pts = _i(getattr(existing_row, "point_count", None), 0)
    return pts < DD_MIN_POINTS_FOR_FINAL


def _pick_price_from_row(row):
    for field in ("open", "close", "high", "low", "price_usd"):
        v = _f(getattr(row, field, None))
        if v is not None:
            return v
    return None


def build_hour_from_10m(rows, slot_start, slot_end):
    if not rows:
        return None
    ordered = sorted(rows, key=lambda r: to_utc(getattr(r, "ts", None)) or slot_start)
    last = ordered[-1]

    open_price = None
    for r in ordered:
        open_price = _pick_price_from_row(r)
        if open_price is not None:
            break

    close = None
    for r in reversed(ordered):
        close = _f(getattr(r, "close", None))
        if close is None:
            close = _pick_price_from_row(r)
        if close is not None:
            break

    if open_price is None and close is None:
        return None
    if open_price is None:
        open_price = close
    if close is None:
        close = open_price

    highs = []
    lows = []
    points = 0
    last_updated = None
    for r in ordered:
        h = _f(getattr(r, "high", None))
        l = _f(getattr(r, "low", None))
        c = _f(getattr(r, "close", None))
        if c is None:
            c = _pick_price_from_row(r)
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

    safe_highs = [v for v in ([open_price] + highs + [close]) if v is not None]
    safe_lows = [v for v in ([open_price] + lows + [close]) if v is not None]
    if not safe_highs or not safe_lows:
        return None
    high = max(safe_highs)
    low = min(safe_lows)
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
    hb = Heartbeat("DD_build_hourly_and_finalize")
    session, cluster = connect_astra()
    tracker = PipelineHealthTracker(session, "DD_build_hourly_and_finalize")
    tracker.set_metric("hourly_finalize_lookback", HOURLY_FINALIZE_LOOKBACK)
    tracker.set_metric("dd_api_mode", DD_API_MODE)
    tracker.set_metric("dd_min_points_for_final", DD_MIN_POINTS_FOR_FINAL)
    tracker.start()
    wrote_partial = 0
    wrote_provisional = 0
    wrote_final = 0
    api_target_slots = 0
    api_calls = 0
    try:
        sel_live = SimpleStatement(
            f"SELECT id, symbol, name, market_cap_rank FROM {TABLE_LIVE}",
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

        sel_10m = session.prepare(
            f"""
            SELECT ts, open, high, low, close, price_usd,
                   market_cap, volume_24h, market_cap_rank,
                   circulating_supply, total_supply,
                   point_count, last_updated
            FROM {TABLE_10M}
            WHERE id=? AND ts>=? AND ts<?
            """
        )
        sel_hourly_one = session.prepare(
            f"""
            SELECT candle_source, point_count
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

        for idx, coin in enumerate(coins, 1):
            if should_log_progress(idx, len(coins), default_every=25):
                print(f"[{now_str()}] coin {idx}/{len(coins)} -> {coin.id}")
            hb.maybe(extra=f"coin={idx}/{len(coins)}")
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
                    wrote_partial += 1
                    print(f"[{now_str()}] {coin.id} upserted partial hour {curr_hour_start}")

            # 2) Closed hour(s): detect unresolved buckets first, then do targeted work.
            closed_hours = []
            for k in range(1, HOURLY_FINALIZE_LOOKBACK + 1):
                hour_start = curr_hour_start - timedelta(hours=k)
                hour_end = hour_start + timedelta(hours=1)
                existing = session.execute(
                    sel_hourly_one,
                    [coin.id, to_cassandra_ts(hour_start)],
                    timeout=REQUEST_TIMEOUT_SEC,
                ).one()
                closed_hours.append(
                    {
                        "hour_start": hour_start,
                        "hour_end": hour_end,
                        "existing": existing,
                        "effective": existing,
                    }
                )

            for item in closed_hours:
                hour_start = item["hour_start"]
                hour_end = item["hour_end"]
                existing = item["existing"]
                if _source(existing) == "cg_hourly_final":
                    # Keep finalized candle stable: never overwrite with 10m reconstruction.
                    continue

                if _needs_provisional_rebuild(existing):
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
                        wrote_provisional += 1
                        item["effective"] = SimpleNamespace(
                            candle_source="10m_final",
                            point_count=int(candle["point_count"]),
                        )

            api_targets = [item for item in closed_hours if _needs_api_finalize(item["effective"])]
            api_target_slots += len(api_targets)
            api_finalize_data = None
            if api_targets:
                api_window_start = min(item["hour_start"] for item in api_targets)
                try:
                    api_finalize_data = cg_market_chart_range(coin.id, api_window_start, curr_hour_start, vs_currency="usd")
                    api_calls += 1
                except Exception as exc:
                    print(f"[{now_str()}] [warn] API finalize preload failed for {coin.id}: {exc}")

            for item in api_targets:
                hour_start = item["hour_start"]
                hour_end = item["hour_end"]
                if api_finalize_data is None:
                    continue

                prices = extract_series_in_window(api_finalize_data.get("prices", []) or [], hour_start, hour_end)
                if not prices:
                    continue
                price_values = [v for _, v in prices]
                open_price = price_values[0]
                close = price_values[-1]
                high = max(price_values)
                low = min(price_values)
                last_price_ts = prices[-1][0]
                mcap, _ = last_value_in_window(api_finalize_data.get("market_caps", []) or [], hour_start, hour_end)
                vol, _ = last_value_in_window(api_finalize_data.get("total_volumes", []) or [], hour_start, hour_end)

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
                wrote_final += 1
                print(f"[{now_str()}] {coin.id} finalized hour via API: {hour_start}")
        tracker.set_metric("rows_partial", wrote_partial)
        tracker.set_metric("rows_provisional", wrote_provisional)
        tracker.set_metric("rows_final_api", wrote_final)
        tracker.set_metric("api_target_slots", api_target_slots)
        tracker.set_metric("api_calls", api_calls)
        tracker.finish("success")
    except Exception as exc:
        tracker.finish("failed", f"{type(exc).__name__}: {exc}")
        raise
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

    print(f"[{now_str()}] Done.")


if __name__ == "__main__":
    main()
