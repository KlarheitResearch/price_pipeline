#!/usr/bin/env python3
from __future__ import annotations

import os
from bisect import bisect_left, bisect_right
from datetime import timedelta

from cassandra.query import SimpleStatement

from common import (
    Heartbeat,
    PipelineHealthTracker,
    TABLE_10M,
    TABLE_LIVE,
    TABLE_ROLLING,
    cg_market_chart_range,
    connect_astra,
    extract_series_in_window,
    floor_10m,
    now_str,
    now_utc,
    should_log_progress,
    scope_label,
    select_coins_from_live_rows,
    to_cassandra_ts,
    to_utc,
)


SLOT_MINUTES = int(os.getenv("PP_SLOT_MINUTES", "10"))
SLOT_DELAY_SEC = int(os.getenv("PP_SLOT_DELAY_SEC", "90"))
SLOTS_BACKFILL = int(os.getenv("PP_SLOTS_BACKFILL", "4"))
REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
CC_HEAL_RECENT_SLOTS = int(os.getenv("PP_CC_HEAL_RECENT_SLOTS", "3"))
CC_HEAL_INTERPOLATE = os.getenv("PP_CC_HEAL_INTERPOLATE", "1") == "1"
try:
    CC_HEAL_MIN_POINTS = max(1, int(os.getenv("PP_CC_HEAL_MIN_POINTS", "2")))
except Exception:
    CC_HEAL_MIN_POINTS = 2


class SeriesAccessor:
    def __init__(self, points: list[tuple]):
        self.points = sorted(points, key=lambda x: x[0])
        self.times = [p[0].timestamp() for p in self.points]
        self.values = [float(p[1]) for p in self.points]

    def values_in_slot(self, start_ts, end_ts_exclusive) -> list[float]:
        if not self.points:
            return []
        s = start_ts.timestamp()
        e = end_ts_exclusive.timestamp()
        i = bisect_left(self.times, s)
        j = bisect_left(self.times, e)
        return self.values[i:j]

    def last_point_in_slot(self, start_ts, end_ts_exclusive):
        if not self.points:
            return None
        s = start_ts.timestamp()
        e = end_ts_exclusive.timestamp()
        i = bisect_left(self.times, s)
        j = bisect_left(self.times, e)
        if i >= j:
            return None
        idx = j - 1
        return self.points[idx]

    def last_value_before(self, ts):
        if not self.points:
            return None
        t = ts.timestamp()
        i = bisect_left(self.times, t) - 1
        if i < 0:
            return None
        return self.values[i]

    def interpolate_at(self, ts):
        if not self.points:
            return None
        t = ts.timestamp()
        i = bisect_right(self.times, t)
        left = i - 1
        right = i
        if left >= 0 and right < len(self.points):
            t0 = self.times[left]
            t1 = self.times[right]
            if t1 <= t0:
                return self.values[left]
            ratio = (t - t0) / (t1 - t0)
            return self.values[left] + (self.values[right] - self.values[left]) * ratio
        if left >= 0:
            return self.values[left]
        if right < len(self.points):
            return self.values[right]
        return None


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


def _source_quality(source: str | None, point_count: int | None) -> int:
    src = (source or "").strip().lower()
    try:
        pts = int(point_count) if point_count is not None else 0
    except Exception:
        pts = 0
    if src == "repair_api_points":
        return 400 + max(0, pts)
    if pts > 0:
        return 300 + pts
    if src == "repair_api_interp":
        return 220
    if src in ("carry_prev", "repair_carry"):
        return 120
    return 0


def _can_overwrite(existing_row, new_source: str, new_point_count: int) -> bool:
    if existing_row is None:
        return True
    old_src = getattr(existing_row, "candle_source", None)
    old_pts = getattr(existing_row, "point_count", None)
    old_q = _source_quality(old_src, old_pts)
    new_q = _source_quality(new_source, new_point_count)
    return new_q >= old_q


def _needs_heal_api(existing_row) -> bool:
    if existing_row is None:
        return True
    src = (getattr(existing_row, "candle_source", None) or "").strip().lower()
    pts = _f(getattr(existing_row, "point_count", None))
    pts_i = int(pts) if pts is not None else 0
    if src in ("", "carry_prev", "repair_carry", "repair_api_interp", "live_partial", "10m_partial"):
        return True
    return pts_i < CC_HEAL_MIN_POINTS


def main() -> None:
    hb = Heartbeat("CC_build_10m_intraday")
    session, cluster = connect_astra()
    tracker = PipelineHealthTracker(session, "CC_build_10m_intraday")
    tracker.set_metric("slots_backfill", SLOTS_BACKFILL)
    tracker.set_metric("heal_recent_slots", CC_HEAL_RECENT_SLOTS)
    tracker.set_metric("heal_min_points", CC_HEAL_MIN_POINTS)
    tracker.start()

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
        try:
            cluster.shutdown()
        except Exception:
            pass
        return
    tracker.set_metric("coins_scoped", len(coins))

    print(f"[{now_str()}] Building 10m intraday for scope={scope_label()} coins={len(coins)}")
    slots = last_n_slots_oldest_first(SLOTS_BACKFILL)
    tracker.set_metric("slots_count", len(slots))
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
    sel_existing_slots = session.prepare(
        f"""
        SELECT ts, candle_source, point_count
        FROM {TABLE_10M}
        WHERE id=? AND ts>=? AND ts<?
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
    skipped_downgrade = 0
    immediate_healed = 0
    heal_target_slots = 0
    heal_api_calls = 0
    try:
        for idx, coin in enumerate(coins, 1):
            if should_log_progress(idx, len(coins), default_every=25):
                print(f"[{now_str()}] coin {idx}/{len(coins)} -> {coin.id}")
            hb.maybe(extra=f"coin={idx}/{len(coins)}")
            existing_rows = list(
                session.execute(
                    sel_existing_slots,
                    [coin.id, to_cassandra_ts(slots[0][0]), to_cassandra_ts(slots[-1][1])],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
            )
            existing_by_slot = {to_utc(r.ts): r for r in existing_rows if getattr(r, "ts", None) is not None}

            recent_slot_count = min(len(slots), max(0, CC_HEAL_RECENT_SLOTS))
            heal_slots = [s for s, _e in slots[-recent_slot_count:]] if recent_slot_count > 0 else []
            heal_slot_set = set(heal_slots)
            heal_window_start = heal_slots[0] if heal_slots else None
            heal_window_end = slots[-1][1] if heal_slots else None
            api_loaded = False
            api_failed = False
            price_series = None
            mcap_series = None
            vol_series = None

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
                existing_row = existing_by_slot.get(slot_start)

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
                    if not _can_overwrite(existing_row, "live_points", len(price_points)):
                        skipped_downgrade += 1
                        continue
                    session.execute(ins_10m, vals, timeout=REQUEST_TIMEOUT_SEC)
                    wrote += 1
                    existing_by_slot[slot_start] = type(
                        "Row",
                        (),
                        {"candle_source": "live_points", "point_count": len(price_points)},
                    )()
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

                slot_needs_api = (
                    slot_start in heal_slot_set
                    and heal_window_start is not None
                    and heal_window_end is not None
                    and _needs_heal_api(existing_row)
                )
                if slot_needs_api:
                    heal_target_slots += 1
                    if not api_loaded and not api_failed:
                        try:
                            data = cg_market_chart_range(
                                coin.id,
                                heal_window_start - timedelta(hours=1),
                                heal_window_end,
                                vs_currency="usd",
                            )
                            price_series = SeriesAccessor(
                                extract_series_in_window(
                                    data.get("prices", []) or [],
                                    heal_window_start - timedelta(hours=1),
                                    heal_window_end,
                                )
                            )
                            mcap_series = SeriesAccessor(
                                extract_series_in_window(
                                    data.get("market_caps", []) or [],
                                    heal_window_start - timedelta(hours=1),
                                    heal_window_end,
                                )
                            )
                            vol_series = SeriesAccessor(
                                extract_series_in_window(
                                    data.get("total_volumes", []) or [],
                                    heal_window_start - timedelta(hours=1),
                                    heal_window_end,
                                )
                            )
                            api_loaded = True
                            heal_api_calls += 1
                        except Exception as exc:
                            api_failed = True
                            print(f"[{now_str()}] [warn] cc-heal API failed for {coin.id}: {exc}")

                    if api_loaded and price_series is not None:
                        api_vals = price_series.values_in_slot(slot_start, slot_end)
                        api_last = price_series.last_point_in_slot(slot_start, slot_end)
                        if api_vals:
                            api_first = api_vals[0]
                            api_close = api_vals[-1]
                            api_open = prev_close if prev_close is not None else api_first
                            api_high = max([api_open] + api_vals)
                            api_low = min([api_open] + api_vals)
                            api_ts = api_last[0] if api_last is not None else (slot_end - timedelta(seconds=1))

                            api_mcap = None
                            api_vol = None
                            if mcap_series is not None:
                                mcap_last = mcap_series.last_point_in_slot(slot_start, slot_end)
                                api_mcap = float(mcap_last[1]) if mcap_last is not None else None
                                if api_mcap is None and CC_HEAL_INTERPOLATE:
                                    api_mcap = mcap_series.interpolate_at(slot_start + timedelta(minutes=5))
                                if api_mcap is None:
                                    api_mcap = mcap_series.last_value_before(slot_end)
                            if vol_series is not None:
                                vol_last = vol_series.last_point_in_slot(slot_start, slot_end)
                                api_vol = float(vol_last[1]) if vol_last is not None else None
                                if api_vol is None and CC_HEAL_INTERPOLATE:
                                    api_vol = vol_series.interpolate_at(slot_start + timedelta(minutes=5))
                                if api_vol is None:
                                    api_vol = vol_series.last_value_before(slot_end)
                            if api_mcap is None:
                                api_mcap = _f(prev_row.market_cap)
                            if api_vol is None:
                                api_vol = _f(prev_row.volume_24h)

                            api_point_count = len(api_vals)
                            if _can_overwrite(existing_row, "repair_api_points", api_point_count):
                                session.execute(
                                    ins_10m,
                                    [
                                        coin.id,
                                        to_cassandra_ts(slot_start),
                                        (coin.symbol or "").upper(),
                                        coin.name,
                                        float(api_open),
                                        float(api_high),
                                        float(api_low),
                                        float(api_close),
                                        float(api_close),
                                        float(api_mcap) if api_mcap is not None else None,
                                        float(api_vol) if api_vol is not None else None,
                                        int(prev_row.market_cap_rank) if prev_row.market_cap_rank is not None else None,
                                        _f(prev_row.circulating_supply),
                                        _f(prev_row.total_supply),
                                        to_cassandra_ts(api_ts),
                                        "repair_api_points",
                                        api_point_count,
                                    ],
                                    timeout=REQUEST_TIMEOUT_SEC,
                                )
                                wrote += 1
                                immediate_healed += 1
                                existing_by_slot[slot_start] = type(
                                    "Row",
                                    (),
                                    {"candle_source": "repair_api_points", "point_count": api_point_count},
                                )()
                                continue
                            skipped_downgrade += 1
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
                if not _can_overwrite(existing_row, "carry_prev", 0):
                    skipped_downgrade += 1
                    continue
                session.execute(ins_10m, vals, timeout=REQUEST_TIMEOUT_SEC)
                wrote += 1
                existing_by_slot[slot_start] = type(
                    "Row",
                    (),
                    {"candle_source": "carry_prev", "point_count": 0},
                )()
        tracker.set_metric("rows_written", wrote)
        tracker.set_metric("rows_skipped", skipped)
        tracker.set_metric("rows_skipped_downgrade", skipped_downgrade)
        tracker.set_metric("rows_healed_api", immediate_healed)
        tracker.set_metric("heal_target_slots", heal_target_slots)
        tracker.set_metric("heal_api_calls", heal_api_calls)
        tracker.finish("success")
    except Exception as exc:
        tracker.finish("failed", f"{type(exc).__name__}: {exc}")
        raise
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

    print(
        f"[{now_str()}] Done. 10m wrote={wrote} skipped={skipped} "
        f"skipped_downgrade={skipped_downgrade} cc_healed_api={immediate_healed}."
    )


if __name__ == "__main__":
    main()
