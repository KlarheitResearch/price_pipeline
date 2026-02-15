#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import time
import pathlib
import subprocess
from collections import deque
from bisect import bisect_left, bisect_right
from datetime import timedelta
from types import SimpleNamespace

from cassandra.query import SimpleStatement

from common import (
    Heartbeat,
    TABLE_10M,
    TABLE_LIVE,
    UTC,
    cg_market_chart_range,
    connect_astra,
    drain_async,
    enqueue_async,
    extract_series_in_window,
    floor_10m,
    is_verbose,
    now_str,
    now_utc,
    should_log_progress,
    scope_label,
    select_coins_from_live_rows,
    to_cassandra_ts,
    to_utc,
    vprint,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
SLOT_DELAY_SEC = int(os.getenv("PP_SLOT_DELAY_SEC", "90"))
REPAIR_10M_HOURS = int(os.getenv("PP_REPAIR_10M_HOURS", "24"))
REPAIR_INTERPOLATE = os.getenv("PP_REPAIR_INTERPOLATE", "1") == "1"
REPAIR_REWRITE_NON_API = os.getenv("PP_REPAIR_REWRITE_NON_API", "1") == "1"

REPAIR_LOCK_ENABLED = os.getenv("PP_REPAIR_LOCK_ENABLED", "1") == "1"
_LOCK_DEFAULT = f"pp_repair_{os.getenv('PP_RANK_START', 'all')}_{os.getenv('PP_RANK_END', 'all')}"
REPAIR_LOCK_JOB = (os.getenv("PP_REPAIR_LOCK_JOB", _LOCK_DEFAULT) or _LOCK_DEFAULT).strip()
REPAIR_LOCK_BUCKET_HOURS = int(os.getenv("PP_REPAIR_LOCK_BUCKET_HOURS", "2"))
REPAIR_LOCK_TTL_SEC = int(os.getenv("PP_REPAIR_LOCK_TTL_SEC", str(2 * 3600 + 300)))
TABLE_JOB_LOCKS = os.getenv("PP_TABLE_JOB_LOCKS", "pp_job_locks")

RUN_HOURLY = os.getenv("PP_REPAIR_RUN_HOURLY", "1") == "1"
RUN_DAILY = os.getenv("PP_REPAIR_RUN_DAILY", "1") == "1"
RUN_MONTHLY = os.getenv("PP_REPAIR_RUN_MONTHLY", "0") == "1"
RUN_MCAP = os.getenv("PP_REPAIR_RUN_MCAP", "1") == "1"
ASTRA_MAX_IN_FLIGHT = int(os.getenv("PP_ASTRA_MAX_IN_FLIGHT", "64"))


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


def _lock_bucket_utc(bucket_hours: int) -> str:
    now = now_utc()
    h = (now.hour // bucket_hours) * bucket_hours
    return f"{now:%Y-%m-%d}T{h:02d}"


def _try_acquire_lock(session) -> bool:
    if not REPAIR_LOCK_ENABLED:
        return True
    bucket = _lock_bucket_utc(REPAIR_LOCK_BUCKET_HOURS)
    ps = session.prepare(
        f"""
        INSERT INTO {TABLE_JOB_LOCKS} (job, bucket, created_at)
        VALUES (?, ?, ?)
        IF NOT EXISTS
        USING TTL {int(REPAIR_LOCK_TTL_SEC)}
        """
    )
    res = session.execute(ps, [REPAIR_LOCK_JOB, bucket, to_cassandra_ts(now_utc())], timeout=REQUEST_TIMEOUT_SEC).one()
    applied = bool(getattr(res, "applied", True)) if res is not None else True
    if not applied:
        print(f"[{now_str()}] repair lock exists: job={REPAIR_LOCK_JOB} bucket={bucket}, skip run.")
    return applied


def _slot_range():
    guarded = now_utc() - timedelta(seconds=SLOT_DELAY_SEC)
    end = floor_10m(guarded) + timedelta(minutes=10)
    start = end - timedelta(hours=REPAIR_10M_HOURS)
    return start, end


def _all_slots(start_ts, end_ts_exclusive):
    out = []
    cur = start_ts
    while cur < end_ts_exclusive:
        out.append(cur)
        cur += timedelta(minutes=10)
    return out


def _run_followups(base_dir: pathlib.Path):
    steps = []
    if RUN_HOURLY:
        steps.append("DD_build_hourly_and_finalize.py")
    if RUN_DAILY:
        steps.append("EE_build_daily_and_finalize.py")
    if RUN_MONTHLY:
        steps.append("EG_build_monthly_from_daily.py")
    if RUN_MCAP:
        steps.append("HH_write_market_caps.py")
    for script in steps:
        path = base_dir / script
        print(f"[{now_str()}] follow-up -> {script}")
        subprocess.run([sys.executable, str(path)], check=True)


def main() -> None:
    if REPAIR_10M_HOURS <= 0:
        raise RuntimeError("PP_REPAIR_10M_HOURS must be > 0")

    window_start, window_end = _slot_range()
    print(
        f"[{now_str()}] Repair run start: scope={scope_label()} "
        f"window={window_start.isoformat()}..{window_end.isoformat()} "
        f"interpolate={REPAIR_INTERPOLATE} rewrite_non_api={REPAIR_REWRITE_NON_API} "
        f"verbose={is_verbose()}"
    )

    session, cluster = connect_astra()
    base_dir = pathlib.Path(__file__).resolve().parent
    total_inserted = 0
    try:
        if not _try_acquire_lock(session):
            return

        sel_live = SimpleStatement(
            f"""
            SELECT id, symbol, name, market_cap_rank
            FROM {TABLE_LIVE}
            """,
            fetch_size=2000,
        )
        live_rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC))
        coins = select_coins_from_live_rows(live_rows)
        if not coins:
            print(f"[{now_str()}] No scoped coins in {TABLE_LIVE} for {scope_label()}.")
            return

        sel_10m_range = session.prepare(
            f"""
            SELECT ts, open, high, low, close, price_usd,
                   market_cap, volume_24h, last_updated,
                   candle_source, point_count
            FROM {TABLE_10M}
            WHERE id=? AND ts>=? AND ts<?
            """
        )
        sel_prev_10m = session.prepare(
            f"""
            SELECT ts, close, market_cap, volume_24h
            FROM {TABLE_10M}
            WHERE id=? AND ts<?
            ORDER BY ts DESC LIMIT 1
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

        slots = _all_slots(window_start, window_end)
        hb = Heartbeat("92_repair_timeseries")
        total_missing = 0
        total_non_api_targets = 0
        total_skipped_downgrade = 0
        for idx, coin in enumerate(coins, 1):
            if should_log_progress(idx, len(coins), default_every=100):
                print(f"[{now_str()}] coin {idx}/{len(coins)} -> {coin.id}")
            hb.maybe(extra=f"coin={idx}/{len(coins)}")
            coin_started = time.monotonic()
            coin_skipped_dg_start = total_skipped_downgrade
            if is_verbose():
                vprint(
                    f"[repair] {coin.id}: loading existing 10m rows for "
                    f"{window_start.isoformat()}..{window_end.isoformat()}"
                )

            rows = list(
                session.execute(
                    sel_10m_range,
                    [coin.id, to_cassandra_ts(window_start), to_cassandra_ts(window_end)],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
            )
            existing = {to_utc(r.ts): r for r in rows if getattr(r, "ts", None) is not None}
            missing_slots = [s for s in slots if s not in existing]
            non_api_slots: list = []
            if REPAIR_REWRITE_NON_API:
                for s in slots:
                    row = existing.get(s)
                    if row is None:
                        continue
                    src = (getattr(row, "candle_source", None) or "").strip().lower()
                    pts = getattr(row, "point_count", None)
                    if src != "repair_api_points" or not isinstance(pts, int) or pts <= 0:
                        non_api_slots.append(s)

            target_slots = sorted(set(missing_slots + non_api_slots))
            if is_verbose():
                vprint(
                    f"[repair] {coin.id}: existing={len(existing)} missing={len(missing_slots)} "
                    f"non_api_targets={len(non_api_slots)} total_targets={len(target_slots)}"
                )
            if not target_slots:
                if is_verbose():
                    vprint(f"[repair] {coin.id}: no targets, skip API.")
                continue

            total_missing += len(missing_slots)
            total_non_api_targets += len(non_api_slots)

            prev = session.execute(
                sel_prev_10m,
                [coin.id, to_cassandra_ts(window_start)],
                timeout=REQUEST_TIMEOUT_SEC,
            ).one()
            last_close = float(prev.close) if prev and getattr(prev, "close", None) is not None else None
            last_mcap = float(prev.market_cap) if prev and getattr(prev, "market_cap", None) is not None else None
            last_vol = float(prev.volume_24h) if prev and getattr(prev, "volume_24h", None) is not None else None

            if is_verbose():
                vprint(
                    f"[repair] {coin.id}: requesting market_chart range "
                    f"{(window_start - timedelta(hours=1)).isoformat()}..{window_end.isoformat()}"
                )
            try:
                data = cg_market_chart_range(
                    coin.id,
                    window_start - timedelta(hours=1),
                    window_end,
                    vs_currency="usd",
                )
            except Exception as exc:
                print(f"[{now_str()}] [warn] repair API failed for {coin.id}: {exc}")
                continue

            price_series = SeriesAccessor(
                extract_series_in_window(
                    data.get("prices", []) or [],
                    window_start - timedelta(hours=1),
                    window_end,
                )
            )
            mcap_series = SeriesAccessor(
                extract_series_in_window(
                    data.get("market_caps", []) or [],
                    window_start - timedelta(hours=1),
                    window_end,
                )
            )
            vol_series = SeriesAccessor(
                extract_series_in_window(
                    data.get("total_volumes", []) or [],
                    window_start - timedelta(hours=1),
                    window_end,
                )
            )

            if is_verbose():
                vprint(
                    f"[repair] {coin.id}: api points prices={len(price_series.points)} "
                    f"mcap={len(mcap_series.points)} vol={len(vol_series.points)}"
                )

            coin_inserted = 0
            target_set = set(target_slots)
            pending = deque()
            for sidx, slot_start in enumerate(slots, 1):
                slot_end = slot_start + timedelta(minutes=10)
                existing_row = existing.get(slot_start)
                if sidx % 72 == 0:
                    hb.maybe(
                        extra=(
                            f"coin={idx}/{len(coins)} slot={sidx}/{len(slots)} "
                            f"inserted={coin_inserted}"
                        )
                    )
                    if is_verbose():
                        vprint(
                            f"[repair] {coin.id}: slot {sidx}/{len(slots)} "
                            f"inserted={coin_inserted}"
                        )
                if slot_start not in target_set:
                    if existing_row is not None:
                        ex_close = getattr(existing_row, "close", None)
                        ex_mcap = getattr(existing_row, "market_cap", None)
                        ex_vol = getattr(existing_row, "volume_24h", None)
                        if ex_close is not None:
                            last_close = float(ex_close)
                        if ex_mcap is not None:
                            last_mcap = float(ex_mcap)
                        if ex_vol is not None:
                            last_vol = float(ex_vol)
                    continue

                point_vals = price_series.values_in_slot(slot_start, slot_end)
                last_point = price_series.last_point_in_slot(slot_start, slot_end)

                source = None
                point_count = len(point_vals)
                if point_vals:
                    first_price = point_vals[0]
                    close = point_vals[-1]
                    open_price = last_close if last_close is not None else first_price
                    high = max([open_price] + point_vals)
                    low = min([open_price] + point_vals)
                    last_price_ts = last_point[0] if last_point else (slot_end - timedelta(seconds=1))
                    source = "repair_api_points"
                else:
                    interp = None
                    if REPAIR_INTERPOLATE:
                        interp = price_series.interpolate_at(slot_start + timedelta(minutes=5))
                    carry = interp if interp is not None else last_close
                    if carry is None:
                        carry = price_series.last_value_before(slot_end)
                    if carry is None:
                        continue
                    open_price = high = low = close = float(carry)
                    last_price_ts = slot_end - timedelta(seconds=1)
                    source = "repair_api_interp" if interp is not None else "repair_carry"
                    point_count = 0

                mcap_last = mcap_series.last_point_in_slot(slot_start, slot_end)
                vol_last = vol_series.last_point_in_slot(slot_start, slot_end)
                mcap = float(mcap_last[1]) if mcap_last is not None else None
                vol = float(vol_last[1]) if vol_last is not None else None
                if mcap is None:
                    mcap = mcap_series.interpolate_at(slot_start + timedelta(minutes=5))
                if vol is None:
                    vol = vol_series.interpolate_at(slot_start + timedelta(minutes=5))
                if mcap is None:
                    mcap = last_mcap
                if vol is None:
                    vol = last_vol

                if existing_row is not None:
                    old_q = _source_quality(
                        getattr(existing_row, "candle_source", None),
                        getattr(existing_row, "point_count", None),
                    )
                    new_q = _source_quality(source, point_count)
                    if new_q < old_q:
                        total_skipped_downgrade += 1
                        ex_close = getattr(existing_row, "close", None)
                        ex_mcap = getattr(existing_row, "market_cap", None)
                        ex_vol = getattr(existing_row, "volume_24h", None)
                        if ex_close is not None:
                            last_close = float(ex_close)
                        if ex_mcap is not None:
                            last_mcap = float(ex_mcap)
                        if ex_vol is not None:
                            last_vol = float(ex_vol)
                        continue

                enqueue_async(
                    session,
                    pending,
                    ins_10m,
                    [
                        coin.id,
                        to_cassandra_ts(slot_start),
                        (coin.symbol or "").upper(),
                        coin.name,
                        float(open_price),
                        float(high),
                        float(low),
                        float(close),
                        float(close),
                        float(mcap) if mcap is not None else None,
                        float(vol) if vol is not None else None,
                        int(coin.market_cap_rank) if isinstance(coin.market_cap_rank, int) else None,
                        None,
                        None,
                        to_cassandra_ts(last_price_ts),
                        source,
                        int(point_count),
                    ],
                    timeout=REQUEST_TIMEOUT_SEC,
                    max_in_flight=ASTRA_MAX_IN_FLIGHT,
                )
                coin_inserted += 1
                total_inserted += 1
                last_close = float(close)
                if mcap is not None:
                    last_mcap = float(mcap)
                if vol is not None:
                    last_vol = float(vol)
                existing[slot_start] = SimpleNamespace(
                    close=float(close),
                    market_cap=float(mcap) if mcap is not None else None,
                    volume_24h=float(vol) if vol is not None else None,
                    candle_source=source,
                    point_count=int(point_count),
                )

            drain_async(pending)
            hb.maybe(extra=f"coin={idx}/{len(coins)} flush=done", force=True)
            coin_elapsed = int(time.monotonic() - coin_started)
            coin_skipped_dg = total_skipped_downgrade - coin_skipped_dg_start
            print(
                f"[{now_str()}] coin_done {idx}/{len(coins)} {coin.id} "
                f"targets={len(target_slots)} inserted={coin_inserted} "
                f"missing={len(missing_slots)} non_api={len(non_api_slots)} "
                f"skipped_downgrade={coin_skipped_dg} elapsed={coin_elapsed}s"
            )

        print(
            f"[{now_str()}] Repair done. missing_slots={total_missing} "
            f"non_api_targets={total_non_api_targets} inserted={total_inserted} "
            f"skipped_downgrade={total_skipped_downgrade}"
        )
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

    if total_inserted > 0:
        _run_followups(base_dir)
    else:
        print(f"[{now_str()}] No repaired inserts, skipping follow-up steps.")


if __name__ == "__main__":
    main()
