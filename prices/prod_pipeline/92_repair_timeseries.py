#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import pathlib
import subprocess
from bisect import bisect_left, bisect_right
from datetime import timedelta

from cassandra.query import SimpleStatement

from common import (
    TABLE_10M,
    TABLE_LIVE,
    UTC,
    cg_market_chart_range,
    connect_astra,
    extract_series_in_window,
    floor_10m,
    now_str,
    now_utc,
    scope_label,
    select_coins_from_live_rows,
    to_cassandra_ts,
    to_utc,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
SLOT_DELAY_SEC = int(os.getenv("PP_SLOT_DELAY_SEC", "90"))
REPAIR_10M_HOURS = int(os.getenv("PP_REPAIR_10M_HOURS", "24"))
REPAIR_INTERPOLATE = os.getenv("PP_REPAIR_INTERPOLATE", "1") == "1"

REPAIR_LOCK_ENABLED = os.getenv("PP_REPAIR_LOCK_ENABLED", "1") == "1"
REPAIR_LOCK_JOB = (os.getenv("PP_REPAIR_LOCK_JOB", "pp_repair_10m") or "pp_repair_10m").strip()
REPAIR_LOCK_BUCKET_HOURS = int(os.getenv("PP_REPAIR_LOCK_BUCKET_HOURS", "2"))
REPAIR_LOCK_TTL_SEC = int(os.getenv("PP_REPAIR_LOCK_TTL_SEC", str(2 * 3600 + 300)))
TABLE_JOB_LOCKS = os.getenv("PP_TABLE_JOB_LOCKS", "pp_job_locks")

RUN_HOURLY = os.getenv("PP_REPAIR_RUN_HOURLY", "1") == "1"
RUN_DAILY = os.getenv("PP_REPAIR_RUN_DAILY", "1") == "1"
RUN_MONTHLY = os.getenv("PP_REPAIR_RUN_MONTHLY", "0") == "1"
RUN_MCAP = os.getenv("PP_REPAIR_RUN_MCAP", "1") == "1"


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
        f"interpolate={REPAIR_INTERPOLATE}"
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
                   market_cap, volume_24h, last_updated
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
        total_missing = 0
        for idx, coin in enumerate(coins, 1):
            if idx == 1 or idx % 100 == 0 or idx == len(coins):
                print(f"[{now_str()}] coin {idx}/{len(coins)} -> {coin.id}")

            rows = list(
                session.execute(
                    sel_10m_range,
                    [coin.id, to_cassandra_ts(window_start), to_cassandra_ts(window_end)],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
            )
            existing = {to_utc(r.ts): r for r in rows if getattr(r, "ts", None) is not None}
            missing_slots = [s for s in slots if s not in existing]
            if not missing_slots:
                continue

            total_missing += len(missing_slots)

            prev = session.execute(
                sel_prev_10m,
                [coin.id, to_cassandra_ts(window_start)],
                timeout=REQUEST_TIMEOUT_SEC,
            ).one()
            last_close = float(prev.close) if prev and getattr(prev, "close", None) is not None else None
            last_mcap = float(prev.market_cap) if prev and getattr(prev, "market_cap", None) is not None else None
            last_vol = float(prev.volume_24h) if prev and getattr(prev, "volume_24h", None) is not None else None

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

            missing_slots.sort()
            coin_inserted = 0
            for slot_start in missing_slots:
                slot_end = slot_start + timedelta(minutes=10)
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

                session.execute(
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
                )
                coin_inserted += 1
                total_inserted += 1
                last_close = float(close)
                if mcap is not None:
                    last_mcap = float(mcap)
                if vol is not None:
                    last_vol = float(vol)

            if coin_inserted:
                print(
                    f"[{now_str()}] repaired {coin_inserted}/{len(missing_slots)} "
                    f"slots for {coin.id}"
                )

        print(
            f"[{now_str()}] Repair done. missing_slots={total_missing} "
            f"inserted={total_inserted}"
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
