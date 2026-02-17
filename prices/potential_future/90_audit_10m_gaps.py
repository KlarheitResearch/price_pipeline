#!/usr/bin/env python3
from __future__ import annotations

import os
from datetime import datetime, timedelta
from collections import defaultdict

from cassandra.query import SimpleStatement

from prices.potential_future.common import (
    Heartbeat,
    PipelineHealthTracker,
    TABLE_10M,
    TABLE_LIVE,
    UTC,
    connect_astra,
    now_str,
    now_utc,
    should_log_progress,
    scope_label,
    select_coins_from_live_rows,
    to_cassandra_ts,
    to_utc,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
AUDIT_WINDOW_DAYS = int(os.getenv("PP_AUDIT_WINDOW_DAYS", "7"))
ONLY_WITH_GAPS = os.getenv("PP_AUDIT_ONLY_WITH_GAPS", "1") == "1"
SHOW_SLOT_DETAILS = os.getenv("PP_AUDIT_SHOW_SLOT_DETAILS", "0") == "1"
MAX_SLOT_DETAIL = int(os.getenv("PP_AUDIT_MAX_SLOT_DETAIL", "12"))


def _slot_start(ts: datetime) -> datetime:
    ts = to_utc(ts)
    return ts.replace(minute=(ts.minute // 10) * 10, second=0, microsecond=0)


def _day_start(d) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def _all_slots_for_day(d) -> list[datetime]:
    start = _day_start(d)
    return [start + timedelta(minutes=10 * i) for i in range(24 * 6)]


def main() -> None:
    if AUDIT_WINDOW_DAYS <= 0:
        raise RuntimeError("PP_AUDIT_WINDOW_DAYS must be > 0")

    session, cluster = connect_astra()
    tracker = PipelineHealthTracker(session, "90_audit_10m_gaps")
    tracker.set_metric("audit_window_days", AUDIT_WINDOW_DAYS)
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

        now = now_utc()
        end_excl = datetime(now.year, now.month, now.day, tzinfo=UTC) + timedelta(days=1)
        last_day = end_excl.date() - timedelta(days=1)
        first_day = last_day - timedelta(days=AUDIT_WINDOW_DAYS - 1)
        start_dt = _day_start(first_day)

        want_days = [first_day + timedelta(days=i) for i in range(AUDIT_WINDOW_DAYS)]
        sel_10m = session.prepare(
            f"""
            SELECT ts
            FROM {TABLE_10M}
            WHERE id=? AND ts>=? AND ts<?
            """
        )

        print(
            f"[{now_str()}] 10m audit: scope={scope_label()} coins={len(coins)} "
            f"window={first_day}..{last_day} days={AUDIT_WINDOW_DAYS}"
        )

        total_with_gaps = 0
        hb = Heartbeat("90_audit_10m_gaps")
        worst: list[tuple[int, str, str]] = []
        for i, coin in enumerate(coins, 1):
            if should_log_progress(i, len(coins), default_every=50):
                print(f"[{now_str()}] coin {i}/{len(coins)} -> {coin.id}")
            hb.maybe(extra=f"coin={i}/{len(coins)}")
            rows = session.execute(
                sel_10m,
                [coin.id, to_cassandra_ts(start_dt), to_cassandra_ts(end_excl)],
                timeout=REQUEST_TIMEOUT_SEC,
            )
            slots_by_day: dict = defaultdict(set)
            for r in rows:
                ts = to_utc(getattr(r, "ts", None))
                if ts is None:
                    continue
                d = ts.date()
                if first_day <= d <= last_day:
                    slots_by_day[d].add(_slot_start(ts))

            missing_days = []
            missing_slots_total = 0
            day_slot_details: list[tuple[str, int, list[str]]] = []

            for d in want_days:
                have = slots_by_day.get(d, set())
                if not have:
                    missing_days.append(d.isoformat())
                    missing_slots_total += 24 * 6
                    if SHOW_SLOT_DETAILS:
                        expected = _all_slots_for_day(d)
                        detail = [x.isoformat() for x in expected[:MAX_SLOT_DETAIL]]
                        day_slot_details.append((d.isoformat(), 24 * 6, detail))
                    continue

                if len(have) < 24 * 6:
                    missing = sorted(set(_all_slots_for_day(d)) - have)
                    missing_slots_total += len(missing)
                    if SHOW_SLOT_DETAILS:
                        detail = [x.isoformat() for x in missing[:MAX_SLOT_DETAIL]]
                        day_slot_details.append((d.isoformat(), len(missing), detail))

            gap_score = len(missing_days) * 1000 + missing_slots_total
            if missing_days or missing_slots_total > 0:
                total_with_gaps += 1
                worst.append((gap_score, coin.symbol or "?", coin.id))
                print(
                    f"[{now_str()}] [{i}/{len(coins)}] {coin.symbol:<8} {coin.id:<24} "
                    f"rank={coin.market_cap_rank} missing_days={len(missing_days)} "
                    f"missing_slots={missing_slots_total}"
                )
                if missing_days:
                    print(f"[{now_str()}]   missing_day_keys={','.join(missing_days)}")
                if day_slot_details:
                    for day_key, miss_cnt, detail in day_slot_details:
                        print(
                            f"[{now_str()}]   day={day_key} missing_slots={miss_cnt} "
                            f"sample={','.join(detail)}"
                        )
            elif not ONLY_WITH_GAPS:
                print(
                    f"[{now_str()}] [{i}/{len(coins)}] {coin.symbol:<8} {coin.id:<24} "
                    f"rank={coin.market_cap_rank} OK"
                )

        worst.sort(reverse=True)
        print(
            f"[{now_str()}] Audit done. coins_with_gaps={total_with_gaps}/{len(coins)} "
            f"top_gap_offenders={','.join([f'{sym}:{cid}' for _s, sym, cid in worst[:5]])}"
        )
        tracker.set_metric("coins_with_gaps", total_with_gaps)
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
