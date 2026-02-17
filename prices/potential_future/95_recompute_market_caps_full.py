#!/usr/bin/env python3
from __future__ import annotations

import os
from datetime import datetime, timezone

from cassandra.query import SimpleStatement

from prices.potential_future.common import (
    Heartbeat,
    TABLE_MCAP_10M,
    TABLE_MCAP_DAILY,
    TABLE_MCAP_HOURLY,
    connect_astra,
    now_str,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
FULL_TOP_N = int(os.getenv("PP_FULL_MCAP_TOP_N", "1000"))
FULL_10M_DAYS = int(os.getenv("PP_FULL_MCAP_10M_DAYS", "7"))
FULL_HOURLY_DAYS = int(os.getenv("PP_FULL_MCAP_HOURLY_DAYS", "30"))
FULL_DAILY_START = (os.getenv("PP_FULL_MCAP_DAILY_START", "2013-04-28") or "2013-04-28").strip()


def _days_since(start_iso: str) -> int:
    start = datetime.strptime(start_iso, "%Y-%m-%d").date()
    today = datetime.now(timezone.utc).date()
    return max(1, (today - start).days + 1)


def main() -> None:
    hb = Heartbeat("95_recompute_market_caps_full")
    daily_days = _days_since(FULL_DAILY_START)
    slots_10m = max(1, FULL_10M_DAYS * 24 * 6)
    hours = max(1, FULL_HOURLY_DAYS * 24)

    print(
        f"[{now_str()}] Full market-cap recompute start: "
        f"top_n={FULL_TOP_N} 10m_days={FULL_10M_DAYS} hourly_days={FULL_HOURLY_DAYS} "
        f"daily_start={FULL_DAILY_START} daily_days={daily_days}"
    )

    session, cluster = connect_astra()
    try:
        hb.maybe(extra="phase=truncate_start", force=True)
        session.execute(SimpleStatement(f"TRUNCATE {TABLE_MCAP_10M}"), timeout=REQUEST_TIMEOUT_SEC)
        session.execute(SimpleStatement(f"TRUNCATE {TABLE_MCAP_HOURLY}"), timeout=REQUEST_TIMEOUT_SEC)
        session.execute(SimpleStatement(f"TRUNCATE {TABLE_MCAP_DAILY}"), timeout=REQUEST_TIMEOUT_SEC)
        print(f"[{now_str()}] Truncated {TABLE_MCAP_10M}, {TABLE_MCAP_HOURLY}, {TABLE_MCAP_DAILY}")
        hb.maybe(extra="phase=truncate_done", force=True)
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

    os.environ["PP_TOP_N"] = str(FULL_TOP_N)
    os.environ["PP_MCAP_10M_SLOTS"] = str(slots_10m)
    os.environ["PP_MCAP_HOURS"] = str(hours)
    os.environ["PP_MCAP_DAYS"] = str(daily_days)
    os.environ["REQUEST_TIMEOUT_SEC"] = str(REQUEST_TIMEOUT_SEC)

    from HH_write_market_caps import main as run_recompute

    run_recompute()
    print(f"[{now_str()}] Full market-cap recompute finished.")


if __name__ == "__main__":
    main()
