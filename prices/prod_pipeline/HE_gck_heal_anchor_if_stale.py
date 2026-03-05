#!/usr/bin/env python3
"""
HE_gck_heal_anchor_if_stale.py

Checks anchor assets for stale/missing intraday coverage and runs
GM_gck_manual_repair_intraday.py only when healing is needed.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from cassandra.cluster import Cluster, Session

from astra_connect.connect import AstraConfig, get_session

AstraConfig.from_env()

UTC = timezone.utc


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")


def parse_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def parse_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        val = int(raw.strip())
    except Exception:
        return default
    if val < minimum:
        return minimum
    return val


def to_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def to_naive_utc(dt: datetime) -> datetime:
    dt = to_utc(dt) or now_utc()
    return dt.replace(tzinfo=None)


def latest_ts(session: Session, table: str, coin_id: str) -> datetime | None:
    stmt = session.prepare(f"SELECT ts FROM {table} WHERE id=? ORDER BY ts DESC LIMIT 1")
    row = session.execute(stmt, [coin_id], timeout=30).one()
    return to_utc(getattr(row, "ts", None)) if row else None


def count_10m_rows(
    session: Session,
    table_10m: str,
    coin_id: str,
    start_utc: datetime,
    end_utc_excl: datetime,
) -> int:
    stmt = session.prepare(f"SELECT ts FROM {table_10m} WHERE id=? AND ts>=? AND ts<?")
    count = 0
    for _ in session.execute(
        stmt,
        [coin_id, to_naive_utc(start_utc), to_naive_utc(end_utc_excl)],
        timeout=60,
    ):
        count += 1
    return count


def iso_z(dt: datetime) -> str:
    return to_utc(dt).isoformat().replace("+00:00", "Z")


def run_repair(stale_ids: list[str]) -> int:
    rank_start = parse_int("HEAL_RANK_START", 1, 1)
    rank_end = parse_int("HEAL_RANK_END", 1000, 1)
    if rank_end < rank_start:
        rank_end = rank_start

    lookback_hours = parse_int("HEAL_LOOKBACK_HOURS", 18, 1)
    granularity = (os.getenv("HEAL_GRANULARITY", "both").strip().lower() or "both")
    if granularity not in {"10m", "hourly", "both"}:
        granularity = "both"

    overwrite_existing = parse_bool("HEAL_OVERWRITE_EXISTING", True)
    dry_run = parse_bool("HEAL_DRY_RUN", False)

    end_utc = now_utc()
    start_utc = end_utc - timedelta(hours=lookback_hours)

    cmd = [
        sys.executable,
        "prices/prod_pipeline/GM_gck_manual_repair_intraday.py",
        "--rank-start",
        str(rank_start),
        "--rank-end",
        str(rank_end),
        "--coin-ids",
        ",".join(stale_ids),
        "--from-utc",
        iso_z(start_utc),
        "--to-utc",
        iso_z(end_utc),
        "--granularity",
        granularity,
    ]
    if overwrite_existing:
        cmd.append("--overwrite-existing")
    if dry_run:
        cmd.append("--dry-run")

    print(f"[{now_str()}] [heal] running repair command:")
    print(" ".join(cmd))
    proc = subprocess.run(cmd, check=False)
    return int(proc.returncode)


def main() -> int:
    table_10m = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")
    table_hourly = os.getenv("HOURLY_TABLE", "gecko_candles_hourly_30d")

    anchor_ids_raw = (os.getenv("HEAL_ANCHOR_IDS", "bitcoin,ethereum,solana") or "").strip()
    anchor_ids = [x.strip().lower() for x in anchor_ids_raw.split(",") if x.strip()]
    if not anchor_ids:
        raise SystemExit("HEAL_ANCHOR_IDS resolved to an empty list.")

    stale_10m_minutes = parse_int("HEAL_STALE_10M_MINUTES", 45, 1)
    stale_hourly_minutes = parse_int("HEAL_STALE_HOURLY_MINUTES", 180, 1)
    coverage_hours = parse_int("HEAL_COVERAGE_HOURS", 6, 1)
    max_missing_10m_slots = parse_int("HEAL_MAX_MISSING_10M_SLOTS", 2, 0)
    force_heal = parse_bool("HEAL_FORCE", False)

    print(
        f"[{now_str()}] [heal] config anchors={anchor_ids} "
        f"stale_10m>{stale_10m_minutes}m stale_hourly>{stale_hourly_minutes}m "
        f"coverage_hours={coverage_hours} max_missing_10m_slots={max_missing_10m_slots} "
        f"force={force_heal}"
    )

    session, cluster = cast(tuple[Session, Cluster], get_session(return_cluster=True))
    try:
        now = now_utc()
        cov_start = now - timedelta(hours=coverage_hours)
        expected_slots = max(1, int((coverage_hours * 60) // 10))

        stale_ids: list[str] = []
        details: dict[str, Any] = {}

        for coin_id in anchor_ids:
            latest_10m = latest_ts(session, table_10m, coin_id)
            latest_hourly = latest_ts(session, table_hourly, coin_id)

            lag_10m = (
                (now - latest_10m).total_seconds() / 60.0 if latest_10m is not None else None
            )
            lag_hourly = (
                (now - latest_hourly).total_seconds() / 60.0 if latest_hourly is not None else None
            )

            rows_10m = count_10m_rows(session, table_10m, coin_id, cov_start, now)
            missing_10m = max(0, expected_slots - rows_10m)

            reasons: list[str] = []
            if lag_10m is None:
                reasons.append("no_10m_data")
            elif lag_10m > stale_10m_minutes:
                reasons.append(f"stale_10m:{lag_10m:.1f}m>{stale_10m_minutes}m")

            if lag_hourly is None:
                reasons.append("no_hourly_data")
            elif lag_hourly > stale_hourly_minutes:
                reasons.append(f"stale_hourly:{lag_hourly:.1f}m>{stale_hourly_minutes}m")

            if missing_10m > max_missing_10m_slots:
                reasons.append(
                    f"missing_10m_slots:{missing_10m}>{max_missing_10m_slots} "
                    f"(window={coverage_hours}h)"
                )

            if reasons:
                stale_ids.append(coin_id)

            details[coin_id] = {
                "latest_10m": latest_10m.isoformat() if latest_10m else None,
                "latest_hourly": latest_hourly.isoformat() if latest_hourly else None,
                "lag_10m_minutes": round(lag_10m, 2) if lag_10m is not None else None,
                "lag_hourly_minutes": round(lag_hourly, 2) if lag_hourly is not None else None,
                "rows_10m_in_window": rows_10m,
                "expected_10m_slots_in_window": expected_slots,
                "missing_10m_slots": missing_10m,
                "reasons": reasons,
            }

        print(f"[{now_str()}] [heal] state={json.dumps(details, separators=(',', ':'))}")

        if force_heal and not stale_ids:
            stale_ids = anchor_ids[:]
            print(f"[{now_str()}] [heal] force enabled: healing all anchors.")

        if not stale_ids:
            print(f"[{now_str()}] [heal] no stale anchors detected; nothing to do.")
            return 0

        stale_ids = sorted(set(stale_ids))
        print(f"[{now_str()}] [heal] stale anchors detected: {stale_ids}")
        return run_repair(stale_ids)
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
