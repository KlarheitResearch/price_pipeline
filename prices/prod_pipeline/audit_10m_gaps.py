#!/usr/bin/env python3
"""
Lists missing UTC days in the 10-minute table over the last N days per coin.
"""

import datetime as dt
import os
import pathlib
import sys
import time
from datetime import timezone

from cassandra.query import SimpleStatement

from astra_connect.connect import AstraConfig, get_session

# Repo root & helpers
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.append(str(_REPO_ROOT))

try:
    from paths import rel, chdir_repo_root
except Exception:
    def rel(*parts: str) -> pathlib.Path:
        return _REPO_ROOT.joinpath(*parts)

    def chdir_repo_root() -> None:
        os.chdir(_REPO_ROOT)

chdir_repo_root()

# Config
KEYSPACE_OVERRIDE = (os.getenv("ASTRA_KEYSPACE_OVERRIDE") or "").strip()
TABLE_LIVE = os.getenv("TABLE_LIVE", "gecko_prices_live")
TEN_MIN_TABLE = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")

REQUEST_TIMEOUT = int(os.getenv("DQ_REQUEST_TIMEOUT_SEC", "30"))
FETCH_SIZE = int(os.getenv("DQ_FETCH_SIZE", "500"))

TOP_N = int(os.getenv("TOP_N_DQ", "210"))
WINDOW_D = int(os.getenv("DQ_WINDOW_10M_DAYS", "7"))


def utcnow() -> dt.datetime:
    return dt.datetime.now(timezone.utc)


def date_seq(last_inclusive: dt.date, days: int) -> list[dt.date]:
    start = last_inclusive - dt.timedelta(days=days - 1)
    return [start + dt.timedelta(days=i) for i in range(days)]


def to_date(x) -> dt.date:
    if isinstance(x, dt.datetime):
        return x.astimezone(timezone.utc).date()
    if isinstance(x, dt.date):
        return x
    return dt.date.fromisoformat(str(x)[:10])


def top_assets(session, sel_live: SimpleStatement, limit: int):
    rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT))
    rows = [r for r in rows if isinstance(r.market_cap_rank, int) and r.market_cap_rank > 0]
    rows.sort(key=lambda r: r.market_cap_rank)
    return rows[:limit]


def existing_days_10m(session, prepared_stmt, coin_id: str, start_dt: dt.datetime, end_dt: dt.datetime) -> set[dt.date]:
    have = set()
    for row in session.execute(prepared_stmt, [coin_id, start_dt, end_dt], timeout=REQUEST_TIMEOUT):
        if getattr(row, "ts", None) is not None:
            have.add(to_date(row.ts))
    return have


def print_gap_report(session, sel_live: SimpleStatement, sel_10m_range_days, window_days: int = WINDOW_D, top_n: int = TOP_N, only_with_gaps: bool = True):
    now = utcnow()
    end_excl = dt.datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + dt.timedelta(days=1)
    last_inclusive = end_excl.date() - dt.timedelta(days=1)

    want_days = set(date_seq(last_inclusive, window_days))
    start_dt = dt.datetime.combine(min(want_days), dt.time.min, tzinfo=timezone.utc)
    end_dt = dt.datetime.combine(last_inclusive + dt.timedelta(days=1), dt.time.min, tzinfo=timezone.utc)

    coins = top_assets(session, sel_live, top_n)

    print("")
    print(f"10m audit window (UTC): {min(want_days)} -> {last_inclusive} (days={window_days})")
    print(f"Coins checked (by rank): {len(coins)}")

    total_with_gaps = 0
    worst = []  # (missing_count, symbol, id, missing_days_sorted)

    t0 = time.perf_counter()
    for i, c in enumerate(coins, 1):
        have = existing_days_10m(session, sel_10m_range_days, c.id, start_dt, end_dt)
        missing = sorted(want_days - have)
        if missing:
            total_with_gaps += 1
            worst.append((len(missing), c.symbol, c.id, missing))
            print(
                f"[{i:>3}/{len(coins)}] {c.symbol:<12} ({c.id}) rank={c.market_cap_rank:<4} "
                f"have={len(have):>2}/{window_days} MISSING={len(missing)}"
            )
            for d in missing:
                print(f"         - {d.isoformat()} (UTC day)")
        elif not only_with_gaps:
            print(
                f"[{i:>3}/{len(coins)}] {c.symbol:<12} ({c.id}) rank={c.market_cap_rank:<4} "
                f"OK ({len(have)}/{window_days})"
            )

    dt_secs = time.perf_counter() - t0
    print(f"Done in {dt_secs:.2f}s. Coins with gaps: {total_with_gaps}/{len(coins)}")
    if worst:
        worst.sort(reverse=True)
        print("Top gap offenders:")
        for miss_cnt, sym, cid, miss_days in worst[:5]:
            first = miss_days[0].isoformat()
            last = miss_days[-1].isoformat()
            print(f"  - {sym:<12} ({cid}) missing {miss_cnt} day(s) first:{first}, last:{last}")


if __name__ == "__main__":
    cfg = AstraConfig.from_env()
    effective_keyspace = KEYSPACE_OVERRIDE or cfg.keyspace
    print(
        f"[{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
        f"Connecting to Astra (target={cfg.target}, keyspace={effective_keyspace})"
    )
    session, cluster = get_session(keyspace=effective_keyspace, return_cluster=True)
    session.default_fetch_size = FETCH_SIZE
    print("[ok] Connected")

    try:
        sel_live = SimpleStatement(
            f"SELECT id, symbol, name, market_cap_rank FROM {TABLE_LIVE}",
            fetch_size=FETCH_SIZE,
        )
        sel_10m_range_days = session.prepare(
            f"""
            SELECT ts FROM {TEN_MIN_TABLE}
            WHERE id = ? AND ts >= ? AND ts < ?
            """
        )
        print_gap_report(session, sel_live, sel_10m_range_days)
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass
