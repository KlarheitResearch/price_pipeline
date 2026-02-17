#!/usr/bin/env python3
# prices/CC_gck_append_10m_from_live.py
#
# Append 10-minute slots into:
#   - gecko_prices_10m_7d      (IF NOT EXISTS; from rolling or carry)
#   - gecko_market_cap_10m_7d  (category aggregates per slot; upsert with ranks)
#
# Reads from:
#   - gecko_prices_live         (for the list of assets, incl. category)
#   - gecko_prices_live_rolling (for latest points per slot / carry)
#
# Notes:
# - Uses shared Astra connector (astra_connect.connect).
# - Works with .env locally or pure env vars in CI/CD.
# - “Carry” uses the latest point before the slot if the slot is empty,
#   capped by ALLOW_CARRY_MAX_SLOTS consecutive slots.

import os, traceback
from datetime import datetime, timedelta, timezone
from typing import Tuple, List, Dict, Any, cast, overload
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

# ───────────────────────── Astra connector ─────────────────────────
from astra_connect.connect import get_session, AstraConfig
from cassandra.cluster import Cluster, Session
AstraConfig.from_env()

# ───────────────────────── Config ─────────────────────────
TOP_N              = int(os.getenv("TOP_N", "1000"))

REQUEST_TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
FETCH_SIZE         = int(os.getenv("FETCH_SIZE", "500"))

SLOT_MINUTES       = int(os.getenv("SLOT_MINUTES", "10"))
SLOT_DELAY_SEC     = int(os.getenv("SLOT_DELAY_SEC", "120"))
SLOTS_BACKFILL     = int(os.getenv("SLOTS_BACKFILL", 4))  # default: 6h safety window
ALLOW_CARRY_MAX_SLOTS = int(os.getenv("ALLOW_CARRY_MAX_SLOTS", 4))  # allow carry across the 6h window

# ───────────────────────── Optional gapfill mode ─────────────────────────
# Designed for hourly/2-hourly runs (NOT every 6 minutes).
GAPFILL_ENABLED     = os.getenv("GAPFILL_ENABLED", "0") == "1"
GAPFILL_HOURS       = int(os.getenv("GAPFILL_HOURS", "12"))
# If 1, attempt a once-per-N-hour lock row in Cassandra to avoid duplicate gapfill runs.
GAPFILL_LOCK_ENABLED  = os.getenv("GAPFILL_LOCK_ENABLED", "1") == "1"
GAPFILL_BUCKET_HOURS  = int(os.getenv("GAPFILL_BUCKET_HOURS", "2"))  # run at most once per 2h bucket
GAPFILL_LOCK_TTL_SEC  = int(os.getenv("GAPFILL_LOCK_TTL_SEC", str(2 * 3600 + 300)))  # 2h + 5m slack
GAPFILL_LOCK_JOB     = os.getenv("GAPFILL_LOCK_JOB", "append_10m_gapfill").strip() or "append_10m_gapfill"
# If 0 (default), do NOT write category aggregates in gapfill mode (avoids incorrect partial totals).
GAPFILL_WRITE_AGG    = os.getenv("GAPFILL_WRITE_AGG", "0") == "1"

# Aggregate safety: skip writing aggregates if too few coins contributed.
_DEFAULT_AGG_MIN = max(1, int(round(TOP_N * 0.7))) if TOP_N > 0 else 0
AGG_MIN_COINS = int(os.getenv("AGG_MIN_COINS", str(_DEFAULT_AGG_MIN)))
if AGG_MIN_COINS < 0:
    AGG_MIN_COINS = 0
AGG_CARRY_FORWARD = os.getenv("AGG_CARRY_FORWARD", "0") == "1"
VERBOSE_MODE = os.getenv("VERBOSE_MODE", "0") == "1"
PROGRESS_EVERY = max(1, int(os.getenv("PROGRESS_EVERY", "100")))
APPEND_SKIP_EXISTING = os.getenv("APPEND_SKIP_EXISTING", "1") == "1"
APPEND_AGG_FROM_EXISTING = os.getenv("APPEND_AGG_FROM_EXISTING", "0") == "1"
LOG_SLOT_LINES = VERBOSE_MODE and (os.getenv("LOG_SLOT_LINES", "0") == "1")
LOG_INSERT_LINES = VERBOSE_MODE and (os.getenv("LOG_INSERT_LINES", "0") == "1")
COIN_WORKERS = max(1, int(os.getenv("COIN_WORKERS", "8")))
WRITE_CONCURRENCY = max(1, int(os.getenv("WRITE_CONCURRENCY", "16")))

# Optional post-run aggregate rebuild (recompute from stored 10m rows).
REBUILD_AGG_AFTER_GAPFILL = os.getenv("REBUILD_AGG_AFTER_GAPFILL", "0") == "1"
REBUILD_AGG_AFTER_APPEND = os.getenv("REBUILD_AGG_AFTER_APPEND", "0") == "1"

# Tables (defaults match your schema)
TABLE_LATEST       = os.getenv("TABLE_LATEST", "gecko_prices_live")
TABLE_ROLLING      = os.getenv("TABLE_ROLLING", "gecko_prices_live_rolling")
TABLE_OUT          = os.getenv("TABLE_OUT", "gecko_prices_10m_7d")
TABLE_MCAP_OUT     = os.getenv("TABLE_MCAP_10M", "gecko_market_cap_10m_7d")

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def should_log_coin(index: int, total: int) -> bool:
    if VERBOSE_MODE:
        return True
    return index == 1 or index == total or (index % PROGRESS_EVERY == 0)

@overload
def to_utc(x: None) -> None:
    ...


@overload
def to_utc(x: datetime) -> datetime:
    ...


def to_utc(x: datetime | None) -> datetime | None:
    if x is None:
        return None
    if x.tzinfo is None:
        return x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)

def floor_slot(dt_utc: datetime, minutes: int = SLOT_MINUTES) -> datetime:
    dt_utc = to_utc(dt_utc)
    return dt_utc.replace(minute=(dt_utc.minute // minutes) * minutes, second=0, microsecond=0)

def slot_start_now() -> datetime:
    now_ = datetime.now(timezone.utc) - timedelta(seconds=SLOT_DELAY_SEC)
    return floor_slot(now_)

def last_n_slots_oldest_first(n: int) -> List[Tuple[datetime, datetime]]:
    end = slot_start_now() + timedelta(minutes=SLOT_MINUTES)
    slots: List[Tuple[datetime, datetime]] = []
    for _ in range(n):
        start = end - timedelta(minutes=SLOT_MINUTES)
        slots.append((start, end))
        end = start
    slots.reverse()
    return slots

def expected_slots_for_last_hours(hours: int) -> List[Tuple[datetime, datetime]]:
    """
    Build contiguous slot windows covering the last N hours up to 'now' (delayed by SLOT_DELAY_SEC),
    in chronological order.
    """
    end = slot_start_now() + timedelta(minutes=SLOT_MINUTES)
    start = floor_slot(end - timedelta(hours=hours))
    slots: List[Tuple[datetime, datetime]] = []
    cur = start
    while cur < end:
        nxt = cur + timedelta(minutes=SLOT_MINUTES)
        slots.append((cur, nxt))
        cur = nxt
    return slots

def lock_bucket_utc(bucket_hours: int) -> str:
    """
    Returns a UTC bucket string floored to bucket_hours.
    Example: bucket_hours=2 and time=13:xx -> bucket=...T12
    """
    now = datetime.now(timezone.utc)
    h0 = (now.hour // bucket_hours) * bucket_hours
    return f"{now:%Y-%m-%d}T{h0:02d}"

# ───────────────────────── Session & prepared statements ─────────────────────────
print(f"[{now_str()}] Connecting to Astra…")
session, cluster = cast(tuple[Session, Cluster], get_session(return_cluster=True))
print(f"[{now_str()}] Connected. keyspace='{session.keyspace}'")

from cassandra.query import SimpleStatement

session_keyspace = (session.keyspace or "").strip()
KEYSPACE = ((os.getenv("ASTRA_KEYSPACE") or session_keyspace).strip() or session_keyspace or "default_keyspace")

SEL_COINS = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank, category FROM {TABLE_LATEST}",
    fetch_size=FETCH_SIZE
)

# Rolling points in the target window (used to avoid per-slot reads).
SEL_ROLLING_RANGE_PS = session.prepare(
    f"""
    SELECT last_updated, price_usd, market_cap, volume_24h,
           market_cap_rank, circulating_supply, total_supply
    FROM {TABLE_ROLLING}
    WHERE id=? AND last_updated>=? AND last_updated<?
    """
)

# Carry: latest point before the slot start
SEL_PREV_PS = session.prepare(
    f"""
    SELECT last_updated, price_usd, market_cap, volume_24h,
           market_cap_rank, circulating_supply, total_supply
    FROM {TABLE_ROLLING}
    WHERE id=? AND last_updated<? LIMIT 1
    """
)

# Insert into 10m table — IF NOT EXISTS
INS_10M_IF_NOT_EXISTS_PS = session.prepare(
    f"""
    INSERT INTO {TABLE_OUT}
      (id, ts, symbol, name, price_usd, market_cap, volume_24h,
       market_cap_rank, circulating_supply, total_supply, last_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS
    """
)

# Existing 10m slots for an id in a time window (used in gapfill mode)
SEL_10M_EXISTING_TS_RANGE_PS = session.prepare(
    f"""
    SELECT ts
    FROM {TABLE_OUT}
    WHERE id=? AND ts>=? AND ts<?
    """
)

# Existing 10m slot by id+ts (used to keep aggregates consistent on skipped inserts)
SEL_10M_ONE_PS = session.prepare(
    f"""
    SELECT market_cap, volume_24h, last_updated
    FROM {TABLE_OUT}
    WHERE id=? AND ts=? LIMIT 1
    """
)

# Read stored 10m rows for aggregate rebuilds
SEL_10M_RANGE_FOR_AGG = session.prepare(
    f"""
    SELECT ts, market_cap, volume_24h, last_updated
    FROM {TABLE_OUT}
    WHERE id=? AND ts>=? AND ts<?
    """
)

# Previous aggregate row (for carry-forward)
SEL_MCAP_10M_PREV = session.prepare(
    f"""
    SELECT ts, market_cap, volume_24h, last_updated, market_cap_rank
    FROM {TABLE_MCAP_OUT}
    WHERE category=? AND ts < ? LIMIT 1
    """
)

# Aggregates: gecko_market_cap_10m_7d(category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
INS_MCAP_10M_UPSERT = session.prepare(
    f"""
    INSERT INTO {TABLE_MCAP_OUT}
      (category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
    VALUES (?, ?, ?, ?, ?, ?)
    """
)

INS_LOCK_IF_NOT_EXISTS_PS = None
if GAPFILL_ENABLED and GAPFILL_LOCK_ENABLED:
    # NOTE: table already exists: {KEYSPACE}.job_locks
    # Use TTL so a crashed run doesn't permanently block future buckets.
    INS_LOCK_IF_NOT_EXISTS_PS = session.prepare(
        f"""
        INSERT INTO {KEYSPACE}.job_locks (job, bucket, created_at)
        VALUES (?, ?, ?)
        IF NOT EXISTS
        USING TTL {int(GAPFILL_LOCK_TTL_SEC)}
        """
    )

def try_acquire_gapfill_lock() -> bool:
    """
    Returns True if gapfill should run now (lock acquired or lock disabled).
    """
    if not GAPFILL_ENABLED:
        return False
    if not GAPFILL_LOCK_ENABLED:
        return True
    bucket = lock_bucket_utc(GAPFILL_BUCKET_HOURS)
    try:
        if INS_LOCK_IF_NOT_EXISTS_PS is None:
            print(f"[{now_str()}] [gapfill][WARN] lock prepared statement missing → skip gapfill for safety")
            return False
        res = session.execute(
            INS_LOCK_IF_NOT_EXISTS_PS,
            [GAPFILL_LOCK_JOB, bucket, datetime.now(timezone.utc)],
            timeout=REQUEST_TIMEOUT
        ).one()
        applied = bool(getattr(res, "applied", True)) if res is not None else True
        if not applied:
            print(f"[{now_str()}] [gapfill] lock already held for bucket={bucket} job={GAPFILL_LOCK_JOB} → skip gapfill")
        return applied
    except Exception as e:
        print(f"[{now_str()}] [gapfill][WARN] lock failed ({e}) → skip gapfill for safety")
        return False

def compute_category_ranks(cat_totals: Dict[str, Dict[str, float]]) -> Dict[str, int]:
    """
    Input: cat_totals[category] = {'market_cap': float, 'volume_24h': float, 'last_updated': datetime}
    Output: ranks per category (ALL=0; others 1..N by market_cap desc; ties stable)
    """
    items = [(cat, float(vals.get('market_cap', 0.0))) for cat, vals in cat_totals.items() if cat != "ALL"]
    items.sort(key=lambda x: x[1], reverse=True)
    ranks: Dict[str, int] = {cat: i + 1 for i, (cat, _m) in enumerate(items)}
    ranks["ALL"] = 0
    return ranks

def ensure_all_bucket(catmap: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Rebuild ALL from the other categories to avoid persisting a stale/partial ALL.
    last_updated = max of categories.
    """
    non_all = [(cat, vals) for cat, vals in catmap.items() if cat != "ALL"]
    if not non_all:
        return catmap

    latest_lu = None
    total_mcap = 0.0
    total_vol = 0.0
    for _cat, vals in non_all:
        lu = vals.get("last_updated")
        if lu is not None and (latest_lu is None or lu > latest_lu):
            latest_lu = lu
        total_mcap += float(vals.get("market_cap") or 0.0)
        total_vol += float(vals.get("volume_24h") or 0.0)

    catmap["ALL"] = {
        "market_cap": total_mcap,
        "volume_24h": total_vol,
        "last_updated": latest_lu,
    }
    return catmap

def read_existing_slot(coin_id: str, slot_start: datetime, slot_end: datetime):
    """
    Fetch the already-stored 10m slot so aggregates always reflect persisted rows
    (especially when INSERT IF NOT EXISTS does not apply).
    """
    try:
        row = session.execute(
            SEL_10M_ONE_PS,
            [coin_id, slot_start],
            timeout=REQUEST_TIMEOUT
        ).one()
    except Exception as e:
        print(f"        [WARN] existing slot fetch failed {coin_id} @ {slot_start}: {e}")
        return None
    if not row:
        return None
    mcap_exist = float(row.market_cap) if row.market_cap is not None else 0.0
    vol_exist = float(row.volume_24h) if row.volume_24h is not None else 0.0
    lu_exist = to_utc(getattr(row, "last_updated", None)) or (slot_end - timedelta(seconds=1))
    return mcap_exist, vol_exist, lu_exist

def recompute_slot_totals_from_table(
    target_slots: List[datetime],
    coins: List[Any],
    accumulate_fn,
) -> Tuple[Dict[datetime, Dict[str, Dict[str, Any]]], Dict[datetime, int]]:
    """
    Recompute aggregates from stored 10m rows for a specific set of slots.
    This is slower but ensures totals reflect what is actually persisted.
    """
    if not target_slots:
        return {}, {}

    target_set = set(target_slots)
    start = min(target_slots)
    end_excl = max(target_slots) + timedelta(minutes=SLOT_MINUTES)

    rebuilt_totals: Dict[datetime, Dict[str, Dict[str, Any]]] = {}
    rebuilt_counts: Dict[datetime, int] = {}

    for idx, c in enumerate(coins, 1):
        if (idx == 1) or (idx % 25 == 0) or (idx == len(coins)):
            print(f"[{now_str()}] [rebuild-agg] coin {idx}/{len(coins)}: {getattr(c, 'symbol', '?')} ({getattr(c, 'id', '?')})")

        coin_id = getattr(c, "id", None)
        if not coin_id:
            continue
        coin_category = (getattr(c, 'category', None) or 'Other').strip() or 'Other'

        try:
            rows = session.execute(
                SEL_10M_RANGE_FOR_AGG,
                [coin_id, start, end_excl],
                timeout=REQUEST_TIMEOUT
            )
        except Exception as e:
            print(f"[{now_str()}] [rebuild-agg][WARN] read failed {coin_id}: {e}")
            continue

        for r in rows:
            ts_raw = getattr(r, "ts", None)
            ts = to_utc(ts_raw) if isinstance(ts_raw, datetime) else None
            if ts is None or ts not in target_set:
                continue
            mcap = float(getattr(r, "market_cap", 0.0) or 0.0)
            vol = float(getattr(r, "volume_24h", 0.0) or 0.0)
            lu = to_utc(getattr(r, "last_updated", None)) or (ts + timedelta(minutes=SLOT_MINUTES) - timedelta(seconds=1))
            accumulate_fn(rebuilt_totals, ts, coin_category, mcap, vol, lu)
            accumulate_fn(rebuilt_totals, ts, "ALL", mcap, vol, lu)
            rebuilt_counts[ts] = rebuilt_counts.get(ts, 0) + 1

    return rebuilt_totals, rebuilt_counts

def read_prev_mcap(category: str, slot_start: datetime):
    try:
        row = session.execute(
            SEL_MCAP_10M_PREV,
            [category, slot_start],
            timeout=REQUEST_TIMEOUT
        ).one()
    except Exception as e:
        print(f"[{now_str()}] [carry][WARN] prev mcap read failed {category} < {slot_start}: {e}")
        return None
    if not row:
        return None
    lu = to_utc(getattr(row, "last_updated", None)) or (slot_start - timedelta(seconds=1))
    return {
        "market_cap": float(getattr(row, "market_cap", 0.0) or 0.0),
        "volume_24h": float(getattr(row, "volume_24h", 0.0) or 0.0),
        "last_updated": lu,
        "market_cap_rank": getattr(row, "market_cap_rank", None),
    }


def _point_from_row(row: Any) -> Dict[str, Any] | None:
    if row is None:
        return None
    if getattr(row, "price_usd", None) is None:
        return None
    lu = to_utc(getattr(row, "last_updated", None))
    if lu is None:
        return None
    return {
        "last_updated": lu,
        "price": float(getattr(row, "price_usd")),
        "mcap": float(getattr(row, "market_cap")) if getattr(row, "market_cap", None) is not None else 0.0,
        "vol": float(getattr(row, "volume_24h")) if getattr(row, "volume_24h", None) is not None else 0.0,
        "rank": int(getattr(row, "market_cap_rank")) if getattr(row, "market_cap_rank", None) is not None else None,
        "circ": float(getattr(row, "circulating_supply")) if getattr(row, "circulating_supply", None) is not None else None,
        "totl": float(getattr(row, "total_supply")) if getattr(row, "total_supply", None) is not None else None,
    }


def plan_coin_slots(
    coin: Any,
    index: int,
    total: int,
    slots: List[Tuple[datetime, datetime]],
    slot_start_set: set[datetime],
    run_mode: str,
) -> Dict[str, Any]:
    coin_id = getattr(coin, "id", None)
    sym = getattr(coin, "symbol", None)
    name = getattr(coin, "name", None)
    rank = getattr(coin, "market_cap_rank", None)
    coin_category = (getattr(coin, "category", None) or "Other").strip() or "Other"

    out: Dict[str, Any] = {
        "index": index,
        "total": total,
        "coin_id": coin_id,
        "symbol": sym,
        "name": name,
        "rank": rank,
        "category": coin_category,
        "entries": [],
        "plan_error": None,
    }

    if not coin_id:
        out["plan_error"] = "missing coin id"
        return out
    if not slots:
        return out

    w_start = slots[0][0]
    w_end = slots[-1][1]

    existing_ts: set[datetime] = set()
    should_prefetch_existing = run_mode == "gapfill" or (run_mode == "append" and APPEND_SKIP_EXISTING)
    if should_prefetch_existing:
        try:
            rs = session.execute(
                SEL_10M_EXISTING_TS_RANGE_PS,
                [coin_id, w_start, w_end],
                timeout=REQUEST_TIMEOUT,
            )
            for r in rs:
                ts0 = getattr(r, "ts", None)
                if isinstance(ts0, datetime):
                    ts_utc = to_utc(ts0)
                    if ts_utc is not None:
                        existing_ts.add(ts_utc)
        except Exception as e:
            out["plan_error"] = f"existing-range read failed: {e}"
            return out

    slot_points: Dict[datetime, Dict[str, Any]] = {}
    try:
        rs = session.execute(
            SEL_ROLLING_RANGE_PS,
            [coin_id, w_start, w_end],
            timeout=REQUEST_TIMEOUT,
        )
        for row in rs:
            point = _point_from_row(row)
            if point is None:
                continue
            slot_start = floor_slot(point["last_updated"])
            if slot_start not in slot_start_set:
                continue
            prev = slot_points.get(slot_start)
            if prev is None or point["last_updated"] > prev["last_updated"]:
                slot_points[slot_start] = point
    except Exception as e:
        out["plan_error"] = f"rolling-range read failed: {e}"
        return out

    try:
        prev_row = session.execute(
            SEL_PREV_PS,
            [coin_id, w_start],
            timeout=REQUEST_TIMEOUT,
        ).one()
        last_real_point = _point_from_row(prev_row)
    except Exception as e:
        out["plan_error"] = f"prev read failed: {e}"
        return out

    carry_used = 0
    entries: List[Dict[str, Any]] = []
    for start, end in slots:
        if start in existing_ts:
            entries.append({"kind": "existing", "start": start, "end": end})
            continue

        point = slot_points.get(start)
        if point is not None:
            source = "hist-in-slot"
            carry_used = 0
            last_real_point = point
        else:
            if last_real_point is None:
                entries.append({"kind": "skip", "start": start, "end": end, "reason": "no-history"})
                continue
            if carry_used >= ALLOW_CARRY_MAX_SLOTS:
                entries.append({"kind": "skip", "start": start, "end": end, "reason": "carry-cap"})
                continue
            source = "hist-carry"
            point = last_real_point
            carry_used += 1

        entries.append(
            {
                "kind": "insert",
                "start": start,
                "end": end,
                "source": source,
                "slot_last_upd": end - timedelta(seconds=1),
                "price": point["price"],
                "mcap": point["mcap"],
                "vol": point["vol"],
                "rank": point["rank"],
                "circ": point["circ"],
                "totl": point["totl"],
            }
        )

    out["entries"] = entries
    return out

# ───────────────────────── Main logic ─────────────────────────
def main():
    # Decide run mode early (append is always allowed; gapfill is optional)
    run_gapfill = try_acquire_gapfill_lock()
    run_mode = "gapfill" if run_gapfill else "append"

    # Pick coins
    coins_rows = list(session.execute(SEL_COINS, timeout=REQUEST_TIMEOUT))
    coins = [r for r in coins_rows if isinstance(r.market_cap_rank, int) and r.market_cap_rank > 0]
    coins.sort(key=lambda r: r.market_cap_rank)
    coins = coins[:TOP_N]
    if run_mode == "gapfill":
        print(f"[{now_str()}] Mode=gapfill | bucket={lock_bucket_utc(GAPFILL_BUCKET_HOURS)} "
              f"(every {GAPFILL_BUCKET_HOURS}h, ttl={GAPFILL_LOCK_TTL_SEC}s) | "
              f"Processing top {len(coins)} coins from {TABLE_LATEST}")
    else:
        print(f"[{now_str()}] Mode=append | Processing top {len(coins)} coins from {TABLE_LATEST}")

    if run_mode == "gapfill":
        slots = expected_slots_for_last_hours(GAPFILL_HOURS)
    else:
        slots = last_n_slots_oldest_first(SLOTS_BACKFILL)
    if slots:
        print(f"[{now_str()}] Slots (oldest→newest): {slots[0][0]} .. {slots[-1][1]} (count={len(slots)})")

    # slot_totals[slot_start][category] = {'market_cap': float, 'volume_24h': float, 'last_updated': datetime}
    slot_totals: Dict[datetime, Dict[str, Dict[str, Any]]] = {}
    # slot_coin_counts[slot_start] = number of coins contributing to that slot
    slot_coin_counts: Dict[datetime, int] = {}
    # categories encountered in this run (for carry-forward)
    categories_seen: set[str] = set()
    carry_forward_slots: set[datetime] = set()

    def bump_slot_total(slot_start: datetime, category: str, mcap_value: float, vol_value: float, last_upd: datetime) -> None:
        catmap = slot_totals.setdefault(slot_start, {})
        entry = catmap.setdefault(category, {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": last_upd})
        entry["market_cap"] += float(mcap_value or 0.0)
        entry["volume_24h"] += float(vol_value or 0.0)
        if last_upd and (entry["last_updated"] is None or last_upd > entry["last_updated"]):
            entry["last_updated"] = last_upd

    def bump_slot_coin_count(slot_start: datetime) -> None:
        slot_coin_counts[slot_start] = slot_coin_counts.get(slot_start, 0) + 1

    def accumulate_into_map(
        target_map: Dict[datetime, Dict[str, Dict[str, Any]]],
        slot_start: datetime,
        category: str,
        mcap_value: float,
        vol_value: float,
        last_upd: datetime,
    ) -> None:
        catmap = target_map.setdefault(slot_start, {})
        entry = catmap.setdefault(category, {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": last_upd})
        entry["market_cap"] += float(mcap_value or 0.0)
        entry["volume_24h"] += float(vol_value or 0.0)
        if last_upd and (entry["last_updated"] is None or last_upd > entry["last_updated"]):
            entry["last_updated"] = last_upd

    wrote = skipped = 0
    slot_start_set = {s for (s, _e) in slots}
    plan_rows: List[Dict[str, Any]] = []

    if coins and slots:
        worker_count = min(COIN_WORKERS, len(coins))
        if worker_count > 1:
            print(f"[{now_str()}] Planning slot candidates with workers={worker_count}")
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_map = {
                    executor.submit(
                        plan_coin_slots,
                        c,
                        ci,
                        len(coins),
                        slots,
                        slot_start_set,
                        run_mode,
                    ): (ci, c)
                    for ci, c in enumerate(coins, 1)
                }
                for fut in as_completed(future_map):
                    ci, c = future_map[fut]
                    try:
                        plan_rows.append(fut.result())
                    except Exception as e:
                        plan_rows.append(
                            {
                                "index": ci,
                                "total": len(coins),
                                "coin_id": getattr(c, "id", None),
                                "symbol": getattr(c, "symbol", None),
                                "name": getattr(c, "name", None),
                                "rank": getattr(c, "market_cap_rank", None),
                                "category": (getattr(c, "category", None) or "Other").strip() or "Other",
                                "entries": [],
                                "plan_error": f"planning failed: {e}",
                            }
                        )
        else:
            for ci, c in enumerate(coins, 1):
                plan_rows.append(plan_coin_slots(c, ci, len(coins), slots, slot_start_set, run_mode))

        plan_rows.sort(key=lambda item: int(item.get("index") or 0))

    coin_stats: Dict[int, Dict[str, int]] = {}
    pending_writes = deque()

    def add_coin_stat(ci: int, *, wrote_delta: int = 0, skipped_delta: int = 0) -> None:
        nonlocal wrote, skipped
        stats = coin_stats.setdefault(ci, {"wrote": 0, "skipped": 0})
        if wrote_delta:
            stats["wrote"] += wrote_delta
            wrote += wrote_delta
        if skipped_delta:
            stats["skipped"] += skipped_delta
            skipped += skipped_delta

    def apply_existing_agg(coin_id: str, coin_category: str, start: datetime, end: datetime) -> None:
        if run_mode == "gapfill" and GAPFILL_WRITE_AGG:
            existing_vals = read_existing_slot(coin_id, start, end)
            if existing_vals:
                mcap_exist, vol_exist, lu_exist = existing_vals
                bump_slot_total(start, coin_category, mcap_exist, vol_exist, lu_exist)
                bump_slot_total(start, "ALL", mcap_exist, vol_exist, lu_exist)
                bump_slot_coin_count(start)
        elif run_mode == "append" and APPEND_AGG_FROM_EXISTING:
            existing_vals = read_existing_slot(coin_id, start, end)
            if existing_vals:
                mcap_exist, vol_exist, lu_exist = existing_vals
                bump_slot_total(start, coin_category, mcap_exist, vol_exist, lu_exist)
                bump_slot_total(start, "ALL", mcap_exist, vol_exist, lu_exist)
                bump_slot_coin_count(start)

    def handle_write_result(ctx: Dict[str, Any], result_obj: Any) -> None:
        ci = int(ctx["ci"])
        start = cast(datetime, ctx["start"])
        end = cast(datetime, ctx["end"])
        coin_id = cast(str, ctx["coin_id"])
        coin_category = cast(str, ctx["coin_category"])
        mcap = float(ctx["mcap"])
        vol = float(ctx["vol"])
        slot_last_upd = cast(datetime, ctx["slot_last_upd"])
        source = cast(str, ctx["source"])

        applied = bool(getattr(result_obj, "applied", True)) if result_obj is not None else True

        # Aggregate in append mode for newly written rows only by default.
        if (run_mode != "gapfill") or GAPFILL_WRITE_AGG:
            if applied:
                bump_slot_total(start, coin_category, mcap, vol, slot_last_upd)
                bump_slot_total(start, "ALL", mcap, vol, slot_last_upd)
                bump_slot_coin_count(start)
            elif run_mode == "append" and APPEND_AGG_FROM_EXISTING:
                existing_vals = read_existing_slot(coin_id, start, end)
                if existing_vals:
                    mcap_exist, vol_exist, lu_exist = existing_vals
                    bump_slot_total(start, coin_category, mcap_exist, vol_exist, lu_exist)
                    bump_slot_total(start, "ALL", mcap_exist, vol_exist, lu_exist)
                    bump_slot_coin_count(start)
            elif run_mode == "gapfill" and GAPFILL_WRITE_AGG:
                existing_vals = read_existing_slot(coin_id, start, end)
                if existing_vals:
                    mcap_exist, vol_exist, lu_exist = existing_vals
                    bump_slot_total(start, coin_category, mcap_exist, vol_exist, lu_exist)
                    bump_slot_total(start, "ALL", mcap_exist, vol_exist, lu_exist)
                    bump_slot_coin_count(start)
                else:
                    bump_slot_total(start, coin_category, mcap, vol, slot_last_upd)
                    bump_slot_total(start, "ALL", mcap, vol, slot_last_upd)
                    bump_slot_coin_count(start)

        if LOG_INSERT_LINES and should_log_coin(ci, len(coins)):
            print(
                f"        insert {'applied' if applied else 'skipped'} "
                f"({source}, price={ctx['price']}, mcap={mcap}, vol={vol}, rank={ctx['rank']}, "
                f"circ={ctx['circ']}, totl={ctx['totl']}, last_upd={slot_last_upd})"
            )

        if applied:
            add_coin_stat(ci, wrote_delta=1)
        else:
            add_coin_stat(ci, skipped_delta=1)

    def drain_one_write() -> None:
        if not pending_writes:
            return
        fut, ctx = pending_writes.popleft()
        ci = int(ctx["ci"])
        try:
            result_obj = fut.result()
            handle_write_result(ctx, result_obj)
        except Exception as e:
            print(f"        [WRITE-ERR] insert {ctx['symbol']} {ctx['start']}: {e} (skip)")
            traceback.print_exc()
            add_coin_stat(ci, skipped_delta=1)

    for plan in plan_rows:
        ci = int(plan.get("index") or 0)
        sym = plan.get("symbol")
        name = plan.get("name")
        mkr = plan.get("rank")
        coin_id_raw = plan.get("coin_id")
        coin_id = str(coin_id_raw) if coin_id_raw is not None else ""
        coin_category = (plan.get("category") or "Other").strip() or "Other"
        categories_seen.add(coin_category)

        log_this_coin = should_log_coin(ci, len(coins))
        if log_this_coin:
            print(f"[{now_str()}] -> [{ci}/{len(coins)}] {sym} ({coin_id}) rank={mkr}")

        plan_error = plan.get("plan_error")
        if plan_error:
            print(f"[{now_str()}] [plan][WARN] {coin_id}: {plan_error}")
            add_coin_stat(ci, skipped_delta=len(slots))
            continue

        for si, entry in enumerate(plan.get("entries", []), 1):
            start = entry.get("start")
            end = entry.get("end")
            if not isinstance(start, datetime) or not isinstance(end, datetime):
                add_coin_stat(ci, skipped_delta=1)
                continue

            if LOG_SLOT_LINES and log_this_coin:
                print(f"    slot {si}/{len(slots)} {start} -> {end}")

            kind = entry.get("kind")
            if kind == "existing":
                apply_existing_agg(coin_id, coin_category, start, end)
                add_coin_stat(ci, skipped_delta=1)
                continue

            if kind != "insert":
                if VERBOSE_MODE and log_this_coin:
                    reason = entry.get("reason") or "skip"
                    print(f"        {reason} -> skip")
                add_coin_stat(ci, skipped_delta=1)
                continue

            price = float(entry.get("price"))
            mcap = float(entry.get("mcap") or 0.0)
            vol = float(entry.get("vol") or 0.0)
            rank = entry.get("rank")
            circ = entry.get("circ")
            totl = entry.get("totl")
            source = entry.get("source") or "hist-unknown"
            slot_last_upd = entry.get("slot_last_upd") or (end - timedelta(seconds=1))

            try:
                fut = session.execute_async(
                    INS_10M_IF_NOT_EXISTS_PS,
                    [coin_id, start, sym, name, price, mcap, vol, rank, circ, totl, slot_last_upd],
                    timeout=REQUEST_TIMEOUT,
                )
                pending_writes.append(
                    (
                        fut,
                        {
                            "ci": ci,
                            "coin_id": coin_id,
                            "coin_category": coin_category,
                            "symbol": sym,
                            "start": start,
                            "end": end,
                            "source": source,
                            "price": price,
                            "mcap": mcap,
                            "vol": vol,
                            "rank": rank,
                            "circ": circ,
                            "totl": totl,
                            "slot_last_upd": slot_last_upd,
                        },
                    )
                )
            except Exception as e:
                print(f"        [WRITE-ERR] insert {sym} {start}: {e} (skip)")
                traceback.print_exc()
                add_coin_stat(ci, skipped_delta=1)
                continue

            while len(pending_writes) >= WRITE_CONCURRENCY:
                drain_one_write()

    while pending_writes:
        drain_one_write()

    # Optional aggregate rebuild from stored rows (max stability)
    if (
        (run_mode == "gapfill" and REBUILD_AGG_AFTER_GAPFILL)
        or (run_mode == "append" and REBUILD_AGG_AFTER_APPEND)
    ):
        target_slots = [s for (s, _e) in slots]
        if target_slots:
            print(f"[{now_str()}] [rebuild-agg] recomputing aggregates for {len(target_slots)} slots (mode={run_mode})")
            rebuilt_totals, rebuilt_counts = recompute_slot_totals_from_table(
                target_slots,
                coins,
                accumulate_into_map,
            )
            slot_totals = rebuilt_totals
            slot_coin_counts = rebuilt_counts
            print(f"[{now_str()}] [rebuild-agg] done: slots={len(slot_totals)}")

    # Carry-forward for low-coverage slots (avoid partial totals)
    if AGG_CARRY_FORWARD and AGG_MIN_COINS:
        for slot_start, count in list(slot_coin_counts.items()):
            if count >= AGG_MIN_COINS:
                continue
            carry_catmap: Dict[str, Dict[str, Any]] = {}
            for category in sorted(categories_seen.union({"ALL"})):
                prev = read_prev_mcap(category, slot_start)
                if prev:
                    carry_catmap[category] = {
                        "market_cap": prev["market_cap"],
                        "volume_24h": prev["volume_24h"],
                        "last_updated": prev["last_updated"],
                        "market_cap_rank": prev.get("market_cap_rank"),
                    }
            if "ALL" not in carry_catmap:
                continue
            slot_totals[slot_start] = carry_catmap
            carry_forward_slots.add(slot_start)

    # Write category aggregates (with ranks)
    if slot_totals and ((run_mode != "gapfill") or GAPFILL_WRITE_AGG):
        print(f"[{now_str()}] [mcap-10m] writing aggregates for {len(slot_totals)} slots into {TABLE_MCAP_OUT}")
        agg_written = 0
        for slot_start in sorted(slot_totals.keys()):
            if AGG_MIN_COINS:
                coin_count = slot_coin_counts.get(slot_start, 0)
                if coin_count < AGG_MIN_COINS and slot_start not in carry_forward_slots:
                    print(
                        f"[{now_str()}] [mcap-10m] skip slot {slot_start} (coins={coin_count} < min={AGG_MIN_COINS})"
                    )
                    continue
            catmap = slot_totals[slot_start]  # Dict[str, {market_cap, volume_24h, last_updated}]
            if slot_start not in carry_forward_slots:
                catmap = ensure_all_bucket(catmap)
            # compute ranks for this slot (ALL=0; others by market_cap desc)
            ranks = compute_category_ranks(catmap)
            # write in defined order: ALL first, then alphabetical for determinism
            for category in sorted(catmap.keys(), key=lambda c: (0 if c == "ALL" else 1, c.lower())):
                totals = catmap[category]
                last_upd = totals.get('last_updated') or (slot_start + timedelta(minutes=SLOT_MINUTES) - timedelta(seconds=1))
                rank_value = ranks.get(category, None)
                try:
                    session.execute(
                        INS_MCAP_10M_UPSERT,
                        [category, slot_start, last_upd, float(totals['market_cap']), rank_value, float(totals['volume_24h'])],
                        timeout=REQUEST_TIMEOUT
                    )
                    agg_written += 1
                except Exception as e:
                    print(f"        [mcap-10m] insert failed for category='{category}' slot={slot_start}: {e}")
        print(f"[{now_str()}] [mcap-10m] rows_written={agg_written}")
    else:
        if run_mode == "gapfill" and not GAPFILL_WRITE_AGG:
            print(f"[{now_str()}] [mcap-10m] skipped aggregates (gapfill mode; GAPFILL_WRITE_AGG=0)")
        else:
            print(f"[{now_str()}] [mcap-10m] no aggregates captured (coins={len(coins)})")

    print(f"[{now_str()}] [10m] wrote={wrote} skipped={skipped}")

# ───────────────────────── Entrypoint ─────────────────────────
if __name__ == "__main__":
    try:
        main()
    finally:
        print(f"[{now_str()}] Shutting down…")
        try:
            cluster.shutdown()
        except Exception as e:
            print(f"[{now_str()}] Error during shutdown: {e}")
        print(f"[{now_str()}] Done.")
