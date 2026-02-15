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
from typing import Tuple, List, Dict, Any

# ───────────────────────── Astra connector ─────────────────────────
from astra_connect.connect import get_session, AstraConfig
AstraConfig.from_env()

# ───────────────────────── Config ─────────────────────────
TOP_N              = int(os.getenv("TOP_N", "300"))

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

def to_utc(x: datetime) -> datetime:
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
session, cluster = get_session(return_cluster=True)
print(f"[{now_str()}] Connected. keyspace='{session.keyspace}'")

from cassandra.query import SimpleStatement

KEYSPACE = (os.getenv("ASTRA_KEYSPACE") or session.keyspace).strip() or session.keyspace

SEL_COINS = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank, category FROM {TABLE_LATEST}",
    fetch_size=FETCH_SIZE
)

# Latest point within the slot (rolling is clustered on last_updated)
SEL_IN_SLOT_PS = session.prepare(
    f"""
    SELECT last_updated, price_usd, market_cap, volume_24h,
           market_cap_rank, circulating_supply, total_supply
    FROM {TABLE_ROLLING}
    WHERE id=? AND last_updated>=? AND last_updated<? LIMIT 1
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

    for ci, c in enumerate(coins, 1):
        sym = getattr(c, "symbol", None)
        name = getattr(c, "name", None)
        mkr = getattr(c, "market_cap_rank", None)
        coin_id = getattr(c, "id")
        print(f"[{now_str()}] → [{ci}/{len(coins)}] {sym} ({coin_id}) rank={mkr}")
        coin_category = (getattr(c, 'category', None) or 'Other').strip() or 'Other'
        categories_seen.add(coin_category)
        carry_used = 0
        # In gapfill mode, discover which 10m slots already exist for this coin in the window.
        existing_ts: set = set()
        if run_mode == "gapfill" and slots:
            w_start = slots[0][0]
            w_end   = slots[-1][1]
            try:
                rs = session.execute(
                    SEL_10M_EXISTING_TS_RANGE_PS,
                    [coin_id, w_start, w_end],
                    timeout=REQUEST_TIMEOUT
                )
                for r in rs:
                    ts0 = getattr(r, "ts", None)
                    if isinstance(ts0, datetime):
                        existing_ts.add(to_utc(ts0))
                print(f"[{now_str()}]    [gapfill] existing_10m_in_window={len(existing_ts)} "
                      f"window={w_start}..{w_end}")
            except Exception as e:
                print(f"[{now_str()}]    [gapfill][WARN] failed to read existing 10m range: {e}")
                existing_ts = set()

        for si, (start, end) in enumerate(slots, 1):
            # print each slot with minimal noise
            print(f"    slot {si}/{len(slots)} {start} → {end}")

            # gapfill mode: skip if this slot already exists
            if run_mode == "gapfill":
                if start in existing_ts:
                    if GAPFILL_WRITE_AGG:
                        existing_vals = read_existing_slot(coin_id, start, end)
                        if existing_vals:
                            mcap_exist, vol_exist, lu_exist = existing_vals
                            bump_slot_total(start, coin_category, mcap_exist, vol_exist, lu_exist)
                            bump_slot_total(start, 'ALL', mcap_exist, vol_exist, lu_exist)
                            bump_slot_coin_count(start)
                        else:
                            print("        exists -> skip (no row for agg)")
                    else:
                        print("        exists -> skip")
                    skipped += 1
                    continue

            # try to read a row *inside* the slot window
            try:
                in_slot = session.execute(SEL_IN_SLOT_PS, [coin_id, start, end], timeout=REQUEST_TIMEOUT).one()
            except Exception as e:
                print(f"        [READ-ERR] in-slot {sym} {start}→{end}: {e} (skip)"); skipped += 1; continue

            if in_slot and in_slot.price_usd is not None:
                price = float(in_slot.price_usd)
                mcap  = float(in_slot.market_cap) if in_slot.market_cap is not None else 0.0
                vol   = float(in_slot.volume_24h) if in_slot.volume_24h is not None else 0.0
                rank  = int(in_slot.market_cap_rank) if in_slot.market_cap_rank is not None else None
                circ  = float(in_slot.circulating_supply) if in_slot.circulating_supply is not None else None
                totl  = float(in_slot.total_supply) if in_slot.total_supply is not None else None
                source = "hist-in-slot"
                # reset carry streak when we have a true in-slot point
                carry_used = 0
            else:
                if carry_used >= ALLOW_CARRY_MAX_SLOTS:
                    print("        carry cap reached → skip"); skipped += 1; continue
                # otherwise carry: grab latest point *before* slot start
                try:
                    prev = session.execute(SEL_PREV_PS, [coin_id, start], timeout=REQUEST_TIMEOUT).one()
                except Exception as e:
                    print(f"        [READ-ERR] prev {sym} < {start}: {e} (skip)"); skipped += 1; continue

                if prev and prev.price_usd is not None:
                    price = float(prev.price_usd)
                    mcap  = float(prev.market_cap) if prev.market_cap is not None else 0.0
                    vol   = float(prev.volume_24h) if prev.volume_24h is not None else 0.0
                    rank  = int(prev.market_cap_rank) if prev.market_cap_rank is not None else None
                    circ  = float(prev.circulating_supply) if prev.circulating_supply is not None else None
                    totl  = float(prev.total_supply) if prev.total_supply is not None else None
                    source = "hist-carry"
                    carry_used += 1
                else:
                    print("        no history for slot (and no previous) → skip")
                    skipped += 1
                    continue

            # clamp last_updated to slot end (represents slot's EoS)
            slot_last_upd = end - timedelta(seconds=1)

            try:
                result = session.execute(
                    INS_10M_IF_NOT_EXISTS_PS,
                    [coin_id, start, sym, name, price, mcap, vol, rank, circ, totl, slot_last_upd],
                    timeout=REQUEST_TIMEOUT
                ).one()
                applied = bool(getattr(result, 'applied', True)) if result is not None else True

                # accumulate aggregates only in normal append mode, OR if explicitly enabled for gapfill
                if (run_mode != "gapfill") or GAPFILL_WRITE_AGG:
                    if applied:
                        bump_slot_total(start, coin_category, mcap, vol, slot_last_upd)
                        bump_slot_total(start, 'ALL',          mcap, vol, slot_last_upd)
                        bump_slot_coin_count(start)
                    else:
                        existing_vals = read_existing_slot(coin_id, start, end)
                        if existing_vals:
                            mcap_exist, vol_exist, lu_exist = existing_vals
                            bump_slot_total(start, coin_category, mcap_exist, vol_exist, lu_exist)
                            bump_slot_total(start, 'ALL',          mcap_exist, vol_exist, lu_exist)
                            bump_slot_coin_count(start)
                        else:
                            # Fallback: avoid dropping totals if the row is unexpectedly missing.
                            bump_slot_total(start, coin_category, mcap, vol, slot_last_upd)
                            bump_slot_total(start, 'ALL',          mcap, vol, slot_last_upd)
                            bump_slot_coin_count(start)
                            print("        [WARN] insert not applied but existing row missing; aggregated computed values")

                print(f"        insert {'applied' if applied else 'skipped'} "
                      f"({source}, price={price}, mcap={mcap}, vol={vol}, rank={rank}, circ={circ}, totl={totl}, last_upd={slot_last_upd})")

                if applied: wrote += 1
                else:       skipped += 1

            except Exception as e:
                print(f"        [WRITE-ERR] insert {sym} {start}: {e} (skip)")
                traceback.print_exc()
                skipped += 1

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
