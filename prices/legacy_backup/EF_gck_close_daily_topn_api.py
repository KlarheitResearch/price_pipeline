#!/usr/bin/env python3
"""
EF_gck_close_daily_topn_api.py

Finalize the previous UTC day (or TARGET_DAY_ISO) with true API-driven daily candles
for top-ranked coins only (default: top 100).

Reads:
  - gecko_prices_live

Writes:
  - gecko_candles_daily_contin
  - optional: gecko_market_cap_daily_contin for the target day
"""

import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import cast

from cassandra.query import BatchStatement, ConsistencyLevel, SimpleStatement
from cassandra.cluster import Cluster, Session

from astra_connect.connect import AstraConfig, get_session
from cg_key_pool import build_key_pool, cg_http_get

AstraConfig.from_env()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")


def to_utc(x: datetime | None) -> datetime | None:
    if x is None:
        return None
    if x.tzinfo is None:
        return x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def to_cassandra_ts(x: datetime) -> datetime:
    x = to_utc(x) or now_utc()
    return x.replace(tzinfo=None)


def fnum(x, fallback=None):
    try:
        if x is None:
            return fallback
        return float(x)
    except Exception:
        return fallback


def equalish(a, b, eps: float = 1e-12) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) <= eps
    except Exception:
        return False


def parse_day_utc() -> date:
    target = (os.getenv("TARGET_DAY_ISO") or "").strip()
    if target:
        return datetime.strptime(target, "%Y-%m-%d").date()
    return (now_utc() - timedelta(days=1)).date()


def day_bounds_utc(d: date) -> tuple[datetime, datetime]:
    s = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return s, s + timedelta(days=1)


TOP_N = int(os.getenv("TOP_N_API_DAILY", "100"))
TOP_N_AGG = int(os.getenv("TOP_N_AGG_DAILY", "1000"))
FETCH_SIZE = int(os.getenv("FETCH_SIZE", "500"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
RETRIES = int(os.getenv("RETRIES", "3"))
PAUSE_PER_COIN_SEC = float(os.getenv("PAUSE_PER_COIN_SEC", "0.05"))
WRITE_BATCH_SIZE = int(os.getenv("WRITE_BATCH_SIZE", "50"))
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
REBUILD_MCAP_DAILY = os.getenv("REBUILD_MCAP_DAILY", "1") == "1"

API_TIER = (os.getenv("COINGECKO_API_TIER") or "demo").strip().lower()
BASE = os.getenv(
    "COINGECKO_BASE_URL",
    "https://api.coingecko.com/api/v3" if API_TIER == "demo" else "https://pro-api.coingecko.com/api/v3",
)
KEY_POOL = build_key_pool()

TABLE_LIVE = os.getenv("TABLE_LIVE", "gecko_prices_live")
TABLE_DAILY = os.getenv("DAILY_TABLE", os.getenv("TABLE_DAILY", "gecko_candles_daily_contin"))
TABLE_MCAP_DAILY = os.getenv("TABLE_MCAP_DAILY", "gecko_market_cap_daily_contin")

print(
    f"[{now_str()}] Config: top_n={TOP_N}, top_n_agg={TOP_N_AGG}, dry_run={DRY_RUN}, "
    f"rebuild_mcap_daily={REBUILD_MCAP_DAILY}, keys={len(KEY_POOL.keys)}, tier={API_TIER}"
)


def http_get(path: str, params: dict | None = None) -> dict:
    t0 = time.perf_counter()
    out = cg_http_get(
        base_url=BASE,
        path=path,
        params=params,
        retries=RETRIES,
        timeout_sec=REQUEST_TIMEOUT,
        key_pool=KEY_POOL,
    )
    dt = time.perf_counter() - t0
    print(f"[{now_str()}] API OK {path} in {dt:.2f}s")
    return out


def bucket_daily_payload(payload: dict, target_day: date) -> dict | None:
    def keep_day(values):
        out = []
        for ms, val in values or []:
            ts = datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc)
            if ts.date() == target_day and val is not None:
                out.append((ts, float(val)))
        out.sort(key=lambda t: t[0])
        return out

    prices = keep_day(payload.get("prices", []))
    if not prices:
        return None

    mcaps = keep_day(payload.get("market_caps", []))
    vols = keep_day(payload.get("total_volumes", []))

    vals = [p for _, p in prices]
    o = vals[0]
    h = max(vals)
    l = min(vals)
    c = vals[-1]
    last_ts = prices[-1][0]
    mcap = mcaps[-1][1] if mcaps else None
    vol = vols[-1][1] if vols else None

    return {
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "price_usd": c,
        "market_cap": mcap,
        "volume_24h": vol,
        "last_updated": last_ts,
        "candle_source": "api_daily_final",
    }


print(f"[{now_str()}] Connecting to Astra...")
session, cluster = cast(tuple[Session, Cluster], get_session(return_cluster=True))
print(f"[{now_str()}] Connected. keyspace='{session.keyspace}'")

SEL_LIVE = SimpleStatement(
    f"""
    SELECT id, symbol, name, market_cap_rank, category, circulating_supply, total_supply
    FROM {TABLE_LIVE}
    """,
    fetch_size=FETCH_SIZE,
)

SEL_DAILY_ONE = session.prepare(
    f"""
    SELECT open, high, low, close, market_cap, volume_24h, candle_source
    FROM {TABLE_DAILY}
    WHERE id = ? AND date = ? LIMIT 1
    """
)

INS_DAILY = session.prepare(
    f"""
    INSERT INTO {TABLE_DAILY}
      (id, date, symbol, name,
       open, high, low, close, price_usd,
       market_cap, volume_24h,
       market_cap_rank, circulating_supply, total_supply,
       candle_source, last_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
)

SEL_DAILY_META = session.prepare(
    f"""
    SELECT market_cap, volume_24h, last_updated
    FROM {TABLE_DAILY}
    WHERE id = ? AND date = ? LIMIT 1
    """
)

INS_MCAP_DAILY = session.prepare(
    f"""
    INSERT INTO {TABLE_MCAP_DAILY}
      (category, date, last_updated, market_cap, market_cap_rank, volume_24h)
    VALUES (?, ?, ?, ?, ?, ?)
    """
)


def row_equal(existing, candidate: dict) -> bool:
    if not existing:
        return False
    return (
        equalish(getattr(existing, "open", None), candidate["open"])
        and equalish(getattr(existing, "high", None), candidate["high"])
        and equalish(getattr(existing, "low", None), candidate["low"])
        and equalish(getattr(existing, "close", None), candidate["close"])
        and equalish(getattr(existing, "market_cap", None), candidate["market_cap"])
        and equalish(getattr(existing, "volume_24h", None), candidate["volume_24h"])
        and (getattr(existing, "candle_source", None) == candidate["candle_source"])
    )


def recompute_mcap_daily_for_day(target_day: date, ranked_rows: list) -> int:
    if not ranked_rows:
        return 0

    totals: dict[str, dict] = {}

    def bump(cat: str, mcap: float, vol: float, last_upd: datetime) -> None:
        entry = totals.setdefault(cat, {"mcap": 0.0, "vol": 0.0, "last_updated": last_upd})
        entry["mcap"] += mcap
        entry["vol"] += vol
        if entry["last_updated"] is None or (last_upd and last_upd > entry["last_updated"]):
            entry["last_updated"] = last_upd

    for row in ranked_rows:
        coin_id = getattr(row, "id", None)
        if not coin_id:
            continue
        daily = session.execute(SEL_DAILY_META, [coin_id, target_day], timeout=REQUEST_TIMEOUT).one()
        if not daily:
            continue
        mcap = fnum(getattr(daily, "market_cap", None), 0.0) or 0.0
        vol = fnum(getattr(daily, "volume_24h", None), 0.0) or 0.0
        lu = to_utc(getattr(daily, "last_updated", None)) or datetime(
            target_day.year, target_day.month, target_day.day, 23, 59, 59, tzinfo=timezone.utc
        )
        category = (getattr(row, "category", None) or "Other").strip() or "Other"
        bump(category, mcap, vol, lu)
        bump("ALL", mcap, vol, lu)

    if not totals:
        return 0

    ranked = [(cat, entry["mcap"]) for cat, entry in totals.items() if cat != "ALL"]
    ranked.sort(key=lambda t: t[1], reverse=True)
    ranks = {cat: i + 1 for i, (cat, _) in enumerate(ranked)}
    ranks["ALL"] = 0

    ordered = sorted(totals.items(), key=lambda kv: (0 if kv[0] == "ALL" else 1, kv[0].lower()))
    wrote = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for cat, entry in ordered:
        if DRY_RUN:
            wrote += 1
            continue
        batch.add(
            INS_MCAP_DAILY,
            [
                cat,
                target_day,
                to_cassandra_ts(entry["last_updated"]),
                float(entry["mcap"]),
                ranks.get(cat),
                float(entry["vol"]),
            ],
        )
        wrote += 1
        if (wrote % WRITE_BATCH_SIZE) == 0:
            session.execute(batch)
            batch.clear()
    if (not DRY_RUN) and len(batch):
        session.execute(batch)
    return wrote


def main() -> None:
    target_day = parse_day_utc()
    day_start, day_end_excl = day_bounds_utc(target_day)
    from_ts = int(day_start.timestamp())
    to_ts = int(day_end_excl.timestamp())

    print(f"[{now_str()}] Target day: {target_day} ({day_start.isoformat()} -> {day_end_excl.isoformat()})")

    live_rows = list(session.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    ranked = [r for r in live_rows if isinstance(getattr(r, "market_cap_rank", None), int) and r.market_cap_rank > 0]
    ranked.sort(key=lambda r: r.market_cap_rank)

    top_for_daily = ranked[:TOP_N]
    top_for_agg = ranked[:max(TOP_N, TOP_N_AGG)]

    print(f"[{now_str()}] Live ranked universe: {len(ranked)} | daily_close_scope={len(top_for_daily)}")

    wrote = 0
    skipped_equal = 0
    skipped_empty = 0
    errors = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    for i, row in enumerate(top_for_daily, 1):
        coin_id = getattr(row, "id", None)
        symbol = (getattr(row, "symbol", None) or coin_id or "?").upper()
        name = getattr(row, "name", None) or coin_id
        rank = getattr(row, "market_cap_rank", None)
        circ = fnum(getattr(row, "circulating_supply", None))
        totl = fnum(getattr(row, "total_supply", None))

        print(f"[{now_str()}] -> {i}/{len(top_for_daily)} {symbol} ({coin_id}) rank={rank}")

        try:
            payload = http_get(
                f"/coins/{coin_id}/market_chart/range",
                params={
                    "vs_currency": "usd",
                    "from": from_ts,
                    "to": to_ts,
                    "precision": "full",
                },
            )
            candle = bucket_daily_payload(payload, target_day)
            if not candle:
                skipped_empty += 1
                print(f"[{now_str()}]    skip: no points for target day")
                continue

            existing = session.execute(SEL_DAILY_ONE, [coin_id, target_day], timeout=REQUEST_TIMEOUT).one()
            if row_equal(existing, candle):
                skipped_equal += 1
                continue

            if not DRY_RUN:
                batch.add(
                    INS_DAILY,
                    [
                        coin_id,
                        target_day,
                        symbol,
                        name,
                        float(candle["open"]),
                        float(candle["high"]),
                        float(candle["low"]),
                        float(candle["close"]),
                        float(candle["price_usd"]),
                        fnum(candle["market_cap"]),
                        fnum(candle["volume_24h"]),
                        int(rank) if rank is not None else None,
                        circ,
                        totl,
                        candle["candle_source"],
                        to_cassandra_ts(candle["last_updated"]),
                    ],
                )
            wrote += 1
            if (wrote % WRITE_BATCH_SIZE) == 0 and (not DRY_RUN):
                session.execute(batch)
                batch.clear()
        except Exception as e:
            errors += 1
            print(f"[{now_str()}]    error: {e}")

        if PAUSE_PER_COIN_SEC > 0:
            time.sleep(PAUSE_PER_COIN_SEC)

    if (not DRY_RUN) and len(batch):
        session.execute(batch)

    mcap_rows = 0
    if REBUILD_MCAP_DAILY:
        print(f"[{now_str()}] Recomputing daily market-cap aggregates for {target_day} from top {len(top_for_agg)} rows...")
        mcap_rows = recompute_mcap_daily_for_day(target_day, top_for_agg)

    print(
        f"[{now_str()}] Done. wrote_daily={wrote}, skipped_equal={skipped_equal}, "
        f"skipped_empty={skipped_empty}, errors={errors}, mcap_rows={mcap_rows}, dry_run={DRY_RUN}"
    )


if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            if "cluster" in globals():
                cluster.shutdown()
        except Exception:
            pass
