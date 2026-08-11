#!/usr/bin/env python3
"""
EF_gck_close_daily_topn_api.py

Finalize UTC day windows with true API-driven daily candles
for top-ranked coins only (default rank window 1..300).

Reads:
  - gecko_prices_live

Writes:
  - gecko_candles_daily_contin
  - optional: gecko_market_cap_daily_contin for the target day(s)
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


def inum(x, fallback=0):
    try:
        if x is None:
            return fallback
        return int(x)
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


def parse_iso_day(raw: str) -> date:
    return datetime.strptime(raw.strip(), "%Y-%m-%d").date()


def normalize_day(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def days_inclusive(start_day: date, end_day: date) -> list[date]:
    out: list[date] = []
    cur = start_day
    while cur <= end_day:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def parse_day_window_utc() -> list[date]:
    start_raw = (os.getenv("TARGET_START_DAY_ISO") or "").strip()
    end_raw = (os.getenv("TARGET_END_DAY_ISO") or "").strip()
    target_raw = (os.getenv("TARGET_DAY_ISO") or "").strip()

    if start_raw or end_raw:
        start_day = parse_iso_day(start_raw or end_raw)
        end_day = parse_iso_day(end_raw or start_raw)
        if end_day < start_day:
            raise SystemExit("Invalid day window. TARGET_END_DAY_ISO must be >= TARGET_START_DAY_ISO.")
        # CoinGecko Demo supports explicit hourly granularity for windows up to
        # 100 days. Keep each request inside that boundary so daily OHLC is
        # built from intraday observations rather than one daily close.
        max_days = min(100, parse_int_env("TARGET_MAX_DAYS", 100))
        days = days_inclusive(start_day, end_day)
        if max_days > 0 and len(days) > max_days:
            raise SystemExit(
                f"Day window too large ({len(days)} > {max_days}). "
                f"Adjust TARGET_MAX_DAYS if you really need this."
            )
        return days

    if target_raw:
        return [parse_iso_day(target_raw)]

    lookback_days = max(1, min(100, parse_int_env("DEFAULT_LOOKBACK_DAYS", 7)))
    end_day = (now_utc() - timedelta(days=1)).date()
    return days_inclusive(end_day - timedelta(days=lookback_days - 1), end_day)


def day_bounds_utc(d: date) -> tuple[datetime, datetime]:
    s = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return s, s + timedelta(days=1)


def parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = str(raw).strip()
    if not text:
        return default
    try:
        return int(float(text))
    except Exception:
        return default


def parse_coin_ids_env(name: str = "COIN_IDS") -> set[str]:
    raw = os.getenv(name) or ""
    return {token.strip().lower() for token in raw.split(",") if token.strip()}


TOP_N = int(os.getenv("TOP_N_API_DAILY", "300"))
TOP_N_AGG = int(os.getenv("TOP_N_AGG_DAILY", "1000"))
RANK_START = parse_int_env("RANK_START", 1)
RANK_END = parse_int_env("RANK_END", TOP_N)
COIN_IDS_FILTER = parse_coin_ids_env("COIN_IDS")
FETCH_SIZE = int(os.getenv("FETCH_SIZE", "500"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
RETRIES = int(os.getenv("RETRIES", "3"))
PAUSE_PER_COIN_SEC = float(os.getenv("PAUSE_PER_COIN_SEC", "0.05"))
WRITE_BATCH_SIZE = int(os.getenv("WRITE_BATCH_SIZE", "50"))
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
REBUILD_MCAP_DAILY = os.getenv("REBUILD_MCAP_DAILY", "1") == "1"
PRESERVE_EXISTING_MARKET_FIELDS = os.getenv("PRESERVE_EXISTING_MARKET_FIELDS", "0") == "1"
REPAIR_QUALITY_ONLY = os.getenv("REPAIR_QUALITY_ONLY", "0") == "1"
API_ATTEMPT_BUDGET = max(0, int(os.getenv("API_ATTEMPT_BUDGET", str(TOP_N))))
MIN_API_POINTS_PER_DAY = max(2, int(os.getenv("MIN_API_POINTS_PER_DAY", "2")))
CG_DAILY_INTERVAL = (os.getenv("CG_DAILY_INTERVAL", "hourly") or "hourly").strip().lower()

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
    f"[{now_str()}] Config: top_n={TOP_N}, top_n_agg={TOP_N_AGG}, rank_start={RANK_START}, "
    f"rank_end={RANK_END}, coin_ids_filter={len(COIN_IDS_FILTER)}, dry_run={DRY_RUN}, "
    f"rebuild_mcap_daily={REBUILD_MCAP_DAILY}, preserve_existing_market_fields={PRESERVE_EXISTING_MARKET_FIELDS}, "
    f"repair_quality_only={REPAIR_QUALITY_ONLY}, "
    f"keys={len(KEY_POOL.keys)}, tier={API_TIER}, "
    f"interval={CG_DAILY_INTERVAL}, min_points_per_day={MIN_API_POINTS_PER_DAY}, "
    f"api_attempt_budget={API_ATTEMPT_BUDGET}"
)


API_ATTEMPTS = 0


def before_api_attempt() -> None:
    global API_ATTEMPTS
    if API_ATTEMPTS >= API_ATTEMPT_BUDGET:
        raise RuntimeError(
            f"CoinGecko API-attempt budget exhausted "
            f"({API_ATTEMPTS}/{API_ATTEMPT_BUDGET})"
        )
    API_ATTEMPTS += 1


def http_get(path: str, params: dict | None = None) -> dict:
    t0 = time.perf_counter()
    out = cg_http_get(
        base_url=BASE,
        path=path,
        params=params,
        retries=RETRIES,
        timeout_sec=REQUEST_TIMEOUT,
        key_pool=KEY_POOL,
        before_attempt=before_api_attempt,
    )
    dt = time.perf_counter() - t0
    print(f"[{now_str()}] API OK {path} in {dt:.2f}s")
    return out


def bucket_daily_payload_map(payload: dict, target_days: set[date]) -> dict[date, dict]:
    def keep_days(values):
        out: dict[date, list[tuple[datetime, float]]] = {}
        for ms, val in values or []:
            ts = datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc)
            d = ts.date()
            if d in target_days and val is not None:
                out.setdefault(d, []).append((ts, float(val)))
        for points in out.values():
            points.sort(key=lambda t: t[0])
        return out

    price_map = keep_days(payload.get("prices", []))
    mcap_map = keep_days(payload.get("market_caps", []))
    vol_map = keep_days(payload.get("total_volumes", []))

    out: dict[date, dict] = {}
    for day in sorted(target_days):
        prices = price_map.get(day) or []
        if len(prices) < MIN_API_POINTS_PER_DAY:
            continue
        mcaps = mcap_map.get(day) or []
        vols = vol_map.get(day) or []

        vals = [p for _, p in prices]
        o = vals[0]
        h = max(vals)
        l = min(vals)
        c = vals[-1]
        last_ts = prices[-1][0]
        mcap = mcaps[-1][1] if mcaps else None
        vol = vols[-1][1] if vols else None

        out[day] = {
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "price_usd": c,
            "market_cap": mcap,
            "volume_24h": vol,
            "last_updated": last_ts,
            "candle_source": "api_daily_final",
            "point_count": len(prices),
        }
    return out


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
    SELECT open, high, low, close, market_cap, volume_24h, candle_source, point_count, last_updated
    FROM {TABLE_DAILY}
    WHERE id = ? AND date = ? LIMIT 1
    """
)

SEL_DAILY_RANGE = session.prepare(
    f"""
    SELECT date, open, high, low, close, market_cap, volume_24h,
           candle_source, point_count, last_updated
    FROM {TABLE_DAILY}
    WHERE id = ? AND date >= ? AND date <= ?
    """
)

INS_DAILY = session.prepare(
    f"""
    INSERT INTO {TABLE_DAILY}
      (id, date, symbol, name,
       open, high, low, close, price_usd,
       market_cap, volume_24h,
       market_cap_rank, circulating_supply, total_supply,
       candle_source, last_updated, point_count)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        and (inum(getattr(existing, "point_count", None), 0) == int(candidate["point_count"]))
    )


def existing_needs_quality_repair(existing, target_day: date) -> bool:
    if not existing:
        return True
    values = [fnum(getattr(existing, key, None)) for key in ("open", "high", "low", "close")]
    if any(value is None for value in values):
        return True
    open_, high, low, close = values
    if high < max(open_, close) or low > min(open_, close) or high < low:
        return True
    last_updated = to_utc(getattr(existing, "last_updated", None))
    if last_updated is not None and last_updated.date() < target_day:
        return True
    source = (getattr(existing, "candle_source", None) or "").strip().lower()
    if any(token in source for token in ("carry", "interp", "partial")):
        return True
    point_count = inum(getattr(existing, "point_count", None), 0)
    is_flat = max(open_, high, low, close) == min(open_, high, low, close)
    api_source = source in {"api_daily_final", "daily_api", "cg_daily_final", "legacy_api"}
    return is_flat and (point_count < 2 or not api_source)


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
    if RANK_START < 1 or RANK_END < RANK_START:
        raise SystemExit("Invalid rank window. Require RANK_START>=1 and RANK_END>=RANK_START.")

    target_days = parse_day_window_utc()
    target_day_set = set(target_days)
    range_start, _ = day_bounds_utc(target_days[0])
    _, range_end_excl = day_bounds_utc(target_days[-1])
    from_ts = int(range_start.timestamp())
    to_ts = int(range_end_excl.timestamp())

    print(
        f"[{now_str()}] Target days: {target_days[0]} .. {target_days[-1]} (count={len(target_days)}) "
        f"[inclusive]"
    )
    print(f"[{now_str()}] API window: {range_start.isoformat()} -> {range_end_excl.isoformat()} (exclusive)")

    live_rows = list(session.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    ranked = [r for r in live_rows if isinstance(getattr(r, "market_cap_rank", None), int) and r.market_cap_rank > 0]
    ranked.sort(key=lambda r: r.market_cap_rank)

    top_for_daily = [
        row
        for row in ranked
        if RANK_START <= int(getattr(row, "market_cap_rank", 0)) <= RANK_END
    ]
    if COIN_IDS_FILTER:
        top_for_daily = [
            row for row in top_for_daily if (getattr(row, "id", "") or "").strip().lower() in COIN_IDS_FILTER
        ]
        selected_ids = {(getattr(r, "id", "") or "").strip().lower() for r in top_for_daily}
        missing_ids = sorted(COIN_IDS_FILTER - selected_ids)
        if missing_ids:
            print(
                f"[{now_str()}] WARN: {len(missing_ids)} filtered ids are not in selected rank window: "
                f"{', '.join(missing_ids[:25])}{' ...' if len(missing_ids) > 25 else ''}"
            )

    agg_cutoff = max(TOP_N_AGG, RANK_END)
    top_for_agg = ranked[:agg_cutoff]

    print(
        f"[{now_str()}] Live ranked universe: {len(ranked)} | daily_close_scope={len(top_for_daily)} "
        f"(rank[{RANK_START}-{RANK_END}])"
    )

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
            existing_by_day = {
                normalize_day(existing.date): existing
                for existing in session.execute(
                    SEL_DAILY_RANGE,
                    [coin_id, target_days[0], target_days[-1]],
                    timeout=REQUEST_TIMEOUT,
                )
            }
            payload = http_get(
                f"/coins/{coin_id}/market_chart/range",
                params={
                    "vs_currency": "usd",
                    "from": from_ts,
                    "to": to_ts,
                    "precision": "full",
                    "interval": CG_DAILY_INTERVAL,
                },
            )
            candles_by_day = bucket_daily_payload_map(payload, target_day_set)
            for target_day in target_days:
                candle = candles_by_day.get(target_day)
                if not candle:
                    skipped_empty += 1
                    continue

                existing = existing_by_day.get(target_day)
                if REPAIR_QUALITY_ONLY and not existing_needs_quality_repair(existing, target_day):
                    skipped_equal += 1
                    continue
                if PRESERVE_EXISTING_MARKET_FIELDS and existing:
                    candle = dict(candle)
                    existing_mcap = fnum(getattr(existing, "market_cap", None))
                    existing_vol = fnum(getattr(existing, "volume_24h", None))
                    if existing_mcap is not None:
                        candle["market_cap"] = existing_mcap
                    if existing_vol is not None:
                        candle["volume_24h"] = existing_vol
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
                            int(candle["point_count"]),
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
        for target_day in target_days:
            print(
                f"[{now_str()}] Recomputing daily market-cap aggregates for {target_day} "
                f"from top {len(top_for_agg)} rows..."
            )
            mcap_rows += recompute_mcap_daily_for_day(target_day, top_for_agg)

    print(
        f"[{now_str()}] Done. days={len(target_days)}, wrote_daily={wrote}, skipped_equal={skipped_equal}, "
        f"skipped_empty={skipped_empty}, errors={errors}, mcap_rows={mcap_rows}, "
        f"api_attempts={API_ATTEMPTS}/{API_ATTEMPT_BUDGET}, dry_run={DRY_RUN}"
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
