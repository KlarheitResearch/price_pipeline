#!/usr/bin/env python3
"""
GM_gck_manual_repair_intraday.py

Credit-aware repair utility for intraday tables.

Strategy:
- Probe local 10m/hourly rows first.
- Build exact target slots that are missing or locally derived (`bf_*`).
- Rebuild from the local rolling table before spending CoinGecko credits.
- Use at most one long-range hourly request per coin (up to 100 days).
- Enforce an explicit API-call budget and validate response coverage before writing.
- Keep API repair limited to recent windows and top ranks by default.
"""

import argparse
import bisect
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, cast

from cassandra.cluster import Cluster, Session
from cassandra.query import BatchStatement, ConsistencyLevel, SimpleStatement

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


def fnum(x: Any, fallback=None):
    try:
        if x is None:
            return fallback
        return float(x)
    except Exception:
        return fallback


def int_or_none(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def floor_10m(dt_: datetime) -> datetime:
    dt_ = to_utc(dt_) or now_utc()
    return dt_.replace(minute=(dt_.minute // 10) * 10, second=0, microsecond=0)


def floor_hour(dt_: datetime) -> datetime:
    dt_ = to_utc(dt_) or now_utc()
    return dt_.replace(minute=0, second=0, microsecond=0)


def ceil_10m(dt_: datetime) -> datetime:
    floored = floor_10m(dt_)
    return floored if to_utc(dt_) == floored else floored + timedelta(minutes=10)


def ceil_hour(dt_: datetime) -> datetime:
    floored = floor_hour(dt_)
    return floored if to_utc(dt_) == floored else floored + timedelta(hours=1)


def parse_utc(value: str) -> datetime:
    value = value.strip()
    if len(value) == 10:
        d = datetime.strptime(value, "%Y-%m-%d")
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)

    dt_ = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt_.tzinfo is None:
        dt_ = dt_.replace(tzinfo=timezone.utc)
    else:
        dt_ = dt_.astimezone(timezone.utc)
    return dt_


def safe_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.encode("ascii", "backslashreplace").decode("ascii")


def source_is_derived(source: str | None) -> bool:
    return str(source or "").strip().lower().startswith("bf_")


def source_is_authoritative_api(source: str | None) -> bool:
    """Sources that should make an overwrite repair credit-idempotent."""
    return str(source or "").strip().lower() in {
        "hourly_api",
        "manual_api",
        "manual_api_hourly",
    }


def iter_10m_slots(start_dt: datetime, end_dt: datetime) -> list[datetime]:
    out: list[datetime] = []
    cur = ceil_10m(start_dt)
    end_exclusive = floor_10m(end_dt)
    while cur < end_exclusive:
        out.append(cur)
        cur += timedelta(minutes=10)
    return out


def iter_hour_slots(start_dt: datetime, end_dt: datetime) -> list[datetime]:
    out: list[datetime] = []
    cur = ceil_hour(start_dt)
    end_exclusive = floor_hour(end_dt)
    while cur < end_exclusive:
        out.append(cur)
        cur += timedelta(hours=1)
    return out


@dataclass
class Coin:
    id: str
    symbol: str
    name: str
    rank: int | None
    circ: float | None
    totl: float | None


API_TIER = (os.getenv("COINGECKO_API_TIER") or "demo").strip().lower()
BASE = os.getenv(
    "COINGECKO_BASE_URL",
    "https://api.coingecko.com/api/v3" if API_TIER == "demo" else "https://pro-api.coingecko.com/api/v3",
)
KEY_POOL = build_key_pool()

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
FETCH_SIZE = int(os.getenv("FETCH_SIZE", "500"))
RETRIES = int(os.getenv("RETRIES", "3"))
PAUSE_PER_CALL_SEC = float(os.getenv("PAUSE_PER_CALL_SEC", "0.05"))
WRITE_BATCH_SIZE = int(os.getenv("WRITE_BATCH_SIZE", "100"))
CG_CHUNK_HOURS = int(os.getenv("CG_CHUNK_HOURS", str(100 * 24)))

TABLE_LIVE = os.getenv("TABLE_LIVE", "gecko_prices_live")
TABLE_ROLLING = os.getenv("TABLE_ROLLING", "gecko_prices_live_rolling")
TABLE_10M = os.getenv("TABLE_OUT", os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d"))
TABLE_HOURLY = os.getenv("HOURLY_TABLE", "gecko_candles_hourly_30d")
_api_calls = 0
_api_call_budget = 0


class ApiCallBudgetExceeded(RuntimeError):
    pass


def http_get(path: str, params: dict | None = None) -> dict:
    global _api_calls
    last_error: Exception | None = None
    max_attempts = max(1, RETRIES)
    for attempt in range(1, max_attempts + 1):
        if _api_calls >= _api_call_budget:
            raise ApiCallBudgetExceeded(
                f"CoinGecko API-attempt budget exhausted ({_api_calls}/{_api_call_budget})"
            )
        # Count immediately before the actual HTTP attempt. The shared helper
        # gets one attempt so internal retries cannot bypass this run's budget.
        _api_calls += 1
        t0 = time.perf_counter()
        try:
            out = cg_http_get(
                base_url=BASE,
                path=path,
                params=params,
                retries=1,
                timeout_sec=REQUEST_TIMEOUT,
                key_pool=KEY_POOL,
            )
            elapsed = time.perf_counter() - t0
            print(f"[{now_str()}] API OK {path} in {elapsed:.2f}s")
            return out
        except RuntimeError as exc:
            last_error = exc
            print(
                f"[{now_str()}] API attempt failed {path} "
                f"({attempt}/{max_attempts}, budget={_api_calls}/{_api_call_budget}): "
                f"{safe_text(exc)}"
            )
    raise RuntimeError(f"CoinGecko request failed after {max_attempts} attempt(s): {last_error}")


def fetch_market_chart_range(
    coin_id: str,
    start_dt: datetime,
    end_dt: datetime,
    *,
    interval: str | None,
) -> tuple[list, list, list]:
    start_dt = to_utc(start_dt) or now_utc()
    end_dt = to_utc(end_dt) or now_utc()
    out_prices: list = []
    out_mcaps: list = []
    out_vols: list = []

    cur = start_dt
    while cur < end_dt:
        nxt = min(end_dt, cur + timedelta(hours=CG_CHUNK_HOURS))
        params = {
            "vs_currency": "usd",
            "from": int(cur.timestamp()),
            "to": int(nxt.timestamp()),
            "precision": "full",
        }
        if interval:
            params["interval"] = interval
        payload = http_get(
            f"/coins/{coin_id}/market_chart/range",
            params=params,
        )
        out_prices.extend(payload.get("prices", []) or [])
        out_mcaps.extend(payload.get("market_caps", []) or [])
        out_vols.extend(payload.get("total_volumes", []) or [])
        cur = nxt
        if PAUSE_PER_CALL_SEC > 0:
            time.sleep(PAUSE_PER_CALL_SEC)

    return out_prices, out_mcaps, out_vols


def rolling_rows_to_series(rows: list[Any]) -> tuple[list, list, list]:
    prices: list[list[float]] = []
    mcaps: list[list[float]] = []
    vols: list[list[float]] = []
    for row in rows:
        ts = to_utc(getattr(row, "last_updated", None))
        if ts is None:
            continue
        ms = int(ts.timestamp() * 1000)
        price = fnum(getattr(row, "price_usd", None))
        mcap = fnum(getattr(row, "market_cap", None))
        vol = fnum(getattr(row, "volume_24h", None))
        if price is not None:
            prices.append([ms, price])
        if mcap is not None:
            mcaps.append([ms, mcap])
        if vol is not None:
            vols.append([ms, vol])
    return prices, mcaps, vols


def coverage_ratio(slots: list[datetime], buckets: dict[datetime, Any]) -> float:
    if not slots:
        return 1.0
    return sum(1 for slot in slots if slot in buckets) / float(len(slots))


def interpolate_local_gaps(
    target_slots: list[datetime],
    price_buckets: dict[datetime, dict[str, Any]],
    value_buckets: tuple[
        dict[datetime, tuple[float, datetime]],
        dict[datetime, tuple[float, datetime]],
    ],
    *,
    max_gap: timedelta,
) -> set[datetime]:
    """Linearly fill bounded gaps; never extrapolate beyond retained rolling data."""
    known = sorted(price_buckets)
    filled: set[datetime] = set()
    if len(known) < 2 or max_gap.total_seconds() <= 0:
        return filled

    for slot in target_slots:
        if slot in price_buckets:
            continue
        pos = bisect.bisect_left(known, slot)
        if pos <= 0 or pos >= len(known):
            continue
        left = known[pos - 1]
        right = known[pos]
        span = right - left
        if span > max_gap or span.total_seconds() <= 0:
            continue
        left_close = fnum(price_buckets[left].get("close"))
        right_close = fnum(price_buckets[right].get("close"))
        if left_close is None or right_close is None:
            continue
        weight = (slot - left).total_seconds() / span.total_seconds()
        close = left_close + (right_close - left_close) * weight
        price_buckets[slot] = {
            "open": left_close,
            "high": max(left_close, close),
            "low": min(left_close, close),
            "close": close,
            "last_ts": slot,
            "point_count": 0,
        }
        for buckets in value_buckets:
            left_value = buckets.get(left)
            right_value = buckets.get(right)
            if left_value is None or right_value is None:
                continue
            value = left_value[0] + (right_value[0] - left_value[0]) * weight
            buckets[slot] = (value, slot)
        filled.add(slot)
    return filled


def resolve_window(args: argparse.Namespace) -> tuple[datetime, datetime]:
    has_explicit = bool(args.from_utc or args.to_utc)
    if has_explicit:
        if not args.from_utc or not args.to_utc:
            raise SystemExit("Provide both --from-utc and --to-utc, or use --lookback-hours.")
        if args.lookback_hours is not None:
            raise SystemExit("Do not combine explicit UTC bounds with --lookback-hours.")
        return parse_utc(args.from_utc), parse_utc(args.to_utc)

    lookback_hours = float(args.lookback_hours if args.lookback_hours is not None else 23.5)
    if lookback_hours <= 0:
        raise SystemExit("--lookback-hours must be positive.")
    end_dt = floor_10m(now_utc())
    return end_dt - timedelta(hours=lookback_hours), end_dt


def bucket_last_value(points: list, floor_fn, start_dt: datetime, end_dt: datetime) -> dict[datetime, tuple[float, datetime]]:
    out: dict[datetime, tuple[float, datetime]] = {}
    for ms, val in points or []:
        if val is None:
            continue
        ts = datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc)
        if ts < start_dt or ts >= end_dt:
            continue
        slot = floor_fn(ts)
        prev = out.get(slot)
        if (prev is None) or (ts >= prev[1]):
            out[slot] = (float(val), ts)
    return out


def bucket_ohlc(points: list, floor_fn, start_dt: datetime, end_dt: datetime) -> dict[datetime, dict[str, Any]]:
    buckets: dict[datetime, dict[str, Any]] = {}
    pts = []
    for ms, val in points or []:
        if val is None:
            continue
        ts = datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc)
        if ts < start_dt or ts >= end_dt:
            continue
        pts.append((ts, float(val)))
    pts.sort(key=lambda x: x[0])

    for ts, price in pts:
        slot = floor_fn(ts)
        b = buckets.get(slot)
        if b is None:
            buckets[slot] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "last_ts": ts,
                "point_count": 1,
            }
            continue
        b["close"] = price
        b["last_ts"] = ts
        b["point_count"] = int(b.get("point_count", 1)) + 1
        if price > b["high"]:
            b["high"] = price
        if price < b["low"]:
            b["low"] = price
    return buckets


def load_existing_sources(
    session: Session,
    prepared_stmt,
    coin_id: str,
    start_dt: datetime,
    end_dt: datetime,
) -> dict[datetime, str | None]:
    rows = session.execute(
        prepared_stmt,
        [coin_id, to_cassandra_ts(start_dt), to_cassandra_ts(end_dt)],
        timeout=REQUEST_TIMEOUT,
    )
    out: dict[datetime, str | None] = {}
    for row in rows:
        slot = to_utc(getattr(row, "ts", None))
        if slot is None:
            continue
        out[slot] = getattr(row, "candle_source", None)
    return out


def build_target_slots(
    expected_slots: list[datetime],
    existing_map: dict[datetime, str | None],
    *,
    overwrite_existing: bool,
    replace_derived: bool,
    overwrite_authoritative: bool = False,
) -> list[datetime]:
    targets: list[datetime] = []
    for slot in expected_slots:
        source = existing_map.get(slot)
        if overwrite_existing:
            if (not overwrite_authoritative) and source_is_authoritative_api(source):
                continue
            targets.append(slot)
            continue
        if source is None:
            targets.append(slot)
            continue
        if replace_derived and source_is_derived(source):
            targets.append(slot)
    return targets


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Manual API repair for 10m/hourly datasets.")
    p.add_argument("--rank-start", type=int, required=True, help="Inclusive start rank (e.g. 1)")
    p.add_argument("--rank-end", type=int, required=True, help="Inclusive end rank (e.g. 100)")
    p.add_argument(
        "--coin-ids",
        type=str,
        default="",
        help=(
            "Optional comma-separated CoinGecko ids. "
            "If set, only matching ids are repaired (still constrained by rank window)."
        ),
    )
    p.add_argument("--from-utc", type=str, default="", help="UTC start (YYYY-MM-DD or ISO timestamp)")
    p.add_argument("--to-utc", type=str, default="", help="UTC end exclusive (YYYY-MM-DD or ISO timestamp)")
    p.add_argument(
        "--lookback-hours",
        type=float,
        default=None,
        help="Dynamic lookback ending now. Defaults to 23.5h when explicit bounds are omitted.",
    )
    p.add_argument("--granularity", choices=["10m", "hourly", "both"], default="both")
    p.add_argument(
        "--replace-derived",
        action="store_true",
        help="Repair slots that currently exist only as local bf_* carry/derived rows.",
    )
    p.add_argument(
        "--precise-rank-end",
        type=int,
        default=int(os.getenv("PRECISE_RANK_END", "100")),
        help="Ranks above this threshold are skipped unless --allow-broad-ranks is set.",
    )
    p.add_argument(
        "--authoritative-10m-max-hours",
        type=float,
        default=float(os.getenv("AUTHORITATIVE_10M_MAX_HOURS", "23.9")),
        help="Maximum window eligible for Demo auto-5m API fallback; local rolling repair may be longer.",
    )
    p.add_argument(
        "--allow-broad-ranks",
        action="store_true",
        help="Allow API repair beyond --precise-rank-end.",
    )
    p.add_argument("--overwrite-existing", action="store_true", help="Overwrite rows even if they already exist")
    p.add_argument(
        "--overwrite-authoritative",
        action="store_true",
        help=(
            "Also replace existing manual_api/hourly_api rows. By default these rows are preserved "
            "so rerunning an overwrite repair does not spend the same credits again."
        ),
    )
    p.add_argument(
        "--repair-source",
        choices=["local-first", "local-only", "api-only"],
        default=os.getenv("REPAIR_SOURCE", "local-first"),
        help="Prefer zero-credit rolling data, prohibit API use, or use API only.",
    )
    p.add_argument(
        "--api-call-budget",
        type=int,
        default=int(os.getenv("API_CALL_BUDGET", "25")),
        help="Hard CoinGecko HTTP-attempt cap for the entire run, including retries.",
    )
    p.add_argument(
        "--min-source-coverage-ratio",
        type=float,
        default=float(os.getenv("MIN_SOURCE_COVERAGE_RATIO", "0.85")),
        help="Reject an API response when it covers less than this share of target slots.",
    )
    p.add_argument(
        "--local-interp-max-gap-hours",
        type=float,
        default=float(os.getenv("LOCAL_INTERP_MAX_GAP_HOURS", "2")),
        help="Zero-credit interpolation limit between retained rolling observations; 0 disables it.",
    )
    p.add_argument(
        "--fail-on-unrepaired",
        action="store_true",
        help="Exit nonzero when target slots remain unrepaired or a coin failed.",
    )
    p.add_argument("--dry-run", action="store_true", help="Compute only; do not write")
    return p.parse_args()


def main() -> None:
    global _api_call_budget
    args = parse_args()
    rank_start = int(args.rank_start)
    rank_end = int(args.rank_end)
    if rank_start <= 0 or rank_end <= 0 or rank_end < rank_start:
        raise SystemExit("Invalid rank window. Require rank_start>=1 and rank_end>=rank_start.")

    start_dt, end_dt = resolve_window(args)
    if end_dt <= start_dt:
        raise SystemExit("Invalid time window. to_utc must be after from_utc.")

    requested_10m = args.granularity in ("10m", "both")
    requested_hourly = args.granularity in ("hourly", "both")
    dry_run = bool(args.dry_run)
    coin_ids_filter = {
        token.strip().lower() for token in str(args.coin_ids or "").split(",") if token.strip()
    }
    window_hours = max(1.0, (end_dt - start_dt).total_seconds() / 3600.0)

    do_10m = requested_10m
    do_hourly = requested_hourly
    allow_local = args.repair_source in ("local-first", "local-only")
    allow_api = args.repair_source in ("local-first", "api-only")
    api_10m_allowed = allow_api and window_hours < float(args.authoritative_10m_max_hours)
    _api_call_budget = max(0, int(args.api_call_budget)) if allow_api else 0
    min_source_coverage = min(1.0, max(0.0, float(args.min_source_coverage_ratio)))

    print(
        f"[{now_str()}] Manual repair config: ranks={rank_start}-{rank_end}, "
        f"window={start_dt.isoformat()} -> {end_dt.isoformat()}, granularity={args.granularity}, "
        f"effective_10m={do_10m}, effective_hourly={do_hourly}, "
        f"overwrite={args.overwrite_existing}, overwrite_authoritative={args.overwrite_authoritative}, "
        f"replace_derived={args.replace_derived}, dry_run={dry_run}, "
        f"repair_source={args.repair_source}, api_budget={_api_call_budget}, "
        f"min_source_coverage={min_source_coverage:.2f}, ids_filter={len(coin_ids_filter)}, "
        f"keys={len(KEY_POOL.keys)}, tier={API_TIER}"
    )
    if requested_10m and allow_api and not api_10m_allowed:
        print(
            f"[{now_str()}] INFO: 10m API fallback disabled because window_hours={window_hours:.2f} "
            f">= authoritative_10m_max_hours={args.authoritative_10m_max_hours}; "
            f"local rolling repair remains enabled={allow_local}."
        )

    print(f"[{now_str()}] Connecting to Astra...")
    session, cluster = cast(tuple[Session, Cluster], get_session(return_cluster=True))
    print(f"[{now_str()}] Connected. keyspace='{session.keyspace}'")

    sel_live = SimpleStatement(
        f"""
        SELECT id, symbol, name, market_cap_rank, circulating_supply, total_supply
        FROM {TABLE_LIVE}
        """,
        fetch_size=FETCH_SIZE,
    )

    sel_10m_range = session.prepare(
        f"""
        SELECT ts, candle_source
        FROM {TABLE_10M}
        WHERE id = ? AND ts >= ? AND ts < ?
        """
    )
    sel_hourly_range = session.prepare(
        f"""
        SELECT ts, candle_source
        FROM {TABLE_HOURLY}
        WHERE id = ? AND ts >= ? AND ts < ?
        """
    )
    sel_rolling_range = session.prepare(
        f"""
        SELECT last_updated, price_usd, market_cap, volume_24h
        FROM {TABLE_ROLLING}
        WHERE id = ? AND last_updated >= ? AND last_updated < ?
        """
    )

    ins_10m = session.prepare(
        f"""
        INSERT INTO {TABLE_10M}
          (id, ts, symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
           market_cap_rank, circulating_supply, total_supply, last_updated,
           candle_source, point_count, volume_interval_est)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    ins_hourly = session.prepare(
        f"""
        INSERT INTO {TABLE_HOURLY}
          (id, ts, symbol, name,
           open, high, low, close, price_usd,
           market_cap, volume_24h,
           market_cap_rank, circulating_supply, total_supply,
           candle_source, last_updated, point_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT))
    ranked = [r for r in rows if isinstance(getattr(r, "market_cap_rank", None), int) and r.market_cap_rank > 0]
    ranked.sort(key=lambda r: r.market_cap_rank)

    effective_precise_rank_end = rank_end
    if not args.allow_broad_ranks and rank_end > args.precise_rank_end:
        effective_precise_rank_end = args.precise_rank_end
        print(
            f"[{now_str()}] INFO: limiting precise API repair to ranks 1-{args.precise_rank_end}. "
            f"Use --allow-broad-ranks to override."
        )

    selected = []
    broad_rank_skips = 0
    for row in ranked:
        rank = int(row.market_cap_rank)
        if rank < rank_start or rank > rank_end:
            continue
        if (not args.allow_broad_ranks) and rank > effective_precise_rank_end:
            broad_rank_skips += 1
            continue
        coin_id = (getattr(row, "id", "") or "").strip().lower()
        if coin_ids_filter and coin_id not in coin_ids_filter:
            continue
        selected.append(row)

    if coin_ids_filter:
        selected_ids = {(getattr(r, "id", "") or "").strip().lower() for r in selected}
        missing_ids = sorted(coin_ids_filter - selected_ids)
        if missing_ids:
            print(
                f"[{now_str()}] WARN: {len(missing_ids)} filtered ids are not in selected rank window: "
                f"{', '.join(missing_ids[:25])}{' ...' if len(missing_ids) > 25 else ''}"
            )
    print(
        f"[{now_str()}] Selected {len(selected)} coin(s) for precise repair. "
        f"broad_rank_skips={broad_rank_skips}"
    )

    expected_10m_slots = iter_10m_slots(start_dt, end_dt) if do_10m else []
    expected_hourly_slots = iter_hour_slots(start_dt, end_dt) if do_hourly else []

    wrote_10m = 0
    wrote_hourly = 0
    skipped_clean = 0
    target_10m_slots = 0
    target_hourly_slots = 0
    unrepaired_10m_slots = 0
    unrepaired_hourly_slots = 0
    wrote_10m_local = 0
    wrote_10m_api = 0
    wrote_hourly_local = 0
    wrote_hourly_api = 0
    errors = 0

    b10 = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    bh = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    for i, r in enumerate(selected, 1):
        coin = Coin(
            id=getattr(r, "id", ""),
            symbol=(getattr(r, "symbol", "") or "").upper(),
            name=getattr(r, "name", "") or getattr(r, "id", ""),
            rank=getattr(r, "market_cap_rank", None),
            circ=fnum(getattr(r, "circulating_supply", None)),
            totl=fnum(getattr(r, "total_supply", None)),
        )
        print(
            f"[{now_str()}] -> {i}/{len(selected)} "
            f"{safe_text(coin.symbol)} ({safe_text(coin.id)}) rank={coin.rank}"
        )

        try:
            existing_10m = (
                load_existing_sources(session, sel_10m_range, coin.id, start_dt, end_dt)
                if do_10m else {}
            )
            existing_hourly = (
                load_existing_sources(session, sel_hourly_range, coin.id, start_dt, end_dt)
                if do_hourly else {}
            )

            target_slots_10m = build_target_slots(
                expected_10m_slots,
                existing_10m,
                overwrite_existing=args.overwrite_existing,
                replace_derived=args.replace_derived,
                overwrite_authoritative=args.overwrite_authoritative,
            ) if do_10m else []
            target_slots_hourly = build_target_slots(
                expected_hourly_slots,
                existing_hourly,
                overwrite_existing=args.overwrite_existing,
                replace_derived=args.replace_derived,
                overwrite_authoritative=args.overwrite_authoritative,
            ) if do_hourly else []

            target_10m_slots += len(target_slots_10m)
            target_hourly_slots += len(target_slots_hourly)

            if not target_slots_10m and not target_slots_hourly:
                skipped_clean += 1
                print(f"[{now_str()}]    local probe clean; no API call")
                continue

            print(
                f"[{now_str()}]    target slots: 10m={len(target_slots_10m)} "
                f"hourly={len(target_slots_hourly)}"
            )

            b10_price: dict[datetime, dict[str, Any]] = {}
            bh_price: dict[datetime, dict[str, Any]] = {}
            b10_mcap: dict[datetime, tuple[float, datetime]] = {}
            bh_mcap: dict[datetime, tuple[float, datetime]] = {}
            b10_vol: dict[datetime, tuple[float, datetime]] = {}
            bh_vol: dict[datetime, tuple[float, datetime]] = {}
            source_10m: dict[datetime, str] = {}
            source_hourly: dict[datetime, str] = {}

            if allow_local and (target_slots_10m or target_slots_hourly):
                rolling_rows = list(
                    session.execute(
                        sel_rolling_range,
                        [coin.id, to_cassandra_ts(start_dt), to_cassandra_ts(end_dt)],
                        timeout=REQUEST_TIMEOUT,
                    )
                )
                local_prices, local_mcaps, local_vols = rolling_rows_to_series(rolling_rows)
                if target_slots_10m:
                    local_b10_price = bucket_ohlc(local_prices, floor_10m, start_dt, end_dt)
                    local_b10_mcap = bucket_last_value(local_mcaps, floor_10m, start_dt, end_dt)
                    local_b10_vol = bucket_last_value(local_vols, floor_10m, start_dt, end_dt)
                    for slot in target_slots_10m:
                        if slot in local_b10_price:
                            b10_price[slot] = local_b10_price[slot]
                            if slot in local_b10_mcap:
                                b10_mcap[slot] = local_b10_mcap[slot]
                            if slot in local_b10_vol:
                                b10_vol[slot] = local_b10_vol[slot]
                            source_10m[slot] = "repair_rolling"
                if target_slots_hourly:
                    local_bh_price = bucket_ohlc(local_prices, floor_hour, start_dt, end_dt)
                    local_bh_mcap = bucket_last_value(local_mcaps, floor_hour, start_dt, end_dt)
                    local_bh_vol = bucket_last_value(local_vols, floor_hour, start_dt, end_dt)
                    for slot in target_slots_hourly:
                        if slot in local_bh_price:
                            bh_price[slot] = local_bh_price[slot]
                            if slot in local_bh_mcap:
                                bh_mcap[slot] = local_bh_mcap[slot]
                            if slot in local_bh_vol:
                                bh_vol[slot] = local_bh_vol[slot]
                            source_hourly[slot] = "repair_rolling"

                interp_gap = timedelta(hours=max(0.0, float(args.local_interp_max_gap_hours)))
                interpolated_10m = interpolate_local_gaps(
                    target_slots_10m,
                    b10_price,
                    (b10_mcap, b10_vol),
                    max_gap=interp_gap,
                )
                for slot in interpolated_10m:
                    source_10m[slot] = "repair_rolling_interp"
                interpolated_hourly = interpolate_local_gaps(
                    target_slots_hourly,
                    bh_price,
                    (bh_mcap, bh_vol),
                    max_gap=interp_gap,
                )
                for slot in interpolated_hourly:
                    source_hourly[slot] = "repair_rolling_interp"
                print(
                    f"[{now_str()}]    rolling coverage: "
                    f"10m={len(source_10m)}/{len(target_slots_10m)}, "
                    f"hourly={len(source_hourly)}/{len(target_slots_hourly)}, "
                    f"interpolated_10m={len(interpolated_10m)}, "
                    f"interpolated_hourly={len(interpolated_hourly)}"
                )

            need_api_10m = [
                slot for slot in target_slots_10m if slot not in b10_price
            ] if api_10m_allowed else []
            need_api_hourly = [slot for slot in target_slots_hourly if slot not in bh_price]

            if allow_api and (need_api_10m or need_api_hourly):
                interval = None if need_api_10m else "hourly"
                try:
                    prices, mcaps, vols = fetch_market_chart_range(
                        coin.id,
                        start_dt,
                        end_dt,
                        interval=interval,
                    )
                except ApiCallBudgetExceeded as e:
                    print(f"[{now_str()}]    {safe_text(e)}; keeping local repairs only")
                    prices, mcaps, vols = [], [], []

                if prices and need_api_10m:
                    api_b10_price = bucket_ohlc(prices, floor_10m, start_dt, end_dt)
                    ratio = coverage_ratio(need_api_10m, api_b10_price)
                    if ratio < min_source_coverage:
                        print(
                            f"[{now_str()}]    rejecting 10m API payload: coverage={ratio:.3f} "
                            f"< {min_source_coverage:.3f} (likely hourly auto-granularity)"
                        )
                    else:
                        api_b10_mcap = bucket_last_value(mcaps, floor_10m, start_dt, end_dt)
                        api_b10_vol = bucket_last_value(vols, floor_10m, start_dt, end_dt)
                        for slot in need_api_10m:
                            if slot in api_b10_price:
                                b10_price[slot] = api_b10_price[slot]
                                if slot in api_b10_mcap:
                                    b10_mcap[slot] = api_b10_mcap[slot]
                                if slot in api_b10_vol:
                                    b10_vol[slot] = api_b10_vol[slot]
                                source_10m[slot] = "manual_api"

                if prices and need_api_hourly:
                    api_bh_price = bucket_ohlc(prices, floor_hour, start_dt, end_dt)
                    ratio = coverage_ratio(need_api_hourly, api_bh_price)
                    if ratio < min_source_coverage:
                        print(
                            f"[{now_str()}]    rejecting hourly API payload: coverage={ratio:.3f} "
                            f"< {min_source_coverage:.3f}"
                        )
                    else:
                        api_bh_mcap = bucket_last_value(mcaps, floor_hour, start_dt, end_dt)
                        api_bh_vol = bucket_last_value(vols, floor_hour, start_dt, end_dt)
                        for slot in need_api_hourly:
                            if slot in api_bh_price:
                                bh_price[slot] = api_bh_price[slot]
                                if slot in api_bh_mcap:
                                    bh_mcap[slot] = api_bh_mcap[slot]
                                if slot in api_bh_vol:
                                    bh_vol[slot] = api_bh_vol[slot]
                                source_hourly[slot] = "manual_api_hourly"

            missing_10m = sum(1 for slot in target_slots_10m if slot not in b10_price)
            missing_hourly = sum(1 for slot in target_slots_hourly if slot not in bh_price)
            unrepaired_10m_slots += missing_10m
            unrepaired_hourly_slots += missing_hourly
            if missing_10m or missing_hourly:
                print(
                    f"[{now_str()}]    unrepaired after source selection: "
                    f"10m={missing_10m}, hourly={missing_hourly}"
                )

            for slot in target_slots_10m:
                p = b10_price.get(slot)
                if not p:
                    continue
                price = fnum(p["close"])
                mcap, ts_m = b10_mcap.get(slot, (None, None))
                vol, ts_v = b10_vol.get(slot, (None, None))
                lu = max([x for x in (p.get("last_ts"), ts_m, ts_v) if x is not None], default=slot)

                if not dry_run:
                    b10.add(
                        ins_10m,
                        [
                            coin.id,
                            to_cassandra_ts(slot),
                            coin.symbol,
                            coin.name,
                            fnum(p.get("open"), price),
                            fnum(p.get("high"), price),
                            fnum(p.get("low"), price),
                            price,
                            price,
                            fnum(mcap),
                            fnum(vol),
                            int(coin.rank) if coin.rank is not None else None,
                            coin.circ,
                            coin.totl,
                            to_cassandra_ts(lu),
                            source_10m.get(slot, "repair_unknown"),
                            int_or_none(p.get("point_count")) or 1,
                            None,
                        ],
                    )
                wrote_10m += 1
                if source_10m.get(slot) == "manual_api":
                    wrote_10m_api += 1
                else:
                    wrote_10m_local += 1
                if (wrote_10m % WRITE_BATCH_SIZE) == 0 and (not dry_run):
                    session.execute(b10)
                    b10.clear()

            for slot in target_slots_hourly:
                p = bh_price.get(slot)
                if not p:
                    continue
                o = fnum(p["open"])
                h = fnum(p["high"])
                l = fnum(p["low"])
                c = fnum(p["close"])
                mcap, ts_m = bh_mcap.get(slot, (None, None))
                vol, ts_v = bh_vol.get(slot, (None, None))
                lu = max([x for x in (p.get("last_ts"), ts_m, ts_v) if x is not None], default=slot)

                if not dry_run:
                    bh.add(
                        ins_hourly,
                        [
                            coin.id,
                            to_cassandra_ts(slot),
                            coin.symbol,
                            coin.name,
                            o,
                            h,
                            l,
                            c,
                            c,
                            fnum(mcap),
                            fnum(vol),
                            int(coin.rank) if coin.rank is not None else None,
                            coin.circ,
                            coin.totl,
                            source_hourly.get(slot, "repair_unknown"),
                            to_cassandra_ts(lu),
                            int_or_none(p.get("point_count")) or 1,
                        ],
                    )
                wrote_hourly += 1
                if source_hourly.get(slot) == "manual_api_hourly":
                    wrote_hourly_api += 1
                else:
                    wrote_hourly_local += 1
                if (wrote_hourly % WRITE_BATCH_SIZE) == 0 and (not dry_run):
                    session.execute(bh)
                    bh.clear()

        except Exception as e:
            errors += 1
            print(f"[{now_str()}]    error: {safe_text(e)}")

    if (not dry_run) and len(b10):
        session.execute(b10)
    if (not dry_run) and len(bh):
        session.execute(bh)

    print(
        f"[{now_str()}] Done. api_calls={_api_calls}, wrote_10m={wrote_10m}, wrote_hourly={wrote_hourly}, "
        f"wrote_10m_local={wrote_10m_local}, wrote_10m_api={wrote_10m_api}, "
        f"wrote_hourly_local={wrote_hourly_local}, wrote_hourly_api={wrote_hourly_api}, "
        f"skipped_clean={skipped_clean}, target_10m_slots={target_10m_slots}, "
        f"target_hourly_slots={target_hourly_slots}, unrepaired_10m_slots={unrepaired_10m_slots}, "
        f"unrepaired_hourly_slots={unrepaired_hourly_slots}, errors={errors}, dry_run={dry_run}"
    )

    try:
        cluster.shutdown()
    except Exception:
        pass

    if args.fail_on_unrepaired and (
        errors > 0 or unrepaired_10m_slots > 0 or unrepaired_hourly_slots > 0
    ):
        raise SystemExit(3)


if __name__ == "__main__":
    main()
