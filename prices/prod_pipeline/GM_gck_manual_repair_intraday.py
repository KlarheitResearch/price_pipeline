#!/usr/bin/env python3
"""
GM_gck_manual_repair_intraday.py

Manual API repair utility for intraday tables.

Strategy:
- Probe local 10m/hourly rows first.
- Build exact target slots that are missing or locally derived (`bf_*`).
- Skip CoinGecko entirely for coins with nothing to repair.
- Keep precise API repair limited to recent windows and top ranks by default.
"""

import argparse
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


def parse_utc(value: str, *, end_if_date: bool) -> datetime:
    value = value.strip()
    if len(value) == 10:
        d = datetime.strptime(value, "%Y-%m-%d")
        base = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        return base + timedelta(days=1) if end_if_date else base

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


def iter_10m_slots(start_dt: datetime, end_dt: datetime) -> list[datetime]:
    out: list[datetime] = []
    cur = floor_10m(start_dt)
    while cur < end_dt:
        out.append(cur)
        cur += timedelta(minutes=10)
    return out


def iter_hour_slots(start_dt: datetime, end_dt: datetime) -> list[datetime]:
    out: list[datetime] = []
    cur = floor_hour(start_dt)
    while cur < end_dt:
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
CG_CHUNK_HOURS = int(os.getenv("CG_CHUNK_HOURS", "72"))

TABLE_LIVE = os.getenv("TABLE_LIVE", "gecko_prices_live")
TABLE_10M = os.getenv("TABLE_OUT", os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d"))
TABLE_HOURLY = os.getenv("HOURLY_TABLE", "gecko_candles_hourly_30d")
_api_calls = 0


def http_get(path: str, params: dict | None = None) -> dict:
    global _api_calls
    _api_calls += 1
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


def fetch_market_chart_range(coin_id: str, start_dt: datetime, end_dt: datetime) -> tuple[list, list, list]:
    start_dt = to_utc(start_dt) or now_utc()
    end_dt = to_utc(end_dt) or now_utc()
    out_prices: list = []
    out_mcaps: list = []
    out_vols: list = []

    cur = start_dt
    while cur < end_dt:
        nxt = min(end_dt, cur + timedelta(hours=CG_CHUNK_HOURS))
        payload = http_get(
            f"/coins/{coin_id}/market_chart/range",
            params={
                "vs_currency": "usd",
                "from": int(cur.timestamp()),
                "to": int(nxt.timestamp()),
                "precision": "full",
            },
        )
        out_prices.extend(payload.get("prices", []) or [])
        out_mcaps.extend(payload.get("market_caps", []) or [])
        out_vols.extend(payload.get("total_volumes", []) or [])
        cur = nxt
        if PAUSE_PER_CALL_SEC > 0:
            time.sleep(PAUSE_PER_CALL_SEC)

    return out_prices, out_mcaps, out_vols


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
) -> list[datetime]:
    targets: list[datetime] = []
    for slot in expected_slots:
        source = existing_map.get(slot)
        if overwrite_existing:
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
    p.add_argument("--from-utc", type=str, required=True, help="UTC start (YYYY-MM-DD or ISO timestamp)")
    p.add_argument("--to-utc", type=str, required=True, help="UTC end exclusive (YYYY-MM-DD or ISO timestamp)")
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
        type=int,
        default=int(os.getenv("AUTHORITATIVE_10M_MAX_HOURS", "24")),
        help="Disable 10m API repair automatically when the selected window exceeds this many hours.",
    )
    p.add_argument(
        "--allow-broad-ranks",
        action="store_true",
        help="Allow API repair beyond --precise-rank-end.",
    )
    p.add_argument("--overwrite-existing", action="store_true", help="Overwrite rows even if they already exist")
    p.add_argument("--dry-run", action="store_true", help="Compute only; do not write")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    rank_start = int(args.rank_start)
    rank_end = int(args.rank_end)
    if rank_start <= 0 or rank_end <= 0 or rank_end < rank_start:
        raise SystemExit("Invalid rank window. Require rank_start>=1 and rank_end>=rank_start.")

    start_dt = parse_utc(args.from_utc, end_if_date=False)
    end_dt = parse_utc(args.to_utc, end_if_date=True)
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
    if requested_10m and window_hours > float(args.authoritative_10m_max_hours):
        do_10m = False
        print(
            f"[{now_str()}] INFO: disabling 10m API repair because window_hours={window_hours:.1f} "
            f"> authoritative_10m_max_hours={args.authoritative_10m_max_hours}"
        )
    do_hourly = requested_hourly

    print(
        f"[{now_str()}] Manual repair config: ranks={rank_start}-{rank_end}, "
        f"window={start_dt.isoformat()} -> {end_dt.isoformat()}, granularity={args.granularity}, "
        f"effective_10m={do_10m}, effective_hourly={do_hourly}, "
        f"overwrite={args.overwrite_existing}, replace_derived={args.replace_derived}, dry_run={dry_run}, "
        f"ids_filter={len(coin_ids_filter)}, keys={len(KEY_POOL.keys)}, tier={API_TIER}"
    )
    if requested_10m and not do_10m:
        print(
            f"[{now_str()}] WARN: CoinGecko market_chart/range is not authoritative for multi-day 10m repair. "
            f"Use continuity backfill for longer windows and keep precise 10m repair to recent windows."
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
            ) if do_10m else []
            target_slots_hourly = build_target_slots(
                expected_hourly_slots,
                existing_hourly,
                overwrite_existing=args.overwrite_existing,
                replace_derived=args.replace_derived,
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

            prices, mcaps, vols = fetch_market_chart_range(coin.id, start_dt, end_dt)
            if not prices:
                print(f"[{now_str()}]    no price payload in range")
                unrepaired_10m_slots += len(target_slots_10m)
                unrepaired_hourly_slots += len(target_slots_hourly)
                continue

            b10_price = bucket_ohlc(prices, floor_10m, start_dt, end_dt) if target_slots_10m else {}
            bh_price = bucket_ohlc(prices, floor_hour, start_dt, end_dt) if target_slots_hourly else {}
            b10_mcap = bucket_last_value(mcaps, floor_10m, start_dt, end_dt) if target_slots_10m else {}
            bh_mcap = bucket_last_value(mcaps, floor_hour, start_dt, end_dt) if target_slots_hourly else {}
            b10_vol = bucket_last_value(vols, floor_10m, start_dt, end_dt) if target_slots_10m else {}
            bh_vol = bucket_last_value(vols, floor_hour, start_dt, end_dt) if target_slots_hourly else {}

            for slot in target_slots_10m:
                p = b10_price.get(slot)
                if not p:
                    unrepaired_10m_slots += 1
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
                            "manual_api",
                            int_or_none(p.get("point_count")) or 1,
                            None,
                        ],
                    )
                wrote_10m += 1
                if (wrote_10m % WRITE_BATCH_SIZE) == 0 and (not dry_run):
                    session.execute(b10)
                    b10.clear()

            for slot in target_slots_hourly:
                p = bh_price.get(slot)
                if not p:
                    unrepaired_hourly_slots += 1
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
                            "manual_api",
                            to_cassandra_ts(lu),
                            int_or_none(p.get("point_count")) or 1,
                        ],
                    )
                wrote_hourly += 1
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
        f"skipped_clean={skipped_clean}, target_10m_slots={target_10m_slots}, "
        f"target_hourly_slots={target_hourly_slots}, unrepaired_10m_slots={unrepaired_10m_slots}, "
        f"unrepaired_hourly_slots={unrepaired_hourly_slots}, errors={errors}, dry_run={dry_run}"
    )

    try:
        cluster.shutdown()
    except Exception:
        pass


if __name__ == "__main__":
    main()
