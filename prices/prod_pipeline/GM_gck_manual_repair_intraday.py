#!/usr/bin/env python3
"""
GM_gck_manual_repair_intraday.py

Manual API repair utility for intraday tables.
Repairs a chosen rank window and UTC time range for:
  - 10m table (gecko_prices_10m_7d)
  - hourly table (gecko_candles_hourly_30d)
  - or both
"""

import argparse
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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


def bucket_last_value(points: list, floor_fn, start_dt: datetime, end_dt: datetime) -> dict:
    out = {}
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


def bucket_ohlc(points: list, floor_fn, start_dt: datetime, end_dt: datetime) -> dict:
    buckets = {}
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
            }
            continue
        b["close"] = price
        b["last_ts"] = ts
        if price > b["high"]:
            b["high"] = price
        if price < b["low"]:
            b["low"] = price
    return buckets


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

    do_10m = args.granularity in ("10m", "both")
    do_hourly = args.granularity in ("hourly", "both")
    dry_run = bool(args.dry_run)
    coin_ids_filter = {
        token.strip().lower() for token in str(args.coin_ids or "").split(",") if token.strip()
    }

    print(
        f"[{now_str()}] Manual repair config: ranks={rank_start}-{rank_end}, "
        f"window={start_dt.isoformat()} -> {end_dt.isoformat()}, granularity={args.granularity}, "
        f"overwrite={args.overwrite_existing}, dry_run={dry_run}, ids_filter={len(coin_ids_filter)}, "
        f"keys={len(KEY_POOL.keys)}, tier={API_TIER}"
    )
    if do_10m and (end_dt - start_dt) > timedelta(days=7):
        print(f"[{now_str()}] WARN: 10m table is typically 7d-scoped; range exceeds 7 days.")
    if do_10m and (end_dt - start_dt) > timedelta(days=1):
        print(
            f"[{now_str()}] WARN: CoinGecko market_chart/range auto-granularity is hourly for "
            f"windows beyond 1 day, so multi-day 10m repair will be sparse/non-authoritative. "
            f"Use this mainly for hourly repair, or limit 10m repair to the most recent <=24h window."
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

    sel_10m_one = session.prepare(f"SELECT ts FROM {TABLE_10M} WHERE id = ? AND ts = ? LIMIT 1")
    sel_hourly_one = session.prepare(f"SELECT ts FROM {TABLE_HOURLY} WHERE id = ? AND ts = ? LIMIT 1")

    ins_10m = session.prepare(
        f"""
        INSERT INTO {TABLE_10M}
          (id, ts, symbol, name, price_usd, market_cap, volume_24h,
           market_cap_rank, circulating_supply, total_supply, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    ins_hourly = session.prepare(
        f"""
        INSERT INTO {TABLE_HOURLY}
          (id, ts, symbol, name,
           open, high, low, close, price_usd,
           market_cap, volume_24h,
           market_cap_rank, circulating_supply, total_supply,
           candle_source, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT))
    ranked = [r for r in rows if isinstance(getattr(r, "market_cap_rank", None), int) and r.market_cap_rank > 0]
    ranked.sort(key=lambda r: r.market_cap_rank)
    selected = []
    for row in ranked:
        rank = int(row.market_cap_rank)
        if rank < rank_start or rank > rank_end:
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
    print(f"[{now_str()}] Selected {len(selected)} coin(s) in rank window.")

    wrote_10m = 0
    wrote_hourly = 0
    skipped_existing = 0
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
        print(f"[{now_str()}] -> {i}/{len(selected)} {coin.symbol} ({coin.id}) rank={coin.rank}")

        try:
            prices, mcaps, vols = fetch_market_chart_range(coin.id, start_dt, end_dt)
            if not prices:
                print(f"[{now_str()}]    no price payload in range")
                continue

            b10_price = bucket_ohlc(prices, floor_10m, start_dt, end_dt)
            bh_price = bucket_ohlc(prices, floor_hour, start_dt, end_dt)
            b10_mcap = bucket_last_value(mcaps, floor_10m, start_dt, end_dt)
            bh_mcap = bucket_last_value(mcaps, floor_hour, start_dt, end_dt)
            b10_vol = bucket_last_value(vols, floor_10m, start_dt, end_dt)
            bh_vol = bucket_last_value(vols, floor_hour, start_dt, end_dt)

            if do_10m:
                for slot in sorted(b10_price.keys()):
                    p = b10_price[slot]
                    price = fnum(p["close"])
                    mcap, ts_m = b10_mcap.get(slot, (None, None))
                    vol, ts_v = b10_vol.get(slot, (None, None))
                    lu = max([x for x in (p.get("last_ts"), ts_m, ts_v) if x is not None], default=slot)

                    if not args.overwrite_existing:
                        exists = session.execute(
                            sel_10m_one, [coin.id, to_cassandra_ts(slot)], timeout=REQUEST_TIMEOUT
                        ).one()
                        if exists:
                            skipped_existing += 1
                            continue

                    if not dry_run:
                        b10.add(
                            ins_10m,
                            [
                                coin.id,
                                to_cassandra_ts(slot),
                                coin.symbol,
                                coin.name,
                                price,
                                fnum(mcap),
                                fnum(vol),
                                int(coin.rank) if coin.rank is not None else None,
                                coin.circ,
                                coin.totl,
                                to_cassandra_ts(lu),
                            ],
                        )
                    wrote_10m += 1
                    if (wrote_10m % WRITE_BATCH_SIZE) == 0 and (not dry_run):
                        session.execute(b10)
                        b10.clear()

            if do_hourly:
                for slot in sorted(bh_price.keys()):
                    p = bh_price[slot]
                    o = fnum(p["open"])
                    h = fnum(p["high"])
                    l = fnum(p["low"])
                    c = fnum(p["close"])
                    mcap, ts_m = bh_mcap.get(slot, (None, None))
                    vol, ts_v = bh_vol.get(slot, (None, None))
                    lu = max([x for x in (p.get("last_ts"), ts_m, ts_v) if x is not None], default=slot)

                    if not args.overwrite_existing:
                        exists = session.execute(
                            sel_hourly_one, [coin.id, to_cassandra_ts(slot)], timeout=REQUEST_TIMEOUT
                        ).one()
                        if exists:
                            skipped_existing += 1
                            continue

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
                            ],
                        )
                    wrote_hourly += 1
                    if (wrote_hourly % WRITE_BATCH_SIZE) == 0 and (not dry_run):
                        session.execute(bh)
                        bh.clear()

        except Exception as e:
            errors += 1
            print(f"[{now_str()}]    error: {e}")

    if (not dry_run) and len(b10):
        session.execute(b10)
    if (not dry_run) and len(bh):
        session.execute(bh)

    print(
        f"[{now_str()}] Done. api_calls={_api_calls}, wrote_10m={wrote_10m}, wrote_hourly={wrote_hourly}, "
        f"skipped_existing={skipped_existing}, errors={errors}, dry_run={dry_run}"
    )

    try:
        cluster.shutdown()
    except Exception:
        pass


if __name__ == "__main__":
    main()
