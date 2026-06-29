#!/usr/bin/env python3
"""
Rebuild live-facing Gecko tables from the freshest local raw rows.

Purpose:
- Recover `gecko_prices_live`, `gecko_prices_live_ranked`, and `gecko_market_cap_live`
  without calling CoinGecko.
- Use continuity-filled raw datasets when live API ingestion is stale.
- Keep lower-rank assets serviceable even if their latest values are carry-forward rows.
"""

from __future__ import annotations

import argparse
import csv
import os
import pathlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional, cast

from cassandra import DriverException, OperationTimedOut, WriteTimeout
from cassandra.cluster import Cluster, Session
from cassandra.query import BatchStatement, ConsistencyLevel, SimpleStatement

from astra_connect.connect import AstraConfig, get_session

AstraConfig.from_env()

UTC = timezone.utc


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")


def to_utc(dt_: datetime | None) -> datetime | None:
    if dt_ is None:
        return None
    if dt_.tzinfo is None:
        return dt_.replace(tzinfo=UTC)
    return dt_.astimezone(UTC)


def to_cassandra_ts(dt_: datetime | None) -> datetime | None:
    dt_ = to_utc(dt_)
    if dt_ is None:
        return None
    return dt_.replace(tzinfo=None)


def fnum(x: Any, fallback: Optional[float] = None) -> Optional[float]:
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


def safe_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.encode("ascii", "backslashreplace").decode("ascii")


def row_price(row: Any) -> Optional[float]:
    close_val = fnum(getattr(row, "close", None))
    if close_val is not None:
        return close_val
    return fnum(getattr(row, "price_usd", None))


@dataclass
class LiveSeed:
    coin_id: str
    symbol: str
    name: str
    rank: Optional[int]
    ath_price: Optional[float]
    ath_date: Optional[datetime]
    max_supply: Optional[float]


@dataclass
class RawSnapshot:
    slot_time: datetime
    last_updated: Optional[datetime]
    symbol: str
    name: str
    price_usd: Optional[float]
    market_cap: Optional[float]
    volume_24h: Optional[float]
    rank: Optional[int]
    circ: Optional[float]
    totl: Optional[float]


TOP_N = int(os.getenv("TOP_N", "1000"))
REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
BATCH_FLUSH_EVERY = int(os.getenv("BATCH_FLUSH_EVERY", "40"))
RAW_LOOKBACK_HOURS = int(os.getenv("LIVE_REFRESH_RAW_LOOKBACK_HOURS", "72"))
DAILY_LOOKBACK_DAYS = int(os.getenv("LIVE_REFRESH_DAILY_LOOKBACK_DAYS", "30"))
REQUIRED_LIVE_MIN = int(os.getenv("REQUIRED_LIVE_MIN", str(int(TOP_N * 0.7))))

TABLE_LIVE = os.getenv("TABLE_GECKO_LIVE", "gecko_prices_live")
TABLE_LIVE_RANKED = os.getenv("TABLE_GECKO_PRICES_LIVE_RANKED", "gecko_prices_live_ranked")
TABLE_MCAP_LIVE = os.getenv("TABLE_GECKO_MCAP_LIVE", "gecko_market_cap_live")
TABLE_10M = os.getenv("TABLE_OUT", os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d"))
TABLE_HOURLY = os.getenv("HOURLY_TABLE", "gecko_candles_hourly_30d")
TABLE_DAILY = os.getenv("DAILY_TABLE", "gecko_candles_daily_contin")
RANK_BUCKET = os.getenv("RANK_BUCKET", "all")
RANK_TOP_N = int(os.getenv("RANK_TOP_N", str(TOP_N)))
SENTINEL_UNRANKED = 2_000_000_000

_THIS_DIR = pathlib.Path(__file__).resolve().parent
_DEFAULT_CATEGORY_FILE_LOCAL = _THIS_DIR / "category_mapping.csv"
_DEFAULT_CATEGORY_FILE_SHARED = _THIS_DIR.parent / "category_mapping.csv"


def resolve_category_file() -> str:
    env_path = (os.getenv("CATEGORY_FILE") or "").strip()
    if env_path:
        return env_path
    if _DEFAULT_CATEGORY_FILE_LOCAL.exists():
        return str(_DEFAULT_CATEGORY_FILE_LOCAL)
    return str(_DEFAULT_CATEGORY_FILE_SHARED)


def load_category_map(path: str) -> tuple[dict[str, str], dict[str, str]]:
    id_map: dict[str, str] = {}
    sym_map: dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            for delim in [",", ";", "\t", "|"]:
                f.seek(0)
                reader = csv.DictReader(f, delimiter=delim)
                fieldnames = reader.fieldnames
                if not fieldnames:
                    continue
                headers = [h.strip().lower() for h in fieldnames]
                if "category" not in headers:
                    continue
                id_key = fieldnames[headers.index("id")] if "id" in headers else None
                sym_key = fieldnames[headers.index("symbol")] if "symbol" in headers else None
                cat_key = fieldnames[headers.index("category")]
                if id_key is None and sym_key is None:
                    continue
                for row in reader:
                    cat = (row.get(cat_key) or "").strip() or "Other"
                    if id_key:
                        cid = (row.get(id_key) or "").strip().lower()
                        if cid:
                            id_map[cid] = cat
                    if sym_key:
                        sym = (row.get(sym_key) or "").strip().upper()
                        if sym:
                            sym_map[sym] = cat
                print(f"[{now_str()}] [category] loaded id={len(id_map)} symbol={len(sym_map)} from {path}")
                break
    except FileNotFoundError:
        print(f"[{now_str()}] [category] file not found: {path}")
    except Exception as exc:
        print(f"[{now_str()}] [category] failed to read {path}: {safe_text(exc)}")
    return id_map, sym_map


CATEGORY_FILE = resolve_category_file()
ID_CATEGORY_MAP, SYMBOL_CATEGORY_MAP = load_category_map(CATEGORY_FILE)


def category_for(coin_id: str, symbol: str) -> str:
    cid = (coin_id or "").strip().lower()
    if cid and cid in ID_CATEGORY_MAP:
        return ID_CATEGORY_MAP[cid]
    sym = (symbol or "").strip().upper()
    if sym and sym in SYMBOL_CATEGORY_MAP:
        return SYMBOL_CATEGORY_MAP[sym]
    return "Other"


def safe_rank_from_live(vals: list[Any]) -> int:
    rank_value = vals[4]
    try:
        rank_int = int(rank_value)
        return rank_int if rank_int > 0 else SENTINEL_UNRANKED
    except Exception:
        return SENTINEL_UNRANKED


def load_latest_snapshot(
    session: Session,
    ps_10m,
    ps_hourly,
    ps_daily,
    coin_id: str,
    intraday_start: datetime,
    intraday_end: datetime,
    daily_start: date,
    daily_end: date,
) -> Optional[RawSnapshot]:
    best: Optional[RawSnapshot] = None

    def consider(slot_time: datetime, row: Any) -> None:
        nonlocal best
        candidate = RawSnapshot(
            slot_time=slot_time,
            last_updated=to_utc(getattr(row, "last_updated", None)),
            symbol=(getattr(row, "symbol", None) or coin_id).upper(),
            name=getattr(row, "name", None) or coin_id,
            price_usd=row_price(row),
            market_cap=fnum(getattr(row, "market_cap", None)),
            volume_24h=fnum(getattr(row, "volume_24h", None)),
            rank=int_or_none(getattr(row, "market_cap_rank", None)),
            circ=fnum(getattr(row, "circulating_supply", None)),
            totl=fnum(getattr(row, "total_supply", None)),
        )
        if candidate.price_usd is None:
            return
        if best is None or candidate.slot_time > best.slot_time:
            best = candidate

    for row in session.execute(
        ps_10m,
        [coin_id, to_cassandra_ts(intraday_start), to_cassandra_ts(intraday_end)],
        timeout=REQUEST_TIMEOUT_SEC,
    ):
        slot = to_utc(getattr(row, "ts", None))
        if slot is not None:
            consider(slot, row)

    for row in session.execute(
        ps_hourly,
        [coin_id, to_cassandra_ts(intraday_start), to_cassandra_ts(intraday_end)],
        timeout=REQUEST_TIMEOUT_SEC,
    ):
        slot = to_utc(getattr(row, "ts", None))
        if slot is not None:
            consider(slot, row)

    for row in session.execute(
        ps_daily,
        [coin_id, daily_start, daily_end],
        timeout=REQUEST_TIMEOUT_SEC,
    ):
        row_date = getattr(row, "date", None)
        if row_date is None:
            continue
        slot = datetime(row_date.year, row_date.month, row_date.day, 23, 59, tzinfo=UTC)
        consider(slot, row)

    return best


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh live Gecko tables from local raw tables.")
    parser.add_argument("--rank-start", type=int, default=1)
    parser.add_argument("--rank-end", type=int, default=TOP_N)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    intraday_end = now_utc()
    intraday_start = intraday_end - timedelta(hours=RAW_LOOKBACK_HOURS)
    daily_start = (intraday_end - timedelta(days=DAILY_LOOKBACK_DAYS)).date()
    daily_end = intraday_end.date()

    print(
        f"[{now_str()}] live-refresh config ranks={args.rank_start}-{args.rank_end} "
        f"intraday={intraday_start.isoformat()} -> {intraday_end.isoformat()} "
        f"daily={daily_start} -> {daily_end} dry_run={args.dry_run}"
    )

    session, cluster = cast(tuple[Session, Cluster], get_session(return_cluster=True))
    try:
        sel_live_seed = SimpleStatement(
            f"""
            SELECT id, symbol, name, market_cap_rank, ath_price, ath_date, max_supply
            FROM {TABLE_LIVE}
            """,
            fetch_size=1000,
        )
        ps_10m = session.prepare(
            f"""
            SELECT ts, symbol, name, close, price_usd, market_cap, volume_24h,
                   market_cap_rank, circulating_supply, total_supply, last_updated
            FROM {TABLE_10M}
            WHERE id=? AND ts>=? AND ts<?
            """
        )
        ps_hourly = session.prepare(
            f"""
            SELECT ts, symbol, name, close, price_usd, market_cap, volume_24h,
                   market_cap_rank, circulating_supply, total_supply, last_updated
            FROM {TABLE_HOURLY}
            WHERE id=? AND ts>=? AND ts<?
            """
        )
        ps_daily = session.prepare(
            f"""
            SELECT date, symbol, name, close, price_usd, market_cap, volume_24h,
                   market_cap_rank, circulating_supply, total_supply, last_updated
            FROM {TABLE_DAILY}
            WHERE id=? AND date>=? AND date<=?
            """
        )

        ins_live = session.prepare(
            f"""
            INSERT INTO {TABLE_LIVE}
              (id, symbol, name, category, market_cap_rank, price_usd, market_cap, volume_24h,
               last_updated, last_fetched, ath_price, ath_date,
               circulating_supply, total_supply, max_supply, vs_currency)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        ins_live_ranked = session.prepare(
            f"""
            INSERT INTO {TABLE_LIVE_RANKED}
              (bucket, market_cap_rank, id, symbol, name, category,
               price_usd, market_cap, volume_24h, circulating_supply, total_supply, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        del_ranked_bucket = session.prepare(
            f"DELETE FROM {TABLE_LIVE_RANKED} WHERE bucket=?"
        )
        ins_mcap_live = session.prepare(
            f"""
            INSERT INTO {TABLE_MCAP_LIVE}
              (category, last_updated, market_cap, market_cap_rank, volume_24h)
            VALUES (?, ?, ?, ?, ?)
            """
        )

        seed_rows = list(session.execute(sel_live_seed, timeout=REQUEST_TIMEOUT_SEC))
        ranked = [r for r in seed_rows if isinstance(getattr(r, "market_cap_rank", None), int) and r.market_cap_rank > 0]
        ranked.sort(key=lambda r: r.market_cap_rank)

        selected: list[LiveSeed] = []
        for row in ranked:
            rank = int(getattr(row, "market_cap_rank", 0))
            if rank < args.rank_start or rank > args.rank_end:
                continue
            selected.append(
                LiveSeed(
                    coin_id=getattr(row, "id", ""),
                    symbol=(getattr(row, "symbol", "") or "").upper(),
                    name=getattr(row, "name", "") or getattr(row, "id", ""),
                    rank=int_or_none(getattr(row, "market_cap_rank", None)),
                    ath_price=fnum(getattr(row, "ath_price", None)),
                    ath_date=to_utc(getattr(row, "ath_date", None)),
                    max_supply=fnum(getattr(row, "max_supply", None)),
                )
            )

        print(f"[{now_str()}] selected live seeds={len(selected)}")

        now_ts = now_utc()
        live_buffer: list[list[Any]] = []
        category_totals: dict[str, dict[str, Any]] = {}
        skipped_no_raw = 0

        def bump_total(cat_name: str, mcap_value: float, vol_value: float, last_upd: datetime) -> None:
            entry = category_totals.setdefault(
                cat_name,
                {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": last_upd},
            )
            entry["market_cap"] += mcap_value
            entry["volume_24h"] += vol_value
            if last_upd and (entry["last_updated"] is None or last_upd > entry["last_updated"]):
                entry["last_updated"] = last_upd

        for idx, seed in enumerate(selected, 1):
            if idx == 1 or idx % 50 == 0 or idx == len(selected):
                print(f"[{now_str()}] snapshot {idx}/{len(selected)} {safe_text(seed.symbol)} ({safe_text(seed.coin_id)})")

            snap = load_latest_snapshot(
                session,
                ps_10m,
                ps_hourly,
                ps_daily,
                seed.coin_id,
                intraday_start,
                intraday_end,
                daily_start,
                daily_end,
            )
            if snap is None:
                skipped_no_raw += 1
                continue

            rank = snap.rank if snap.rank is not None else seed.rank
            circ = snap.circ
            totl = snap.totl
            last_updated = snap.slot_time
            category = category_for(seed.coin_id, snap.symbol or seed.symbol)

            mcap_total = float(snap.market_cap or 0.0)
            vol_total = float(snap.volume_24h or 0.0)
            bump_total(category, mcap_total, vol_total, last_updated)
            bump_total("ALL", mcap_total, vol_total, last_updated)

            live_buffer.append(
                [
                    seed.coin_id,
                    snap.symbol or seed.symbol,
                    snap.name or seed.name,
                    category,
                    rank,
                    snap.price_usd,
                    snap.market_cap,
                    snap.volume_24h,
                    to_cassandra_ts(last_updated),
                    to_cassandra_ts(now_ts),
                    seed.ath_price,
                    to_cassandra_ts(seed.ath_date),
                    circ,
                    totl,
                    seed.max_supply,
                    "usd",
                ]
            )

        print(
            f"[{now_str()}] [live-buffer] prepared={len(live_buffer)} "
            f"(required_min={REQUIRED_LIVE_MIN}; skipped_no_raw={skipped_no_raw})"
        )

        if len(live_buffer) < REQUIRED_LIVE_MIN:
            raise SystemExit(
                f"Not enough rows to refresh live tables safely: {len(live_buffer)} < {REQUIRED_LIVE_MIN}"
            )

        live_buffer.sort(key=safe_rank_from_live)
        rank_source = live_buffer[:RANK_TOP_N]

        if args.dry_run:
            print(
                f"[{now_str()}] dry-run summary live_rows={len(live_buffer)} "
                f"ranked_rows={len(rank_source)} categories={len(category_totals)}"
            )
            return

        session.execute(SimpleStatement(f"TRUNCATE {TABLE_LIVE}"))
        session.execute(del_ranked_bucket, [RANK_BUCKET], timeout=REQUEST_TIMEOUT_SEC)
        session.execute(SimpleStatement(f"TRUNCATE {TABLE_MCAP_LIVE}"))
        print(f"[{now_str()}] truncated {TABLE_LIVE}, bucket={RANK_BUCKET}, {TABLE_MCAP_LIVE}")

        batch_live = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch_ranked = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        wrote_live = 0
        for vals in live_buffer:
            batch_live.add(ins_live, vals)
            wrote_live += 1
            if (wrote_live % BATCH_FLUSH_EVERY) == 0:
                session.execute(batch_live)
                batch_live.clear()
        if len(batch_live):
            session.execute(batch_live)

        wrote_ranked = 0
        for vals in rank_source:
            (
                gid,
                sym,
                name,
                cat,
                rank_val,
                price,
                mcap,
                vol,
                lu,
                _last_fetched,
                _ath_price,
                _ath_date,
                circ,
                totl,
                _maxs,
                _vs,
            ) = vals
            batch_ranked.add(
                ins_live_ranked,
                [
                    RANK_BUCKET,
                    safe_rank_from_live(vals),
                    gid,
                    sym,
                    name,
                    cat,
                    price,
                    mcap,
                    vol,
                    circ,
                    totl,
                    to_cassandra_ts(lu),
                ],
            )
            wrote_ranked += 1
            if (wrote_ranked % BATCH_FLUSH_EVERY) == 0:
                session.execute(batch_ranked)
                batch_ranked.clear()
        if len(batch_ranked):
            session.execute(batch_ranked)

        totals_items = []
        for cat_name, totals in category_totals.items():
            totals_items.append(
                (
                    cat_name,
                    float(totals["market_cap"]),
                    float(totals["volume_24h"]),
                    totals.get("last_updated") or now_ts,
                )
            )
        totals_items.sort(key=lambda entry: (0 if entry[0] == "ALL" else 1, entry[0].lower()))
        ranked_entries = [entry for entry in totals_items if entry[0] != "ALL"]
        ranked_entries.sort(key=lambda entry: entry[1], reverse=True)
        ranks = {cat: idx + 1 for idx, (cat, *_rest) in enumerate(ranked_entries)}
        if "ALL" in category_totals:
            ranks["ALL"] = 0

        wrote_mcap = 0
        for cat_name, total_mcap, total_vol, last_upd in totals_items:
            try:
                session.execute(
                    ins_mcap_live,
                    [cat_name, to_cassandra_ts(last_upd), total_mcap, ranks.get(cat_name), total_vol],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
                wrote_mcap += 1
            except (WriteTimeout, OperationTimedOut, DriverException) as exc:
                print(f"[{now_str()}] [mcap-live] failed for category='{cat_name}': {safe_text(exc)}")

        print(
            f"[{now_str()}] done wrote_live={wrote_live} wrote_ranked={wrote_ranked} "
            f"wrote_mcap={wrote_mcap}"
        )

    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
