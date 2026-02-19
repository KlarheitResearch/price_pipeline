#!/usr/bin/env python3
"""
Audit 10m aggregate drift:
  gecko_market_cap_10m_7d (category='ALL')
vs
  sum(gecko_prices_10m_7d.market_cap) across top-N live universe.

Optionally repairs flagged slots by recomputing category aggregates from
persisted 10m rows and upserting them into gecko_market_cap_10m_7d.
"""

import argparse
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any

from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, SimpleStatement

from astra_connect.connect import AstraConfig, get_session


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_utc(x: datetime | None) -> datetime | None:
    if x is None:
        return None
    if x.tzinfo is None:
        return x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def floor_slot_utc(ts: datetime, slot_minutes: int) -> datetime:
    ts_utc = to_utc(ts)
    assert ts_utc is not None
    return ts_utc.replace(
        minute=(ts_utc.minute // slot_minutes) * slot_minutes,
        second=0,
        microsecond=0,
    )


def parse_utc(text: str, *, end_if_date: bool) -> datetime:
    s = (text or "").strip()
    if not s:
        raise ValueError("empty datetime")
    if len(s) == 10 and s.count("-") == 2:
        y, m, d = map(int, s.split("-"))
        base = datetime(y, m, d, tzinfo=timezone.utc)
        if end_if_date:
            return base + timedelta(days=1)
        return base
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt_obj = datetime.fromisoformat(s)
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return dt_obj.astimezone(timezone.utc)


def compute_ranks(cat_to_mcap: dict[str, float]) -> dict[str, int]:
    non_all = [(cat, mcap) for (cat, mcap) in cat_to_mcap.items() if cat != "ALL"]
    non_all.sort(key=lambda x: x[1], reverse=True)
    ranks = {cat: idx + 1 for idx, (cat, _mcap) in enumerate(non_all)}
    ranks["ALL"] = 0
    return ranks


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit and optionally repair 10m aggregate drift.")
    parser.add_argument("--from-utc", type=str, default="", help="UTC start (inclusive). Date or ISO.")
    parser.add_argument("--to-utc", type=str, default="", help="UTC end (exclusive). Date or ISO.")
    parser.add_argument("--top-n", type=int, default=int(os.getenv("TOP_N_DQ", "1000")))
    parser.add_argument("--lookback-hours", type=int, default=int(os.getenv("DRIFT_LOOKBACK_HOURS", "36")))
    parser.add_argument("--slot-minutes", type=int, default=int(os.getenv("SLOT_MINUTES", "10")))
    parser.add_argument(
        "--rel-threshold-pct",
        type=float,
        default=float(os.getenv("DRIFT_REL_THRESHOLD_PCT", "0.03")),
        help="Relative abs diff threshold. 0.03 = 3%",
    )
    parser.add_argument(
        "--abs-threshold-usd",
        type=float,
        default=float(os.getenv("DRIFT_ABS_THRESHOLD_USD", "50000000000")),
        help="Absolute abs diff threshold in USD.",
    )
    parser.add_argument(
        "--min-coins",
        type=int,
        default=int(os.getenv("DRIFT_MIN_COINS", "700")),
        help="Ignore slots with fewer raw contributors than this.",
    )
    parser.add_argument("--repair", action="store_true", default=(os.getenv("DRIFT_AUTO_REPAIR", "0") == "1"))
    parser.add_argument(
        "--max-repairs",
        type=int,
        default=int(os.getenv("DRIFT_MAX_REPAIRS", "8")),
        help="Cap repaired slots per run to keep runtime bounded.",
    )
    parser.add_argument(
        "--fail-on-finding",
        action="store_true",
        default=(os.getenv("DRIFT_FAIL_ON_FINDING", "1") == "1"),
        help="Exit non-zero when drift remains after optional repair.",
    )
    args = parser.parse_args()

    AstraConfig.from_env()
    session, cluster = get_session(return_cluster=True)

    try:
        request_timeout = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
        fetch_size = int(os.getenv("FETCH_SIZE", "500"))

        table_live = os.getenv("TABLE_LIVE", "gecko_prices_live")
        table_10m = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")
        table_mcap_10m = os.getenv("TABLE_MCAP_10M", "gecko_market_cap_10m_7d")

        delay_sec = int(os.getenv("SLOT_DELAY_SEC", "120"))
        now_cutoff = datetime.now(timezone.utc) - timedelta(seconds=delay_sec)
        end_default = floor_slot_utc(now_cutoff, args.slot_minutes) + timedelta(minutes=args.slot_minutes)
        start_default = end_default - timedelta(hours=max(1, args.lookback_hours))

        start_dt = parse_utc(args.from_utc, end_if_date=False) if args.from_utc else start_default
        end_dt = parse_utc(args.to_utc, end_if_date=True) if args.to_utc else end_default

        if end_dt <= start_dt:
            raise SystemExit(f"Invalid window: start={start_dt.isoformat()} end={end_dt.isoformat()}")

        sel_live = SimpleStatement(
            f"SELECT id, symbol, market_cap_rank, category FROM {table_live}",
            fetch_size=fetch_size,
        )
        live_rows = list(session.execute(sel_live, timeout=request_timeout))
        coins = [r for r in live_rows if isinstance(getattr(r, "market_cap_rank", None), int) and r.market_cap_rank > 0]
        coins.sort(key=lambda r: r.market_cap_rank)
        if args.top_n > 0:
            coins = coins[: args.top_n]

        if not coins:
            raise SystemExit("No ranked live coins found.")

        print(
            f"[{now_str()}] Drift audit window: {start_dt.isoformat()} -> {end_dt.isoformat()} | "
            f"top_n={len(coins)} rel_thr={args.rel_threshold_pct:.4f} abs_thr={args.abs_threshold_usd:,.0f} "
            f"min_coins={args.min_coins} repair={args.repair}"
        )

        sel_10m_range = session.prepare(
            f"""
            SELECT ts, market_cap, volume_24h, last_updated
            FROM {table_10m}
            WHERE id=? AND ts>=? AND ts<?
            ORDER BY ts ASC
            """
        )
        sel_mcap_all_range = session.prepare(
            f"""
            SELECT ts, market_cap, last_updated
            FROM {table_mcap_10m}
            WHERE category=? AND ts>=? AND ts<?
            """
        )
        ins_mcap = session.prepare(
            f"""
            INSERT INTO {table_mcap_10m}
              (category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )

        # raw_slot[ts][category] = {"market_cap": float, "volume_24h": float, "last_updated": datetime}
        raw_slot: dict[datetime, dict[str, dict[str, Any]]] = {}
        slot_coin_counts: dict[datetime, int] = defaultdict(int)

        def bump(ts: datetime, category: str, mcap: float, vol: float, lu: datetime) -> None:
            catmap = raw_slot.setdefault(ts, {})
            entry = catmap.setdefault(
                category,
                {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": lu},
            )
            entry["market_cap"] += mcap
            entry["volume_24h"] += vol
            if entry["last_updated"] is None or (lu and lu > entry["last_updated"]):
                entry["last_updated"] = lu

        t0 = perf_counter()
        for idx, coin in enumerate(coins, 1):
            if idx == 1 or idx % 100 == 0 or idx == len(coins):
                print(f"[{now_str()}] reading raw 10m {idx}/{len(coins)}: {coin.id}")
            cat = (getattr(coin, "category", None) or "Other").strip() or "Other"

            rows = session.execute(sel_10m_range, [coin.id, start_dt, end_dt], timeout=request_timeout)
            for row in rows:
                ts = to_utc(getattr(row, "ts", None))
                if ts is None:
                    continue
                mcap = float(getattr(row, "market_cap", 0.0) or 0.0)
                vol = float(getattr(row, "volume_24h", 0.0) or 0.0)
                lu = to_utc(getattr(row, "last_updated", None)) or (ts + timedelta(minutes=args.slot_minutes) - timedelta(seconds=1))
                bump(ts, cat, mcap, vol, lu)
                bump(ts, "ALL", mcap, vol, lu)
                slot_coin_counts[ts] += 1

        print(f"[{now_str()}] raw aggregation built for {len(raw_slot)} slots in {perf_counter() - t0:.1f}s")

        agg_all_map: dict[datetime, tuple[float, datetime | None]] = {}
        for row in session.execute(sel_mcap_all_range, ["ALL", start_dt, end_dt], timeout=request_timeout):
            ts = to_utc(getattr(row, "ts", None))
            if ts is None:
                continue
            agg_all_map[ts] = (
                float(getattr(row, "market_cap", 0.0) or 0.0),
                to_utc(getattr(row, "last_updated", None)),
            )

        findings: list[dict[str, Any]] = []
        for ts in sorted(raw_slot.keys()):
            count = int(slot_coin_counts.get(ts, 0))
            if count < args.min_coins:
                continue
            raw_all = float(raw_slot[ts].get("ALL", {}).get("market_cap", 0.0))
            if raw_all <= 0.0:
                continue
            agg_entry = agg_all_map.get(ts)
            if agg_entry is None:
                findings.append(
                    {
                        "ts": ts,
                        "kind": "missing_all",
                        "raw_all": raw_all,
                        "agg_all": None,
                        "diff": None,
                        "rel": None,
                        "count": count,
                        "lu": None,
                    }
                )
                continue
            agg_all, lu = agg_entry
            diff = agg_all - raw_all
            rel = abs(diff) / raw_all
            if abs(diff) >= args.abs_threshold_usd and rel >= args.rel_threshold_pct:
                findings.append(
                    {
                        "ts": ts,
                        "kind": "mismatch_all",
                        "raw_all": raw_all,
                        "agg_all": agg_all,
                        "diff": diff,
                        "rel": rel,
                        "count": count,
                        "lu": lu,
                    }
                )

        findings.sort(key=lambda x: abs(float(x["diff"] or 0.0)), reverse=True)

        if findings:
            print(f"[{now_str()}] DRIFT findings={len(findings)}")
            for f in findings[:20]:
                ts = f["ts"].isoformat()
                kind = f["kind"]
                raw_all = f["raw_all"]
                agg_all = f["agg_all"]
                diff = f["diff"]
                rel = f["rel"]
                lu = f["lu"]
                count = f["count"]
                if agg_all is None:
                    print(f"  {ts} kind={kind} raw={raw_all:,.2f} agg=MISSING count={count} lu={lu}")
                else:
                    print(
                        f"  {ts} kind={kind} raw={raw_all:,.2f} agg={agg_all:,.2f} "
                        f"diff={diff:,.2f} rel={rel:.4%} count={count} lu={lu}"
                    )
        else:
            print(f"[{now_str()}] No drift findings in window.")

        repaired = 0
        if findings and args.repair:
            targets = findings[: max(1, args.max_repairs)]
            print(f"[{now_str()}] Repairing {len(targets)} slot(s)")
            for f in targets:
                ts = f["ts"]
                catmap = raw_slot.get(ts, {})
                if "ALL" not in catmap:
                    continue

                # Rebuild ALL from non-ALL categories for strict consistency.
                total_mcap = 0.0
                total_vol = 0.0
                latest_lu: datetime | None = None
                for cat, vals in catmap.items():
                    if cat == "ALL":
                        continue
                    total_mcap += float(vals.get("market_cap") or 0.0)
                    total_vol += float(vals.get("volume_24h") or 0.0)
                    lu = vals.get("last_updated")
                    if isinstance(lu, datetime) and (latest_lu is None or lu > latest_lu):
                        latest_lu = lu
                catmap["ALL"] = {
                    "market_cap": total_mcap,
                    "volume_24h": total_vol,
                    "last_updated": latest_lu or (ts + timedelta(minutes=args.slot_minutes) - timedelta(seconds=1)),
                }

                ranks = compute_ranks({cat: float(vals.get("market_cap") or 0.0) for cat, vals in catmap.items()})

                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                for cat in sorted(catmap.keys(), key=lambda c: (0 if c == "ALL" else 1, c.lower())):
                    vals = catmap[cat]
                    lu = vals.get("last_updated") or (ts + timedelta(minutes=args.slot_minutes) - timedelta(seconds=1))
                    batch.add(
                        ins_mcap,
                        [
                            cat,
                            ts,
                            lu,
                            float(vals.get("market_cap") or 0.0),
                            int(ranks.get(cat, 0 if cat == "ALL" else 1)),
                            float(vals.get("volume_24h") or 0.0),
                        ],
                    )
                session.execute(batch, timeout=request_timeout)
                repaired += 1
                print(f"[{now_str()}] repaired slot {ts.isoformat()} categories={len(catmap)}")

        if repaired:
            print(f"[{now_str()}] repaired_slots={repaired}")

        unresolved = 0
        if findings:
            # Re-check only previously flagged slots for fast post-repair verification.
            check_ps = session.prepare(
                f"""
                SELECT market_cap, last_updated
                FROM {table_mcap_10m}
                WHERE category=? AND ts=?
                LIMIT 1
                """
            )
            for f in findings:
                ts = f["ts"]
                raw_all = float(raw_slot.get(ts, {}).get("ALL", {}).get("market_cap", 0.0))
                row = session.execute(check_ps, ["ALL", ts], timeout=request_timeout).one()
                if not row or getattr(row, "market_cap", None) is None:
                    unresolved += 1
                    continue
                agg_all = float(row.market_cap)
                diff = agg_all - raw_all
                rel = abs(diff) / raw_all if raw_all > 0 else 1.0
                if abs(diff) >= args.abs_threshold_usd and rel >= args.rel_threshold_pct:
                    unresolved += 1

        if findings:
            if unresolved:
                print(f"[{now_str()}] unresolved_findings={unresolved}/{len(findings)}")
            else:
                print(f"[{now_str()}] all findings resolved ({len(findings)}/{len(findings)})")

        if args.fail_on_finding and findings and unresolved > 0:
            return 2
        return 0

    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
