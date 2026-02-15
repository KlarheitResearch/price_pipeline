#!/usr/bin/env python3
from __future__ import annotations

import os
from collections import deque
from collections import defaultdict
from datetime import datetime, timezone
from datetime import timedelta

from cassandra.query import SimpleStatement

from common import (
    Heartbeat,
    PipelineHealthTracker,
    TABLE_LIVE,
    TABLE_LIVE_RANKED,
    TABLE_MCAP_LIVE,
    connect_astra,
    drain_async,
    enqueue_async,
    is_verbose,
    now_str,
    should_log_progress,
    to_cassandra_ts,
    to_utc,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
PP_TOP_N = int(os.getenv("PP_TOP_N", "1000"))
RANK_BUCKET = (os.getenv("PP_RANK_BUCKET", "all") or "all").strip() or "all"
SENTINEL_UNRANKED = int(os.getenv("PP_SENTINEL_UNRANKED", "2000000000"))
ASTRA_MAX_IN_FLIGHT = int(os.getenv("PP_ASTRA_MAX_IN_FLIGHT", "64"))
ENFORCE_UNIQUE_LIVE_RANKS = os.getenv("PP_ENFORCE_UNIQUE_LIVE_RANKS", "1") == "1"
DUPLICATE_RANK_ACTION = (os.getenv("PP_DUPLICATE_RANK_ACTION", "demote") or "demote").strip().lower()
PRUNE_UNRANKED_STALE_HOURS = int(os.getenv("PP_PRUNE_UNRANKED_STALE_HOURS", "72"))


def _f(x):
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def _rank_int(x):
    try:
        r = int(x)
        if r > 0:
            return r
    except Exception:
        pass
    return SENTINEL_UNRANKED


def _raw_rank(x):
    try:
        r = int(x)
        if r > 0:
            return r
    except Exception:
        pass
    return None


def _ts_sort_key(x):
    ts = to_utc(x)
    if ts is None:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    return ts


def main() -> None:
    hb = Heartbeat("BB_refresh_live_derivatives")
    session, cluster = connect_astra()
    tracker = PipelineHealthTracker(session, "BB_refresh_live_derivatives")
    tracker.set_metric("top_n", PP_TOP_N)
    tracker.set_metric("rank_bucket", RANK_BUCKET)
    tracker.start()
    try:
        print(f"[{now_str()}] Refresh derivatives start: top_n={PP_TOP_N} bucket={RANK_BUCKET}")
        sel_live = SimpleStatement(
            f"""
            SELECT id, symbol, name, category, market_cap_rank,
                   price_usd, market_cap, volume_24h,
                   circulating_supply, total_supply, last_updated
            FROM {TABLE_LIVE}
            """,
            fetch_size=2000,
        )
        rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC))
        if not rows:
            print(f"[{now_str()}] No rows in {TABLE_LIVE}; skip derivatives refresh.")
            tracker.mark_noop()
            tracker.set_metric("rows_live", 0)
            tracker.finish("noop")
            return
        tracker.set_metric("rows_live", len(rows))
        print(f"[{now_str()}] Loaded {len(rows)} rows from {TABLE_LIVE}, sorting by rank...")

        if DUPLICATE_RANK_ACTION not in ("demote", "delete"):
            print(f"[{now_str()}] [rank-dedupe] invalid PP_DUPLICATE_RANK_ACTION={DUPLICATE_RANK_ACTION}; using demote")
        action = DUPLICATE_RANK_ACTION if DUPLICATE_RANK_ACTION in ("demote", "delete") else "demote"
        tracker.set_metric("duplicate_rank_action", action)

        if ENFORCE_UNIQUE_LIVE_RANKS:
            by_rank = defaultdict(list)
            for r in rows:
                rr = _raw_rank(getattr(r, "market_cap_rank", None))
                if rr is not None:
                    by_rank[rr].append(r)

            duplicate_ranks = {rk: vals for rk, vals in by_rank.items() if len(vals) > 1}
            if duplicate_ranks:
                update_live_rank = session.prepare(f"UPDATE {TABLE_LIVE} SET market_cap_rank=? WHERE id=?")
                delete_live = session.prepare(f"DELETE FROM {TABLE_LIVE} WHERE id=?")
                cleared = 0
                for rank_value, items in duplicate_ranks.items():
                    # Keep the freshest row for a rank; tie-break by market_cap then id for deterministic behavior.
                    sorted_items = sorted(
                        items,
                        key=lambda r: (
                            _ts_sort_key(getattr(r, "last_updated", None)),
                            _f(getattr(r, "market_cap", None)),
                            getattr(r, "id", "") or "",
                        ),
                        reverse=True,
                    )
                    winner = sorted_items[0]
                    losers = sorted_items[1:]
                    print(
                        f"[{now_str()}] [rank-dedupe] rank={rank_value} winner={getattr(winner, 'id', None)} "
                        f"losers={','.join((getattr(x, 'id', '') or '') for x in losers)}"
                    )
                    for loser in losers:
                        loser_id = getattr(loser, "id", None)
                        if not loser_id:
                            continue
                        if action == "delete":
                            session.execute(delete_live, [loser_id], timeout=REQUEST_TIMEOUT_SEC)
                        else:
                            session.execute(update_live_rank, [None, loser_id], timeout=REQUEST_TIMEOUT_SEC)
                        cleared += 1

                tracker.set_metric("duplicate_ranks", len(duplicate_ranks))
                tracker.set_metric("duplicate_rank_rows_cleared", cleared)
                print(f"[{now_str()}] [rank-dedupe] duplicate_ranks={len(duplicate_ranks)} cleared={cleared}")
                if cleared > 0:
                    rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC))
            else:
                tracker.set_metric("duplicate_ranks", 0)
                tracker.set_metric("duplicate_rank_rows_cleared", 0)

        # Prevent long-term buildup of unranked stale rows in live snapshot table.
        if PRUNE_UNRANKED_STALE_HOURS > 0:
            delete_live = session.prepare(f"DELETE FROM {TABLE_LIVE} WHERE id=?")
            cutoff = datetime.now(timezone.utc) - timedelta(hours=PRUNE_UNRANKED_STALE_HOURS)
            pruned = 0
            for r in rows:
                if _raw_rank(getattr(r, "market_cap_rank", None)) is not None:
                    continue
                lu = _ts_sort_key(getattr(r, "last_updated", None))
                if lu < cutoff:
                    rid = getattr(r, "id", None)
                    if rid:
                        session.execute(delete_live, [rid], timeout=REQUEST_TIMEOUT_SEC)
                        pruned += 1
            if pruned > 0:
                rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC))
                print(
                    f"[{now_str()}] [rank-dedupe] pruned_unranked={pruned} "
                    f"older_than={PRUNE_UNRANKED_STALE_HOURS}h"
                )
            tracker.set_metric("unranked_pruned", pruned)

        rows.sort(key=lambda r: (_rank_int(getattr(r, "market_cap_rank", None)), getattr(r, "id", "")))
        ranked_rows = [r for r in rows if _raw_rank(getattr(r, "market_cap_rank", None)) is not None][:PP_TOP_N]
        print(f"[{now_str()}] Ranked subset selected: {len(ranked_rows)}")
        hb.maybe(extra="ranking=done", force=True)

        del_ranked = session.prepare(f"DELETE FROM {TABLE_LIVE_RANKED} WHERE bucket=?")
        ins_ranked = session.prepare(
            f"""
            INSERT INTO {TABLE_LIVE_RANKED}
              (bucket, market_cap_rank, id, symbol, name, category,
               price_usd, market_cap, volume_24h, circulating_supply, total_supply, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        session.execute(del_ranked, [RANK_BUCKET], timeout=REQUEST_TIMEOUT_SEC)
        print(f"[{now_str()}] Cleared ranked bucket '{RANK_BUCKET}' in {TABLE_LIVE_RANKED}")

        wrote_ranked = 0
        ranked_pending = deque()
        for idx, r in enumerate(ranked_rows, 1):
            if should_log_progress(idx, len(ranked_rows), default_every=100):
                cid = getattr(r, "id", None)
                print(f"[{now_str()}] ranked write {idx}/{len(ranked_rows)} -> {cid}")
            hb.maybe(extra=f"ranked={idx}/{len(ranked_rows)}")
            enqueue_async(
                session,
                ranked_pending,
                ins_ranked,
                [
                    RANK_BUCKET,
                    _rank_int(getattr(r, "market_cap_rank", None)),
                    getattr(r, "id", None),
                    getattr(r, "symbol", None),
                    getattr(r, "name", None),
                    getattr(r, "category", None) or "Other",
                    getattr(r, "price_usd", None),
                    getattr(r, "market_cap", None),
                    getattr(r, "volume_24h", None),
                    getattr(r, "circulating_supply", None),
                    getattr(r, "total_supply", None),
                    to_cassandra_ts(to_utc(getattr(r, "last_updated", None))) if getattr(r, "last_updated", None) is not None else None,
                ],
                timeout=REQUEST_TIMEOUT_SEC,
                max_in_flight=ASTRA_MAX_IN_FLIGHT,
            )
            wrote_ranked += 1
        drain_async(ranked_pending)
        hb.maybe(extra="ranked_flush=done", force=True)

        totals = defaultdict(lambda: {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": None})
        for r in ranked_rows:
            cat = (getattr(r, "category", None) or "Other").strip() or "Other"
            lu = to_utc(getattr(r, "last_updated", None))
            for c in (cat, "ALL"):
                totals[c]["market_cap"] += _f(getattr(r, "market_cap", None))
                totals[c]["volume_24h"] += _f(getattr(r, "volume_24h", None))
                if lu is not None and (totals[c]["last_updated"] is None or lu > totals[c]["last_updated"]):
                    totals[c]["last_updated"] = lu

        ranked_cats = sorted(
            [(c, vals["market_cap"]) for c, vals in totals.items() if c != "ALL"],
            key=lambda t: t[1],
            reverse=True,
        )
        cat_ranks = {c: i + 1 for i, (c, _m) in enumerate(ranked_cats)}
        cat_ranks["ALL"] = 0

        ins_mcap_live = session.prepare(
            f"""
            INSERT INTO {TABLE_MCAP_LIVE}
              (category, last_updated, market_cap, market_cap_rank, volume_24h)
            VALUES (?, ?, ?, ?, ?)
            """
        )
        session.execute(SimpleStatement(f"TRUNCATE {TABLE_MCAP_LIVE}"), timeout=REQUEST_TIMEOUT_SEC)
        print(f"[{now_str()}] Truncated {TABLE_MCAP_LIVE}, writing category totals...")

        wrote_mcap = 0
        mcap_items = list(totals.items())
        mcap_pending = deque()
        for idx, (cat, vals) in enumerate(mcap_items, 1):
            if is_verbose() and should_log_progress(idx, len(mcap_items), default_every=10):
                print(f"[{now_str()}] mcap write {idx}/{len(mcap_items)} -> {cat}")
            hb.maybe(extra=f"mcap={idx}/{len(mcap_items)}")
            lu = vals["last_updated"]
            enqueue_async(
                session,
                mcap_pending,
                ins_mcap_live,
                [
                    cat,
                    to_cassandra_ts(lu) if lu is not None else None,
                    float(vals["market_cap"]),
                    cat_ranks.get(cat),
                    float(vals["volume_24h"]),
                ],
                timeout=REQUEST_TIMEOUT_SEC,
                max_in_flight=ASTRA_MAX_IN_FLIGHT,
            )
            wrote_mcap += 1
        drain_async(mcap_pending)
        hb.maybe(extra="mcap_flush=done", force=True)

        print(
            f"[{now_str()}] Refreshed derivatives from {TABLE_LIVE}: "
            f"ranked_rows={wrote_ranked} bucket={RANK_BUCKET} mcap_live_rows={wrote_mcap}"
        )
        tracker.set_metric("ranked_rows", wrote_ranked)
        tracker.set_metric("mcap_rows", wrote_mcap)
        tracker.finish("success")
    except Exception as exc:
        tracker.finish("failed", f"{type(exc).__name__}: {exc}")
        raise
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
