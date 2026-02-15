#!/usr/bin/env python3
from __future__ import annotations

import os
from collections import deque
from collections import defaultdict

from cassandra.query import SimpleStatement

from common import (
    Heartbeat,
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


def main() -> None:
    hb = Heartbeat("BB_refresh_live_derivatives")
    session, cluster = connect_astra()
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
            return
        print(f"[{now_str()}] Loaded {len(rows)} rows from {TABLE_LIVE}, sorting by rank...")

        rows.sort(key=lambda r: (_rank_int(getattr(r, "market_cap_rank", None)), getattr(r, "id", "")))
        ranked_rows = rows[:PP_TOP_N]
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
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
