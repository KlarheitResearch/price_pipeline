#!/usr/bin/env python3
from __future__ import annotations

import os
from collections import deque

from cassandra.query import SimpleStatement

from common import (
    Heartbeat,
    ID_CATEGORY_MAP,
    TABLE_LIVE,
    category_for,
    connect_astra,
    drain_async,
    enqueue_async,
    now_str,
    should_log_progress,
    to_cassandra_ts,
    now_utc,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
ASTRA_MAX_IN_FLIGHT = int(os.getenv("PP_ASTRA_MAX_IN_FLIGHT", "64"))
TABLE_ASSET_CATEGORIES = os.getenv("PP_TABLE_ASSET_CATEGORIES", "pp_asset_categories")
TRUNCATE_FIRST = os.getenv("PP_CATEGORY_TRUNCATE_FIRST", "1") == "1"
CSV_ONLY = os.getenv("PP_CATEGORY_CSV_ONLY", "0") == "1"


def main() -> None:
    session, cluster = connect_astra()
    try:
        if TRUNCATE_FIRST:
            session.execute(SimpleStatement(f"TRUNCATE {TABLE_ASSET_CATEGORIES}"), timeout=REQUEST_TIMEOUT_SEC)

        ins = session.prepare(
            f"""
            INSERT INTO {TABLE_ASSET_CATEGORIES}
              (id, symbol, category, updated_at, source)
            VALUES (?, ?, ?, ?, ?)
            """
        )

        wrote = 0
        now_ts = now_utc()
        hb = Heartbeat("94_materialize_asset_categories")
        pending = deque()

        if CSV_ONLY:
            csv_items = sorted(ID_CATEGORY_MAP.items())
            for idx, (cid, cat) in enumerate(csv_items, 1):
                if should_log_progress(idx, len(csv_items), default_every=100):
                    print(f"[{now_str()}] csv row {idx}/{len(csv_items)} -> {cid}")
                hb.maybe(extra=f"csv={idx}/{len(csv_items)}")
                enqueue_async(
                    session,
                    pending,
                    ins,
                    [cid, None, cat, to_cassandra_ts(now_ts), "manual_csv"],
                    timeout=REQUEST_TIMEOUT_SEC,
                    max_in_flight=ASTRA_MAX_IN_FLIGHT,
                )
                wrote += 1
        else:
            sel_live = SimpleStatement(
                f"SELECT id, symbol FROM {TABLE_LIVE}",
                fetch_size=2000,
            )
            live_rows = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC))
            for idx, r in enumerate(live_rows, 1):
                cid = getattr(r, "id", None)
                if not cid:
                    continue
                if should_log_progress(idx, len(live_rows), default_every=100):
                    print(f"[{now_str()}] live row {idx}/{len(live_rows)} -> {cid}")
                hb.maybe(extra=f"live={idx}/{len(live_rows)}")
                sym = getattr(r, "symbol", None)
                cat = category_for(cid, sym)
                source = "manual_csv" if cid in ID_CATEGORY_MAP else "default_other"
                enqueue_async(
                    session,
                    pending,
                    ins,
                    [cid, sym, cat, to_cassandra_ts(now_ts), source],
                    timeout=REQUEST_TIMEOUT_SEC,
                    max_in_flight=ASTRA_MAX_IN_FLIGHT,
                )
                wrote += 1
        drain_async(pending)
        hb.maybe(extra="flush=done", force=True)

        print(
            f"[{now_str()}] asset categories materialized: table={TABLE_ASSET_CATEGORIES} "
            f"rows={wrote} csv_only={CSV_ONLY}"
        )
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    main()
