#!/usr/bin/env python3
from __future__ import annotations

import os

from cassandra.query import SimpleStatement

from common import (
    ID_CATEGORY_MAP,
    TABLE_LIVE,
    category_for,
    connect_astra,
    now_str,
    to_cassandra_ts,
    now_utc,
)


REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
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

        if CSV_ONLY:
            for cid, cat in sorted(ID_CATEGORY_MAP.items()):
                session.execute(
                    ins,
                    [cid, None, cat, to_cassandra_ts(now_ts), "manual_csv"],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
                wrote += 1
        else:
            sel_live = SimpleStatement(
                f"SELECT id, symbol FROM {TABLE_LIVE}",
                fetch_size=2000,
            )
            for r in session.execute(sel_live, timeout=REQUEST_TIMEOUT_SEC):
                cid = getattr(r, "id", None)
                if not cid:
                    continue
                sym = getattr(r, "symbol", None)
                cat = category_for(cid, sym)
                source = "manual_csv" if cid in ID_CATEGORY_MAP else "default_other"
                session.execute(
                    ins,
                    [cid, sym, cat, to_cassandra_ts(now_ts), source],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
                wrote += 1

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
