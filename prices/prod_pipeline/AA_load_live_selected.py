#!/usr/bin/env python3
from __future__ import annotations

import math
from datetime import datetime

from common import (
    TABLE_LIVE,
    TABLE_ROLLING,
    category_for,
    cg_get,
    connect_astra,
    get_test_coin_ids,
    get_rank_window,
    now_str,
    now_utc,
    parse_cg_iso,
    scope_label,
    to_cassandra_ts,
)


def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def main() -> None:
    rank_window = get_rank_window()
    rows = []
    by_id = {}

    if rank_window:
        start_rank, end_rank = rank_window
        per_page = 250
        pages = int(math.ceil(end_rank / per_page))
        print(f"[{now_str()}] Loading live prices for scope={scope_label()} across {pages} page(s)")

        for page in range(1, pages + 1):
            data = cg_get(
                "/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": per_page,
                    "page": page,
                    "locale": "en",
                    "precision": "full",
                },
                hint=f"coins_markets_page_{page}",
            )
            page_rows = data or []
            for row in page_rows:
                rank = row.get("market_cap_rank")
                try:
                    rank = int(rank) if rank is not None else None
                except Exception:
                    rank = None
                if rank is None:
                    continue
                if start_rank <= rank <= end_rank:
                    cid = row.get("id")
                    if cid:
                        by_id[cid] = row
            print(f"[{now_str()}] page={page}/{pages} collected={len(by_id)}")

        rows = list(by_id.values())
    else:
        coin_ids = get_test_coin_ids()
        if not coin_ids:
            raise RuntimeError("No PP_TEST_COIN_IDS configured.")

        print(f"[{now_str()}] Loading selected live prices for scope={scope_label()}")
        data = cg_get(
            "/coins/markets",
            params={
                "vs_currency": "usd",
                "ids": ",".join(coin_ids),
                "order": "market_cap_desc",
                "per_page": max(1, len(coin_ids)),
                "page": 1,
                "locale": "en",
                "precision": "full",
            },
            hint="coins_markets_selected",
        )
        rows = data or []
        by_id = {row.get("id"): row for row in rows if row.get("id")}

    print(f"[{now_str()}] CoinGecko returned {len(rows)} scoped row(s).")

    session, cluster = connect_astra()
    now_ts = now_utc()

    ins_live = session.prepare(
        f"""
        INSERT INTO {TABLE_LIVE}
          (id, symbol, name, category, market_cap_rank,
           price_usd, market_cap, volume_24h,
           last_updated, last_fetched,
           ath_price, ath_date,
           circulating_supply, total_supply, max_supply,
           vs_currency)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    ins_rolling = session.prepare(
        f"""
        INSERT INTO {TABLE_ROLLING}
          (id, last_updated, symbol, name, category, market_cap_rank,
           price_usd, market_cap, volume_24h,
           last_fetched,
           ath_price, ath_date,
           circulating_supply, total_supply, max_supply,
           vs_currency)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    wrote = 0
    try:
        scoped_ids = sorted(by_id.keys())
        for cid in scoped_ids:
            row = by_id[cid]

            lu = parse_cg_iso(row.get("last_updated")) or now_ts
            ath_date = parse_cg_iso(row.get("ath_date"))
            sym = (row.get("symbol") or "").upper()
            name = row.get("name") or cid
            category = category_for(cid, sym)
            rank = row.get("market_cap_rank")
            try:
                rank = int(rank) if rank is not None else None
            except Exception:
                rank = None

            vals_live = [
                cid,
                sym,
                name,
                category,
                rank,
                _f(row.get("current_price")),
                _f(row.get("market_cap")),
                _f(row.get("total_volume")),
                to_cassandra_ts(lu),
                to_cassandra_ts(now_ts),
                _f(row.get("ath")),
                to_cassandra_ts(ath_date) if isinstance(ath_date, datetime) else None,
                _f(row.get("circulating_supply")),
                _f(row.get("total_supply")),
                _f(row.get("max_supply")),
                "usd",
            ]

            vals_rolling = [
                cid,
                to_cassandra_ts(lu),
                sym,
                name,
                category,
                rank,
                _f(row.get("current_price")),
                _f(row.get("market_cap")),
                _f(row.get("total_volume")),
                to_cassandra_ts(now_ts),
                _f(row.get("ath")),
                to_cassandra_ts(ath_date) if isinstance(ath_date, datetime) else None,
                _f(row.get("circulating_supply")),
                _f(row.get("total_supply")),
                _f(row.get("max_supply")),
                "usd",
            ]

            session.execute(ins_live, vals_live)
            session.execute(ins_rolling, vals_rolling)
            wrote += 1
            print(f"[{now_str()}] upserted live+rolling: {cid} ({sym})")
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

    print(f"[{now_str()}] Done. Wrote {wrote} selected live rows.")


if __name__ == "__main__":
    main()
