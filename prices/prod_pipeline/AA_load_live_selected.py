#!/usr/bin/env python3
from __future__ import annotations

import math
import os
from collections import deque
from datetime import datetime, timedelta

from common import (
    Heartbeat,
    PipelineHealthTracker,
    TABLE_10M,
    TABLE_DAILY,
    TABLE_HOURLY,
    TABLE_LIVE,
    TABLE_ROLLING,
    category_for,
    cg_get,
    connect_astra,
    drain_async,
    enqueue_async,
    floor_10m,
    floor_hour,
    get_test_coin_ids,
    get_rank_window,
    is_verbose,
    now_str,
    now_utc,
    parse_cg_iso,
    should_log_progress,
    scope_label,
    to_cassandra_ts,
    to_utc,
)

REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
ASTRA_MAX_IN_FLIGHT = int(os.getenv("PP_ASTRA_MAX_IN_FLIGHT", "64"))
LIVE_PROJECT_PARTIALS = os.getenv("PP_LIVE_PROJECT_PARTIALS", "1") == "1"
LIVE_PROJECT_10M = os.getenv("PP_LIVE_PROJECT_10M", "1") == "1"
LIVE_PROJECT_HOURLY = os.getenv("PP_LIVE_PROJECT_HOURLY", "1") == "1"
LIVE_PROJECT_DAILY = os.getenv("PP_LIVE_PROJECT_DAILY", "1") == "1"
SLOT_DELAY_SEC = int(os.getenv("PP_SLOT_DELAY_SEC", "90"))


def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _i(x, default=0):
    try:
        return int(x) if x is not None else default
    except Exception:
        return default


def _is_source(existing_row, expected: str) -> bool:
    if existing_row is None:
        return False
    src = (getattr(existing_row, "candle_source", None) or "").strip().lower()
    return src == expected.strip().lower()


def _merge_partial(existing_row, live_price: float, live_last_updated: datetime):
    if live_price is None:
        return None

    prev_open = _f(getattr(existing_row, "open", None)) if existing_row is not None else None
    prev_high = _f(getattr(existing_row, "high", None)) if existing_row is not None else None
    prev_low = _f(getattr(existing_row, "low", None)) if existing_row is not None else None
    prev_close = _f(getattr(existing_row, "close", None)) if existing_row is not None else None

    open_price = prev_open if prev_open is not None else (prev_close if prev_close is not None else live_price)
    high_candidates = [v for v in [prev_high, open_price, prev_close, live_price] if v is not None]
    low_candidates = [v for v in [prev_low, open_price, prev_close, live_price] if v is not None]
    high = max(high_candidates) if high_candidates else live_price
    low = min(low_candidates) if low_candidates else live_price
    close = live_price

    prev_points = _i(getattr(existing_row, "point_count", None), 0) if existing_row is not None else 0
    point_count = max(1, prev_points)

    prev_lu = to_utc(getattr(existing_row, "last_updated", None)) if existing_row is not None else None
    last_updated = live_last_updated
    if prev_lu is not None and prev_lu > last_updated:
        last_updated = prev_lu

    return open_price, high, low, close, point_count, last_updated


def main() -> None:
    hb = Heartbeat("AA_load_live_selected")
    rank_window = get_rank_window()
    rows = []
    by_id = {}

    if rank_window:
        start_rank, end_rank = rank_window
        per_page = 250
        start_page = max(1, ((start_rank - 1) // per_page) + 1)
        end_page = max(start_page, int(math.ceil(end_rank / per_page)))
        pages = (end_page - start_page) + 1
        print(
            f"[{now_str()}] Loading live prices for scope={scope_label()} "
            f"across pages {start_page}..{end_page} (count={pages})"
        )

        for page in range(start_page, end_page + 1):
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
            page_idx = page - start_page + 1
            print(f"[{now_str()}] page={page_idx}/{pages} (api_page={page}) collected={len(by_id)}")

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
    tracker = PipelineHealthTracker(session, "AA_load_live_selected")
    tracker.set_metric("coins_returned", len(rows))
    tracker.set_metric("project_partials_enabled", 1 if LIVE_PROJECT_PARTIALS else 0)
    tracker.start()
    now_ts = now_utc()
    now_guarded = now_ts - timedelta(seconds=SLOT_DELAY_SEC)
    current_10m = floor_10m(now_guarded)
    current_hour = floor_hour(now_guarded)
    current_day_key = now_guarded.date()

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

    sel_10m_one = None
    ins_10m = None
    sel_hourly_one = None
    ins_hourly = None
    sel_daily_one = None
    ins_daily = None
    if LIVE_PROJECT_PARTIALS:
        if LIVE_PROJECT_10M:
            sel_10m_one = session.prepare(
                f"""
                SELECT open, high, low, close, point_count, last_updated, candle_source
                FROM {TABLE_10M}
                WHERE id=? AND ts=? LIMIT 1
                """
            )
            ins_10m = session.prepare(
                f"""
                INSERT INTO {TABLE_10M}
                  (id, ts, symbol, name,
                   open, high, low, close, price_usd,
                   market_cap, volume_24h,
                   market_cap_rank, circulating_supply, total_supply,
                   last_updated, candle_source, point_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )
        if LIVE_PROJECT_HOURLY:
            sel_hourly_one = session.prepare(
                f"""
                SELECT open, high, low, close, point_count, last_updated, candle_source
                FROM {TABLE_HOURLY}
                WHERE id=? AND ts=? LIMIT 1
                """
            )
            ins_hourly = session.prepare(
                f"""
                INSERT INTO {TABLE_HOURLY}
                  (id, ts, symbol, name,
                   open, high, low, close, price_usd,
                   market_cap, volume_24h, market_cap_rank, circulating_supply, total_supply,
                   candle_source, point_count, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )
        if LIVE_PROJECT_DAILY:
            sel_daily_one = session.prepare(
                f"""
                SELECT open, high, low, close, point_count, last_updated, candle_source
                FROM {TABLE_DAILY}
                WHERE id=? AND date=? LIMIT 1
                """
            )
            ins_daily = session.prepare(
                f"""
                INSERT INTO {TABLE_DAILY}
                  (id, date, symbol, name,
                   open, high, low, close, price_usd,
                   market_cap, volume_24h,
                   market_cap_rank, circulating_supply, total_supply,
                   candle_source, point_count, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )

    wrote = 0
    wrote_10m = 0
    wrote_hourly = 0
    wrote_daily = 0
    pending = deque()
    try:
        scoped_ids = sorted(by_id.keys())
        for idx, cid in enumerate(scoped_ids, 1):
            if should_log_progress(idx, len(scoped_ids), default_every=50):
                print(f"[{now_str()}] coin {idx}/{len(scoped_ids)} -> {cid}")
            hb.maybe(extra=f"coin={idx}/{len(scoped_ids)}")
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

            live_price = _f(row.get("current_price"))
            market_cap = _f(row.get("market_cap"))
            volume_24h = _f(row.get("total_volume"))
            circulating_supply = _f(row.get("circulating_supply"))
            total_supply = _f(row.get("total_supply"))
            max_supply = _f(row.get("max_supply"))

            vals_live = [
                cid,
                sym,
                name,
                category,
                rank,
                live_price,
                market_cap,
                volume_24h,
                to_cassandra_ts(lu),
                to_cassandra_ts(now_ts),
                _f(row.get("ath")),
                to_cassandra_ts(ath_date) if isinstance(ath_date, datetime) else None,
                circulating_supply,
                total_supply,
                max_supply,
                "usd",
            ]

            vals_rolling = [
                cid,
                to_cassandra_ts(lu),
                sym,
                name,
                category,
                rank,
                live_price,
                market_cap,
                volume_24h,
                to_cassandra_ts(now_ts),
                _f(row.get("ath")),
                to_cassandra_ts(ath_date) if isinstance(ath_date, datetime) else None,
                circulating_supply,
                total_supply,
                max_supply,
                "usd",
            ]

            enqueue_async(
                session,
                pending,
                ins_live,
                vals_live,
                timeout=REQUEST_TIMEOUT_SEC,
                max_in_flight=ASTRA_MAX_IN_FLIGHT,
            )
            enqueue_async(
                session,
                pending,
                ins_rolling,
                vals_rolling,
                timeout=REQUEST_TIMEOUT_SEC,
                max_in_flight=ASTRA_MAX_IN_FLIGHT,
            )

            if LIVE_PROJECT_PARTIALS and live_price is not None:
                if LIVE_PROJECT_10M and sel_10m_one is not None and ins_10m is not None:
                    existing_10m = session.execute(
                        sel_10m_one,
                        [cid, to_cassandra_ts(current_10m)],
                        timeout=REQUEST_TIMEOUT_SEC,
                    ).one()
                    merged = _merge_partial(existing_10m, live_price, lu)
                    if merged is not None:
                        open_price, high, low, close, point_count, merged_lu = merged
                        enqueue_async(
                            session,
                            pending,
                            ins_10m,
                            [
                                cid, to_cassandra_ts(current_10m), sym, name,
                                open_price, high, low, close, close,
                                market_cap, volume_24h, rank, circulating_supply, total_supply,
                                to_cassandra_ts(merged_lu), "live_partial", point_count,
                            ],
                            timeout=REQUEST_TIMEOUT_SEC,
                            max_in_flight=ASTRA_MAX_IN_FLIGHT,
                        )
                        wrote_10m += 1

                if LIVE_PROJECT_HOURLY and sel_hourly_one is not None and ins_hourly is not None:
                    existing_hourly = session.execute(
                        sel_hourly_one,
                        [cid, to_cassandra_ts(current_hour)],
                        timeout=REQUEST_TIMEOUT_SEC,
                    ).one()
                    if not _is_source(existing_hourly, "cg_hourly_final"):
                        merged = _merge_partial(existing_hourly, live_price, lu)
                        if merged is not None:
                            open_price, high, low, close, point_count, merged_lu = merged
                            enqueue_async(
                                session,
                                pending,
                                ins_hourly,
                                [
                                    cid, to_cassandra_ts(current_hour), sym, name,
                                    open_price, high, low, close, close,
                                    market_cap, volume_24h, rank, circulating_supply, total_supply,
                                    "live_partial", point_count, to_cassandra_ts(merged_lu),
                                ],
                                timeout=REQUEST_TIMEOUT_SEC,
                                max_in_flight=ASTRA_MAX_IN_FLIGHT,
                            )
                            wrote_hourly += 1

                if LIVE_PROJECT_DAILY and sel_daily_one is not None and ins_daily is not None:
                    existing_daily = session.execute(
                        sel_daily_one,
                        [cid, current_day_key],
                        timeout=REQUEST_TIMEOUT_SEC,
                    ).one()
                    if not _is_source(existing_daily, "cg_daily_final"):
                        merged = _merge_partial(existing_daily, live_price, lu)
                        if merged is not None:
                            open_price, high, low, close, point_count, merged_lu = merged
                            enqueue_async(
                                session,
                                pending,
                                ins_daily,
                                [
                                    cid, current_day_key, sym, name,
                                    open_price, high, low, close, close,
                                    market_cap, volume_24h, rank, circulating_supply, total_supply,
                                    "live_partial", point_count, to_cassandra_ts(merged_lu),
                                ],
                                timeout=REQUEST_TIMEOUT_SEC,
                                max_in_flight=ASTRA_MAX_IN_FLIGHT,
                            )
                            wrote_daily += 1

            wrote += 1
            if is_verbose():
                extra = ""
                if LIVE_PROJECT_PARTIALS:
                    extra = f" projected(10m={LIVE_PROJECT_10M},hourly={LIVE_PROJECT_HOURLY},daily={LIVE_PROJECT_DAILY})"
                print(f"[{now_str()}] upserted live+rolling: {cid} ({sym}){extra}")
        drain_async(pending)
        hb.maybe(extra="flush=done", force=True)
        tracker.set_metric("coins_scoped", len(scoped_ids))
        tracker.set_metric("rows_live", wrote)
        tracker.set_metric("rows_projected_10m", wrote_10m)
        tracker.set_metric("rows_projected_hourly", wrote_hourly)
        tracker.set_metric("rows_projected_daily", wrote_daily)
        tracker.finish("success")
    except Exception as exc:
        tracker.finish("failed", f"{type(exc).__name__}: {exc}")
        raise
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

    if LIVE_PROJECT_PARTIALS:
        print(
            f"[{now_str()}] Done. Wrote live={wrote} projected_10m={wrote_10m} "
            f"projected_hourly={wrote_hourly} projected_daily={wrote_daily}."
        )
    else:
        print(f"[{now_str()}] Done. Wrote {wrote} selected live rows.")


if __name__ == "__main__":
    main()
