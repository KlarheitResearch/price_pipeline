#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import time
import random
import zlib
import pathlib
import csv
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import requests

_BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.append(str(_BACKEND_ROOT))

from astra_connect.connect import get_session, AstraConfig


UTC = timezone.utc


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")


def to_utc(ts: Optional[datetime]) -> Optional[datetime]:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def to_cassandra_ts(ts: datetime) -> datetime:
    ts = to_utc(ts)
    return ts.replace(tzinfo=None)


def parse_cg_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def floor_10m(ts: datetime) -> datetime:
    ts = to_utc(ts)
    return ts.replace(minute=(ts.minute // 10) * 10, second=0, microsecond=0)


def floor_hour(ts: datetime) -> datetime:
    ts = to_utc(ts)
    return ts.replace(minute=0, second=0, microsecond=0)


def day_bounds(day_key) -> tuple[datetime, datetime]:
    start = datetime(day_key.year, day_key.month, day_key.day, tzinfo=UTC)
    return start, start + timedelta(days=1)


def get_test_coin_ids() -> list[str]:
    raw = (os.getenv("PP_TEST_COIN_IDS", "bitcoin,ethereum") or "").strip()
    out: list[str] = []
    for part in raw.split(","):
        cid = part.strip().lower()
        if cid and cid not in out:
            out.append(cid)
    return out


def get_rank_window() -> Optional[tuple[int, int]]:
    try:
        start = int((os.getenv("PP_RANK_START") or "0").strip() or "0")
        end = int((os.getenv("PP_RANK_END") or "0").strip() or "0")
    except Exception:
        return None
    if start > 0 and end >= start:
        return (start, end)
    return None


def scope_label() -> str:
    rw = get_rank_window()
    if rw:
        return f"rank[{rw[0]}-{rw[1]}]"
    ids = get_test_coin_ids()
    return f"ids[{','.join(ids)}]"


def select_coins_from_live_rows(rows: list[Any]) -> list[Any]:
    rw = get_rank_window()
    if rw:
        start, end = rw
        out = []
        for r in rows:
            rank = getattr(r, "market_cap_rank", None)
            if isinstance(rank, int) and start <= rank <= end:
                out.append(r)
        out.sort(key=lambda x: (x.market_cap_rank, x.id))
        return out

    ids = set(get_test_coin_ids())
    out = [r for r in rows if getattr(r, "id", None) in ids]
    out.sort(key=lambda x: (x.market_cap_rank if isinstance(getattr(x, "market_cap_rank", None), int) else 10**9, x.id))
    return out


TABLE_LIVE = os.getenv("PP_TABLE_LIVE", "pp_prices_live")
TABLE_LIVE_RANKED = os.getenv("PP_TABLE_LIVE_RANKED", "pp_prices_live_ranked")
TABLE_ROLLING = os.getenv("PP_TABLE_ROLLING", "pp_prices_live_rolling")
TABLE_10M = os.getenv("PP_TABLE_10M", "pp_prices_10m_7d")
TABLE_HOURLY = os.getenv("PP_TABLE_HOURLY", "pp_candles_hourly_30d")
TABLE_DAILY = os.getenv("PP_TABLE_DAILY", "pp_candles_daily_contin")
TABLE_MONTHLY = os.getenv("PP_TABLE_MONTHLY", "pp_candles_monthly")
TABLE_MCAP_LIVE = os.getenv("PP_TABLE_MCAP_LIVE", "pp_market_cap_live")
TABLE_MCAP_10M = os.getenv("PP_TABLE_MCAP_10M", "pp_market_cap_10m_7d")
TABLE_MCAP_HOURLY = os.getenv("PP_TABLE_MCAP_HOURLY", "pp_market_cap_hourly_30d")
TABLE_MCAP_DAILY = os.getenv("PP_TABLE_MCAP_DAILY", "pp_market_cap_daily_contin")


def _load_category_maps() -> tuple[dict[str, str], dict[str, str]]:
    default_file = _BACKEND_ROOT / "prices" / "category_mapping.csv"
    path = pathlib.Path(os.getenv("PP_CATEGORY_FILE", str(default_file)))
    id_map: dict[str, str] = {}
    sym_map: dict[str, str] = {}

    if not path.exists():
        print(f"[{now_str()}] [category] mapping file missing: {path} (fallback='Other')")
        return id_map, sym_map

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames:
                print(f"[{now_str()}] [category] mapping header missing in {path} (fallback='Other')")
                return id_map, sym_map

            headers = {h.strip().lower(): h for h in reader.fieldnames if h}
            id_key = headers.get("id")
            sym_key = headers.get("symbol")
            cat_key = headers.get("category")
            if not cat_key:
                print(f"[{now_str()}] [category] no 'category' column in {path} (fallback='Other')")
                return id_map, sym_map

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
        return id_map, sym_map
    except Exception as exc:
        print(f"[{now_str()}] [category] failed reading {path}: {exc} (fallback='Other')")
        return {}, {}


ID_CATEGORY_MAP, SYMBOL_CATEGORY_MAP = _load_category_maps()


def category_for(coin_id: Optional[str], symbol: Optional[str]) -> str:
    cid = (coin_id or "").strip().lower()
    if cid:
        cat = ID_CATEGORY_MAP.get(cid)
        if cat:
            return cat
    sym = (symbol or "").strip().upper()
    if sym:
        cat = SYMBOL_CATEGORY_MAP.get(sym)
        if cat:
            return cat
    return "Other"


def connect_astra():
    AstraConfig.from_env()
    return get_session(return_cluster=True)


AstraConfig.from_env()

API_TIER = (os.getenv("COINGECKO_API_TIER") or "demo").strip().lower()
API_BASE = os.getenv(
    "COINGECKO_BASE_URL",
    "https://api.coingecko.com/api/v3" if API_TIER == "demo" else "https://pro-api.coingecko.com/api/v3",
).strip()
CG_TIMEOUT_SEC = int(os.getenv("CG_TIMEOUT_SEC", "45"))
CG_RETRIES = int(os.getenv("CG_RETRIES", "3"))
CG_REQ_INTERVAL_S = float(os.getenv("CG_REQUEST_INTERVAL_S", "1.2"))
CG_MAX_RPM_PER_KEY = int(os.getenv("CG_MAX_RPM_PER_KEY", "25"))
CG_BACKOFF_BASE_S = float(os.getenv("CG_BACKOFF_BASE_S", "2.5"))


def _load_api_keys() -> list[str]:
    keys: list[str] = []
    packed = (os.getenv("COINGECKO_API_KEYS") or "").strip()
    if packed:
        for item in packed.split(","):
            k = item.strip()
            if k and k not in keys:
                keys.append(k)

    for env_name in ("COINGECKO_API_KEY_AA", "COINGECKO_API_KEY_BB", "COINGECKO_API_KEY"):
        k = (os.getenv(env_name) or "").strip()
        if k and k not in keys:
            keys.append(k)

    return keys


class KeyPool:
    def __init__(self, keys: list[str], req_interval_s: float, max_rpm: int):
        if not keys:
            raise RuntimeError("No CoinGecko API key found. Set COINGECKO_API_KEY_AA/BB or COINGECKO_API_KEYS.")
        self.keys = keys
        self.req_interval_s = req_interval_s
        self.max_rpm = max_rpm
        self._times: dict[str, deque[float]] = {k: deque() for k in keys}
        self._rr = 0

    def _throttle(self, key: str) -> None:
        dq = self._times[key]
        now = time.time()

        if dq and (now - dq[-1]) < self.req_interval_s:
            time.sleep(self.req_interval_s - (now - dq[-1]))
            now = time.time()

        cutoff = now - 60.0
        while dq and dq[0] < cutoff:
            dq.popleft()

        if len(dq) >= self.max_rpm:
            sleep_for = 60.0 - (now - dq[0]) + 0.01
            time.sleep(max(0.01, sleep_for))

    def reserve(self, hint: Optional[str] = None, retry_offset: int = 0) -> str:
        if hint and len(self.keys) > 1:
            base = zlib.crc32(hint.encode("utf-8")) % len(self.keys)
            idx = (base + max(0, int(retry_offset))) % len(self.keys)
        else:
            idx = self._rr % len(self.keys)
            self._rr += 1
        key = self.keys[idx]
        self._throttle(key)
        self._times[key].append(time.time())
        return key


KEY_POOL = KeyPool(_load_api_keys(), CG_REQ_INTERVAL_S, CG_MAX_RPM_PER_KEY)


def cg_get(path: str, params: Optional[dict[str, Any]] = None, *, hint: Optional[str] = None) -> dict[str, Any]:
    url = f"{API_BASE}{path}"
    base_params = dict(params or {})
    last_err: Optional[Exception] = None

    for attempt in range(1, CG_RETRIES + 1):
        key = KEY_POOL.reserve(hint=hint, retry_offset=(attempt - 1))
        req_params = dict(base_params)
        headers: dict[str, str] = {}

        if API_TIER == "demo":
            headers["x-cg-demo-api-key"] = key
            req_params["x_cg_demo_api_key"] = key
        else:
            headers["x-cg-pro-api-key"] = key
            req_params["x_cg_pro_api_key"] = key

        try:
            resp = requests.get(url, params=req_params, headers=headers, timeout=CG_TIMEOUT_SEC)

            if resp.status_code in (408, 429, 500, 502, 503, 504):
                wait_s = CG_BACKOFF_BASE_S * attempt + random.uniform(0.0, 0.5)
                print(f"[{now_str()}] [cg] {path} -> {resp.status_code}; retry in {wait_s:.1f}s ({attempt}/{CG_RETRIES})")
                time.sleep(wait_s)
                last_err = requests.HTTPError(f"{resp.status_code}", response=resp)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as exc:
            last_err = exc
            wait_s = CG_BACKOFF_BASE_S * attempt + random.uniform(0.0, 0.5)
            print(f"[{now_str()}] [cg] {path} request error: {exc}; retry in {wait_s:.1f}s ({attempt}/{CG_RETRIES})")
            time.sleep(wait_s)

    raise RuntimeError(f"CoinGecko request failed: {path}; last_error={last_err}")


def cg_market_chart_range(coin_id: str, start_ts: datetime, end_ts_exclusive: datetime, *, vs_currency: str = "usd") -> dict[str, Any]:
    start_ts = to_utc(start_ts)
    end_ts_exclusive = to_utc(end_ts_exclusive)
    to_ts = end_ts_exclusive - timedelta(seconds=1)
    if to_ts <= start_ts:
        to_ts = start_ts + timedelta(seconds=1)
    return cg_get(
        f"/coins/{coin_id}/market_chart/range",
        params={
            "vs_currency": vs_currency,
            "from": int(start_ts.timestamp()),
            "to": int(to_ts.timestamp()),
            "precision": "full",
        },
        hint=coin_id,
    )


def extract_series_in_window(points: list[list[float]], start_ts: datetime, end_ts_exclusive: datetime) -> list[tuple[datetime, float]]:
    out: list[tuple[datetime, float]] = []
    start_ts = to_utc(start_ts)
    end_ts_exclusive = to_utc(end_ts_exclusive)
    for ms, value in points or []:
        ts = datetime.fromtimestamp(float(ms) / 1000.0, tz=UTC)
        if start_ts <= ts < end_ts_exclusive and value is not None:
            out.append((ts, float(value)))
    out.sort(key=lambda x: x[0])
    return out


def last_value_in_window(points: list[list[float]], start_ts: datetime, end_ts_exclusive: datetime) -> tuple[Optional[float], Optional[datetime]]:
    series = extract_series_in_window(points, start_ts, end_ts_exclusive)
    if not series:
        return None, None
    ts, value = series[-1]
    return value, ts
