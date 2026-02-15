#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import time
import random
import zlib
import pathlib
import csv
import json
import socket
import uuid
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import requests

_BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.append(str(_BACKEND_ROOT))

from astra_connect.connect import get_session, AstraConfig

# Load .env early so verbosity/table envs are available during module import.
AstraConfig.from_env()


UTC = timezone.utc


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def is_verbose() -> bool:
    return _env_flag("VERBOSE_PRINTS", False) or _env_flag("PP_VERBOSE_PRINTS", False)


def heartbeat_sec() -> int:
    return max(5, int(os.getenv("PP_HEARTBEAT_SEC", "20")))


def progress_every_verbose() -> int:
    return max(1, int(os.getenv("PP_PROGRESS_EVERY", "10")))


# Backward-compatible constant for scripts importing VERBOSE_PRINTS directly.
VERBOSE_PRINTS = is_verbose()


def should_log_progress(idx: int, total: int, default_every: int = 100) -> bool:
    if total <= 0:
        return True
    if idx <= 1 or idx >= total:
        return True
    every = progress_every_verbose() if is_verbose() else max(1, int(default_every))
    return idx % every == 0


class Heartbeat:
    def __init__(self, label: str, interval_sec: Optional[int] = None):
        self.label = label
        self.interval_sec = max(5, int(interval_sec if interval_sec is not None else heartbeat_sec()))
        now_mono = time.monotonic()
        self._started = now_mono
        self._last = now_mono

    def maybe(self, extra: Optional[str] = None, *, force: bool = False) -> None:
        if not is_verbose():
            return
        now_mono = time.monotonic()
        if not force and (now_mono - self._last) < self.interval_sec:
            return
        elapsed = int(now_mono - self._started)
        suffix = f" {extra}" if extra else ""
        print(f"[{now_str()}] [heartbeat] {self.label} alive elapsed={elapsed}s{suffix}")
        self._last = now_mono


def enqueue_async(session, pending: deque, query, params, *, timeout: Optional[float] = None, max_in_flight: int = 64) -> None:
    pending.append(session.execute_async(query, params, timeout=timeout))
    while len(pending) >= max(1, int(max_in_flight)):
        pending.popleft().result()


def drain_async(pending: deque) -> None:
    while pending:
        pending.popleft().result()


def vprint(msg: str) -> None:
    if is_verbose():
        print(f"[{now_str()}] {msg}")


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


def _parse_utc_instant(raw: Optional[str]) -> Optional[datetime]:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def _test_mode_active() -> bool:
    if os.getenv("PP_FORCE_TEST_MODE", "0") == "1":
        return True
    until = _parse_utc_instant(os.getenv("PP_TEST_MODE_UNTIL_UTC"))
    if until is None:
        return False
    return now_utc() < until


def get_rank_window() -> Optional[tuple[int, int]]:
    if _test_mode_active():
        return None
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
TABLE_PIPELINE_RUNS = os.getenv("PP_TABLE_PIPELINE_RUNS", "pp_pipeline_runs")
TABLE_PIPELINE_LATEST = os.getenv("PP_TABLE_PIPELINE_LATEST", "pp_pipeline_latest")
PIPELINE_HEALTH_ENABLED = _env_flag("PP_HEALTH_ENABLED", True)


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


class PipelineHealthTracker:
    def __init__(self, session, script: str):
        self.session = session
        self.script = script
        self.run_id = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:12]}"
        self.started_at = now_utc()
        self.ended_at: Optional[datetime] = None
        self.workflow = (os.getenv("GITHUB_WORKFLOW") or os.getenv("PP_WORKFLOW_NAME") or "").strip() or None
        gh_event = (os.getenv("GITHUB_EVENT_NAME") or "").strip()
        if gh_event:
            self.trigger_source = f"github:{gh_event}"
        elif (os.getenv("CF_WORKER_NAME") or os.getenv("WORKER_NAME") or "").strip():
            self.trigger_source = "cloudflare:cron"
        else:
            self.trigger_source = "manual"
        self.scope = scope_label()
        rw = get_rank_window()
        self.rank_start = rw[0] if rw else None
        self.rank_end = rw[1] if rw else None
        self.host = (os.getenv("HOSTNAME") or socket.gethostname() or "").strip() or None
        self.metrics: dict[str, Any] = {}
        self._final_status = "success"
        self._started = False
        self._finished = False
        self._disabled = not PIPELINE_HEALTH_ENABLED
        self._warned = False
        self._error_text: Optional[str] = None

    def set_metric(self, name: str, value: Any) -> None:
        self.metrics[str(name)] = value

    def inc_metric(self, name: str, delta: int | float = 1) -> None:
        key = str(name)
        prev = self.metrics.get(key, 0)
        try:
            self.metrics[key] = prev + delta
        except Exception:
            self.metrics[key] = delta

    def mark_noop(self) -> None:
        self._final_status = "noop"

    def start(self) -> None:
        if self._started or self._disabled:
            return
        self._started = True
        self._write_row(status="running", error_text=None)

    def finish(self, status: str | None = None, error_text: Optional[str] = None) -> None:
        if self._finished:
            return
        self._finished = True
        self.ended_at = now_utc()
        if status is None:
            status = self._final_status
        if error_text is not None:
            self._error_text = str(error_text)
        self._write_row(status=status, error_text=self._error_text)

    def _warn_disable(self, msg: str) -> None:
        if not self._warned:
            print(f"[{now_str()}] [health] disabled for {self.script}: {msg}")
            self._warned = True
        self._disabled = True

    def _metrics_json(self) -> str:
        try:
            text = json.dumps(self.metrics, separators=(",", ":"), sort_keys=True, default=str)
        except Exception:
            text = "{}"
        if len(text) > 16000:
            text = text[:16000]
        return text

    def _write_row(self, status: str, error_text: Optional[str]) -> None:
        if self._disabled:
            return
        try:
            now_ts = now_utc()
            duration_sec = int((now_ts - self.started_at).total_seconds()) if self.started_at else None
            metrics_json = self._metrics_json()
            err = (error_text or "").strip() or None
            if err and len(err) > 1000:
                err = err[:1000]

            ps_run = self.session.prepare(
                f"""
                INSERT INTO {TABLE_PIPELINE_RUNS}
                  (script, started_at, run_id,
                   workflow, trigger_source, scope, rank_start, rank_end,
                   status, ended_at, duration_sec,
                   metrics_json, error, host, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )
            self.session.execute(
                ps_run,
                [
                    self.script,
                    to_cassandra_ts(self.started_at),
                    self.run_id,
                    self.workflow,
                    self.trigger_source,
                    self.scope,
                    self.rank_start,
                    self.rank_end,
                    status,
                    to_cassandra_ts(self.ended_at) if self.ended_at is not None else None,
                    duration_sec,
                    metrics_json,
                    err,
                    self.host,
                    to_cassandra_ts(now_ts),
                ],
            )

            ps_latest = self.session.prepare(
                f"""
                INSERT INTO {TABLE_PIPELINE_LATEST}
                  (script, run_id,
                   workflow, trigger_source, scope, rank_start, rank_end,
                   status, started_at, ended_at, duration_sec,
                   metrics_json, error, host, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )
            self.session.execute(
                ps_latest,
                [
                    self.script,
                    self.run_id,
                    self.workflow,
                    self.trigger_source,
                    self.scope,
                    self.rank_start,
                    self.rank_end,
                    status,
                    to_cassandra_ts(self.started_at),
                    to_cassandra_ts(self.ended_at) if self.ended_at is not None else None,
                    duration_sec,
                    metrics_json,
                    err,
                    self.host,
                    to_cassandra_ts(now_ts),
                ],
            )
        except Exception as exc:
            self._warn_disable(str(exc))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, _tb):
        if exc is None:
            self.finish(self._final_status, self._error_text)
            return False
        err = f"{exc_type.__name__}: {exc}" if exc_type is not None else str(exc)
        self.finish("failed", err)
        return False


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
CG_RATE_LIMIT_COOLDOWN_S = float(os.getenv("CG_RATE_LIMIT_COOLDOWN_S", "75"))
CG_CREDIT_EXHAUSTED_COOLDOWN_S = float(os.getenv("CG_CREDIT_EXHAUSTED_COOLDOWN_S", "43200"))
CG_AUTH_FAILURE_COOLDOWN_S = float(os.getenv("CG_AUTH_FAILURE_COOLDOWN_S", "21600"))
CG_WAIT_ON_ALL_KEYS_SUSPENDED = os.getenv("CG_WAIT_ON_ALL_KEYS_SUSPENDED", "1") == "1"
CG_ALL_KEYS_MAX_WAIT_S = float(os.getenv("CG_ALL_KEYS_MAX_WAIT_S", "120"))
CG_ALL_KEYS_WAIT_PAD_S = float(os.getenv("CG_ALL_KEYS_WAIT_PAD_S", "0.25"))
CG_CREDIT_EXHAUSTED_UNTIL_MONTH_END = os.getenv("CG_CREDIT_EXHAUSTED_UNTIL_MONTH_END", "1") == "1"


def _parse_csv_env(raw: Optional[str]) -> list[str]:
    out: list[str] = []
    for part in (raw or "").split(","):
        val = part.strip()
        if val and val not in out:
            out.append(val)
    return out


def _resolve_disabled_key_values(tokens: list[str]) -> set[str]:
    out: set[str] = set()
    for token in tokens:
        t = token.strip()
        if not t:
            continue
        # Allow either raw key values or env var names (e.g. COINGECKO_API_KEY_CC).
        env_val = (os.getenv(t) or "").strip()
        if env_val:
            out.add(env_val)
        else:
            out.add(t)
    return out


def _mask_key(key: str) -> str:
    if len(key) <= 8:
        return "***"
    return f"{key[:4]}...{key[-4:]}"


def _is_credit_exhaustion_text(text: str) -> bool:
    t = (text or "").lower()
    if not t:
        return False
    positive = ("monthly", "credit", "quota", "usage cap", "plan limit", "billing", "upgrade plan")
    if not any(tok in t for tok in positive):
        return False
    # Distinguish short-term rate limits from monthly plan exhaustion.
    if ("per minute" in t or "minute rate" in t or "too many requests" in t) and ("monthly" not in t and "credit" not in t):
        return False
    return True


def _is_monthly_credit_exhaustion_text(text: str) -> bool:
    t = (text or "").lower()
    if not t:
        return False
    has_credit_like = any(tok in t for tok in ("credit", "credits", "quota", "allowance", "usage cap", "plan limit"))
    has_month_like = any(tok in t for tok in ("monthly", "month", "billing cycle", "next month", "resets"))
    return has_credit_like and has_month_like


def _seconds_until_next_utc_month() -> float:
    now = now_utc()
    year = now.year + (1 if now.month == 12 else 0)
    month = 1 if now.month == 12 else now.month + 1
    next_month = datetime(year, month, 1, tzinfo=UTC)
    return max(1.0, (next_month - now).total_seconds())


def _extract_error_text(resp: requests.Response) -> str:
    text = ""
    try:
        payload = resp.json()
        if isinstance(payload, dict):
            if isinstance(payload.get("error"), str):
                text = payload.get("error", "")
            elif isinstance(payload.get("status"), dict):
                status_obj = payload.get("status") or {}
                text = str(status_obj.get("error_message") or status_obj.get("error_code") or "")
            elif isinstance(payload.get("message"), str):
                text = payload.get("message", "")
            else:
                text = str(payload)
        else:
            text = str(payload)
    except Exception:
        text = (resp.text or "").strip()
    text = " ".join(text.split())
    return text[:280]


def _load_api_keys() -> list[str]:
    keys: list[str] = []
    disabled = _resolve_disabled_key_values(_parse_csv_env(os.getenv("COINGECKO_DISABLED_KEYS")))

    packed = (os.getenv("COINGECKO_API_KEYS") or "").strip()
    if packed:
        for item in packed.split(","):
            k = item.strip()
            if k and k not in disabled and k not in keys:
                keys.append(k)

    preferred = [
        "COINGECKO_API_KEY_AA",
        "COINGECKO_API_KEY_BB",
        "COINGECKO_API_KEY_CC",
        "COINGECKO_API_KEY_DD",
        "COINGECKO_API_KEY",
    ]
    extras = sorted(
        n for n in os.environ.keys()
        if n.startswith("COINGECKO_API_KEY_") and n not in preferred
    )
    for env_name in preferred + extras:
        k = (os.getenv(env_name) or "").strip()
        if k and k not in disabled and k not in keys:
            keys.append(k)

    return keys


class KeyPool:
    def __init__(self, keys: list[str], req_interval_s: float, max_rpm: int):
        if not keys:
            raise RuntimeError(
                "No CoinGecko API key found. Set COINGECKO_API_KEY_AA/BB/CC/DD, "
                "COINGECKO_API_KEY, COINGECKO_API_KEY_* or COINGECKO_API_KEYS."
            )
        self.keys = keys
        self.req_interval_s = req_interval_s
        self.max_rpm = max_rpm
        self._times: dict[str, deque[float]] = {k: deque() for k in keys}
        self._suspend_until: dict[str, float] = {k: 0.0 for k in keys}
        self._suspend_reason: dict[str, str] = {k: "" for k in keys}
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

    def _pick_available_index(self, base_idx: int) -> tuple[Optional[int], float]:
        now = time.time()
        best_wait = 0.0
        best_idx: Optional[int] = None
        for offset in range(len(self.keys)):
            idx = (base_idx + offset) % len(self.keys)
            key = self.keys[idx]
            wait_for = self._suspend_until.get(key, 0.0) - now
            if wait_for <= 0:
                return idx, 0.0
            if best_idx is None or wait_for < best_wait:
                best_idx = idx
                best_wait = wait_for
        return None, best_wait

    def suspend(self, key: str, for_seconds: float, reason: str) -> None:
        until = time.time() + max(1.0, float(for_seconds))
        prev = self._suspend_until.get(key, 0.0)
        if until > prev:
            self._suspend_until[key] = until
            self._suspend_reason[key] = reason
            mins = max(1, int(round((until - time.time()) / 60.0)))
            print(f"[{now_str()}] [cg] suspend key {_mask_key(key)} for ~{mins}m ({reason})")

    def note_response(self, key: str, status_code: int, err_text: str) -> None:
        txt = (err_text or "").strip()

        def _credit_suspend(reason_prefix: str) -> None:
            cooldown = max(1.0, CG_CREDIT_EXHAUSTED_COOLDOWN_S)
            reason = "credit_exhausted"
            if CG_CREDIT_EXHAUSTED_UNTIL_MONTH_END and _is_monthly_credit_exhaustion_text(txt):
                cooldown = max(cooldown, _seconds_until_next_utc_month() + 60.0)
                reason = "credit_exhausted_monthly"
            self.suspend(key, cooldown, reason if not reason_prefix else f"{reason_prefix}_{reason}")

        if status_code == 429:
            if _is_credit_exhaustion_text(txt):
                _credit_suspend("")
            else:
                self.suspend(key, CG_RATE_LIMIT_COOLDOWN_S, "rate_limited")
            return

        if status_code in (402, 403) and _is_credit_exhaustion_text(txt):
            _credit_suspend(f"http_{status_code}")
            return

        if status_code == 401:
            self.suspend(key, CG_AUTH_FAILURE_COOLDOWN_S, "auth_failed")

    def reserve(self, hint: Optional[str] = None, retry_offset: int = 0) -> str:
        if hint and len(self.keys) > 1:
            base = zlib.crc32(hint.encode("utf-8")) % len(self.keys)
            idx = (base + max(0, int(retry_offset))) % len(self.keys)
        else:
            idx = self._rr % len(self.keys)
            self._rr += 1

        deadline = time.time() + max(0.0, CG_ALL_KEYS_MAX_WAIT_S)
        wait_logged = False
        while True:
            usable_idx, next_wait = self._pick_available_index(idx)
            if usable_idx is not None:
                key = self.keys[usable_idx]
                self._throttle(key)
                self._times[key].append(time.time())
                return key

            details = []
            short_term_waits: list[float] = []
            now = time.time()
            for key_i in self.keys:
                wait_s = max(0.0, self._suspend_until.get(key_i, 0.0) - now)
                reason = self._suspend_reason.get(key_i, "") or "unknown"
                details.append(f"{_mask_key(key_i)}={int(wait_s)}s({reason})")
                if wait_s > 0 and reason in ("rate_limited", "unknown"):
                    short_term_waits.append(wait_s)

            remaining = deadline - now
            can_wait = (
                CG_WAIT_ON_ALL_KEYS_SUSPENDED
                and len(short_term_waits) > 0
                and remaining > 0
            )
            if can_wait:
                sleep_for = min(min(short_term_waits) + max(0.0, CG_ALL_KEYS_WAIT_PAD_S), remaining)
                sleep_for = max(0.1, sleep_for)
                if not wait_logged:
                    print(
                        f"[{now_str()}] [cg] all keys temporarily suspended; "
                        f"waiting {sleep_for:.1f}s before retry. details: {', '.join(details)}"
                    )
                    wait_logged = True
                time.sleep(sleep_for)
                continue

            raise RuntimeError(
                "All CoinGecko keys are temporarily suspended; "
                f"next key available in {max(1, int(next_wait))}s; details: {', '.join(details)}"
            )


_KEY_POOL: Optional[KeyPool] = None


def _get_key_pool() -> KeyPool:
    global _KEY_POOL
    if _KEY_POOL is None:
        _KEY_POOL = KeyPool(_load_api_keys(), CG_REQ_INTERVAL_S, CG_MAX_RPM_PER_KEY)
    return _KEY_POOL


def cg_get(path: str, params: Optional[dict[str, Any]] = None, *, hint: Optional[str] = None) -> dict[str, Any]:
    url = f"{API_BASE}{path}"
    base_params = dict(params or {})
    last_err: Optional[Exception] = None

    for attempt in range(1, CG_RETRIES + 1):
        try:
            key = _get_key_pool().reserve(hint=hint, retry_offset=(attempt - 1))
        except RuntimeError as exc:
            last_err = exc
            if attempt >= CG_RETRIES:
                break
            wait_s = CG_BACKOFF_BASE_S * attempt + random.uniform(0.0, 0.5)
            print(
                f"[{now_str()}] [cg] {path} reserve error: {exc}; "
                f"retry in {wait_s:.1f}s ({attempt}/{CG_RETRIES})"
            )
            time.sleep(wait_s)
            continue

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
            status_code = resp.status_code
            err_text = _extract_error_text(resp) if status_code >= 400 else ""
            if status_code >= 400:
                _get_key_pool().note_response(key, status_code, err_text)

            if status_code in (401, 402, 403, 408, 429, 500, 502, 503, 504):
                wait_s = CG_BACKOFF_BASE_S * attempt + random.uniform(0.0, 0.5)
                extra = f" err={err_text}" if err_text else ""
                print(
                    f"[{now_str()}] [cg] {path} -> {status_code}; retry in {wait_s:.1f}s "
                    f"({attempt}/{CG_RETRIES}); key={_mask_key(key)}{extra}"
                )
                time.sleep(wait_s)
                last_err = requests.HTTPError(f"{status_code}", response=resp)
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
