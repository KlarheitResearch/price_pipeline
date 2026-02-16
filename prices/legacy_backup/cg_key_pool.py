from __future__ import annotations

import os
import random
import time
import zlib
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional

import requests


UTC = timezone.utc


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")


API_TIER = (os.getenv("COINGECKO_API_TIER") or "demo").strip().lower()

CG_REQ_INTERVAL_S = float(os.getenv("CG_REQUEST_INTERVAL_S", "1.2"))
CG_MAX_RPM_PER_KEY = int(os.getenv("CG_MAX_RPM_PER_KEY", "25"))
CG_BACKOFF_BASE_S = float(os.getenv("CG_BACKOFF_BASE_S", "2.5"))
CG_RATE_LIMIT_COOLDOWN_S = float(os.getenv("CG_RATE_LIMIT_COOLDOWN_S", "75"))
CG_CREDIT_EXHAUSTED_COOLDOWN_S = float(os.getenv("CG_CREDIT_EXHAUSTED_COOLDOWN_S", "43200"))
CG_AUTH_FAILURE_COOLDOWN_S = float(os.getenv("CG_AUTH_FAILURE_COOLDOWN_S", "21600"))
CG_WAIT_ON_ALL_KEYS_SUSPENDED = os.getenv("CG_WAIT_ON_ALL_KEYS_SUSPENDED", "1") == "1"
CG_ALL_KEYS_MAX_WAIT_S = float(os.getenv("CG_ALL_KEYS_MAX_WAIT_S", "120"))
CG_ALL_KEYS_WAIT_PAD_S = float(os.getenv("CG_ALL_KEYS_WAIT_PAD_S", "0.25"))
CG_CREDIT_EXHAUSTED_UNTIL_MONTH_END = os.getenv("CG_CREDIT_EXHAUSTED_UNTIL_MONTH_END", "0") == "1"


def _parse_csv_env(raw: Optional[str]) -> list[str]:
    out: list[str] = []
    for part in (raw or "").split(","):
        val = part.strip()
        if val and val not in out:
            out.append(val)
    return out


def _normalize_key(raw: str) -> str:
    key = (raw or "").strip()
    if key.lower().startswith("api key:"):
        key = key.split(":", 1)[1].strip()
    return key


def _resolve_disabled_key_values(tokens: list[str]) -> set[str]:
    out: set[str] = set()
    for token in tokens:
        t = token.strip()
        if not t:
            continue
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

    def _calls_cap_reached() -> bool:
        has_calls_cap = any(tok in t for tok in ("calls limit", "call limit", "api call limit", "request limit"))
        if not has_calls_cap:
            return False
        has_reached = any(tok in t for tok in ("you've reached", "you have reached", "limit reached", "exceeded"))
        has_plan_hint = any(tok in t for tok in ("developer dashboard", "subscribe", "pricing", "plan"))
        return has_reached or has_plan_hint

    calls_cap = _calls_cap_reached()
    positive = ("monthly", "credit", "quota", "usage cap", "plan limit", "billing", "upgrade plan")
    if not calls_cap and not any(tok in t for tok in positive):
        return False

    per_minute_like = ("per minute" in t or "minute rate" in t or "too many requests" in t)
    has_quota_hint = ("monthly" in t or "credit" in t or "quota" in t or "usage cap" in t)
    if per_minute_like and not (calls_cap or has_quota_hint):
        return False
    return True


def _is_monthly_credit_exhaustion_text(text: str) -> bool:
    t = (text or "").lower()
    if not t:
        return False

    has_call_cap_like = (
        ("calls limit" in t or "call limit" in t)
        and ("you've reached" in t or "you have reached" in t or "developer dashboard" in t or "pricing" in t)
    )
    has_credit_like = has_call_cap_like or any(
        tok in t for tok in ("credit", "credits", "quota", "allowance", "usage cap", "plan limit")
    )
    has_month_like = any(tok in t for tok in ("monthly", "month", "billing cycle", "next month", "resets"))

    if has_call_cap_like and not has_month_like:
        if "10,000" in t or "10000" in t or API_TIER == "demo":
            has_month_like = True

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


def load_api_keys() -> list[str]:
    keys: list[str] = []
    disabled = _resolve_disabled_key_values(_parse_csv_env(os.getenv("COINGECKO_DISABLED_KEYS")))

    packed = (os.getenv("COINGECKO_API_KEYS") or "").strip()
    if packed:
        for item in packed.split(","):
            k = _normalize_key(item)
            if k and k not in disabled and k not in keys:
                keys.append(k)

    preferred_pool = [
        "COINGECKO_API_KEY_AA",
        "COINGECKO_API_KEY_BB",
        "COINGECKO_API_KEY_CC",
        "COINGECKO_API_KEY_DD",
    ]
    extras = sorted(
        n for n in os.environ.keys()
        if n.startswith("COINGECKO_API_KEY_") and n not in preferred_pool
    )
    for env_name in preferred_pool + extras:
        k = _normalize_key(os.getenv(env_name) or "")
        if k and k not in disabled and k not in keys:
            keys.append(k)

    allow_generic_fallback = os.getenv("COINGECKO_ALLOW_GENERIC_KEY_FALLBACK", "0") == "1"
    if allow_generic_fallback:
        fallback_single = _normalize_key(os.getenv("COINGECKO_API_KEY") or "")
        if fallback_single and fallback_single not in disabled and fallback_single not in keys:
            if not keys:
                keys.append(fallback_single)

    return keys


class KeyPool:
    def __init__(self, keys: list[str], req_interval_s: float, max_rpm: int):
        if not keys:
            raise RuntimeError(
                "No CoinGecko API key found. Set COINGECKO_API_KEY_AA/BB/CC/DD, "
                "COINGECKO_API_KEYS, or enable COINGECKO_ALLOW_GENERIC_KEY_FALLBACK=1."
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


def build_key_pool() -> KeyPool:
    keys = load_api_keys()
    return KeyPool(keys, CG_REQ_INTERVAL_S, CG_MAX_RPM_PER_KEY)


def cg_http_get(
    *,
    base_url: str,
    path: str,
    params: Optional[dict[str, Any]] = None,
    retries: int = 3,
    timeout_sec: int = 45,
    hint: Optional[str] = None,
    key_pool: Optional[KeyPool] = None,
) -> dict[str, Any]:
    pool = key_pool or build_key_pool()
    url = f"{base_url}{path}"
    base_params = dict(params or {})
    last_err: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            key = pool.reserve(hint=hint, retry_offset=(attempt - 1))
        except RuntimeError as exc:
            last_err = exc
            if attempt >= retries:
                break
            wait_s = CG_BACKOFF_BASE_S * attempt + random.uniform(0.0, 0.5)
            print(f"[{now_str()}] [cg] {path} reserve error: {exc}; retry in {wait_s:.1f}s ({attempt}/{retries})")
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
            resp = requests.get(url, params=req_params, headers=headers, timeout=timeout_sec)
            status_code = resp.status_code
            err_text = _extract_error_text(resp) if status_code >= 400 else ""
            if status_code >= 400:
                pool.note_response(key, status_code, err_text)

            if status_code in (401, 402, 403, 408, 429, 500, 502, 503, 504):
                wait_s = CG_BACKOFF_BASE_S * attempt + random.uniform(0.0, 0.5)
                extra = f" err={err_text}" if err_text else ""
                print(
                    f"[{now_str()}] [cg] {path} -> {status_code}; retry in {wait_s:.1f}s "
                    f"({attempt}/{retries}); key={_mask_key(key)}{extra}"
                )
                time.sleep(wait_s)
                last_err = requests.HTTPError(f"{status_code}", response=resp)
                continue

            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_err = exc
            wait_s = CG_BACKOFF_BASE_S * attempt + random.uniform(0.0, 0.5)
            print(f"[{now_str()}] [cg] {path} request error: {exc}; retry in {wait_s:.1f}s ({attempt}/{retries})")
            time.sleep(wait_s)

    raise RuntimeError(f"CoinGecko request failed: {path}; last_error={last_err}")
