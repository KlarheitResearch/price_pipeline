# load_fx_daily.py
import os
import pathlib
import random
import sys
import time
import datetime as dt
import requests

_THIS_FILE = pathlib.Path(__file__).resolve()
_SEARCH_ROOTS = (
    _THIS_FILE.parent.parent,          # backend/ (current repo layout)
    _THIS_FILE.parent.parent / "backend",  # repo root + backend/ (pipeline repo layout)
)
for _root in _SEARCH_ROOTS:
    if (_root / "astra_connect").is_dir() and str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
        break

from astra_connect.connect import AstraConfig, get_session

KEYSPACE_OVERRIDE = (os.getenv("ASTRA_KEYSPACE_OVERRIDE") or "").strip()

PROVIDER = "frankfurter"
BASE     = "USD"
TARGETS  = [
    "EUR","GBP","JPY","CNY","KRW","INR","AUD","CAD","CHF",
    "HKD","SGD","BRL","MXN","TRY","ZAR","IDR","TWD"
]

# Tables
TBL_DAILY  = "fx_rates_daily_by_symbol"
TBL_LIVE   = "fx_rates_live"
TBL_STATUS = "fx_status"

def _env_bool(name: str, default: str = "0") -> bool:
    raw = (os.getenv(name, default) or "").strip().lower()
    return raw in {"1", "true", "t", "yes", "y", "on"}

HTTP_MAX_ATTEMPTS = max(1, int(os.getenv("FX_HTTP_MAX_ATTEMPTS", "7")))
HTTP_CONNECT_TIMEOUT = max(1.0, float(os.getenv("FX_HTTP_CONNECT_TIMEOUT", "5")))
HTTP_READ_TIMEOUT = max(1.0, float(os.getenv("FX_HTTP_READ_TIMEOUT", "25")))
HTTP_BACKOFF_BASE_SECS = max(0.1, float(os.getenv("FX_HTTP_BACKOFF_BASE_SECS", "1")))
HTTP_BACKOFF_CAP_SECS = max(0.5, float(os.getenv("FX_HTTP_BACKOFF_CAP_SECS", "30")))
HTTP_JITTER_SECS = max(0.0, float(os.getenv("FX_HTTP_JITTER_SECS", "0.5")))

ALLOW_STALE_ON_FETCH_FAILURE = _env_bool("FX_ALLOW_STALE_ON_FETCH_FAILURE", "1")
MAX_STALE_DAYS = max(0, int(os.getenv("FX_STALE_MAX_DAYS", "3")))
ALLOW_RANGE_DEGRADED_SUCCESS = _env_bool("FX_ALLOW_RANGE_DEGRADED_SUCCESS", "1")

_HTTP = requests.Session()
_HTTP.headers.update({"User-Agent": "blockviz-fx-loader/2"})

def log(msg: str) -> None:
    now = dt.datetime.now().isoformat(timespec="seconds")
    print(f"[{now}] {msg}", flush=True)

def to_pydate(x):
    """Convert Cassandra driver Date/timestamp or strings to datetime.date."""
    if x is None:
        return None
    if isinstance(x, dt.date) and not isinstance(x, dt.datetime):
        return x
    if isinstance(x, dt.datetime):
        return x.date()
    try:
        if hasattr(x, "year") and hasattr(x, "month") and hasattr(x, "day"):
            return dt.date(int(x.year), int(x.month), int(x.day))
    except Exception:
        pass
    try:
        if hasattr(x, "date") and callable(getattr(x, "date")):
            d = x.date()
            if isinstance(d, dt.date):
                return d
    except Exception:
        pass
    try:
        s = str(x)
        y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
        return dt.date(y, m, d)
    except Exception:
        return None

def cassandra_session():
    cfg = AstraConfig.from_env()
    keyspace = KEYSPACE_OVERRIDE or cfg.keyspace
    log(
        f"Connecting to Cassandra keyspace '{keyspace}' "
        f"(target={cfg.target}, bundle='{cfg.bundle_path}')"
    )
    session, cluster = get_session(keyspace=keyspace, return_cluster=True)
    log("Cassandra session established")
    return session, cluster

def _request_json(url: str, params: dict, label: str):
    last_err = None
    for attempt in range(1, HTTP_MAX_ATTEMPTS + 1):
        try:
            log(
                f"{label}: attempt {attempt}/{HTTP_MAX_ATTEMPTS} "
                f"(connect_timeout={HTTP_CONNECT_TIMEOUT}s, read_timeout={HTTP_READ_TIMEOUT}s)"
            )
            r = _HTTP.get(url, params=params, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT))
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            last_err = e
            if attempt == HTTP_MAX_ATTEMPTS:
                break
            wait = min(HTTP_BACKOFF_CAP_SECS, HTTP_BACKOFF_BASE_SECS * (2 ** (attempt - 1)))
            wait += random.uniform(0, HTTP_JITTER_SECS)
            log(
                f"{label}: attempt {attempt}/{HTTP_MAX_ATTEMPTS} failed: {e}. "
                f"Retrying in {wait:.1f}s..."
            )
            time.sleep(wait)
    raise RuntimeError(f"{label} failed after {HTTP_MAX_ATTEMPTS} attempts: {last_err}") from last_err

def _allow_stale_success(prev: dt.date | None) -> bool:
    if not ALLOW_STALE_ON_FETCH_FAILURE:
        return False
    if prev is None:
        log("Stale-success disabled: no previous watermark in fx_status")
        return False
    today_utc = dt.datetime.now(dt.timezone.utc).date()
    age_days = max(0, (today_utc - prev).days)
    if age_days <= MAX_STALE_DAYS:
        log(
            f"Allowing stale-success: latest watermark {prev} is {age_days} day(s) old "
            f"(max allowed={MAX_STALE_DAYS})"
        )
        return True
    log(
        f"Stale-success rejected: latest watermark {prev} is {age_days} day(s) old "
        f"(max allowed={MAX_STALE_DAYS})"
    )
    return False

def get_latest(base: str, symbols):
    url = "https://api.frankfurter.app/latest"
    params = {"from": base, "to": ",".join(symbols)}
    data = _request_json(
        url=url,
        params=params,
        label=f"Fetching latest FX rates for base={base} ({len(symbols)} symbols)",
    )
    log(f"Fetched latest provider date {data.get('date')} with {len(data.get('rates', {}))} symbol(s)")
    return data

def frankfurter_range(start_date: str, end_date: str, base: str, symbols):
    url = f"https://api.frankfurter.app/{start_date}..{end_date}"
    params = {"from": base, "to": ",".join(symbols)}
    log(f"Fetching range {start_date}..{end_date} for base={base} ({len(symbols)} symbols)")
    data = _request_json(
        url=url,
        params=params,
        label=f"Fetching FX range {start_date}..{end_date}",
    )
    days = len(data.get("rates", {}))
    log(f"Range {start_date}..{end_date} returned {days} day(s) with data")
    return data

def read_latest_status(s):
    row = s.execute(
        f"SELECT latest_date FROM {TBL_STATUS} WHERE provider=%s AND base=%s",
        (PROVIDER, BASE)
    ).one()
    latest = row.latest_date if row else None
    log(f"Current DB latest_date (observed watermark): {latest}")
    return latest

def write_status(s, asof_date: dt.date):
    log(f"Writing status latest_date={asof_date}")
    s.execute(f"""
        INSERT INTO {TBL_STATUS} (provider, base, latest_date, last_fetch_ts)
        VALUES (%s, %s, %s, toTimestamp(now()))
    """, (PROVIDER, BASE, asof_date))

# Prepared statements
_PREPARED = False
_INS_DAILY = None
_INS_LIVE  = None
_INS_WITH_SOURCE = False

def _prepare_inserts(s):
    """Prepare inserts for by-symbol daily table and live table. Prefer 'source' if present."""
    global _PREPARED, _INS_DAILY, _INS_LIVE, _INS_WITH_SOURCE
    if _PREPARED:
        return

    # Daily by-symbol: must include asof_year
    try:
        _INS_DAILY = s.prepare(f"""
            INSERT INTO {TBL_DAILY} (provider, base, symbol, asof_year, asof_date, rate, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
         """)
        _INS_LIVE = s.prepare(f"""
            INSERT INTO {TBL_LIVE} (provider, base, symbol, asof_date, rate, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """)
        _INS_WITH_SOURCE = True
        log("Prepared INSERT statements with 'source' column")
    except Exception:
        # Fallback if 'source' missing (kept for safety during rollout)
        _INS_DAILY = s.prepare(f"""
            INSERT INTO {TBL_DAILY} (provider, base, symbol, asof_year, asof_date, rate)
             VALUES (?, ?, ?, ?, ?, ?)
        """)
        _INS_LIVE = s.prepare(f"""
            INSERT INTO {TBL_LIVE} (provider, base, symbol, asof_date, rate)
            VALUES (?, ?, ?, ?, ?)
        """)
        _INS_WITH_SOURCE = False
        log("'source' column not found; using inserts without it")
    finally:
        _PREPARED = True

def publish_observed_day(s, asof_date: dt.date, rates: dict, source: str = "observed"):
    """
    Upsert one observed day into:
      - fx_rates_daily_by_symbol (partitioned by symbol+year)
      - fx_rates_live (latest snapshot per symbol)
    """
    _prepare_inserts(s)

    asof_year = int(asof_date.year)
    # Only write rates we actually received (no synthetic fill)
    symbols_written = 0

    log(f"Upserting observed rates for {asof_date} (year={asof_year}): payload has {len(rates)} symbol(s) [source={source}]")
    for sym in TARGETS:
        if sym not in rates:
            continue
        val = float(rates[sym])
        if _INS_WITH_SOURCE:
            s.execute(_INS_DAILY, (PROVIDER, BASE, sym, asof_year, asof_date, val, source))
        else:
            s.execute(_INS_DAILY, (PROVIDER, BASE, sym, asof_year, asof_date, val))
        symbols_written += 1

    # Refresh live rows to this provider date (still observed-only)
    log(f"Refreshing live snapshot for {asof_date}: writing {symbols_written} symbol(s) [source={source}]")
    for sym in TARGETS:
        if sym not in rates:
            continue
        val = float(rates[sym])
        if _INS_WITH_SOURCE:
            s.execute(_INS_LIVE, (PROVIDER, BASE, sym, asof_date, val, source))
        else:
            s.execute(_INS_LIVE, (PROVIDER, BASE, sym, asof_date, val))

def _parse_payload_date(asof_str: str) -> dt.date:
    y, m, d = map(int, asof_str.split("-"))
    return dt.date(y, m, d)

def main():
    log(f"Starting daily FX load v2 (provider={PROVIDER}, base={BASE})")
    s, cluster = cassandra_session()
    try:
        # 1) Fetch provider latest (do NOT use local server date for business logic)
        try:
            payload = get_latest(BASE, TARGETS)
        except Exception as latest_err:
            log(f"Failed to fetch latest provider payload: {latest_err}")
            prev_raw = read_latest_status(s)
            prev = to_pydate(prev_raw)
            if _allow_stale_success(prev):
                log("Exiting successfully with stale FX data because provider latest fetch is unavailable")
                return
            raise
        asof_str = payload.get("date")  # latest provider business date
        if not asof_str:
            raise RuntimeError("Frankfurter payload missing 'date'")
        latest_provider_date = _parse_payload_date(asof_str)

        latest_rates = payload.get("rates", {}) or {}
        log(f"Latest provider date: {latest_provider_date} ({len(latest_rates)} symbol(s) in payload)")

        # 2) Determine watermark
        prev_raw = read_latest_status(s)
        prev = to_pydate(prev_raw)

        if prev is None:
            # First run: publish latest provider date only
            log("No previous status found; publishing latest provider date only")
            publish_observed_day(s, latest_provider_date, latest_rates, source="observed")
            write_status(s, latest_provider_date)
            log(f"Published {latest_provider_date} (observed) with {len(latest_rates)} symbol(s)")

        elif prev < latest_provider_date:
            # Gap: fetch observed dates from prev..latest_provider_date, then ingest only returned dates
            start = prev.isoformat()
            end   = latest_provider_date.isoformat()

            try:
                data = frankfurter_range(start, end, BASE, TARGETS)
            except Exception as range_err:
                if not ALLOW_RANGE_DEGRADED_SUCCESS:
                    raise
                gap_days = max(0, (latest_provider_date - prev).days - 1)
                log(
                    f"Range fetch failed ({range_err}); entering degraded mode. "
                    f"Publishing latest payload only and keeping watermark at {prev}. "
                    f"Missing historical days to recover later: up to {gap_days}"
                )
                publish_observed_day(s, latest_provider_date, latest_rates, source="observed")
                log("Degraded run completed: live/latest date refreshed without watermark advancement")
                return

            # Frankfurter returns a dict { 'YYYY-MM-DD': { 'EUR': 0.9, ... }, ... }
            rates_by_day = data.get("rates", {}) or {}

            if not rates_by_day:
                # If nothing returned, still consider updating live from latest payload,
                # but do not advance status unless we actually published observed dates.
                log(f"Range {start}..{end} returned no rates; publishing latest payload only (no status advance beyond provider date if write succeeds)")
                publish_observed_day(s, latest_provider_date, latest_rates, source="observed")
                write_status(s, latest_provider_date)
            else:
                # Ingest only observed dates, in chronological order, strictly newer than prev
                observed_dates = sorted(_parse_payload_date(k) for k in rates_by_day.keys())
                published_any = False
                last_published = None

                for day in observed_dates:
                    if day <= prev:
                        continue
                    day_rates = rates_by_day.get(day.isoformat(), {}) or rates_by_day.get(str(day), {}) or {}
                    log(f"Publishing observed day {day}")
                    publish_observed_day(s, day, day_rates, source="observed")
                    last_published = day
                    published_any = True

                # Ensure we at least publish the latest payload day if for some reason it wasn't in the range
                # (rare, but keeps live consistent)
                if latest_provider_date > (last_published or dt.date(1, 1, 1)):
                    if latest_provider_date not in observed_dates:
                        log(f"Latest provider date {latest_provider_date} not present in range payload; publishing latest payload explicitly")
                        publish_observed_day(s, latest_provider_date, latest_rates, source="observed")
                        last_published = latest_provider_date
                        published_any = True

                if published_any and last_published:
                    write_status(s, last_published)
                    log(f"Advanced watermark to latest observed date {last_published}")
                else:
                    log("No new observed dates to publish; leaving watermark unchanged")

        else:
            # prev >= latest_provider_date: republish latest provider date (idempotent refresh)
            # Useful if provider corrected rates or for ensuring live is in sync.
            log(f"Watermark {prev} is >= provider latest date {latest_provider_date}; refreshing live/history for provider date (idempotent)")
            publish_observed_day(s, latest_provider_date, latest_rates, source="observed")
            # Do not change status (already at/after provider date)

        log("FX load v2 completed successfully (observed-only, no forward fill, no projection)")

    finally:
        log("Shutting down Cassandra session")
        try:
            cluster.shutdown()
        except Exception:
            pass

if __name__ == "__main__":
    main()
