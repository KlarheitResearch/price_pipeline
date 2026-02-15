#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any

_BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.append(str(_BACKEND_ROOT))

from astra_connect.connect import AstraConfig, get_session


UTC = timezone.utc
TABLE_PIPELINE_LATEST = os.getenv("PP_TABLE_PIPELINE_LATEST", "pp_pipeline_latest")
STALE_MINUTES = max(1, int(os.getenv("PP_HEALTH_STALE_MINUTES", "30")))
ERROR_PREVIEW = max(60, int(os.getenv("PP_HEALTH_ERROR_PREVIEW", "180")))


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")


def to_utc(ts) -> datetime | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def connect_astra():
    AstraConfig.from_env()
    return get_session(return_cluster=True)


def _fmt_ts(ts) -> str:
    dt = to_utc(ts)
    if dt is None:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _age_seconds(ts) -> int | None:
    dt = to_utc(ts)
    if dt is None:
        return None
    return max(0, int((now_utc() - dt).total_seconds()))


def _load_metrics(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    text = str(raw).strip()
    if not text:
        return {}
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _metrics_preview(metrics: dict[str, Any]) -> str:
    if not metrics:
        return "-"

    preferred = [
        "coins",
        "coins_scoped",
        "wrote",
        "wrote_10m",
        "wrote_hourly",
        "wrote_daily",
        "wrote_monthly",
        "wrote_live",
        "wrote_rolling",
        "wrote_mcap",
        "missing_slots",
        "api_failures",
        "errors",
    ]

    parts: list[str] = []
    for key in preferred:
        if key in metrics:
            parts.append(f"{key}={metrics[key]}")
        if len(parts) >= 4:
            break

    if not parts:
        for key in sorted(metrics.keys()):
            val = metrics[key]
            if isinstance(val, (int, float, str, bool)):
                parts.append(f"{key}={val}")
            if len(parts) >= 4:
                break

    return ", ".join(parts) if parts else "-"


def main() -> None:
    session, cluster = connect_astra()
    try:
        rows = list(
            session.execute(
                f"""
                SELECT script, status, scope, rank_start, rank_end,
                       workflow, trigger_source, started_at, ended_at,
                       duration_sec, metrics_json, error, updated_at
                FROM {TABLE_PIPELINE_LATEST}
                """
            )
        )
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

    if not rows:
        print(f"[{now_str()}] No rows in {TABLE_PIPELINE_LATEST}.")
        return

    rows.sort(
        key=lambda r: (
            to_utc(getattr(r, "updated_at", None)) or datetime(1970, 1, 1, tzinfo=UTC),
            str(getattr(r, "script", "")),
        ),
        reverse=True,
    )

    counts: dict[str, int] = {}
    for row in rows:
        status = (getattr(row, "status", "") or "unknown").strip().lower()
        counts[status] = counts.get(status, 0) + 1
    count_summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    print(
        f"[{now_str()}] Pipeline health snapshot: scripts={len(rows)} "
        f"stale_threshold={STALE_MINUTES}m ({count_summary})"
    )
    print(
        "script                             status   age   dur   "
        "scope                source            metrics"
    )
    print("-" * 120)

    stale_seconds = STALE_MINUTES * 60
    for row in rows:
        script = str(getattr(row, "script", "") or "-")[:33]
        status = str(getattr(row, "status", "") or "unknown")[:7]
        age_s = _age_seconds(getattr(row, "updated_at", None))
        age_txt = f"{age_s:>4}s" if age_s is not None else "   -"
        if age_s is not None and age_s > stale_seconds:
            age_txt = f"{age_txt}*"
        else:
            age_txt = f"{age_txt} "

        duration = getattr(row, "duration_sec", None)
        dur_txt = f"{int(duration):>4}s" if isinstance(duration, int) else "   -"

        rank_start = getattr(row, "rank_start", None)
        rank_end = getattr(row, "rank_end", None)
        if isinstance(rank_start, int) and isinstance(rank_end, int):
            scope = f"{rank_start}-{rank_end}"
        else:
            scope = str(getattr(row, "scope", "") or "-")
        scope = scope[:20]

        source = str(getattr(row, "trigger_source", "") or "-")
        source = source[:16]

        metrics = _metrics_preview(_load_metrics(getattr(row, "metrics_json", None)))
        print(
            f"{script:<33} {status:<7} {age_txt:<6} {dur_txt:>4}   "
            f"{scope:<20} {source:<16} {metrics}"
        )

        err = (getattr(row, "error", None) or "").strip()
        if err:
            err_line = err.replace("\n", " ")
            if len(err_line) > ERROR_PREVIEW:
                err_line = err_line[:ERROR_PREVIEW] + "..."
            print(f"  error: {err_line}")
        ended = _fmt_ts(getattr(row, "ended_at", None))
        if ended != "-":
            print(f"  ended_at={ended}")

    print("* age marked with '*' is stale by PP_HEALTH_STALE_MINUTES")


if __name__ == "__main__":
    main()
