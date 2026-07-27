#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import json
import os
import socket
import subprocess
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional, Sequence, cast

from astra_connect.connect import AstraConfig, get_session
from cassandra.cluster import Cluster, Session


UTC = timezone.utc
TABLE_PIPELINE_RUNS = os.getenv("PP_TABLE_PIPELINE_RUNS", "pp_pipeline_runs")
TABLE_PIPELINE_LATEST = os.getenv("PP_TABLE_PIPELINE_LATEST", "pp_pipeline_latest")
HEALTH_ENABLED = (os.getenv("PP_HEALTH_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"})


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")


def to_cassandra_ts(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def parse_float(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        return float(str(value).strip())
    except Exception:
        return default


def first_env(*names: str) -> Optional[str]:
    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue
        value = raw.strip()
        if value:
            return value
    return None


ASTRA_TRANSIENT_STRONG_PATTERNS = (
    "unable to connect to the metadata service",
    "cassandra.driverexception",
    "nohostavailable",
    "datastax/cloud",
    "astra_connect",
)

ASTRA_TRANSIENT_CONTEXTUAL_PATTERNS = (
    "urlopen error timed out",
    "timeouterror: timed out",
    "connection refused",
    "connection reset",
    "temporarily unavailable",
)


def is_transient_astra_failure(output_tail: str) -> bool:
    text = output_tail.lower()
    if any(pattern in text for pattern in ASTRA_TRANSIENT_STRONG_PATTERNS):
        return True
    has_astra_context = "metadata" in text or "astra" in text or "cassandra" in text
    return has_astra_context and any(pattern in text for pattern in ASTRA_TRANSIENT_CONTEXTUAL_PATTERNS)


@contextlib.contextmanager
def temporary_env(overrides: dict[str, Optional[str]]):
    prev: dict[str, Optional[str]] = {}
    try:
        for key, value in overrides.items():
            prev[key] = os.getenv(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)
        yield
    finally:
        for key, value in prev.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def detect_trigger_source() -> str:
    gh_event = first_env("GITHUB_EVENT_NAME")
    if gh_event:
        return f"github:{gh_event}"
    if first_env("CF_WORKER_NAME", "WORKER_NAME"):
        return "cloudflare:cron"
    return "manual"


def derive_rank_bounds(
    explicit_start: Optional[int], explicit_end: Optional[int], command: Sequence[str]
) -> tuple[Optional[int], Optional[int]]:
    if explicit_start is not None and explicit_end is not None:
        return explicit_start, explicit_end

    env_start = parse_int(first_env("PP_RANK_START", "RANK_START", "TOP_N_DQ_START"))
    env_end = parse_int(first_env("PP_RANK_END", "RANK_END", "TOP_N_DQ_END"))
    if env_start is not None and env_end is not None:
        return env_start, env_end

    cmd_start: Optional[int] = None
    cmd_end: Optional[int] = None
    for i, token in enumerate(command):
        if token == "--rank-start" and (i + 1) < len(command):
            cmd_start = parse_int(command[i + 1])
        if token == "--rank-end" and (i + 1) < len(command):
            cmd_end = parse_int(command[i + 1])

    if cmd_start is not None and cmd_end is not None:
        return cmd_start, cmd_end

    top_n = parse_int(first_env("TOP_N", "RANK_TOP_N", "TOP_N_API_DAILY", "TOP_N_AGG_DAILY", "TOP_N_DQ"))
    if top_n is not None and top_n > 0:
        return 1, top_n

    return explicit_start, explicit_end


def derive_scope(
    explicit_scope: Optional[str], rank_start: Optional[int], rank_end: Optional[int]
) -> Optional[str]:
    if explicit_scope:
        scoped = explicit_scope.strip()
        if scoped:
            return scoped
    if rank_start is not None and rank_end is not None:
        return f"rank[{rank_start}-{rank_end}]"
    if first_env("GAPFILL_ENABLED") == "1":
        return "gapfill"
    return None


class PipelineHealthTracker:
    def __init__(
        self,
        session: Session,
        script: str,
        *,
        scope: Optional[str],
        rank_start: Optional[int],
        rank_end: Optional[int],
    ):
        self.session = session
        self.script = script
        self.scope = scope
        self.rank_start = rank_start
        self.rank_end = rank_end
        self.run_id = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:12]}"
        self.started_at = now_utc()
        self.ended_at: Optional[datetime] = None
        self.workflow = first_env("GITHUB_WORKFLOW", "PP_WORKFLOW_NAME")
        self.trigger_source = detect_trigger_source()
        self.host = first_env("HOSTNAME") or socket.gethostname()
        self.metrics: dict[str, Any] = {}
        self.error_text: Optional[str] = None
        self.disabled = False
        self.ps_run = None
        self.ps_latest = None

        try:
            self.ps_run = self.session.prepare(
                f"""
                INSERT INTO {TABLE_PIPELINE_RUNS}
                  (script, started_at, run_id,
                   workflow, trigger_source, scope, rank_start, rank_end,
                   status, ended_at, duration_sec,
                   metrics_json, error, host, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )
            self.ps_latest = self.session.prepare(
                f"""
                INSERT INTO {TABLE_PIPELINE_LATEST}
                  (script, run_id,
                   workflow, trigger_source, scope, rank_start, rank_end,
                   status, started_at, ended_at, duration_sec,
                   metrics_json, error, host, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )
        except Exception as exc:
            print(f"[{now_str()}] [health] disabled for {script}: {exc}")
            self.disabled = True

    def set_metric(self, name: str, value: Any) -> None:
        self.metrics[str(name)] = value

    def _metrics_json(self) -> str:
        try:
            text = json.dumps(self.metrics, separators=(",", ":"), sort_keys=True, default=str)
        except Exception:
            text = "{}"
        if len(text) > 16000:
            text = text[:16000]
        return text

    def _write_row(self, status: str, error_text: Optional[str]) -> None:
        if self.disabled or self.ps_run is None or self.ps_latest is None:
            return
        try:
            now_ts = now_utc()
            duration_sec = int((now_ts - self.started_at).total_seconds())
            err = (error_text or "").strip() or None
            if err is not None and len(err) > 1000:
                err = err[:1000]
            metrics_json = self._metrics_json()

            self.session.execute(
                self.ps_run,
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
                    to_cassandra_ts(self.ended_at),
                    duration_sec,
                    metrics_json,
                    err,
                    self.host,
                    to_cassandra_ts(now_ts),
                ],
            )

            self.session.execute(
                self.ps_latest,
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
                    to_cassandra_ts(self.ended_at),
                    duration_sec,
                    metrics_json,
                    err,
                    self.host,
                    to_cassandra_ts(now_ts),
                ],
            )
        except Exception as exc:
            self.disabled = True
            print(f"[{now_str()}] [health] disabled for {self.script}: {exc}")

    def start(self) -> None:
        self._write_row("running", None)

    def finish(self, status: str, error_text: Optional[str] = None) -> None:
        self.ended_at = now_utc()
        self.error_text = error_text
        self._write_row(status, error_text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Wrap legacy scripts and write pipeline health rows to Astra."
    )
    parser.add_argument("--script-id", required=True, help="Logical script id for health tables.")
    parser.add_argument("--scope", default=None, help="Optional scope label.")
    parser.add_argument("--rank-start", type=int, default=None, help="Optional inclusive rank start.")
    parser.add_argument("--rank-end", type=int, default=None, help="Optional inclusive rank end.")
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to execute, e.g. -- python prices/prod_pipeline/AA_gck_load_prices_live.py",
    )
    return parser.parse_args()


def normalize_command(raw_command: Sequence[str]) -> list[str]:
    command = list(raw_command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("No command provided. Use -- <command...> after wrapper args.")
    return command


def run_command_with_retry(command: Sequence[str], script_id: str) -> tuple[int, int, str]:
    attempts = max(1, parse_int(first_env("PP_COMMAND_ATTEMPTS")) or 1)
    delay_sec = max(0.0, parse_float(first_env("PP_COMMAND_RETRY_DELAY_SEC"), 120.0))
    tail_lines_limit = max(20, parse_int(first_env("PP_COMMAND_OUTPUT_TAIL_LINES")) or 160)
    final_tail = ""

    for attempt in range(1, attempts + 1):
        attempt_started = time.time()
        prefix = f"[{now_str()}] [runner] start {script_id}"
        if attempts > 1:
            prefix += f" attempt={attempt}/{attempts}"
        print(f"{prefix}: {' '.join(command)}")

        tail: deque[str] = deque(maxlen=tail_lines_limit)
        proc = subprocess.Popen(
            list(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end="")
            tail.append(line)
        return_code = proc.wait()
        elapsed = int(time.time() - attempt_started)
        final_tail = "".join(tail)
        print(
            f"[{now_str()}] [runner] end {script_id} attempt={attempt}/{attempts}: "
            f"exit={return_code} elapsed={elapsed}s"
        )

        if return_code == 0:
            return return_code, attempt, final_tail

        if attempt >= attempts or not is_transient_astra_failure(final_tail):
            return return_code, attempt, final_tail

        print(
            f"[{now_str()}] [runner] transient Astra connectivity failure for {script_id}; "
            f"retrying in {delay_sec:.0f}s"
        )
        time.sleep(delay_sec)

    return 1, attempts, final_tail


def main() -> int:
    args = parse_args()
    command = normalize_command(args.command)

    rank_start, rank_end = derive_rank_bounds(args.rank_start, args.rank_end, command)
    scope = derive_scope(args.scope, rank_start, rank_end)

    tracker: Optional[PipelineHealthTracker] = None
    cluster: Optional[Cluster] = None

    if HEALTH_ENABLED:
        try:
            health_attempts = first_env("PP_HEALTH_CONNECT_ATTEMPTS") or "1"
            health_retry_base = first_env("PP_HEALTH_CONNECT_RETRY_BASE_SEC") or "1"
            health_retry_max = first_env("PP_HEALTH_CONNECT_RETRY_MAX_SEC") or health_retry_base
            health_request_timeout = first_env("PP_HEALTH_REQUEST_TIMEOUT_SEC") or "30"
            health_connect_timeout = first_env("PP_HEALTH_CONNECT_TIMEOUT_SEC") or "10"
            with temporary_env(
                {
                    "ASTRA_CONNECT_ATTEMPTS": health_attempts,
                    "ASTRA_CONNECT_RETRY_BASE_SEC": health_retry_base,
                    "ASTRA_CONNECT_RETRY_MAX_SEC": health_retry_max,
                    "REQUEST_TIMEOUT_SEC": health_request_timeout,
                    "CONNECT_TIMEOUT_SEC": health_connect_timeout,
                }
            ):
                AstraConfig.from_env()
                session, cluster = cast(tuple[Session, Cluster], get_session(return_cluster=True))
            tracker = PipelineHealthTracker(
                session,
                args.script_id,
                scope=scope,
                rank_start=rank_start,
                rank_end=rank_end,
            )
            tracker.set_metric("runner", "pipeline_health_runner")
            tracker.set_metric("command", " ".join(command)[:600])
            tracker.start()
        except Exception as exc:
            print(f"[{now_str()}] [health] unavailable for {args.script_id}: {exc}")
            tracker = None
    else:
        print(f"[{now_str()}] [health] disabled by PP_HEALTH_ENABLED=0")

    started = time.time()
    return_code, command_attempts_used, output_tail = run_command_with_retry(command, args.script_id)
    elapsed = int(time.time() - started)
    print(f"[{now_str()}] [runner] final {args.script_id}: exit={return_code} elapsed={elapsed}s")

    if tracker is not None:
        tracker.set_metric("exit_code", return_code)
        tracker.set_metric("elapsed_sec", elapsed)
        tracker.set_metric("command_attempts", command_attempts_used)
        if return_code == 0:
            tracker.finish("success")
        else:
            error_text = f"Command exited with code {return_code}"
            if is_transient_astra_failure(output_tail):
                error_text += " after transient Astra connectivity retries"
            tracker.finish("failed", error_text)

    if cluster is not None:
        try:
            cluster.shutdown()
        except Exception:
            pass

    return int(return_code)


if __name__ == "__main__":
    raise SystemExit(main())
