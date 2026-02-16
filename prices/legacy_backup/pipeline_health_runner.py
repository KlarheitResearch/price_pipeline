#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import time
import uuid
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


def first_env(*names: str) -> Optional[str]:
    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue
        value = raw.strip()
        if value:
            return value
    return None


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
        help="Command to execute, e.g. -- python prices/legacy_backup/AA_gck_load_prices_live.py",
    )
    return parser.parse_args()


def normalize_command(raw_command: Sequence[str]) -> list[str]:
    command = list(raw_command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("No command provided. Use -- <command...> after wrapper args.")
    return command


def main() -> int:
    args = parse_args()
    command = normalize_command(args.command)

    rank_start, rank_end = derive_rank_bounds(args.rank_start, args.rank_end, command)
    scope = derive_scope(args.scope, rank_start, rank_end)

    tracker: Optional[PipelineHealthTracker] = None
    cluster: Optional[Cluster] = None

    if HEALTH_ENABLED:
        try:
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
    print(f"[{now_str()}] [runner] start {args.script_id}: {' '.join(command)}")
    completed = subprocess.run(command, check=False)
    elapsed = int(time.time() - started)
    print(f"[{now_str()}] [runner] end {args.script_id}: exit={completed.returncode} elapsed={elapsed}s")

    if tracker is not None:
        tracker.set_metric("exit_code", completed.returncode)
        tracker.set_metric("elapsed_sec", elapsed)
        if completed.returncode == 0:
            tracker.finish("success")
        else:
            tracker.finish("failed", f"Command exited with code {completed.returncode}")

    if cluster is not None:
        try:
            cluster.shutdown()
        except Exception:
            pass

    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
