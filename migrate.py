#!/usr/bin/env python3
"""
One-time Astra-to-Astra table copy utility.

Features:
- Idempotent upsert copy (safe to rerun/restart).
- Paging from source, bounded concurrency to target.
- Optional target table truncation (explicit flag only).
- Table subset selection and dry-run mode.
- Optional recent-row filter by timestamp/date column.
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from time import perf_counter
from typing import Any, Mapping, Sequence, cast

from cassandra.cluster import Cluster, Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import PreparedStatement, SimpleStatement

from astra_connect.connect import TARGET_BACKUP, TARGET_MAIN, AstraConfig, get_session


MAX_ERROR_SAMPLES = 20
RECENT_COLUMN_CANDIDATES = ("ts", "date", "last_updated", "updated_at", "created_at")


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def fqtn(keyspace: str, table: str) -> str:
    return f"{qident(keyspace)}.{qident(table)}"


def row_value(row: object, column: str):
    try:
        return row[column]  # type: ignore[index]
    except Exception:
        return getattr(row, column)


def parse_tables_arg(raw: str) -> list[str]:
    if not raw.strip():
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


def choose_recent_column(columns: Sequence[str]) -> str | None:
    column_set = set(columns)
    for candidate in RECENT_COLUMN_CANDIDATES:
        if candidate in column_set:
            return candidate
    return None


def coerce_to_datetime_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            parsed = datetime.fromisoformat(raw)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _cluster_keyspaces(cluster: Cluster, *, role: str) -> Mapping[str, Any]:
    metadata = getattr(cluster, "metadata", None)
    if metadata is None:
        raise RuntimeError(f"{role} cluster metadata is unavailable.")
    keyspaces = getattr(metadata, "keyspaces", None)
    if keyspaces is None:
        raise RuntimeError(f"{role} cluster keyspace metadata is unavailable.")
    return cast(Mapping[str, Any], keyspaces)


def get_table_columns(cluster: Cluster, keyspace: str, table: str) -> list[str]:
    keyspaces = _cluster_keyspaces(cluster, role="source")
    ks_meta = keyspaces.get(keyspace)
    if ks_meta is None:
        raise RuntimeError(f"Keyspace not found in source metadata: {keyspace}")
    table_meta = cast(Any, ks_meta).tables.get(table)
    if table_meta is None:
        raise RuntimeError(f"Table not found in source metadata: {keyspace}.{table}")
    return list(cast(Any, table_meta).columns.keys())


def discover_tables(cluster: Cluster, keyspace: str) -> list[str]:
    keyspaces = _cluster_keyspaces(cluster, role="source")
    ks_meta = keyspaces.get(keyspace)
    if ks_meta is None:
        raise RuntimeError(f"Keyspace not found in source metadata: {keyspace}")
    return sorted(cast(Any, ks_meta).tables.keys())


def _build_create_table_cql_for_target(
    *,
    source_cluster: Cluster,
    source_keyspace: str,
    target_keyspace: str,
    table: str,
) -> str:
    keyspaces = _cluster_keyspaces(source_cluster, role="source")
    ks_meta = keyspaces.get(source_keyspace)
    if ks_meta is None:
        raise RuntimeError(f"Source keyspace not found in metadata: {source_keyspace}")
    table_meta = cast(Any, ks_meta).tables.get(table)
    if table_meta is None:
        raise RuntimeError(f"Source table metadata not found: {source_keyspace}.{table}")

    raw_cql = cast(Any, table_meta).as_cql_query().strip()
    patched = re.sub(
        r"(?is)^\s*CREATE\s+TABLE\s+[^\(]+\(",
        f"CREATE TABLE IF NOT EXISTS {fqtn(target_keyspace, table)} (",
        raw_cql,
        count=1,
    )
    if patched == raw_cql:
        raise RuntimeError(f"Failed to patch CREATE TABLE CQL for {source_keyspace}.{table}")
    if not patched.endswith(";"):
        patched += ";"
    return patched


def ensure_target_tables(
    *,
    source_cluster: Cluster,
    source_keyspace: str,
    target_cluster: Cluster,
    target_session: Session,
    target_keyspace: str,
    tables: Sequence[str],
) -> tuple[int, list[str]]:
    keyspaces = _cluster_keyspaces(target_cluster, role="target")
    target_ks = keyspaces.get(target_keyspace)
    if target_ks is None:
        raise RuntimeError(
            f"Target keyspace not found: {target_keyspace}. Create the keyspace first in Astra, then rerun migration."
        )

    missing = [t for t in tables if t not in cast(Any, target_ks).tables]
    created = 0
    if not missing:
        print(f"[{utc_now_str()}] schema check: all target tables already exist")
        return created, missing

    print(f"[{utc_now_str()}] schema check: creating {len(missing)} missing table(s) in target")
    for table in missing:
        cql = _build_create_table_cql_for_target(
            source_cluster=source_cluster,
            source_keyspace=source_keyspace,
            target_keyspace=target_keyspace,
            table=table,
        )
        print(f"[{utc_now_str()}] schema create: {target_keyspace}.{table}")
        target_session.execute(cql, timeout=None)
        created += 1

    try:
        target_cluster.refresh_schema_metadata()
    except Exception:
        pass

    return created, missing


@dataclass
class TableSummary:
    table: str
    rows_scanned: int = 0
    rows_written: int = 0
    rows_skipped_recent_filter: int = 0
    errors: int = 0
    duration_sec: float = 0.0
    error_samples: list[str] = field(default_factory=list)

    def add_error(self, err: Exception) -> None:
        self.errors += 1
        if len(self.error_samples) < MAX_ERROR_SAMPLES:
            self.error_samples.append(f"{type(err).__name__}: {err}")


def flush_args(
    *,
    session: Session,
    prepared: PreparedStatement,
    args_chunk: Sequence[tuple],
    max_concurrency: int,
    summary: TableSummary,
) -> None:
    results = execute_concurrent_with_args(
        session,
        prepared,
        args_chunk,
        concurrency=max_concurrency,
        raise_on_first_error=False,
        results_generator=True,
    )
    for success, payload in results:
        if success:
            summary.rows_written += 1
        else:
            summary.add_error(payload)  # type: ignore[arg-type]


def copy_table(
    *,
    source_session: Session,
    source_cluster: Cluster,
    source_keyspace: str,
    target_session: Session,
    target_keyspace: str,
    table: str,
    dry_run: bool,
    truncate_target: bool,
    page_size: int,
    max_concurrency: int,
    progress_every: int,
    recent_hours: int,
) -> TableSummary:
    started = perf_counter()
    summary = TableSummary(table=table)
    source_name = fqtn(source_keyspace, table)
    target_name = fqtn(target_keyspace, table)

    columns = get_table_columns(source_cluster, source_keyspace, table)
    if not columns:
        raise RuntimeError(f"Table has no columns: {source_keyspace}.{table}")

    col_sql = ", ".join(qident(c) for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    recent_column = choose_recent_column(columns) if recent_hours > 0 else None
    recent_cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=recent_hours)
        if recent_hours > 0 and recent_column is not None
        else None
    )

    print(f"[{utc_now_str()}] [table={table}] start columns={len(columns)}")
    if recent_hours > 0:
        if recent_column and recent_cutoff:
            print(
                f"[{utc_now_str()}] [table={table}] recent filter: "
                f"column={recent_column} cutoff={recent_cutoff.isoformat()}"
            )
        else:
            print(
                f"[{utc_now_str()}] [table={table}] recent filter requested but no supported "
                f"time column found; writing all scanned rows"
            )
    if truncate_target:
        if dry_run:
            print(f"[{utc_now_str()}] [table={table}] dry-run: would TRUNCATE {target_name}")
        else:
            print(f"[{utc_now_str()}] [table={table}] truncating target table")
            target_session.execute(f"TRUNCATE {target_name}")

    insert_prepared: PreparedStatement | None = None
    if not dry_run:
        insert_prepared = target_session.prepare(
            f"INSERT INTO {target_name} ({col_sql}) VALUES ({placeholders})"
        )

    select_stmt = SimpleStatement(
        f"SELECT {col_sql} FROM {source_name}",
        fetch_size=page_size,
    )

    chunk_size = max(page_size, max_concurrency * 4)
    pending: list[tuple] = []

    for row in source_session.execute(select_stmt, timeout=None):
        summary.rows_scanned += 1
        should_write = True

        if recent_cutoff and recent_column:
            row_dt = coerce_to_datetime_utc(row_value(row, recent_column))
            if row_dt is not None and row_dt < recent_cutoff:
                should_write = False
                summary.rows_skipped_recent_filter += 1

        if dry_run:
            if should_write:
                summary.rows_written += 1
        elif should_write:
            pending.append(tuple(row_value(row, c) for c in columns))
            if len(pending) >= chunk_size:
                flush_args(
                    session=target_session,
                    prepared=insert_prepared,  # type: ignore[arg-type]
                    args_chunk=pending,
                    max_concurrency=max_concurrency,
                    summary=summary,
                )
                pending.clear()

        if summary.rows_scanned % progress_every == 0:
            print(
                f"[{utc_now_str()}] [table={table}] progress "
                f"scanned={summary.rows_scanned} written={summary.rows_written} "
                f"skipped_recent={summary.rows_skipped_recent_filter} errors={summary.errors}"
            )

    if not dry_run and pending:
        flush_args(
            session=target_session,
            prepared=insert_prepared,  # type: ignore[arg-type]
            args_chunk=pending,
            max_concurrency=max_concurrency,
            summary=summary,
        )

    summary.duration_sec = perf_counter() - started
    print(
        f"[{utc_now_str()}] [table={table}] done "
        f"scanned={summary.rows_scanned} written={summary.rows_written} "
        f"skipped_recent={summary.rows_skipped_recent_filter} "
        f"errors={summary.errors} duration={summary.duration_sec:.1f}s"
    )
    return summary


def run(args: argparse.Namespace) -> int:
    if args.source_target == args.target_target:
        raise SystemExit(
            f"source-target and target-target are both {args.source_target!r}. "
            "Choose different targets."
        )
    if args.schema_only and args.dry_run:
        raise SystemExit("--schema-only cannot be combined with --dry-run.")
    if args.schema_only and not args.ensure_schema:
        raise SystemExit("--schema-only requires schema creation (remove --skip-ensure-schema).")
    if args.page_size <= 0:
        raise SystemExit("--page-size must be > 0")
    if args.max_concurrency <= 0:
        raise SystemExit("--max-concurrency must be > 0")
    if args.progress_every <= 0:
        raise SystemExit("--progress-every must be > 0")
    if args.recent_hours < 0:
        raise SystemExit("--recent-hours must be >= 0")

    source_cfg = AstraConfig.from_env(target=args.source_target)
    target_cfg = AstraConfig.from_env(target=args.target_target)

    print(
        f"[{utc_now_str()}] migration start "
        f"source_target={source_cfg.target} target_target={target_cfg.target} "
        f"source_keyspace={source_cfg.keyspace} target_keyspace={target_cfg.keyspace} "
        f"dry_run={args.dry_run} truncate_target={args.truncate_target} "
        f"page_size={args.page_size} max_concurrency={args.max_concurrency} "
        f"recent_hours={args.recent_hours}"
    )
    print(f"[{utc_now_str()}] idempotency: inserts are upserts; reruns are safe.")

    source_session, source_cluster = cast(
        tuple[Session, Cluster],
        get_session(
            target=args.source_target,
            keyspace=source_cfg.keyspace,
            return_cluster=True,
        ),
    )
    target_session, target_cluster = cast(
        tuple[Session, Cluster],
        get_session(
            target=args.target_target,
            keyspace=target_cfg.keyspace,
            return_cluster=True,
        ),
    )
    source_session.default_fetch_size = args.page_size

    summaries: list[TableSummary] = []
    started = perf_counter()
    exit_code = 0

    try:
        all_tables = discover_tables(source_cluster, source_cfg.keyspace)
        requested_tables = parse_tables_arg(args.tables)
        tables = requested_tables if requested_tables else all_tables

        unknown_tables = [t for t in tables if t not in all_tables]
        if unknown_tables:
            raise SystemExit(
                "Unknown source table(s): " + ", ".join(unknown_tables)
            )

        print(f"[{utc_now_str()}] tables to process ({len(tables)}): {', '.join(tables)}")

        if args.ensure_schema and not args.dry_run:
            created, missing = ensure_target_tables(
                source_cluster=source_cluster,
                source_keyspace=source_cfg.keyspace,
                target_cluster=target_cluster,
                target_session=target_session,
                target_keyspace=target_cfg.keyspace,
                tables=tables,
            )
            if missing:
                print(
                    f"[{utc_now_str()}] schema ensure complete: "
                    f"created={created} preexisting={len(tables) - len(missing)}"
                )
        elif args.ensure_schema and args.dry_run:
            print(f"[{utc_now_str()}] dry-run mode: skipping schema creation in target")

        if args.schema_only:
            print(f"[{utc_now_str()}] schema-only mode complete")
            tables = []

        for table in tables:
            try:
                summary = copy_table(
                    source_session=source_session,
                    source_cluster=source_cluster,
                    source_keyspace=source_cfg.keyspace,
                    target_session=target_session,
                    target_keyspace=target_cfg.keyspace,
                    table=table,
                    dry_run=args.dry_run,
                    truncate_target=args.truncate_target,
                    page_size=args.page_size,
                    max_concurrency=args.max_concurrency,
                    progress_every=args.progress_every,
                    recent_hours=args.recent_hours,
                )
            except Exception as exc:
                summary = TableSummary(table=table)
                summary.add_error(exc)
                summary.duration_sec = 0.0
                exit_code = 1
                print(f"[{utc_now_str()}] [table={table}] failed: {type(exc).__name__}: {exc}")
            summaries.append(summary)

            if summary.errors > 0:
                exit_code = 1

    finally:
        try:
            source_cluster.shutdown()
        except Exception:
            pass
        try:
            target_cluster.shutdown()
        except Exception:
            pass

    total_duration = perf_counter() - started
    total_scanned = sum(s.rows_scanned for s in summaries)
    total_written = sum(s.rows_written for s in summaries)
    total_skipped_recent = sum(s.rows_skipped_recent_filter for s in summaries)
    total_errors = sum(s.errors for s in summaries)

    print("")
    print("Migration summary")
    print(
        f"total_tables={len(summaries)} total_scanned={total_scanned} "
        f"total_written={total_written} total_skipped_recent={total_skipped_recent} "
        f"total_errors={total_errors} "
        f"duration={total_duration:.1f}s"
    )
    for s in summaries:
        print(
            f" - {s.table}: scanned={s.rows_scanned} "
            f"written={s.rows_written} skipped_recent={s.rows_skipped_recent_filter} "
            f"errors={s.errors} duration={s.duration_sec:.1f}s"
        )
        for msg in s.error_samples:
            print(f"    error: {msg}")

    if total_errors > 0:
        print(f"[{utc_now_str()}] migration completed with errors")
    else:
        print(f"[{utc_now_str()}] migration completed successfully")
    return exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Copy rows 1:1 from source Astra DB target to destination Astra DB target."
    )
    parser.add_argument(
        "--source-target",
        choices=[TARGET_MAIN, TARGET_BACKUP],
        default=os.getenv("MIGRATION_SOURCE_TARGET", TARGET_MAIN),
        help="Source Astra target profile (main|backup).",
    )
    parser.add_argument(
        "--target-target",
        choices=[TARGET_MAIN, TARGET_BACKUP],
        default=os.getenv("MIGRATION_TARGET_TARGET", TARGET_BACKUP),
        help="Destination Astra target profile (main|backup).",
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table list. Default: all tables in source keyspace.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan source and report counts without writing to target.",
    )
    parser.add_argument(
        "--truncate-target",
        action="store_true",
        default=False,
        help="Explicitly truncate each target table before copy.",
    )
    parser.add_argument(
        "--ensure-schema",
        dest="ensure_schema",
        action="store_true",
        default=True,
        help="Create missing target tables from source metadata before copying (default: enabled).",
    )
    parser.add_argument(
        "--skip-ensure-schema",
        dest="ensure_schema",
        action="store_false",
        help="Do not auto-create missing target tables.",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=int(os.getenv("MIGRATE_MAX_CONCURRENCY", "64")),
        help="Max concurrent writes to target.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=int(os.getenv("MIGRATE_PAGE_SIZE", "500")),
        help="Source read page size.",
    )
    parser.add_argument(
        "--recent-hours",
        type=int,
        default=int(os.getenv("MIGRATE_RECENT_HOURS", "0")),
        help=(
            "Only upsert rows whose ts/date/last_updated/updated_at/created_at is within "
            "the last N hours. 0 disables the recent-row filter."
        ),
    )
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Only create missing target tables, do not copy table data.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=int(os.getenv("MIGRATE_PROGRESS_EVERY", "10000")),
        help="Progress log interval in scanned rows.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
