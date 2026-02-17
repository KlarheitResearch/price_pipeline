#!/usr/bin/env python3
from __future__ import annotations

import pathlib

from prices.potential_future.common import Heartbeat, connect_astra, now_str, should_log_progress


def split_cql_statements(text: str) -> list[str]:
    stmts: list[str] = []
    current: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue
        current.append(raw_line)
        if ";" in raw_line:
            joined = "\n".join(current).strip()
            joined = joined.rstrip(";").strip()
            if joined and not joined.upper().startswith("USE "):
                stmts.append(joined)
            current = []
    if current:
        joined = "\n".join(current).strip()
        if joined and not joined.upper().startswith("USE "):
            stmts.append(joined)
    return stmts


def main() -> None:
    hb = Heartbeat("bootstrap_tables")
    cql_path = pathlib.Path(__file__).with_name("00_create_test_tables.cql")
    if not cql_path.exists():
        raise FileNotFoundError(f"Missing CQL file: {cql_path}")

    text = cql_path.read_text(encoding="utf-8")
    stmts = split_cql_statements(text)
    if not stmts:
        print(f"[{now_str()}] No CQL statements found.")
        return

    print(f"[{now_str()}] Connecting to Astra and applying {len(stmts)} statements...")
    session, cluster = connect_astra()
    try:
        for i, stmt in enumerate(stmts, 1):
            session.execute(stmt)
            first_line = stmt.splitlines()[0].strip()
            if should_log_progress(i, len(stmts), default_every=5):
                print(f"[{now_str()}] [{i}/{len(stmts)}] OK: {first_line}")
            hb.maybe(extra=f"stmt={i}/{len(stmts)}")
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

    print(f"[{now_str()}] Bootstrap complete.")


if __name__ == "__main__":
    main()
