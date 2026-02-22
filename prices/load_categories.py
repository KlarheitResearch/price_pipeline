"""Load asset categories into a canonical table keyed by CoinGecko id."""

import csv
import os
import pathlib
import sys
from datetime import datetime, timezone

# Repo root & helpers
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.append(str(_REPO_ROOT))

try:
    from paths import rel, chdir_repo_root
except Exception:
    def rel(*parts: str) -> pathlib.Path:
        return _REPO_ROOT.joinpath(*parts)

    def chdir_repo_root() -> None:
        os.chdir(_REPO_ROOT)

from astra_connect.connect import AstraConfig, get_session

chdir_repo_root()

KEYSPACE_OVERRIDE = (os.getenv("ASTRA_KEYSPACE_OVERRIDE") or "").strip()

_DEFAULT_CATEGORY_FILE_PROD = rel("prices", "prod_pipeline", "category_mapping.csv")
_DEFAULT_CATEGORY_FILE_SHARED = rel("prices", "category_mapping.csv")


def resolve_category_file() -> str:
    env_path = (os.getenv("CATEGORY_FILE") or "").strip()
    if env_path:
        return env_path
    if _DEFAULT_CATEGORY_FILE_PROD.exists():
        return str(_DEFAULT_CATEGORY_FILE_PROD)
    return str(_DEFAULT_CATEGORY_FILE_SHARED)


CATEGORY_FILE = resolve_category_file()
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


cfg = AstraConfig.from_env()
effective_keyspace = KEYSPACE_OVERRIDE or cfg.keyspace
log(
    f"Config: target='{cfg.target}', bundle='{cfg.bundle_path}', "
    f"keyspace='{effective_keyspace}'"
)
log(
    f"Config: category_file='{CATEGORY_FILE}', "
    f"timeouts(connect={CONNECT_TIMEOUT}s, request={REQUEST_TIMEOUT}s)"
)
log("Connecting to Astra")
s, cluster = get_session(keyspace=effective_keyspace, return_cluster=True)
log("Connected")

UP_CAT = s.prepare(
    """
    INSERT INTO asset_categories (id, symbol, category, updated_at, source)
    VALUES (?, ?, ?, ?, ?)
    """
)


def autodetect_and_load(path: str) -> list[dict]:
    """
    Return records with keys: id (CoinGecko), symbol, category.
    Tries common delimiters and is robust to header cases.
    """
    records: list[dict] = []
    total_rows = 0
    used_delim = None

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        for delim in [",", ";", "\t", "|"]:
            f.seek(0)
            dr = csv.DictReader(f, delimiter=delim)
            fieldnames = dr.fieldnames
            if not fieldnames:
                continue

            headers = [h.strip().lower() for h in fieldnames]
            if "id" in headers and "category" in headers:
                used_delim = delim
                id_key = fieldnames[headers.index("id")]
                sym_key = fieldnames[headers.index("symbol")] if "symbol" in headers else None
                cat_key = fieldnames[headers.index("category")]

                for row in dr:
                    total_rows += 1
                    idv = (row.get(id_key) or "").strip()
                    sym = (row.get(sym_key) or "").strip().upper() if sym_key else None
                    cat = (row.get(cat_key) or "").strip() or "Other"
                    if idv:
                        records.append({"id": idv, "symbol": sym, "category": cat})
                break

    log(f"CSV parsed: delimiter='{used_delim}', raw_rows={total_rows}, records_kept={len(records)}")

    dedup: dict[str, dict] = {}
    conflicts = 0
    for rec in records:
        cid = rec["id"]
        if cid in dedup and dedup[cid]["category"] != rec["category"]:
            conflicts += 1
        dedup[cid] = rec

    if conflicts:
        log(f"Warning: category conflicts for {conflicts} id(s); last value kept")
    return list(dedup.values())


def main() -> None:
    records = autodetect_and_load(CATEGORY_FILE)
    now = datetime.now(timezone.utc)
    up_count = 0
    none_symbol = 0

    log("Truncating table asset_categories")
    s.execute(f"TRUNCATE {effective_keyspace}.asset_categories")

    for rec in records:
        cid = rec["id"]
        sym = rec.get("symbol") or None
        cat = rec["category"]
        if sym is None:
            none_symbol += 1
        s.execute(UP_CAT, [cid, sym, cat, now, "manual_csv"])
        up_count += 1

    log(
        f"Loaded categories: rows_upserted={up_count}, records_no_symbol={none_symbol} "
        f"(from {CATEGORY_FILE})"
    )


if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass
