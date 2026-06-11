# astra_connect/connect.py
import os
import pathlib
import sys
import threading
from dataclasses import dataclass, replace
from typing import Optional, Tuple

from dotenv import find_dotenv, load_dotenv

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import RoundRobinPolicy


TARGET_MAIN = "main"
TARGET_BACKUP = "backup"
VALID_TARGETS = {TARGET_MAIN, TARGET_BACKUP}


def _find_repo_root(start: Optional[pathlib.Path] = None) -> pathlib.Path:
    """
    Walk upward from `start` (or CWD) until a project marker is found.
    Fallback to the package's parent (i.e., the directory containing `astra_connect/`),
    not the drive root.
    """
    here = (start or pathlib.Path.cwd()).resolve()
    markers = (".env", "pyproject.toml", ".git")
    for p in [here, *here.parents]:
        if any((p / m).exists() for m in markers):
            return p
    return pathlib.Path(__file__).resolve().parents[1]


def _load_env(*, override: bool = False) -> pathlib.Path:
    """
    Load .env from (in order):
      1) DOTENV_FILE (if set),
      2) repo_root/.env,
      3) repo_root/backend/.env,
      4) package_dir/.env,
      5) find_dotenv(usecwd=True).
    Returns the directory from which a .env was loaded; if none, returns repo_root.
    """
    explicit = os.getenv("DOTENV_FILE")
    if explicit:
        c = pathlib.Path(explicit).expanduser()
        if c.is_file():
            load_dotenv(dotenv_path=str(c), override=override)
            return c.parent

    pkg_dir = pathlib.Path(__file__).resolve().parents[1]
    repo_root = _find_repo_root()
    candidates = [
        repo_root / ".env",
        repo_root / "backend" / ".env",
        pkg_dir / ".env",
    ]
    for c in candidates:
        if c.is_file():
            load_dotenv(dotenv_path=str(c), override=override)
            return c.parent

    found = find_dotenv(usecwd=True)
    if found:
        load_dotenv(dotenv_path=found, override=override)
        return pathlib.Path(found).resolve().parent

    return repo_root


def _norm_path(p: Optional[str]) -> Optional[str]:
    if not p:
        return p
    p = os.path.expandvars(os.path.expanduser(p.strip().strip('"').strip("'")))
    return str(pathlib.Path(p))


def _normalize_target(value: Optional[str]) -> str:
    v = (value or os.getenv("ASTRA_TARGET") or TARGET_MAIN).strip().lower()
    if v not in VALID_TARGETS:
        raise RuntimeError(f"Invalid ASTRA target: {v!r}. Expected one of: {', '.join(sorted(VALID_TARGETS))}.")
    return v


def _target_env_name(base: str, target: str) -> str:
    return f"{base}_BACKUP" if target == TARGET_BACKUP else base


def _get_targeted_env(
    base: str,
    *,
    target: str,
    allow_unsuffixed_fallback: bool = True,
) -> Optional[str]:
    names = [_target_env_name(base, target)]
    if allow_unsuffixed_fallback and names[0] != base:
        names.append(base)
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip() != "":
            return value
    return None


def _to_absolute(path_value: pathlib.Path, *, env_root: pathlib.Path) -> pathlib.Path:
    return path_value if path_value.is_absolute() else (env_root / path_value)


def _resolve_bundle_path(*, target: str, env_root: pathlib.Path) -> pathlib.Path:
    bundle_name = _get_targeted_env(
        "ASTRA_BUNDLE_NAME",
        target=target,
        allow_unsuffixed_fallback=(target == TARGET_MAIN),
    )
    raw_bundle_path = _get_targeted_env(
        "ASTRA_BUNDLE_PATH",
        target=target,
        allow_unsuffixed_fallback=True,
    )
    raw_legacy_bundle = _get_targeted_env(
        "ASTRA_BUNDLE",
        target=target,
        allow_unsuffixed_fallback=True,
    )

    # Prefer explicit path var first, then legacy ASTRA_BUNDLE.
    candidate_raw = raw_bundle_path or raw_legacy_bundle

    if candidate_raw:
        candidate = pathlib.Path(_norm_path(candidate_raw) or "")
        candidate_abs = _to_absolute(candidate, env_root=env_root)

        if candidate_abs.exists():
            if candidate_abs.is_file():
                return candidate_abs
            if candidate_abs.is_dir():
                if bundle_name:
                    return candidate_abs / bundle_name
                bundle_name_var = _target_env_name("ASTRA_BUNDLE_NAME", target)
                raise RuntimeError(
                    f"{bundle_name_var} is required when ASTRA_BUNDLE_PATH points to a folder."
                )

        # Backward-compatible mode: ASTRA_BUNDLE_PATH points directly to a zip file path
        # that may not exist yet at config parsing time.
        if candidate.suffix.lower() == ".zip":
            return candidate_abs

        # New mode: folder path + bundle filename.
        if bundle_name:
            return candidate_abs / bundle_name

        bundle_name_var = _target_env_name("ASTRA_BUNDLE_NAME", target)
        raise RuntimeError(
            f"{bundle_name_var} is required when ASTRA_BUNDLE_PATH is not a zip file path."
        )

    # If no path was provided, allow bundle name alone.
    if bundle_name:
        return _to_absolute(pathlib.Path(bundle_name), env_root=env_root)

    if target == TARGET_BACKUP:
        raise RuntimeError(
            "Missing backup secure bundle configuration. Set ASTRA_BUNDLE_PATH + "
            "ASTRA_BUNDLE_NAME_BACKUP (or ASTRA_BUNDLE_BACKUP)."
        )

    # Backward-compatible default for main target.
    return _to_absolute(pathlib.Path("secure-connect.zip"), env_root=env_root)


@dataclass(frozen=True)
class AstraConfig:
    target: str
    bundle_path: str
    token: str
    keyspace: str
    request_timeout_sec: int = 90
    connect_timeout_sec: int = 60
    fetch_size: int = 1000

    @staticmethod
    def from_env(
        *,
        target: Optional[str] = None,
        override_env_with_dotenv: bool = False,
    ) -> "AstraConfig":
        env_root = _load_env(override=override_env_with_dotenv)
        resolved_target = _normalize_target(target)

        token = _get_targeted_env(
            "ASTRA_TOKEN",
            target=resolved_target,
            allow_unsuffixed_fallback=(resolved_target == TARGET_MAIN),
        ) or ""
        keyspace = (
            _get_targeted_env(
                "ASTRA_KEYSPACE",
                target=resolved_target,
                allow_unsuffixed_fallback=True,
            )
            or "default_keyspace"
        )
        bundle_path = _resolve_bundle_path(target=resolved_target, env_root=env_root)

        req = int(os.getenv("REQUEST_TIMEOUT_SEC", "60"))
        conn = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
        fetch_size = int(os.getenv("FETCH_SIZE", "1000"))

        errors = []
        if not token:
            token_var = _target_env_name("ASTRA_TOKEN", resolved_target)
            errors.append(
                f"{token_var} is missing. Create a Database Admin token in Astra DB and set {token_var}."
            )

        if not bundle_path.exists():
            bundle_name_var = _target_env_name("ASTRA_BUNDLE_NAME", resolved_target)
            errors.append(
                f"Secure bundle not found at: {str(bundle_path)!r}. "
                f"Set ASTRA_BUNDLE_PATH to a folder and {bundle_name_var} to the bundle filename."
            )

        if errors:
            msg = "Astra configuration error:\n  - " + "\n  - ".join(errors)
            raise RuntimeError(msg)

        if not token.startswith("AstraCS:"):
            print("[astra_connect] Warning: ASTRA token does not start with 'AstraCS:'.")

        return AstraConfig(
            target=resolved_target,
            bundle_path=str(bundle_path),
            token=token,
            keyspace=keyspace,
            request_timeout_sec=req,
            connect_timeout_sec=conn,
            fetch_size=fetch_size,
        )


ClusterCacheKey = Tuple[str, str, str, int, int]
_cluster_cache: dict[ClusterCacheKey, Tuple[Cluster, ExecutionProfile]] = {}
_cluster_cache_lock = threading.Lock()


def _cluster_cache_key(cfg: AstraConfig) -> ClusterCacheKey:
    return (
        cfg.target,
        cfg.bundle_path,
        cfg.token,
        cfg.request_timeout_sec,
        cfg.connect_timeout_sec,
    )


def _get_cluster_and_profile(cfg: AstraConfig) -> Tuple[Cluster, ExecutionProfile]:
    key = _cluster_cache_key(cfg)

    with _cluster_cache_lock:
        cached = _cluster_cache.get(key)
        if cached is not None:
            return cached

    auth = PlainTextAuthProvider("token", cfg.token)
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        request_timeout=cfg.request_timeout_sec,
    )
    cluster = Cluster(
        cloud={"secure_connect_bundle": cfg.bundle_path},
        auth_provider=auth,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        connect_timeout=cfg.connect_timeout_sec,
    )

    with _cluster_cache_lock:
        cached = _cluster_cache.get(key)
        if cached is not None:
            try:
                cluster.shutdown()
            except Exception:
                pass
            return cached
        _cluster_cache[key] = (cluster, profile)

    return cluster, profile


def get_session(
    keyspace: Optional[str] = None,
    *,
    target: Optional[str] = None,
    override_env_with_dotenv: bool = False,
    return_cluster: bool = False,
):
    """
    Get an Astra Session (and optional Cluster).
    - keyspace: override ASTRA_KEYSPACE.
    - target: 'main' or 'backup'. Defaults to ASTRA_TARGET (or 'main').
    - override_env_with_dotenv: if True, .env values overwrite existing env vars.
    - return_cluster: if True, returns (session, cluster); otherwise session only.
    """
    cfg = AstraConfig.from_env(
        target=target,
        override_env_with_dotenv=override_env_with_dotenv,
    )
    if keyspace:
        cfg = replace(cfg, keyspace=keyspace)

    cluster, _ = _get_cluster_and_profile(cfg)
    session = cluster.connect(cfg.keyspace)
    session.default_fetch_size = cfg.fetch_size

    if return_cluster:
        return session, cluster
    return session


def close_cached_cluster(*, target: Optional[str] = None) -> None:
    """
    Close cached cluster connections.
    - target=None: close all cached clusters.
    - target='main'|'backup': close only that target's cluster entries.
    """
    resolved_target = _normalize_target(target) if target else None
    to_close: list[Tuple[Cluster, ExecutionProfile]] = []

    with _cluster_cache_lock:
        keys = list(_cluster_cache.keys())
        for key in keys:
            if resolved_target and key[0] != resolved_target:
                continue
            item = _cluster_cache.pop(key, None)
            if item:
                to_close.append(item)

    for cluster, _profile in to_close:
        try:
            cluster.shutdown()
        except Exception:
            pass


def ensure_repo_root_on_sys_path() -> pathlib.Path:
    """
    Add repo root to sys.path (so `from paths import rel`-type imports work).
    Returns the path that was added (or already present).
    """
    root = _find_repo_root()
    if str(root) not in sys.path:
        sys.path.append(str(root))
    return root
