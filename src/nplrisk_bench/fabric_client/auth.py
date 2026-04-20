"""Service-principal OAuth2 client_credentials flow for the Fabric API.

Tokens are cached in-process for ~85% of their advertised lifetime so
busy loops (Livy `sql()` calls, LRO polls) don't re-POST to Entra for
every single call. The cache also retries once on transient network
errors from the token endpoint.
"""

from __future__ import annotations

import threading
import time

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError, Timeout

from .config import FabricConfig

_SCOPE = "https://api.fabric.microsoft.com/.default"

_lock = threading.Lock()
_cache: dict[tuple[str, str], tuple[str, float]] = {}


def _fetch_token(cfg: FabricConfig, attempts: int = 3) -> tuple[str, float]:
    """POST to the AAD token endpoint and return (token, expires_at_unix)."""
    url = f"https://login.microsoftonline.com/{cfg.tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": cfg.client_id,
        "client_secret": cfg.client_secret,
        "scope": _SCOPE,
    }
    last_exc: Exception | None = None
    for i in range(1, attempts + 1):
        try:
            resp = requests.post(url, data=data, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            ttl = int(body.get("expires_in", 3600))
            return body["access_token"], time.time() + ttl
        except (RequestsConnectionError, Timeout) as exc:
            last_exc = exc
            if i == attempts:
                raise
            time.sleep(2 * i)
    raise RuntimeError(f"unreachable; last_exc={last_exc}")  # pragma: no cover


def get_token(config: FabricConfig | None = None, *, force_refresh: bool = False) -> str:
    """Return a bearer token, reusing a cached one while it still has >15% of life.

    Thread-safe. On a transient network error, retries up to 3 times with
    exponential backoff before surfacing the exception.
    """
    cfg = config or FabricConfig.from_env()
    key = (cfg.tenant_id, cfg.client_id)
    now = time.time()

    with _lock:
        cached = _cache.get(key)
        if cached and not force_refresh:
            token, expires_at = cached
            # Refresh when less than 15% of lifetime (or 5 min) remains
            if expires_at - now > max(300, 0.15 * 3600):
                return token

        token, expires_at = _fetch_token(cfg)
        _cache[key] = (token, expires_at)
        return token


def get_headers(config: FabricConfig | None = None) -> dict[str, str]:
    """Return Fabric-API-ready request headers including the bearer token."""
    return {"Authorization": f"Bearer {get_token(config)}"}
