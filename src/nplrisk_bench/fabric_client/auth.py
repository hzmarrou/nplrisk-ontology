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
from requests.exceptions import ConnectionError as RequestsConnectionError, HTTPError, Timeout

from .config import FabricConfig

_SCOPE = "https://api.fabric.microsoft.com/.default"

_lock = threading.Lock()
# Cache value: (token, expires_at_unix, refresh_margin_seconds)
_cache: dict[tuple[str, str], tuple[str, float, float]] = {}


def _is_retryable_http(exc: HTTPError) -> bool:
    """AAD 5xx and 429 are transient; 4xx authz failures are not."""
    if exc.response is None:
        return False
    code = exc.response.status_code
    return code == 429 or (500 <= code < 600)


def _fetch_token(cfg: FabricConfig, attempts: int = 3) -> tuple[str, float, float]:
    """POST to the AAD token endpoint.

    Returns ``(token, expires_at_unix, refresh_margin_seconds)``. The
    margin is 15% of the advertised TTL (minimum 60s), so the cache
    decides when to refresh based on the server's reported lifetime
    rather than a hard-coded 1-hour assumption.
    """
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
            margin = max(60.0, 0.15 * ttl)
            return body["access_token"], time.time() + ttl, margin
        except (RequestsConnectionError, Timeout) as exc:
            last_exc = exc
        except HTTPError as exc:
            last_exc = exc
            if not _is_retryable_http(exc):
                raise
        if i == attempts:
            raise last_exc  # type: ignore[misc]
        time.sleep(2 * i)
    raise RuntimeError(f"unreachable; last_exc={last_exc}")  # pragma: no cover


def get_token(config: FabricConfig | None = None, *, force_refresh: bool = False) -> str:
    """Return a bearer token, reusing a cached one while the refresh margin hasn't elapsed.

    Thread-safe. The refresh margin is derived from the AAD-reported
    ``expires_in``, not a hard-coded TTL. On a transient network or
    5xx/429 error from AAD, retries up to 3 times with backoff before
    surfacing the exception.
    """
    cfg = config or FabricConfig.from_env()
    key = (cfg.tenant_id, cfg.client_id)
    now = time.time()

    with _lock:
        cached = _cache.get(key)
        if cached and not force_refresh:
            token, expires_at, margin = cached
            if expires_at - now > margin:
                return token

        token, expires_at, margin = _fetch_token(cfg)
        _cache[key] = (token, expires_at, margin)
        return token


def get_headers(config: FabricConfig | None = None) -> dict[str, str]:
    """Return Fabric-API-ready request headers including the bearer token."""
    return {"Authorization": f"Bearer {get_token(config)}"}
