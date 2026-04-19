"""Service-principal OAuth2 client_credentials flow for the Fabric API."""

from __future__ import annotations

import requests

from .config import FabricConfig

_SCOPE = "https://api.fabric.microsoft.com/.default"


def get_token(config: FabricConfig | None = None) -> str:
    """Acquire a bearer token for the Fabric API. No in-memory cache."""
    cfg = config or FabricConfig.from_env()
    resp = requests.post(
        f"https://login.microsoftonline.com/{cfg.tenant_id}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
            "scope": _SCOPE,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_headers(config: FabricConfig | None = None) -> dict[str, str]:
    """Return Fabric-API-ready request headers including the bearer token."""
    return {"Authorization": f"Bearer {get_token(config)}"}
