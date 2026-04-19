"""Load Fabric credentials and target resource IDs from an ``.env`` file.

``FabricConfig.from_env()`` walks up the directory tree from the caller so
a script run from anywhere under the repo root still finds the same
``.env`` at the root.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


def _walk_and_load_env(start: Path | None = None) -> None:
    start = start or Path.cwd()
    for parent in [start, *start.parents]:
        env_file = parent / ".env"
        if env_file.exists():
            load_dotenv(env_file)
            return
    load_dotenv()  # default: current dir


@dataclass(frozen=True)
class FabricConfig:
    """Strongly-typed view of the five environment variables this repo needs."""

    tenant_id: str
    client_id: str
    client_secret: str
    workspace_id: str
    lakehouse_id: str
    api_base: str = FABRIC_API_BASE

    @classmethod
    def from_env(cls, start: Path | None = None) -> "FabricConfig":
        """Build a ``FabricConfig`` from the process environment.

        Parameters
        ----------
        start:
            Directory to start searching for ``.env`` from. Defaults to
            ``Path(__file__).resolve()`` so library code can be invoked
            from arbitrary working directories.
        """
        if start is None:
            start = Path(__file__).resolve().parent
        _walk_and_load_env(start)

        missing: list[str] = []

        def _get(name: str) -> str:
            val = os.environ.get(name, "").strip()
            if not val:
                missing.append(name)
            return val

        cfg = cls(
            tenant_id=_get("AZURE_TENANT_ID"),
            client_id=_get("AZURE_CLIENT_ID"),
            client_secret=_get("AZURE_CLIENT_SECRET"),
            workspace_id=_get("FABRIC_WORKSPACE_ID"),
            lakehouse_id=_get("FABRIC_LAKEHOUSE_ID"),
        )
        if missing:
            raise RuntimeError(
                "Missing required environment variables: "
                + ", ".join(missing)
                + ". Set them in the repo-root .env file (see .env.example)."
            )
        return cfg
