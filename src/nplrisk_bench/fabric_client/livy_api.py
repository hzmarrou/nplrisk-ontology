"""Client for the Fabric Livy API - execute Spark SQL against a Lakehouse.

Docs:
  https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session
"""

from __future__ import annotations

import json
import time

import requests

from .auth import get_headers
from .config import FabricConfig


class LivyClient:
    """Context-managed Livy session wrapper.

    Example
    -------
    .. code:: python

        with LivyClient(config) as livy:
            livy.sql("CREATE TABLE t (a STRING, b BIGINT) USING DELTA")
    """

    def __init__(self, config: FabricConfig | None = None, *, lakehouse_id: str | None = None):
        self.config = config or FabricConfig.from_env()
        self.lakehouse_id = lakehouse_id or self.config.lakehouse_id
        self.workspace_id = self.config.workspace_id
        self.base_url = (
            f"{self.config.api_base}/workspaces/{self.workspace_id}"
            f"/lakehouses/{self.lakehouse_id}/livyapi/versions/2023-12-01/sessions"
        )
        self.session_id: str | None = None
        self.session_url: str | None = None

    def __enter__(self) -> "LivyClient":
        self.create_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close_session()

    def _headers(self) -> dict[str, str]:
        return {**get_headers(self.config), "Content-Type": "application/json"}

    def create_session(self, poll_interval: int = 5, idle_timeout: int = 600) -> str:
        resp = requests.post(self.base_url, headers=self._headers(), json={})
        if resp.status_code not in (200, 202):
            raise RuntimeError(f"Failed to create session: {resp.status_code} {resp.text}")
        session = resp.json()
        self.session_id = session["id"]
        self.session_url = f"{self.base_url}/{self.session_id}"
        self._wait_for_session_idle(poll_interval, idle_timeout)
        return self.session_id

    def close_session(self) -> None:
        if not self.session_url:
            return
        requests.delete(self.session_url, headers=self._headers())
        self.session_id = None
        self.session_url = None

    def sql(self, statement: str) -> str | None:
        return self.execute(f'spark.sql("{self._escape(statement)}").show()', kind="spark")

    def execute(self, code: str, kind: str = "spark") -> str | None:
        if not self.session_url:
            raise RuntimeError("No active session. Call create_session() first.")
        statements_url = f"{self.session_url}/statements"
        resp = requests.post(
            statements_url, headers=self._headers(), json={"code": code, "kind": kind}
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to submit statement: {resp.status_code} {resp.text}")

        stmt = resp.json()
        stmt_id = stmt["id"]
        stmt_url = f"{statements_url}/{stmt_id}"

        while stmt.get("state") not in ("available", "error", "cancelled"):
            time.sleep(3)
            stmt = requests.get(stmt_url, headers=self._headers()).json()

        if stmt.get("state") == "error":
            raise RuntimeError(f"Statement failed: {json.dumps(stmt.get('output', {}), indent=2)}")

        output = stmt.get("output", {})
        if output.get("status") == "error":
            raise RuntimeError(f"Spark error: {output.get('ename')}: {output.get('evalue')}")
        return output.get("data", {}).get("text/plain")

    def _wait_for_session_idle(self, poll_interval: int = 5, timeout: int = 600) -> None:
        """Poll the session until it reaches ``idle``.

        Raises ``TimeoutError`` if Spark scheduling takes longer than
        ``timeout`` seconds so callers don't hang indefinitely on a
        backed-up capacity.
        """
        deadline = time.time() + timeout
        last_state = "unknown"
        while time.time() < deadline:
            resp = requests.get(self.session_url, headers=self._headers())
            last_state = resp.json().get("state", "unknown")
            if last_state == "idle":
                print(f"  livy session {self.session_id} is idle", flush=True)
                return
            if last_state in ("dead", "killed", "error"):
                raise RuntimeError(f"Session entered bad state: {last_state}")
            time.sleep(poll_interval)
        raise TimeoutError(
            f"Livy session {self.session_id} did not reach idle within "
            f"{timeout}s (last state: {last_state}). Capacity may be "
            f"backed up — try again in a few minutes."
        )

    @staticmethod
    def _escape(s: str) -> str:
        return s.replace("\\", "\\\\").replace('"', '\\"')
