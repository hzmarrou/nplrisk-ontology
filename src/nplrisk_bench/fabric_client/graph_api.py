"""Wrapper over the Fabric Graph Model REST API.

Docs:
  https://learn.microsoft.com/en-us/rest/api/fabric/graphmodel/items
"""

from __future__ import annotations

import base64
import json
import time

import requests

from .auth import get_headers
from .config import FabricConfig


class GraphClient:
    """Thin REST client for Fabric graph models (list, refresh, executeQuery)."""

    def __init__(self, config: FabricConfig | None = None):
        self.config = config or FabricConfig.from_env()
        self.workspace_id = self.config.workspace_id
        self.base_url = f"{self.config.api_base}/workspaces/{self.workspace_id}/graphModels"

    def _headers(self) -> dict[str, str]:
        return {**get_headers(self.config), "Content-Type": "application/json"}

    def _url(self, graph_id: str | None = None, action: str | None = None) -> str:
        url = self.base_url
        if graph_id:
            url = f"{url}/{graph_id}"
        if action:
            url = f"{url}/{action}"
        return url

    def _handle_lro(self, response: requests.Response, poll_interval: int = 5) -> dict | None:
        if response.status_code != 202:
            return None
        operation_url = response.headers.get("Location")
        retry_after = int(response.headers.get("Retry-After", poll_interval))
        while True:
            time.sleep(retry_after)
            poll = requests.get(operation_url, headers=get_headers(self.config))
            poll.raise_for_status()
            body = poll.json()
            status = body.get("status", "Unknown")
            if status == "Succeeded":
                result_resp = requests.get(f"{operation_url}/result", headers=get_headers(self.config))
                if result_resp.status_code == 200:
                    return result_resp.json()
                return body
            if status in ("Failed", "Cancelled"):
                error = body.get("error", {})
                raise RuntimeError(
                    f"LRO {status}: {error.get('errorCode', 'unknown')} - "
                    f"{error.get('message', body)}"
                )

    def list_graph_models(self) -> list[dict]:
        results: list[dict] = []
        url = self._url()
        params: dict[str, str] = {}
        while url:
            response = requests.get(url, headers=get_headers(self.config), params=params)
            response.raise_for_status()
            body = response.json()
            results.extend(body.get("value", []))
            url = body.get("continuationUri")
            params = {}
        return results

    def get_graph_model(self, graph_id: str) -> dict:
        response = requests.get(self._url(graph_id), headers=get_headers(self.config))
        response.raise_for_status()
        return response.json()

    def get_definition(self, graph_id: str) -> dict:
        response = requests.post(self._url(graph_id, "getDefinition"), headers=self._headers())
        if response.status_code == 202:
            return self._handle_lro(response)  # type: ignore[return-value]
        response.raise_for_status()
        return response.json()

    def get_definition_decoded(self, graph_id: str) -> dict:
        raw = self.get_definition(graph_id)
        parts = raw.get("definition", {}).get("parts", [])
        decoded: dict = {}
        for part in parts:
            payload = part.get("payload", "")
            try:
                decoded[part["path"]] = json.loads(base64.b64decode(payload))
            except Exception:
                decoded[part["path"]] = base64.b64decode(payload).decode("utf-8", errors="replace")
        return decoded

    def execute_query(self, graph_id: str, query: str) -> dict:
        response = requests.post(
            self._url(graph_id, "executeQuery"),
            headers=self._headers(),
            params={"beta": "true"},
            json={"query": query},
        )
        response.raise_for_status()
        return response.json()

    def get_queryable_graph_type(self, graph_id: str) -> dict:
        response = requests.get(
            self._url(graph_id, "getQueryableGraphType"),
            headers=get_headers(self.config),
            params={"beta": "true"},
        )
        response.raise_for_status()
        return response.json()

    def refresh(self, graph_id: str, *, wait: bool = True, poll_interval: int = 15) -> dict:
        """Trigger an on-demand graph refresh.

        Uses the generic ``jobs/instances`` endpoint with
        ``jobType=RefreshGraph``. When Fabric has queued overlapping
        refresh jobs the platform can auto-cancel them in under a second;
        callers should wait and retry a single clean refresh if they see
        ``status=Cancelled`` with no ``failureReason``.
        """
        url = self._url(graph_id, "jobs/instances")
        response = requests.post(
            url,
            headers=self._headers(),
            params={"jobType": "RefreshGraph"},
        )
        if response.status_code == 200:
            return {"status": "Completed"}
        if response.status_code == 202:
            location = response.headers.get("Location")
            retry_after = int(response.headers.get("Retry-After", poll_interval))
            if not wait:
                return {"status": "Accepted", "location": location}
            while True:
                time.sleep(retry_after)
                poll = requests.get(location, headers=get_headers(self.config))
                poll.raise_for_status()
                body = poll.json()
                status = body.get("status", "Unknown")
                if status in ("Completed", "Failed", "Cancelled"):
                    if body.get("failureReason"):
                        raise RuntimeError(
                            f"Refresh {status}: "
                            f"{body['failureReason'].get('message', body['failureReason'])}"
                        )
                    return body
                retry_after = min(retry_after, 15)
        response.raise_for_status()
        return {}

    def delete_graph_model(self, graph_id: str) -> int:
        response = requests.delete(self._url(graph_id), headers=get_headers(self.config))
        response.raise_for_status()
        return response.status_code
