"""Wrapper over the Fabric Data Agent REST API.

Docs:
  https://learn.microsoft.com/en-us/rest/api/fabric/dataagent/items
"""

from __future__ import annotations

import base64
import json
import time

import requests

from .auth import get_headers
from .config import FabricConfig


class DataAgentClient:
    """CRUD + definition parts for Fabric Data Agents."""

    def __init__(self, config: FabricConfig | None = None):
        self.config = config or FabricConfig.from_env()
        self.workspace_id = self.config.workspace_id
        self.base_url = f"{self.config.api_base}/workspaces/{self.workspace_id}/dataAgents"

    def _headers(self) -> dict[str, str]:
        return {**get_headers(self.config), "Content-Type": "application/json"}

    def _url(self, data_agent_id: str | None = None, action: str | None = None) -> str:
        url = self.base_url
        if data_agent_id:
            url = f"{url}/{data_agent_id}"
        if action:
            url = f"{url}/{action}"
        return url

    def _poll_lro(self, response: requests.Response, poll_interval: int = 5) -> dict | None:
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

    def list_data_agents(self) -> list[dict]:
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

    def create_data_agent(
        self,
        display_name: str,
        *,
        description: str | None = None,
        definition: dict | None = None,
    ) -> dict:
        body: dict = {"displayName": display_name}
        if description is not None:
            body["description"] = description
        if definition is not None:
            body["definition"] = definition
        response = requests.post(self._url(), headers=self._headers(), json=body)
        if response.status_code == 202:
            lro_result = self._poll_lro(response)
            if lro_result:
                return lro_result
            return {"status": "created_async", "displayName": display_name}
        response.raise_for_status()
        return response.json()

    def get_data_agent(self, data_agent_id: str) -> dict:
        response = requests.get(self._url(data_agent_id), headers=get_headers(self.config))
        response.raise_for_status()
        return response.json()

    def update_data_agent(
        self,
        data_agent_id: str,
        *,
        display_name: str | None = None,
        description: str | None = None,
    ) -> dict:
        body: dict = {}
        if display_name is not None:
            body["displayName"] = display_name
        if description is not None:
            body["description"] = description
        response = requests.patch(self._url(data_agent_id), headers=self._headers(), json=body)
        response.raise_for_status()
        return response.json()

    def delete_data_agent(self, data_agent_id: str) -> int:
        response = requests.delete(self._url(data_agent_id), headers=get_headers(self.config))
        response.raise_for_status()
        return response.status_code

    def get_definition(self, data_agent_id: str) -> dict:
        response = requests.post(self._url(data_agent_id, "getDefinition"), headers=self._headers())
        if response.status_code == 202:
            lro_result = self._poll_lro(response)
            if lro_result is not None:
                return lro_result
        response.raise_for_status()
        return response.json()

    def update_definition(self, data_agent_id: str, definition: dict) -> int:
        response = requests.post(
            self._url(data_agent_id, "updateDefinition"),
            headers=self._headers(),
            json={"definition": definition},
        )
        if response.status_code == 202:
            self._poll_lro(response)
            return 202
        response.raise_for_status()
        return response.status_code

    @staticmethod
    def encode_part(path: str, content: dict | str) -> dict:
        """Encode a single definition part for create/update calls."""
        if isinstance(content, dict):
            content = json.dumps(content, ensure_ascii=False)
        payload = base64.b64encode(content.encode("utf-8")).decode("ascii")
        return {"path": path, "payload": payload, "payloadType": "InlineBase64"}

    @staticmethod
    def decode_definition_parts(raw_definition: dict) -> tuple[list[dict], dict[str, dict | str]]:
        """Return (raw_parts, decoded_by_path) for a getDefinition response."""
        parts = raw_definition.get("definition", {}).get("parts", [])
        decoded: dict = {}
        for part in parts:
            payload = base64.b64decode(part.get("payload", ""))
            try:
                decoded[part["path"]] = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                decoded[part["path"]] = payload.decode("utf-8", errors="replace")
        return parts, decoded
