"""Wrapper over the Fabric Ontology REST API.

Docs:
  https://learn.microsoft.com/en-us/rest/api/fabric/ontology/items
"""

from __future__ import annotations

import base64
import json
import time

import requests

from .auth import get_headers
from .config import FabricConfig
from .lro import poll_lro


class OntologyClient:
    """Thin REST client for Fabric ontologies (CRUD + definition handling)."""

    def __init__(self, config: FabricConfig | None = None):
        self.config = config or FabricConfig.from_env()
        self.workspace_id = self.config.workspace_id
        self.base_url = f"{self.config.api_base}/workspaces/{self.workspace_id}/ontologies"

    def _headers(self) -> dict[str, str]:
        return {**get_headers(self.config), "Content-Type": "application/json"}

    def _url(self, ontology_id: str | None = None, action: str | None = None) -> str:
        url = self.base_url
        if ontology_id:
            url = f"{url}/{ontology_id}"
        if action:
            url = f"{url}/{action}"
        return url

    def _handle_lro(
        self,
        response: requests.Response,
        poll_interval: int = 5,
        *,
        max_wait_seconds: int = 1800,
        network_retries: int = 3,
    ) -> dict | None:
        """Thin wrapper over the shared :func:`poll_lro`.

        Kept as an instance method because the existing callers reach it
        as ``self._handle_lro(response)``; the real LRO contract lives
        in ``fabric_client.lro``.
        """
        return poll_lro(
            self.config,
            response,
            poll_interval=poll_interval,
            max_wait_seconds=max_wait_seconds,
            network_retries=network_retries,
            debug_label="Ontology LRO",
        )

    # -- CRUD ------------------------------------------------------------

    def list_ontologies(self) -> list[dict]:
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

    def create_ontology(self, display_name: str, *, description: str | None = None,
                        definition: dict | None = None) -> dict:
        body: dict = {"displayName": display_name}
        if description:
            body["description"] = description
        if definition:
            body["definition"] = definition
        response = requests.post(self._url(), headers=self._headers(), json=body)
        if response.status_code == 202:
            self._handle_lro(response)
            return {"status": "created_async", "displayName": display_name}
        response.raise_for_status()
        return response.json()

    def get_ontology(self, ontology_id: str) -> dict:
        response = requests.get(self._url(ontology_id), headers=get_headers(self.config))
        response.raise_for_status()
        return response.json()

    def delete_ontology(self, ontology_id: str, *, hard_delete: bool = False) -> int:
        params = {"hardDelete": "True"} if hard_delete else {}
        response = requests.delete(self._url(ontology_id), headers=get_headers(self.config), params=params)
        response.raise_for_status()
        return response.status_code

    # -- Definition ------------------------------------------------------

    def get_definition(self, ontology_id: str) -> dict:
        response = requests.post(
            self._url(ontology_id, "getDefinition"),
            headers=self._headers(),
        )
        if response.status_code == 202:
            return self._handle_lro(response)  # type: ignore[return-value]
        response.raise_for_status()
        return response.json()

    def get_definition_decoded(self, ontology_id: str) -> dict:
        raw = self.get_definition(ontology_id)
        parts = raw.get("definition", {}).get("parts", [])
        decoded: dict = {}
        for part in parts:
            payload = part.get("payload", "")
            try:
                decoded[part["path"]] = json.loads(base64.b64decode(payload))
            except Exception:
                decoded[part["path"]] = base64.b64decode(payload).decode("utf-8", errors="replace")
        return decoded

    def update_definition(self, ontology_id: str, definition: dict) -> int:
        body = {"definition": definition}
        response = requests.post(
            self._url(ontology_id, "updateDefinition"),
            headers=self._headers(),
            json=body,
        )
        if response.status_code == 202:
            self._handle_lro(response)
            return 202
        response.raise_for_status()
        return response.status_code
