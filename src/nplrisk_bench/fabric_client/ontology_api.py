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
        """Poll a long-running operation until it reaches a terminal state.

        Hardens over the Fabric LRO contract:
          * honours ``Retry-After`` from every poll response, not just the
            initial 202
          * retries transient 5xx / network errors on the poll endpoint
            up to ``network_retries`` times with exponential backoff
          * enforces a wall-clock ``max_wait_seconds`` cap so a wedged
            LRO cannot block the script forever
        """
        if response.status_code != 202:
            return None
        operation_url = response.headers.get("Location")
        if not operation_url:
            raise RuntimeError("LRO: 202 response lacked Location header")
        retry_after = int(response.headers.get("Retry-After", poll_interval))
        print(f"  LRO accepted - polling {operation_url}")

        deadline = time.time() + max_wait_seconds
        while True:
            if time.time() >= deadline:
                raise TimeoutError(
                    f"LRO exceeded {max_wait_seconds}s timeout: {operation_url}"
                )
            sleep_for = min(retry_after, max(1, int(deadline - time.time())))
            time.sleep(sleep_for)

            poll = self._poll_once(operation_url, network_retries)
            status = poll.get("status", "Unknown")
            print(f"  LRO status: {status}")
            if status == "Succeeded":
                result_resp = requests.get(
                    f"{operation_url}/result",
                    headers=get_headers(self.config),
                    timeout=60,
                )
                if result_resp.status_code == 200:
                    return result_resp.json()
                return poll
            if status in ("Failed", "Cancelled"):
                error = poll.get("error", {})
                raise RuntimeError(
                    f"LRO {status}: {error.get('errorCode', 'unknown')} - "
                    f"{error.get('message', poll)}"
                )
            # Refresh retry_after for the next iteration from the most
            # recent poll headers; fall back to the initial value.
            retry_after = int(poll.get("_retry_after", retry_after))

    def _poll_once(self, operation_url: str, network_retries: int) -> dict:
        """GET the LRO status, retrying transient failures."""
        last_exc: Exception | None = None
        for i in range(1, network_retries + 1):
            try:
                poll = requests.get(
                    operation_url,
                    headers=get_headers(self.config),
                    timeout=60,
                )
                if poll.status_code in (429, 500, 502, 503, 504) and i < network_retries:
                    time.sleep(2 * i)
                    continue
                poll.raise_for_status()
                body = poll.json()
                # Surface Retry-After so _handle_lro can pick it up.
                ra = poll.headers.get("Retry-After")
                if ra:
                    try:
                        body["_retry_after"] = int(ra)
                    except ValueError:
                        pass
                return body
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_exc = exc
                if i == network_retries:
                    raise
                time.sleep(2 * i)
        raise RuntimeError(f"unreachable; last_exc={last_exc}")  # pragma: no cover

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
