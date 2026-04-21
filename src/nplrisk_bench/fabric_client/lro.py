"""Shared long-running-operation poller for every Fabric client.

Fabric REST endpoints hand back three different 202 shapes:

* ``POST /ontologies/{id}/updateDefinition`` and its peers -> generic
  "operations" LRO with ``{status: Succeeded|Failed|Cancelled}`` and a
  ``/{operation_id}/result`` tail on success.
* ``POST /graphModels/{id}/getDefinition`` -> same shape.
* ``POST /dataAgents/{id}/updateDefinition`` -> same shape.
* ``POST /graphModels/{id}/jobs/instances?jobType=RefreshGraph`` -> job
  instance with ``{status: Completed|Failed|Cancelled, failureReason:...}``
  and NO separate result endpoint.

The two pollers below share the same hardening — wall-clock cap,
``Retry-After`` honoured on every poll, transient 429 / 5xx / network
errors retried, ``Location``-header guard — so a regression in one place
is a regression everywhere.
"""

from __future__ import annotations

import time
from typing import Iterable

import requests

from .auth import get_headers
from .config import FabricConfig


class FabricLROError(RuntimeError):
    """Raised when a Fabric LRO terminates in a failure/cancelled state."""


def _poll_once(
    config: FabricConfig,
    operation_url: str,
    network_retries: int,
) -> dict:
    """GET the LRO status once with transient-failure retries.

    The response body is returned as a dict. If the server returned a
    ``Retry-After`` header, it is stashed on the body under
    ``_retry_after`` so the outer loop can honour it.
    """
    last_exc: Exception | None = None
    for i in range(1, network_retries + 1):
        try:
            poll = requests.get(
                operation_url,
                headers=get_headers(config),
                timeout=60,
            )
            if poll.status_code in (429, 500, 502, 503, 504) and i < network_retries:
                time.sleep(2 * i)
                continue
            poll.raise_for_status()
            body = poll.json()
            ra = poll.headers.get("Retry-After")
            if ra:
                try:
                    body["_retry_after"] = int(ra)
                except ValueError:
                    pass
            return body
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            last_exc = exc
            if i == network_retries:
                raise
            time.sleep(2 * i)
    raise RuntimeError(f"unreachable; last_exc={last_exc}")  # pragma: no cover


def poll_lro(
    config: FabricConfig,
    response: requests.Response,
    *,
    poll_interval: int = 5,
    max_wait_seconds: int = 1800,
    network_retries: int = 3,
    success_states: Iterable[str] = ("Succeeded",),
    failure_states: Iterable[str] = ("Failed", "Cancelled"),
    fetch_result: bool = True,
    debug_label: str = "LRO",
) -> dict | None:
    """Poll a Fabric operation LRO from an accepted 202 response.

    Contract:
      * Returns ``None`` if the original response is not a 202.
      * Raises ``RuntimeError`` if the 202 has no Location header.
      * Polls the Location URL every ``Retry-After`` (or ``poll_interval``)
        seconds until the status reaches a success_state or a
        failure_state.
      * Transient 5xx / 429 / network errors are retried up to
        ``network_retries`` times with exponential backoff.
      * On success, GETs ``<operation_url>/result`` (``fetch_result=True``;
        default for ontology/graph-definition/data-agent LROs). Set
        ``fetch_result=False`` for job-instance LROs that return the
        whole body directly.
      * Raises ``TimeoutError`` if the deadline is exceeded.
      * Raises ``FabricLROError`` on Failed / Cancelled.
    """
    if response.status_code != 202:
        return None
    operation_url = response.headers.get("Location")
    if not operation_url:
        raise RuntimeError(f"{debug_label}: 202 response lacked Location header")

    retry_after = int(response.headers.get("Retry-After", poll_interval))
    success_set = set(success_states)
    failure_set = set(failure_states)
    started_at = time.time()
    deadline = started_at + max_wait_seconds

    last_printed_status: str | None = None
    last_status_change = started_at

    while True:
        if time.time() >= deadline:
            raise TimeoutError(
                f"{debug_label} exceeded {max_wait_seconds}s timeout: "
                f"{operation_url}"
            )
        sleep_for = min(retry_after, max(1, int(deadline - time.time())))
        time.sleep(sleep_for)

        body = _poll_once(config, operation_url, network_retries)
        status = body.get("status", "Unknown")

        # Print every status transition, plus a heartbeat every ~60 s
        # so an operator knows the poll isn't wedged during long runs.
        now = time.time()
        elapsed = int(now - started_at)
        if status != last_printed_status:
            print(f"  [{debug_label}] {status} (t+{elapsed}s)")
            last_printed_status = status
            last_status_change = now
        elif now - last_status_change >= 60:
            print(f"  [{debug_label}] still {status} (t+{elapsed}s)")
            last_status_change = now

        if status in success_set:
            if fetch_result:
                result_resp = requests.get(
                    f"{operation_url}/result",
                    headers=get_headers(config),
                    timeout=60,
                )
                if result_resp.status_code == 200:
                    return result_resp.json()
            # Drop the synthetic Retry-After we stashed on the body.
            body.pop("_retry_after", None)
            return body

        if status in failure_set:
            error = body.get("error") or body.get("failureReason") or {}
            raise FabricLROError(
                f"{debug_label} {status}: "
                f"{error.get('errorCode', error.get('code', 'unknown'))} - "
                f"{error.get('message', body)}"
            )

        retry_after = int(body.get("_retry_after", retry_after))
