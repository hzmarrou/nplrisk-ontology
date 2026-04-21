"""Contract tests for the Fabric REST client layer.

These exercise the moving parts that are too easy to break silently:

* auth.get_token's cache + refresh margin
* auth._fetch_token retry on 429 / 5xx
* OntologyClient._handle_lro's Retry-After, 5xx retry, wall-clock cap

All HTTP is stubbed — no network, no real credentials, no Fabric.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from nplrisk_bench.fabric_client import auth
from nplrisk_bench.fabric_client.config import FabricConfig
from nplrisk_bench.fabric_client.ontology_api import OntologyClient


@pytest.fixture(autouse=True)
def clear_cache() -> None:
    auth._cache.clear()
    yield
    auth._cache.clear()


def _cfg() -> FabricConfig:
    return FabricConfig(
        tenant_id="t", client_id="c", client_secret="s",
        workspace_id="w", lakehouse_id="l",
    )


def _token_resp(access_token: str, expires_in: int) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"access_token": access_token, "expires_in": expires_in}
    resp.raise_for_status.return_value = None
    return resp


def test_get_token_caches_within_margin() -> None:
    """A second call inside the refresh margin must reuse the cached token."""
    with patch.object(auth.requests, "post", return_value=_token_resp("tok1", 3600)) as post:
        t1 = auth.get_token(_cfg())
        t2 = auth.get_token(_cfg())
    assert t1 == t2 == "tok1"
    assert post.call_count == 1


def test_get_token_refreshes_when_margin_exceeded() -> None:
    """Force-refresh should fetch a new token even with a warm cache."""
    responses = [_token_resp("tok1", 3600), _token_resp("tok2", 3600)]
    with patch.object(auth.requests, "post", side_effect=responses):
        t1 = auth.get_token(_cfg())
        t2 = auth.get_token(_cfg(), force_refresh=True)
    assert t1 == "tok1"
    assert t2 == "tok2"


def test_get_token_margin_follows_expires_in() -> None:
    """Short-TTL token: margin is 15% of TTL, so a 100s TTL needs ~15s margin."""
    with patch.object(auth.requests, "post", return_value=_token_resp("tok", 100)):
        auth.get_token(_cfg())
    _, expires_at, margin = auth._cache[("t", "c")]
    assert 60.0 <= margin <= 60.1  # floor is 60 (>= 0.15 * 100 = 15)


def test_fetch_token_retries_on_http_429() -> None:
    """AAD 429 responses must be retried with backoff, not surface as an error."""
    transient = MagicMock()
    transient.status_code = 429
    err = requests.exceptions.HTTPError(response=transient)
    transient.raise_for_status.side_effect = err

    success = _token_resp("tok", 3600)

    with patch.object(auth.requests, "post", side_effect=[transient, success]):
        with patch.object(auth.time, "sleep"):  # skip real backoff
            token = auth.get_token(_cfg())
    assert token == "tok"


def test_fetch_token_surfaces_4xx_immediately() -> None:
    """Client-side errors (bad credentials) must NOT be retried."""
    bad = MagicMock()
    bad.status_code = 401
    err = requests.exceptions.HTTPError(response=bad)
    bad.raise_for_status.side_effect = err

    with patch.object(auth.requests, "post", return_value=bad):
        with pytest.raises(requests.exceptions.HTTPError):
            auth.get_token(_cfg())


# -- LRO contract ----------------------------------------------------------


@pytest.fixture
def stub_headers():
    """Stub get_headers in the shared LRO module so tests don't hit AAD."""
    with patch("nplrisk_bench.fabric_client.lro.get_headers",
               return_value={"Authorization": "Bearer test-token"}):
        yield


def _lro_accept_response(location: str = "https://fabric/ops/abc",
                         retry_after: int = 1) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 202
    resp.headers = {"Location": location, "Retry-After": str(retry_after)}
    return resp


def _lro_poll_response(status: str, retry_after: int | None = None,
                       http_status: int = 200) -> MagicMock:
    poll = MagicMock()
    poll.status_code = http_status
    poll.headers = {"Retry-After": str(retry_after)} if retry_after else {}
    poll.json.return_value = {"status": status}
    poll.raise_for_status.return_value = None
    return poll


def test_lro_raises_when_location_missing() -> None:
    client = OntologyClient(_cfg())
    resp = MagicMock()
    resp.status_code = 202
    resp.headers = {}  # no Location
    with pytest.raises(RuntimeError, match="Location"):
        client._handle_lro(resp)


def test_lro_times_out(stub_headers) -> None:
    """A wedged LRO must fail with TimeoutError rather than spinning forever."""
    client = OntologyClient(_cfg())
    accept = _lro_accept_response(retry_after=1)
    running_forever = _lro_poll_response("InProgress")

    times = iter([0, 0, 0] + [9999] * 20)

    with patch("nplrisk_bench.fabric_client.lro.requests.get",
               return_value=running_forever):
        with patch("nplrisk_bench.fabric_client.lro.time.sleep"):
            with patch("nplrisk_bench.fabric_client.lro.time.time",
                       side_effect=lambda: next(times)):
                with pytest.raises(TimeoutError):
                    client._handle_lro(accept, max_wait_seconds=60)


def test_lro_raises_on_failed_status(stub_headers) -> None:
    client = OntologyClient(_cfg())
    accept = _lro_accept_response(retry_after=1)
    failed = MagicMock()
    failed.status_code = 200
    failed.headers = {}
    failed.json.return_value = {
        "status": "Failed",
        "error": {"errorCode": "ValidationError", "message": "boom"},
    }
    failed.raise_for_status.return_value = None

    with patch("nplrisk_bench.fabric_client.lro.requests.get",
               return_value=failed):
        with patch("nplrisk_bench.fabric_client.lro.time.sleep"):
            with pytest.raises(RuntimeError, match="ValidationError"):
                client._handle_lro(accept)


def test_lro_retries_transient_5xx_on_poll(stub_headers) -> None:
    """A 503 during polling must be retried, then succeed."""
    client = OntologyClient(_cfg())
    accept = _lro_accept_response(retry_after=1)

    flaky = MagicMock()
    flaky.status_code = 503
    flaky.headers = {}
    flaky.raise_for_status.side_effect = requests.exceptions.HTTPError()

    succeeded = MagicMock()
    succeeded.status_code = 200
    succeeded.headers = {}
    succeeded.json.return_value = {"status": "Succeeded"}
    succeeded.raise_for_status.return_value = None

    result_resp = MagicMock()
    result_resp.status_code = 200
    result_resp.json.return_value = {"ok": True}

    with patch("nplrisk_bench.fabric_client.lro.requests.get",
               side_effect=[flaky, succeeded, result_resp]):
        with patch("nplrisk_bench.fabric_client.lro.time.sleep"):
            body = client._handle_lro(accept)
    assert body == {"ok": True}
