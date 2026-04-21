"""Contract tests for 06_score.py — scenario_id join + sha256 hash lock.

These exercise the two guarantees F03/F04 introduced:

* perQuestion rows are indexed by scenario_id (never by question text);
  missing or duplicate IDs fail hard.
* The comparison JSON's scenariosPayload is the default source of
  truth; a local override must match its sha256 or pass
  --override-scenario-hash.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "06_score.py"


@pytest.fixture(scope="module")
def score_module():
    """Import 06_score.py as a module (its filename isn't a valid id)."""
    spec = importlib.util.spec_from_file_location("score_06", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["score_06"] = mod
    spec.loader.exec_module(mod)
    return mod


def _minimal_scenario_dict(sid: str, question: str = "q") -> dict:
    return {
        "scenario_id": sid,
        "domain": "sanity",
        "user_question": question,
        "gold_label": "",
        "ontology_signals": [],
    }


def test_index_rows_rejects_missing_scenario_id(score_module) -> None:
    rows = [
        {"scenario_id": "Q01"},
        {"question": "x"},  # missing scenario_id
    ]
    with pytest.raises(RuntimeError, match="missing scenario_id"):
        score_module._index_rows_by_scenario_id(rows)


def test_index_rows_rejects_duplicate_scenario_id(score_module) -> None:
    rows = [{"scenario_id": "Q01"}, {"scenario_id": "Q01"}]
    with pytest.raises(RuntimeError, match="duplicate scenario_id"):
        score_module._index_rows_by_scenario_id(rows)


def test_canonical_sha_is_stable_across_pretty_print(score_module) -> None:
    """Canonical-sorted JSON hash should ignore formatting differences."""
    payload = [{"scenario_id": "Q1", "user_question": "q", "domain": "x",
                "gold_label": "", "ontology_signals": []}]
    h1 = score_module._canonical_sha256(payload)
    # Different insertion order of a new key, same content after sort_keys:
    alt = [{"gold_label": "", "ontology_signals": [], "user_question": "q",
            "domain": "x", "scenario_id": "Q1"}]
    h2 = score_module._canonical_sha256(alt)
    assert h1 == h2


def test_comparison_without_scenarios_payload_rejected(tmp_path, score_module, monkeypatch) -> None:
    comparison = tmp_path / "_agent_comparison.json"
    comparison.write_text(json.dumps({"perQuestion": []}), encoding="utf-8")
    scenarios = tmp_path / "scenarios.json"
    scenarios.write_text(json.dumps([_minimal_scenario_dict("Q01")]), encoding="utf-8")

    md_out = tmp_path / "card.md"
    json_out = tmp_path / "card.json"
    cfg_out = tmp_path / "cfg.json"
    cfg_out.write_text(json.dumps({"entities": [], "relationships": []}), encoding="utf-8")

    monkeypatch.setattr(sys, "argv", [
        "06_score.py",
        "--comparison", str(comparison),
        "--scenarios", str(scenarios),
        "--config", str(cfg_out),
        "--md-out", str(md_out),
        "--json-out", str(json_out),
    ])
    with pytest.raises(RuntimeError, match="no scenariosPayload"):
        score_module.main()


def test_local_scenarios_hash_mismatch_rejected(tmp_path, score_module, monkeypatch) -> None:
    """Local scenarios file with a different sha256 than the embedded one
    must fail unless --override-scenario-hash is passed."""
    scenarios_payload = [_minimal_scenario_dict("Q01")]
    embedded_sha = "ff" * 32  # deliberate mismatch

    comparison = tmp_path / "_agent_comparison.json"
    comparison.write_text(json.dumps({
        "scenariosSha256": embedded_sha,
        "scenariosPayload": scenarios_payload,
        "perQuestion": [],
    }), encoding="utf-8")

    scenarios = tmp_path / "scenarios.json"
    scenarios.write_text(json.dumps(scenarios_payload), encoding="utf-8")

    md_out = tmp_path / "card.md"
    json_out = tmp_path / "card.json"
    cfg_out = tmp_path / "cfg.json"
    cfg_out.write_text(json.dumps({"entities": [], "relationships": []}), encoding="utf-8")

    monkeypatch.setattr(sys, "argv", [
        "06_score.py",
        "--comparison", str(comparison),
        "--scenarios", str(scenarios),
        "--scenarios-from", "local",
        "--config", str(cfg_out),
        "--md-out", str(md_out),
        "--json-out", str(json_out),
    ])
    with pytest.raises(RuntimeError, match="does not match"):
        score_module.main()


def test_unknown_scenario_id_in_rows_rejected(tmp_path, score_module, monkeypatch) -> None:
    """A perQuestion row whose scenario_id isn't in the scenarios payload
    must fail hard rather than silently skipping."""
    payload = [_minimal_scenario_dict("Q01")]
    comparison = tmp_path / "_agent_comparison.json"
    comparison.write_text(json.dumps({
        "scenariosSha256": "irrelevant",
        "scenariosPayload": payload,
        "perQuestion": [
            {"scenario_id": "Q01", "actual_answer_naked": "a", "actual_answer_ontology": "b"},
            {"scenario_id": "Q99", "actual_answer_naked": "a", "actual_answer_ontology": "b"},
        ],
    }), encoding="utf-8")
    cfg_out = tmp_path / "cfg.json"
    cfg_out.write_text(json.dumps({"entities": [], "relationships": []}), encoding="utf-8")

    md_out = tmp_path / "card.md"
    json_out = tmp_path / "card.json"

    monkeypatch.setattr(sys, "argv", [
        "06_score.py",
        "--comparison", str(comparison),
        "--config", str(cfg_out),
        "--md-out", str(md_out),
        "--json-out", str(json_out),
    ])
    with pytest.raises(RuntimeError, match="not in the scenarios payload"):
        score_module.main()
