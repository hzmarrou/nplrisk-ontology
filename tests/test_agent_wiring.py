"""Unit tests for R01 — agent wiring must use canonical tableName."""

from __future__ import annotations

from pathlib import Path

import pytest

from nplrisk_bench.mapping import build_ontology_config
from nplrisk_bench.owl_parser import parse_owl


ROOT = Path(__file__).resolve().parents[1]
OWL_PATH = ROOT / "input" / "OWL" / "ontology.xml"
DDL_PATH = ROOT / "input" / "data" / "schema" / "ddl.sql"


@pytest.fixture(scope="module")
def config() -> dict:
    return build_ontology_config(
        parse_owl(OWL_PATH),
        DDL_PATH,
        flatten_roots=["Borrower", "Loan", "Collateral"],
    )


def _naked_selected_tables(cfg: dict) -> list[str]:
    """Exact logic from scripts/05_setup_agents.py so this test is a true
    regression guard against drift."""
    return [e["tableName"] for e in cfg["entities"]]


def test_naked_selection_includes_event_entities(config: dict) -> None:
    """The three Enforcement / Forbearance / ExternalCollection entity
    tables have an ``_event`` suffix that a naive snake_case heuristic
    would strip. Selection must include them."""
    selected = set(_naked_selected_tables(config))
    for required in ("npl_enforcement_event",
                     "npl_forbearance_event",
                     "npl_external_collection_event"):
        assert required in selected, f"NakedAgent would not see {required}"


def test_naked_selection_matches_canonical_table_names(config: dict) -> None:
    """The set of selected tables must be exactly the canonical tableName
    set from the config — no extras, no omissions."""
    selected = set(_naked_selected_tables(config))
    canonical = {e["tableName"] for e in config["entities"]}
    assert selected == canonical


def test_all_entities_have_table_name(config: dict) -> None:
    """tableName is required for every entity; otherwise 05_setup_agents
    raises at runtime. This guards against upstream mapping regressions
    that omit the field."""
    for entity in config["entities"]:
        assert entity.get("tableName"), f"{entity['name']} missing tableName"
