"""Unit tests for the OWL-to-Fabric mapping."""

from __future__ import annotations

from pathlib import Path

import pytest

from nplrisk_bench.mapping import build_ontology_config, load_ddl_tables
from nplrisk_bench.mapping.owl_to_fabric import sql_type_to_fabric
from nplrisk_bench.owl_parser import parse_owl


ROOT = Path(__file__).resolve().parents[1]
OWL_PATH = ROOT / "input" / "OWL" / "ontology.xml"
DDL_PATH = ROOT / "input" / "data" / "schema" / "ddl.sql"


@pytest.fixture(scope="module")
def ddl_tables() -> dict:
    return load_ddl_tables(DDL_PATH)


@pytest.fixture(scope="module")
def config() -> dict:
    parsed = parse_owl(OWL_PATH)
    return build_ontology_config(
        parsed,
        DDL_PATH,
        flatten_roots=["Borrower", "Loan", "Collateral"],
    )


# ---------------------------------------------------------------------------
# DDL parsing
# ---------------------------------------------------------------------------


def test_ddl_loads_all_npl_tables(ddl_tables: dict) -> None:
    expected = {
        "borrower", "loan", "loan_borrower_link", "collateral",
        "property_collateral", "counterparty_group", "collection_agent",
        "insolvency_practitioner", "insurance_provider", "receiver",
        "rating_agency", "forbearance_event", "enforcement_event",
        "external_collection_event",
    }
    assert expected.issubset(set(ddl_tables))


def test_ddl_detects_primary_keys(ddl_tables: dict) -> None:
    assert ddl_tables["borrower"].primary_key_columns == ["borrower_id"]
    assert ddl_tables["loan"].primary_key_columns == ["loan_id"]
    # Composite PK on the junction table
    assert ddl_tables["loan_borrower_link"].composite_primary_key == [
        "loan_id", "borrower_id", "role_type"
    ]


def test_ddl_detects_foreign_keys(ddl_tables: dict) -> None:
    fks = {(c.name, c.references_table) for c in ddl_tables["collateral"].foreign_keys}
    assert ("concerns_loan_id", "loan") in fks
    assert ("concerns_borrower_id", "borrower") in fks
    assert ("insurance_provider_id", "insurance_provider") in fks


# ---------------------------------------------------------------------------
# SQL -> Fabric type mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("sql, fabric", [
    ("BIGINT", "BigInt"),
    ("INTEGER", "BigInt"),
    ("NUMERIC(18,2)", "Double"),
    ("NUMERIC(10,4)", "Double"),
    ("DATE", "DateTime"),
    ("DATETIME", "DateTime"),
    ("TIMESTAMP", "DateTime"),
    ("TIMESTAMPTZ", "DateTime"),
    ("BOOLEAN", "Boolean"),
    ("TEXT", "String"),
    ("CHAR(2)", "String"),
    ("VARCHAR(100)", "String"),
])
def test_sql_type_mapping(sql: str, fabric: str) -> None:
    assert sql_type_to_fabric(sql) == fabric


# ---------------------------------------------------------------------------
# End-to-end config construction
# ---------------------------------------------------------------------------


def test_produces_13_entities(config: dict) -> None:
    names = [e["name"] for e in config["entities"]]
    assert len(names) == 13
    assert "Borrower" in names
    assert "Counterparty" not in names  # skipped (no backing table)
    assert "IndividualBorrower" not in names  # flattened into Borrower


def test_entities_have_primary_key(config: dict) -> None:
    for entity in config["entities"]:
        assert "keyProperty" in entity
        assert entity["keyProperty"], f"{entity['name']} missing keyProperty"


def test_loan_entity_carries_key_columns(config: dict) -> None:
    loan = next(e for e in config["entities"] if e["name"] == "Loan")
    prop_names = {p["name"] for p in loan["properties"]}
    for expected in ("loan_id", "ifrs_stage", "principal_balance",
                     "balance_at_default", "is_non_performing", "write_off_flag"):
        assert expected in prop_names, f"Loan missing column {expected}"


def test_owl_relationships_are_present(config: dict) -> None:
    names = {r["name"] for r in config["relationships"]}
    for expected in ("has_borrower", "has_borrowed_loan",
                     "collateral_concerns_loan", "collateral_concerns_borrower",
                     "enforcement_concerns_loan", "forbearance_concerns_loan",
                     "cp_is_part_of_group"):
        assert expected in names


def test_auto_fk_relationships_cover_unmapped_fks(config: dict) -> None:
    names = {r["name"] for r in config["relationships"]}
    # PropertyCollateral and Enforcement insolvency / receiver aren't in OWL
    assert "property_collateral_references_collateral" in names
    assert "enforcement_references_insolvency_practitioner" in names
    assert "enforcement_references_receiver" in names


def test_no_duplicate_edges(config: dict) -> None:
    edges = [
        (r["source"], r["target"], r.get("contextTable"), r.get("sourceKeyColumns"), r.get("targetKeyColumns"))
        for r in config["relationships"]
    ]
    assert len(edges) == len(set(edges)), "duplicate edges in relationship list"


def test_relationship_context_tables_exist(config: dict, ddl_tables: dict) -> None:
    prefix = config.get("tablePrefix", "npl")
    for r in config["relationships"]:
        ctx = r.get("contextTable")
        assert ctx, f"relationship {r['name']} missing contextTable"
        assert ctx.startswith(prefix + "_")
        raw = ctx[len(prefix) + 1:]
        assert raw in ddl_tables, f"context table {raw} not found in DDL"
