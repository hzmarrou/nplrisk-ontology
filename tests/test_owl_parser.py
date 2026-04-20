"""Unit tests for the OWL parser + model."""

from __future__ import annotations

from pathlib import Path

import pytest

from nplrisk_bench.owl_parser import parse_owl
from nplrisk_bench.owl_parser.model import DatatypeProperty, OwlClass, ParsedOntology


ROOT = Path(__file__).resolve().parents[1]
OWL_PATH = ROOT / "input" / "OWL" / "ontology.xml"


@pytest.fixture(scope="module")
def ontology() -> ParsedOntology:
    assert OWL_PATH.exists(), f"ontology fixture not found: {OWL_PATH}"
    return parse_owl(OWL_PATH)


# ---------------------------------------------------------------------------
# parse_owl — structural checks against the real NPLO file
# ---------------------------------------------------------------------------


def test_parses_ontology_metadata(ontology: ParsedOntology) -> None:
    assert "Non Performing Loan Ontology" in ontology.title
    assert ontology.namespace.startswith("https://www.openriskmanual.org/ns/nplo.owl")


def test_counts_top_level_entities(ontology: ParsedOntology) -> None:
    assert len(ontology.classes) == 18
    assert len(ontology.object_properties) == 16
    # Datatype properties are large in NPLO; just assert above a sane floor.
    assert len(ontology.datatype_properties) > 100


def test_known_classes_present(ontology: ParsedOntology) -> None:
    names = {c.name for c in ontology.classes}
    for expected in ["Borrower", "Loan", "Collateral", "PropertyCollateral",
                     "CounterpartyGroup", "Enforcement", "Forbearance",
                     "ExternalCollection", "CollectionAgent",
                     "InsolvencyPractitioner", "InsuranceProvider", "Receiver",
                     "RatingAgency", "Counterparty",
                     "IndividualBorrower", "CorporateBorrower",
                     "PersonalLoan", "CorporateLoan"]:
        assert expected in names, f"class {expected!r} missing"


def test_subclass_relationships(ontology: ParsedOntology) -> None:
    borrower_kids = {c.name for c in ontology.subclasses_of("Borrower")}
    assert borrower_kids == {"IndividualBorrower", "CorporateBorrower"}

    loan_kids = {c.name for c in ontology.subclasses_of("Loan")}
    assert loan_kids == {"PersonalLoan", "CorporateLoan"}


def test_counterparty_is_parent_of_borrower(ontology: ParsedOntology) -> None:
    borrower = ontology.class_by_name("Borrower")
    assert borrower is not None
    assert borrower.parent_name == "Counterparty"


def test_object_property_domain_range(ontology: ParsedOntology) -> None:
    has_borrower = ontology.object_property_by_name("has_borrower")
    assert has_borrower is not None
    assert has_borrower.domain_name == "Loan"
    assert has_borrower.range_name == "Borrower"

    # The OWL file declares owl:inverseOf on has_borrowed_loan -> has_borrower.
    # rdflib does not infer the reverse direction automatically, so we assert
    # from the declared side.
    has_borrowed_loan = ontology.object_property_by_name("has_borrowed_loan")
    assert has_borrowed_loan is not None
    assert has_borrowed_loan.inverse_name == "has_borrower"


# ---------------------------------------------------------------------------
# ParsedOntology transformations
# ---------------------------------------------------------------------------


def test_inherited_datatype_properties(ontology: ParsedOntology) -> None:
    # Borrower has 0 own datatype props in NPLO; everything is on Counterparty
    assert len(ontology.datatype_properties_for_class("Borrower")) == 0
    inherited = ontology.all_datatype_properties_for_class("Borrower")
    assert len(inherited) >= 80
    names = {p.name for p in inherited}
    assert "has_annual_income" in names or any("income" in n for n in names)


def test_flatten_hierarchy_drops_empty_subclasses(ontology: ParsedOntology) -> None:
    flat = ontology.flatten_hierarchy(["Borrower", "Loan", "Collateral"])
    names = {c.name for c in flat.classes}
    assert "IndividualBorrower" not in names
    assert "CorporateBorrower" not in names
    assert "PersonalLoan" not in names
    assert "CorporateLoan" not in names
    # Parents remain
    assert {"Borrower", "Loan", "Collateral"} <= names


# ---------------------------------------------------------------------------
# Type mapping (no OWL fixture needed)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("xsd, fabric", [
    ("xsd:decimal", "Double"),
    ("xsd:double", "Double"),
    ("xsd:integer", "BigInt"),
    ("xsd:long", "BigInt"),
    ("xsd:nonNegativeInteger", "BigInt"),
    ("xsd:dateTime", "DateTime"),
    ("xsd:date", "DateTime"),
    ("xsd:boolean", "Boolean"),
    ("xsd:string", "String"),
    ("xsd:anyURI", "String"),
    ("unknown", "String"),
])
def test_xsd_to_fabric_value_type(xsd: str, fabric: str) -> None:
    prop = DatatypeProperty(iri="x", name="x", domain_iri="x", range_xsd=xsd)
    assert prop.fabric_value_type == fabric


# ---------------------------------------------------------------------------
# OWL-lite contract — unsupported constructs must warn (R20)
# ---------------------------------------------------------------------------


def test_parse_owl_warns_on_unsupported_constructs(tmp_path: Path) -> None:
    """An ontology that uses owl:Restriction / unionOf / TransitiveProperty
    must emit a UserWarning so the maintainer knows those axioms were
    dropped during the flattening."""
    import warnings

    owl_xml = """<?xml version="1.0"?>
<rdf:RDF xmlns="http://example.org/o#"
         xml:base="http://example.org/o"
         xmlns:owl="http://www.w3.org/2002/07/owl#"
         xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
         xmlns:xsd="http://www.w3.org/2001/XMLSchema#">
  <owl:Ontology rdf:about="http://example.org/o"/>
  <owl:Class rdf:about="http://example.org/o#A"/>
  <owl:Class rdf:about="http://example.org/o#B">
    <rdfs:subClassOf>
      <owl:Restriction>
        <owl:onProperty rdf:resource="http://example.org/o#p"/>
        <owl:someValuesFrom rdf:resource="http://example.org/o#A"/>
      </owl:Restriction>
    </rdfs:subClassOf>
  </owl:Class>
  <owl:ObjectProperty rdf:about="http://example.org/o#p">
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#TransitiveProperty"/>
  </owl:ObjectProperty>
</rdf:RDF>
"""
    owl_path = tmp_path / "mini.owl"
    owl_path.write_text(owl_xml, encoding="utf-8")

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        parse_owl(owl_path)

    messages = [str(w.message) for w in captured]
    assert any("owl:Restriction" in m for m in messages), messages
    assert any("owl:TransitiveProperty" in m for m in messages), messages


def test_parse_owl_suppresses_warnings_when_disabled(tmp_path: Path) -> None:
    import warnings

    owl_xml = """<?xml version="1.0"?>
<rdf:RDF xmlns:owl="http://www.w3.org/2002/07/owl#"
         xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
  <owl:Ontology rdf:about="http://example.org/o"/>
  <owl:ObjectProperty rdf:about="http://example.org/o#p">
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#TransitiveProperty"/>
  </owl:ObjectProperty>
</rdf:RDF>
"""
    owl_path = tmp_path / "mini2.owl"
    owl_path.write_text(owl_xml, encoding="utf-8")

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        parse_owl(owl_path, warn_on_unsupported=False)

    assert not any("OWL parser" in str(w.message) for w in captured)
