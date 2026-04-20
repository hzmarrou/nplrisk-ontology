"""Parse an OWL/RDF-XML ontology file using rdflib.

Supported subset ("OWL-lite, flattened")
----------------------------------------
The downstream Fabric mapping is a relational/graph layer, so this
parser only extracts the OWL constructs that have a direct relational
counterpart:

* ``owl:Class`` with optional single ``rdfs:subClassOf`` parent. We
  take the FIRST parent axiom only — multiple-inheritance classes are
  flattened to one discriminator column during mapping.
* ``owl:ObjectProperty`` with zero-or-one ``rdfs:domain`` /
  ``rdfs:range``; optional ``owl:FunctionalProperty``,
  ``owl:inverseOf``, ``owl:propertyDisjointWith``.
* ``owl:DatatypeProperty`` with exactly one ``rdfs:domain`` and
  (optionally) an XSD ``rdfs:range``.

Unsupported constructs
----------------------
These are common OWL features that DO NOT round-trip through this
parser. If your source ontology relies on them, the flattened output
will drop semantic information and the downstream Fabric bindings may
be wrong. ``parse_owl`` emits a ``UserWarning`` for each such axiom so
you are not surprised at runtime.

* ``owl:Restriction`` + ``owl:onProperty`` /
  ``owl:someValuesFrom`` / ``owl:allValuesFrom`` / cardinality
  constraints.
* ``owl:intersectionOf`` / ``owl:unionOf`` / ``owl:complementOf``
  complex class expressions.
* ``owl:equivalentClass`` (the axiom is dropped; only one named class
  survives).
* Role hierarchies (``rdfs:subPropertyOf``),
  ``owl:TransitiveProperty``, ``owl:SymmetricProperty``,
  ``owl:ReflexiveProperty``, ``owl:IrreflexiveProperty``,
  ``owl:AsymmetricProperty``.
* Individuals (``owl:NamedIndividual``, ``rdf:type`` assertions).

The parser also filters out a handful of named meta-properties used in
NPLO-style ontologies purely as annotation tags (Confidentiality,
Importance, Temporality, ...). Domain ontologies that don't use those
tags are unaffected.
"""

from __future__ import annotations

import warnings
from pathlib import Path

from rdflib import OWL, RDF, RDFS, XSD, Graph, URIRef
from rdflib.term import Node

from .model import DatatypeProperty, ObjectProperty, OwlClass, ParsedOntology

# Meta-properties that classify data attributes (not real domain attributes)
_META_PROPERTY_NAMES = {
    "DataAttribute",
    "Confidentiality",
    "Confidential",
    "NonConfidential",
    "VariableConfidentiality",
    "Importance",
    "Critical",
    "Important",
    "Moderate",
    "Temporality",
    "Current",
    "Future",
    "Historical",
    "Variability",
    "Dynamic",
    "Static",
}


def _local_name(iri: URIRef | Node | str) -> str:
    """Extract the fragment (local name) from an IRI."""
    s = str(iri)
    if "#" in s:
        return s.rsplit("#", 1)[1]
    return s.rsplit("/", 1)[-1]


def _xsd_short(iri: URIRef | Node | str) -> str:
    """Convert a full XSD IRI to its short form (e.g. `xsd:decimal`)."""
    s = str(iri)
    xsd_ns = str(XSD)
    if s.startswith(xsd_ns):
        return "xsd:" + s[len(xsd_ns):]
    return s


def _is_owl_thing(iri: str | None) -> bool:
    if iri is None:
        return True
    return str(iri) == str(OWL.Thing) or "owl#Thing" in str(iri)


_UNSUPPORTED_CLASS_AXIOMS = [
    (OWL.Restriction, "owl:Restriction"),
    (OWL.intersectionOf, "owl:intersectionOf"),
    (OWL.unionOf, "owl:unionOf"),
    (OWL.complementOf, "owl:complementOf"),
    (OWL.equivalentClass, "owl:equivalentClass"),
]

_UNSUPPORTED_PROPERTY_AXIOMS = [
    (OWL.TransitiveProperty, "owl:TransitiveProperty"),
    (OWL.SymmetricProperty, "owl:SymmetricProperty"),
    (OWL.AsymmetricProperty, "owl:AsymmetricProperty"),
    (OWL.ReflexiveProperty, "owl:ReflexiveProperty"),
    (OWL.IrreflexiveProperty, "owl:IrreflexiveProperty"),
]


def _warn_unsupported(g: Graph) -> None:
    """Emit a UserWarning for each OWL axiom this parser cannot round-trip.

    Callers keep going with a best-effort parse, but the warning makes
    the semantic loss explicit at ingest time so a maintainer can decide
    whether the flattening is acceptable.
    """
    for iri, label in _UNSUPPORTED_CLASS_AXIOMS:
        # predicate-position uses
        count = sum(1 for _ in g.subject_objects(iri))
        # type-position uses (e.g. `_:x rdf:type owl:Restriction`)
        type_count = sum(1 for _ in g.subjects(RDF.type, iri))
        total = count + type_count
        if total:
            warnings.warn(
                f"OWL parser: {total} use(s) of {label} will be dropped — "
                f"this parser supports named classes + single subClassOf only.",
                UserWarning,
                stacklevel=3,
            )

    for iri, label in _UNSUPPORTED_PROPERTY_AXIOMS:
        count = sum(1 for _ in g.subjects(RDF.type, iri))
        if count:
            warnings.warn(
                f"OWL parser: {count} property typed as {label} — the "
                f"axiom is ignored; downstream bindings treat it as a "
                f"plain ObjectProperty.",
                UserWarning,
                stacklevel=3,
            )

    sub_prop_count = sum(1 for _ in g.subject_objects(RDFS.subPropertyOf))
    if sub_prop_count:
        warnings.warn(
            f"OWL parser: {sub_prop_count} rdfs:subPropertyOf axiom(s) "
            f"dropped — role hierarchies do not round-trip.",
            UserWarning,
            stacklevel=3,
        )


def parse_owl(path: Path, *, warn_on_unsupported: bool = True) -> ParsedOntology:
    """Parse an OWL/RDF-XML file and return a `ParsedOntology`.

    Parameters
    ----------
    path
        Path to the OWL/RDF-XML file.
    warn_on_unsupported
        If True (default), emits a ``UserWarning`` for each OWL construct
        outside the supported OWL-lite subset. Set False when you are
        intentionally consuming a lossy subset and don't want the noise.
    """
    g = Graph()
    g.parse(str(path), format="xml")
    if warn_on_unsupported:
        _warn_unsupported(g)

    # -- Ontology metadata --
    ont_iri: URIRef | None = None
    for s in g.subjects(RDF.type, OWL.Ontology):
        ont_iri = s  # type: ignore[assignment]
        break

    namespace = str(ont_iri) if ont_iri else ""
    title = ""
    version = ""
    if ont_iri:
        for o in g.objects(ont_iri, URIRef("http://purl.org/dc/elements/1.1/title")):
            title = str(o)
        for o in g.objects(ont_iri, OWL.versionInfo):
            version = str(o)

    # -- Classes --
    classes: list[OwlClass] = []
    for cls_iri in g.subjects(RDF.type, OWL.Class):
        name = _local_name(cls_iri)
        if not name or name.startswith("http"):
            continue

        parent_iri: str | None = None
        for parent in g.objects(cls_iri, RDFS.subClassOf):
            parent_str = str(parent)
            parent_iri = None if _is_owl_thing(parent_str) else parent_str
            break  # take first

        comment = ""
        for c in g.objects(cls_iri, RDFS.comment):
            comment = str(c)
            break

        classes.append(OwlClass(
            iri=str(cls_iri),
            name=name,
            parent_iri=parent_iri,
            comment=comment,
        ))

    # -- Object properties --
    object_properties: list[ObjectProperty] = []
    for prop_iri in g.subjects(RDF.type, OWL.ObjectProperty):
        name = _local_name(prop_iri)
        if not name:
            continue

        domain_iri = ""
        for d in g.objects(prop_iri, RDFS.domain):
            domain_iri = str(d)
            break
        range_iri = ""
        for r in g.objects(prop_iri, RDFS.range):
            range_iri = str(r)
            break

        is_functional = (prop_iri, RDF.type, OWL.FunctionalProperty) in g

        inverse_iri: str | None = None
        for inv in g.objects(prop_iri, OWL.inverseOf):
            inverse_iri = str(inv)
            break

        disjoint_iris: list[str] = []
        for disj in g.objects(prop_iri, OWL.propertyDisjointWith):
            disjoint_iris.append(str(disj))

        object_properties.append(ObjectProperty(
            iri=str(prop_iri),
            name=name,
            domain_iri=domain_iri,
            range_iri=range_iri,
            is_functional=is_functional,
            inverse_iri=inverse_iri,
            disjoint_with_iris=disjoint_iris,
        ))

    # -- Datatype properties --
    datatype_properties: list[DatatypeProperty] = []
    for prop_iri in g.subjects(RDF.type, OWL.DatatypeProperty):
        name = _local_name(prop_iri)
        if not name or name in _META_PROPERTY_NAMES:
            continue

        domain_iri = ""
        for d in g.objects(prop_iri, RDFS.domain):
            domain_iri = str(d)
            break

        # Skip properties without a domain (abstract/meta)
        if not domain_iri:
            continue

        # Range — direct XSD type, or an owl:oneOf enumeration (treat as string)
        range_xsd = "xsd:string"
        for r in g.objects(prop_iri, RDFS.range):
            r_str = str(r)
            if str(XSD) in r_str:
                range_xsd = _xsd_short(r)
            break

        # Comment — prefer a non-reference comment; fall back to any
        comment = ""
        fallback = ""
        for c in g.objects(prop_iri, RDFS.comment):
            c_str = str(c)
            if c_str.startswith("EBA"):
                fallback = c_str
            elif not comment:
                comment = c_str
        if not comment:
            comment = fallback

        datatype_properties.append(DatatypeProperty(
            iri=str(prop_iri),
            name=name,
            domain_iri=domain_iri,
            range_xsd=range_xsd,
            comment=comment,
        ))

    return ParsedOntology(
        namespace=namespace,
        title=title,
        version=version,
        classes=sorted(classes, key=lambda c: c.name),
        object_properties=sorted(object_properties, key=lambda p: p.name),
        datatype_properties=sorted(datatype_properties, key=lambda p: p.name),
    )
