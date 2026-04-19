"""Parse an OWL/RDF-XML ontology file using rdflib.

The parser is domain-neutral, but it filters out a handful of named
meta-properties (Confidentiality, Importance, Temporality, ...) that
are used in NPLO-style ontologies purely as annotation tags rather than
as real domain attributes. Domain ontologies that don't use those tags
are unaffected.
"""

from __future__ import annotations

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


def parse_owl(path: Path) -> ParsedOntology:
    """Parse an OWL/RDF-XML file and return a `ParsedOntology`."""
    g = Graph()
    g.parse(str(path), format="xml")

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
