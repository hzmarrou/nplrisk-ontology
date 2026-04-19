"""Dataclasses representing a parsed OWL ontology.

The model is deliberately domain-neutral: the same types describe any
OWL/RDF ontology. Consumers pick out the parts they care about via
`ParsedOntology.class_by_name()`, `datatype_properties_for_class()`,
etc.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class OwlClass:
    """An OWL class (entity type)."""

    iri: str
    name: str
    parent_iri: str | None = None
    comment: str = ""

    @property
    def parent_name(self) -> str | None:
        if self.parent_iri and "#" in self.parent_iri:
            return self.parent_iri.rsplit("#", 1)[1]
        return None


@dataclass
class ObjectProperty:
    """An OWL object property (relationship between classes)."""

    iri: str
    name: str
    domain_iri: str
    range_iri: str
    is_functional: bool = False
    inverse_iri: str | None = None
    disjoint_with_iris: list[str] = field(default_factory=list)

    @property
    def domain_name(self) -> str:
        return self.domain_iri.rsplit("#", 1)[1] if "#" in self.domain_iri else self.domain_iri

    @property
    def range_name(self) -> str:
        return self.range_iri.rsplit("#", 1)[1] if "#" in self.range_iri else self.range_iri

    @property
    def inverse_name(self) -> str | None:
        if self.inverse_iri and "#" in self.inverse_iri:
            return self.inverse_iri.rsplit("#", 1)[1]
        return None


@dataclass
class DatatypeProperty:
    """An OWL datatype property (attribute of a class)."""

    iri: str
    name: str
    domain_iri: str
    range_xsd: str = "xsd:string"
    comment: str = ""

    @property
    def domain_name(self) -> str:
        return self.domain_iri.rsplit("#", 1)[1] if "#" in self.domain_iri else self.domain_iri

    @property
    def fabric_value_type(self) -> str:
        """Map an XSD type to a Fabric ontology `valueType`.

        Fabric supports: String, BigInt, Double, Boolean, DateTime.
        Unknown / non-XSD types fall back to String.
        """
        mapping = {
            "xsd:decimal": "Double",
            "xsd:float": "Double",
            "xsd:double": "Double",
            "xsd:integer": "BigInt",
            "xsd:int": "BigInt",
            "xsd:long": "BigInt",
            "xsd:nonNegativeInteger": "BigInt",
            "xsd:positiveInteger": "BigInt",
            "xsd:dateTime": "DateTime",
            "xsd:date": "DateTime",
            "xsd:boolean": "Boolean",
            "xsd:string": "String",
            "xsd:anyURI": "String",
        }
        return mapping.get(self.range_xsd, "String")


@dataclass
class ParsedOntology:
    """Complete parsed OWL ontology."""

    namespace: str
    title: str
    version: str
    classes: list[OwlClass] = field(default_factory=list)
    object_properties: list[ObjectProperty] = field(default_factory=list)
    datatype_properties: list[DatatypeProperty] = field(default_factory=list)

    # -- Lookups ----------------------------------------------------------

    def class_by_name(self, name: str) -> OwlClass | None:
        return next((c for c in self.classes if c.name == name), None)

    def object_property_by_name(self, name: str) -> ObjectProperty | None:
        return next((p for p in self.object_properties if p.name == name), None)

    def datatype_properties_for_class(self, class_name: str) -> list[DatatypeProperty]:
        """Return datatype properties whose domain is the given class."""
        return [p for p in self.datatype_properties if p.domain_name == class_name]

    def all_datatype_properties_for_class(self, class_name: str) -> list[DatatypeProperty]:
        """Return the class's own datatype properties plus all inherited ones.

        Walks up the parent chain (rdfs:subClassOf) and concatenates every
        ancestor's direct datatype properties. Duplicates are dropped by
        property name, preferring the most-derived class's version.
        """
        seen: set[str] = set()
        out: list[DatatypeProperty] = []
        current: OwlClass | None = self.class_by_name(class_name)
        while current is not None:
            for p in self.datatype_properties_for_class(current.name):
                if p.name in seen:
                    continue
                seen.add(p.name)
                out.append(p)
            parent = current.parent_name
            current = self.class_by_name(parent) if parent else None
        return out

    def subclasses_of(self, class_name: str) -> list[OwlClass]:
        """Return direct subclasses of the given class."""
        return [c for c in self.classes if c.parent_name == class_name]

    def descendants_of(self, class_name: str) -> list[OwlClass]:
        """Return all descendant classes (direct + transitive subclasses)."""
        out: list[OwlClass] = []
        frontier = self.subclasses_of(class_name)
        while frontier:
            nxt: list[OwlClass] = []
            for c in frontier:
                out.append(c)
                nxt.extend(self.subclasses_of(c.name))
            frontier = nxt
        return out

    # -- Transformation ---------------------------------------------------

    def flatten_hierarchy(self, root_names: list[str]) -> ParsedOntology:
        """Return a new ParsedOntology with subclass chains collapsed into roots.

        For every root in `root_names`, all datatype properties declared
        on its descendants are re-homed onto the root class, and the
        descendant classes are dropped. Object properties whose domain
        or range is a descendant are rewritten to use the root. This is
        useful when the physical data layer (CSVs, DDL) uses a
        discriminator column instead of separate tables per subclass.

        Classes that are neither roots nor descendants of any root are
        passed through unchanged.
        """
        # Build descendant closure per root
        flattened = {root: {root} | {d.name for d in self.descendants_of(root)} for root in root_names}
        descendant_to_root: dict[str, str] = {}
        for root, names in flattened.items():
            for n in names:
                if n != root:
                    descendant_to_root[n] = root

        # Filter classes: keep roots and anything not in any descendant set
        drop = set(descendant_to_root)
        kept_classes = [c for c in self.classes if c.name not in drop]

        # Datatype properties: remap domain to root where applicable, dedup by (class, name)
        new_datatypes: list[DatatypeProperty] = []
        seen: set[tuple[str, str]] = set()
        for p in self.datatype_properties:
            target_class = descendant_to_root.get(p.domain_name, p.domain_name)
            key = (target_class, p.name)
            if key in seen:
                continue
            seen.add(key)
            new_datatypes.append(
                DatatypeProperty(
                    iri=p.iri,
                    name=p.name,
                    domain_iri=p.domain_iri.replace(p.domain_name, target_class) if p.domain_name != target_class else p.domain_iri,
                    range_xsd=p.range_xsd,
                    comment=p.comment,
                )
            )

        # Object properties: rewrite domain/range + drop self-loops introduced by flattening
        new_objects: list[ObjectProperty] = []
        seen_obj: set[tuple[str, str, str]] = set()
        for p in self.object_properties:
            d = descendant_to_root.get(p.domain_name, p.domain_name)
            r = descendant_to_root.get(p.range_name, p.range_name)
            key = (p.name, d, r)
            if key in seen_obj:
                continue
            seen_obj.add(key)
            new_objects.append(
                ObjectProperty(
                    iri=p.iri,
                    name=p.name,
                    domain_iri=p.domain_iri.replace(p.domain_name, d) if p.domain_name != d else p.domain_iri,
                    range_iri=p.range_iri.replace(p.range_name, r) if p.range_name != r else p.range_iri,
                    is_functional=p.is_functional,
                    inverse_iri=p.inverse_iri,
                    disjoint_with_iris=p.disjoint_with_iris,
                )
            )

        return ParsedOntology(
            namespace=self.namespace,
            title=self.title,
            version=self.version,
            classes=sorted(kept_classes, key=lambda c: c.name),
            object_properties=sorted(new_objects, key=lambda p: p.name),
            datatype_properties=sorted(new_datatypes, key=lambda p: p.name),
        )

    # -- Persistence ------------------------------------------------------

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")

    @property
    def summary(self) -> str:
        lines = [
            f"Ontology: {self.title} (v{self.version})",
            f"Namespace: {self.namespace}",
            f"Classes: {len(self.classes)}",
            f"Object Properties: {len(self.object_properties)}",
            f"Datatype Properties: {len(self.datatype_properties)}",
            "",
            "Class hierarchy:",
        ]
        roots = [c for c in self.classes if c.parent_iri is None or "owl#Thing" in (c.parent_iri or "")]
        for root in sorted(roots, key=lambda c: c.name):
            lines.append(f"  {root.name}")
            for child in sorted(self.subclasses_of(root.name), key=lambda c: c.name):
                lines.append(f"    {child.name}")
                for grandchild in sorted(self.subclasses_of(child.name), key=lambda c: c.name):
                    lines.append(f"      {grandchild.name}")

        lines.append("")
        lines.append("Object properties:")
        for p in sorted(self.object_properties, key=lambda p: p.name):
            inv = f" (inverse: {p.inverse_name})" if p.inverse_name else ""
            func = " [functional]" if p.is_functional else ""
            lines.append(f"  {p.domain_name} --{p.name}--> {p.range_name}{func}{inv}")

        lines.append("")
        lines.append("Datatype properties per class:")
        for cls in sorted(self.classes, key=lambda c: c.name):
            props = self.datatype_properties_for_class(cls.name)
            if props:
                lines.append(f"  {cls.name}: {len(props)} properties")

        return "\n".join(lines)
