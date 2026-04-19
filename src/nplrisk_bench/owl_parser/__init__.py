"""Parse an OWL/RDF-XML ontology file into a neutral dataclass model."""

from .model import DatatypeProperty, ObjectProperty, OwlClass, ParsedOntology
from .parser import parse_owl

__all__ = [
    "DatatypeProperty",
    "ObjectProperty",
    "OwlClass",
    "ParsedOntology",
    "parse_owl",
]
