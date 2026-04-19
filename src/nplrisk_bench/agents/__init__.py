"""Provision NakedAgent + OntologyAgent against a Fabric workspace."""

from .provision import upsert_naked_agent, upsert_ontology_agent

__all__ = ["upsert_naked_agent", "upsert_ontology_agent"]
