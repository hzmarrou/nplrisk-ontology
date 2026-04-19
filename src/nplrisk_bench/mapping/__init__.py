"""Map a ParsedOntology + physical schema (DDL + CSV headers) into a
Fabric-ready ontology config dict."""

from .owl_to_fabric import build_ontology_config, load_ddl_tables

__all__ = ["build_ontology_config", "load_ddl_tables"]
