"""Upsert NakedAgent and OntologyAgent against the target Fabric workspace.

Agent-level definitions in Fabric are made of base64 parts under
``Files/Config/draft/`` (stage_config.json, <datasource>/datasource.json).
This module assembles the draft parts for both agents and pushes them
through the Fabric Data Agent REST API.
"""

from __future__ import annotations

import re
import uuid
from typing import Iterable

import requests

from ..fabric_client.auth import get_headers
from ..fabric_client.config import FabricConfig
from ..fabric_client.data_agent_api import DataAgentClient
from .instructions import (
    LAKEHOUSE_DS_DESCRIPTION,
    LAKEHOUSE_DS_INSTRUCTIONS,
    NAKED_AGENT_INSTRUCTIONS,
    ONTOLOGY_AGENT_INSTRUCTIONS,
    ONTOLOGY_DS_DESCRIPTION,
    ONTOLOGY_DS_INSTRUCTIONS,
)

DATA_AGENT_SCHEMA_URL = (
    "https://developer.microsoft.com/json-schemas/fabric/item/"
    "dataAgent/definition/dataAgent/2.1.0/schema.json"
)
STAGE_SCHEMA_URL = (
    "https://developer.microsoft.com/json-schemas/fabric/item/"
    "dataAgent/definition/stageConfiguration/1.0.0/schema.json"
)

# ontology valueType -> lakehouse_tables SQL type (for the datasource
# element schema)
_TYPE_TO_LAKEHOUSE_SQL = {
    "String": "varchar",
    "BigInt": "bigint",
    "Double": "float",
    "Boolean": "bit",
    "DateTime": "datetime2",
}


def _sanitize_path_segment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "_", value.strip())


def _list_workspace_items(config: FabricConfig) -> list[dict]:
    results: list[dict] = []
    url = f"{config.api_base}/workspaces/{config.workspace_id}/items"
    params: dict[str, str] = {}
    while url:
        response = requests.get(url, headers=get_headers(config), params=params)
        response.raise_for_status()
        body = response.json()
        results.extend(body.get("value", []))
        url = body.get("continuationUri")
        params = {}
    return results


def _find_item_by_id(items: list[dict], item_id: str) -> dict | None:
    for item in items:
        if item.get("id") == item_id:
            return item
    return None


# -- Datasource payload builders --------------------------------------------

def _lakehouse_ds_elements(ontology_config: dict, selected_tables: Iterable[str]) -> list[dict]:
    """Build the tree of table + column elements for the lakehouse datasource.

    Uses the canonical ``tableName`` recorded on each entity by the mapping
    step, NOT a recomputation from the entity name. An entity like
    ``Enforcement`` is backed by ``npl_enforcement_event`` and the
    snake_case heuristic would silently miswire it.
    """
    entities: dict[str, list[dict]] = {}
    for entity_cfg in ontology_config["entities"]:
        table_name = entity_cfg.get("tableName")
        if not table_name:
            raise ValueError(
                f"Entity '{entity_cfg.get('name', '?')}' is missing tableName; "
                f"rerun scripts/02_build_mapping.py to regenerate the config."
            )
        entities[table_name] = [
            {
                "id": str(uuid.uuid4()),
                "is_selected": True,
                "display_name": p["name"],
                "type": "lakehouse_tables.column",
                "data_type": _TYPE_TO_LAKEHOUSE_SQL.get(p["valueType"], "varchar"),
                "description": None,
                "children": [],
            }
            for p in entity_cfg["properties"]
        ]

    selected = set(selected_tables)
    table_elements: list[dict] = []
    for table_name in sorted(entities):
        if table_name not in selected:
            continue
        table_elements.append({
            "id": str(uuid.uuid4()),
            "is_selected": True,
            "display_name": table_name,
            "type": "lakehouse_tables.table",
            "description": None,
            "children": entities[table_name],
        })

    return [{
        "id": str(uuid.uuid4()),
        "is_selected": False,
        "display_name": "Schemas",
        "type": "schema_grouping",
        "description": None,
        "children": [{
            "id": str(uuid.uuid4()),
            "is_selected": False,
            "display_name": "dbo",
            "type": "lakehouse_tables.schema",
            "description": None,
            "children": [{
                "id": str(uuid.uuid4()),
                "is_selected": False,
                "display_name": "Tables",
                "type": "table_grouping",
                "description": None,
                "children": table_elements,
            }],
        }],
    }]


def _ontology_ds_elements(ontology_config: dict) -> list[dict]:
    elements = []
    for entity in sorted(ontology_config["entities"], key=lambda e: e["name"]):
        prop_names = [p["name"] for p in entity["properties"]]
        elements.append({
            "id": entity["name"],
            "is_selected": True,
            "display_name": entity["name"],
            "type": "ontology.entity",
            "description": ",".join(prop_names),
            "children": [],
        })
    return elements


# -- Definition assembly ----------------------------------------------------

def _build_updated_definition(
    existing_parts: list[dict],
    decoded_existing: dict[str, dict | str],
    ai_instructions: str,
    datasource_payloads: dict[str, dict],
) -> dict:
    """Merge the existing agent definition with new stage + datasource parts.

    Existing non-draft parts are preserved untouched; draft/* parts are
    replaced with the new stage_config + per-datasource payloads.
    """
    preserved: dict[str, dict | str] = {}
    for path, content in decoded_existing.items():
        if path.startswith("Files/Config/draft/"):
            continue
        preserved[path] = content

    if "Files/Config/data_agent.json" not in preserved:
        preserved["Files/Config/data_agent.json"] = {"$schema": DATA_AGENT_SCHEMA_URL}

    draft_stage = decoded_existing.get("Files/Config/draft/stage_config.json", {})
    if not isinstance(draft_stage, dict):
        draft_stage = {}
    draft_stage["$schema"] = draft_stage.get("$schema", STAGE_SCHEMA_URL)
    draft_stage["aiInstructions"] = ai_instructions.strip()
    preserved["Files/Config/draft/stage_config.json"] = draft_stage

    preserved.update(datasource_payloads)

    platform_part = next((p for p in existing_parts if p.get("path") == ".platform"), None)

    ordered_paths: list[str] = []
    if "Files/Config/data_agent.json" in preserved:
        ordered_paths.append("Files/Config/data_agent.json")
    if "Files/Config/draft/stage_config.json" in preserved:
        ordered_paths.append("Files/Config/draft/stage_config.json")

    draft_ds_paths = sorted(
        p for p in preserved
        if p.startswith("Files/Config/draft/") and p.endswith("/datasource.json")
    )
    ordered_paths.extend(draft_ds_paths)
    ordered_paths.extend(
        sorted(
            p for p in preserved
            if p not in set(ordered_paths) and p != ".platform"
        )
    )
    if ".platform" in preserved:
        ordered_paths.append(".platform")

    parts = []
    for path in ordered_paths:
        if path == ".platform" and platform_part:
            parts.append(platform_part)
            continue
        parts.append(DataAgentClient.encode_part(path, preserved[path]))

    return {"parts": parts}


def _upsert(
    client: DataAgentClient,
    *,
    name: str,
    description: str,
    ai_instructions: str,
    datasource_payloads: dict[str, dict],
) -> dict:
    existing = next((a for a in client.list_data_agents() if a.get("displayName") == name), None)
    if existing:
        agent = existing
        client.update_data_agent(agent["id"], description=description)
        print(f"Updated agent metadata: {name} ({agent['id']})")
    else:
        created = client.create_data_agent(name, description=description)
        agent_id = created.get("id")
        if not agent_id:
            refreshed = next((a for a in client.list_data_agents() if a.get("displayName") == name), None)
            if not refreshed:
                raise RuntimeError(f"Could not resolve created DataAgent '{name}'")
            agent = refreshed
        else:
            agent = created
        print(f"Created agent: {name} ({agent['id']})")

    raw_definition = client.get_definition(agent["id"])
    existing_parts, decoded_existing = DataAgentClient.decode_definition_parts(raw_definition)
    updated = _build_updated_definition(
        existing_parts,
        decoded_existing,
        ai_instructions,
        datasource_payloads,
    )
    client.update_definition(agent["id"], updated)
    print(f"Updated definition for: {name}")
    return {"id": agent["id"], "displayName": name}


# -- Public API -------------------------------------------------------------

def upsert_naked_agent(
    *,
    config: FabricConfig,
    ontology_config: dict,
    selected_tables: Iterable[str],
    lakehouse_display_name: str,
    name: str = "NakedAgent",
    instructions: str | None = None,
    description: str = "Lakehouse-only baseline data agent (no ontology source).",
) -> dict:
    """Upsert a NakedAgent wired to the lakehouse only."""
    client = DataAgentClient(config)
    lakehouse_ds = {
        "artifactId": config.lakehouse_id,
        "workspaceId": config.workspace_id,
        "displayName": lakehouse_display_name,
        "type": "lakehouse_tables",
        "userDescription": LAKEHOUSE_DS_DESCRIPTION,
        "dataSourceInstructions": LAKEHOUSE_DS_INSTRUCTIONS,
        "elements": _lakehouse_ds_elements(ontology_config, selected_tables),
    }
    lakehouse_ds_path = (
        f"Files/Config/draft/lakehouse-tables-"
        f"{_sanitize_path_segment(lakehouse_display_name)}/datasource.json"
    )
    return _upsert(
        client,
        name=name,
        description=description,
        ai_instructions=instructions or NAKED_AGENT_INSTRUCTIONS,
        datasource_payloads={lakehouse_ds_path: lakehouse_ds},
    )


def upsert_ontology_agent(
    *,
    config: FabricConfig,
    ontology_id: str,
    ontology_name: str,
    ontology_config: dict,
    selected_tables: Iterable[str] = (),       # unused; kept for API symmetry with naked
    lakehouse_display_name: str | None = None,  # unused; kept for API symmetry with naked
    name: str = "OntologyAgent",
    instructions: str | None = None,
    description: str = "Ontology-grounded data agent for semantic reasoning.",
) -> dict:
    """Upsert an OntologyAgent wired to ONLY the ontology.

    Per the Fabric ontology tutorial (Tutorial part 4: Create data agent),
    the agent is attached only to the ontology. The agent generates GQL
    against the ontology's auto-provisioned graph model, which is already
    bound to the lakehouse via bindings + contextualizations. Attaching
    the lakehouse as a second source would let the agent bypass the
    ontology with raw SQL and dilute the benchmark.
    """
    del selected_tables, lakehouse_display_name  # explicitly unused
    client = DataAgentClient(config)

    ontology_ds = {
        "artifactId": ontology_id,
        "workspaceId": config.workspace_id,
        "displayName": ontology_name,
        "type": "ontology",
        "userDescription": ONTOLOGY_DS_DESCRIPTION,
        "dataSourceInstructions": ONTOLOGY_DS_INSTRUCTIONS,
        "elements": _ontology_ds_elements(ontology_config),
    }
    ontology_ds_path = (
        f"Files/Config/draft/ontology-{_sanitize_path_segment(ontology_name)}/datasource.json"
    )

    return _upsert(
        client,
        name=name,
        description=description,
        ai_instructions=instructions or ONTOLOGY_AGENT_INSTRUCTIONS,
        datasource_payloads={ontology_ds_path: ontology_ds},
    )


def lookup_lakehouse_display_name(config: FabricConfig) -> str:
    """Resolve the display name of the configured lakehouse."""
    items = _list_workspace_items(config)
    lakehouse = _find_item_by_id(items, config.lakehouse_id)
    if not lakehouse or lakehouse.get("type") != "Lakehouse":
        raise ValueError(
            f"Lakehouse {config.lakehouse_id} not found in workspace {config.workspace_id}"
        )
    return lakehouse["displayName"]
