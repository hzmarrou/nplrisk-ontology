"""Build and manipulate Fabric ontology item definitions.

A Fabric ontology definition is a list of base64-encoded parts. This
module works with *decoded* parts (plain dicts with ``path`` and
``content``) and converts to/from the API's base64 format at the edges.

All entity-type, property, and relationship IDs are positive 64-bit
integers represented as strings, per the Fabric schema.
"""

from __future__ import annotations

import base64
import json
import random
import uuid
from typing import Callable

_ENTITY_TYPE_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/ontology/entityType/1.0.0/schema.json"
_RELATIONSHIP_TYPE_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/ontology/relationshipType/1.0.0/schema.json"


def generate_id() -> str:
    """Generate a random positive 64-bit integer ID as a string."""
    return str(random.randint(10**15, 10**18))


def generate_guid() -> str:
    """Generate a UUID (for data binding / contextualization IDs)."""
    return str(uuid.uuid4())


# -- Decode / Encode ---------------------------------------------------------

def decode_definition(raw: dict) -> list[dict]:
    """Decode a ``getDefinition`` response into a list of ``{path, content}``."""
    parts = raw.get("definition", {}).get("parts", [])
    decoded: list[dict] = []
    for part in parts:
        payload_b64 = part.get("payload", "")
        try:
            payload_str = base64.b64decode(payload_b64).decode("utf-8")
            content = json.loads(payload_str) if payload_str.strip() else {}
        except Exception:
            content = base64.b64decode(payload_b64).decode("utf-8", errors="replace")
        decoded.append({"path": part["path"], "content": content})
    return decoded


def encode_definition(parts: list[dict]) -> dict:
    """Encode decoded parts back into the Fabric API's base64 envelope."""
    encoded_parts = []
    for part in parts:
        content = part["content"]
        payload_str = json.dumps(content) if isinstance(content, dict) else str(content)
        payload_b64 = base64.b64encode(payload_str.encode("utf-8")).decode("ascii")
        encoded_parts.append({
            "path": part["path"],
            "payload": payload_b64,
            "payloadType": "InlineBase64",
        })
    return {"parts": encoded_parts}


# -- Entity types ------------------------------------------------------------

def make_property(name: str, value_type: str, prop_id: str | None = None) -> dict:
    return {"id": prop_id or generate_id(), "name": name, "valueType": value_type}


def make_entity_type(
    name: str,
    *,
    properties: list[dict] | None = None,
    entity_id: str | None = None,
    entity_id_parts: list[str] | None = None,
    display_name_property_id: str | None = None,
    timeseries_properties: list[dict] | None = None,
) -> tuple[str, dict]:
    et_id = entity_id or generate_id()
    props = []
    for p in properties or []:
        props.append({
            "id": p.get("id", generate_id()),
            "name": p["name"],
            "redefines": None,
            "baseTypeNamespaceType": None,
            "valueType": p["valueType"],
        })
    definition = {
        "$schema": _ENTITY_TYPE_SCHEMA,
        "id": et_id,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": name,
        "entityIdParts": entity_id_parts or [],
        "displayNamePropertyId": display_name_property_id,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": props,
        "timeseriesProperties": timeseries_properties or [],
    }
    return et_id, definition


def add_entity_type(parts: list[dict], et_id: str, definition: dict) -> list[dict]:
    return parts + [{"path": f"EntityTypes/{et_id}/definition.json", "content": definition}]


def get_entity_type(parts: list[dict], et_id: str) -> dict | None:
    path = f"EntityTypes/{et_id}/definition.json"
    for part in parts:
        if part["path"] == path:
            return part["content"]
    return None


def update_entity_type(parts: list[dict], et_id: str, definition: dict) -> list[dict]:
    path = f"EntityTypes/{et_id}/definition.json"
    return [
        {"path": p["path"], "content": definition} if p["path"] == path else p
        for p in parts
    ]


def remove_entity_type(parts: list[dict], et_id: str) -> list[dict]:
    prefix = f"EntityTypes/{et_id}/"
    return [p for p in parts if not p["path"].startswith(prefix)]


def list_entity_types(parts: list[dict]) -> list[dict]:
    return [
        p["content"] for p in parts
        if "EntityTypes/" in p["path"]
        and p["path"].endswith("/definition.json")
        and "Overviews" not in p["path"]
    ]


# -- Relationship types ------------------------------------------------------

def make_relationship_type(
    name: str,
    source_entity_type_id: str,
    target_entity_type_id: str,
    relationship_id: str | None = None,
) -> tuple[str, dict]:
    rt_id = relationship_id or generate_id()
    definition = {
        "$schema": _RELATIONSHIP_TYPE_SCHEMA,
        "namespace": "usertypes",
        "id": rt_id,
        "name": name,
        "namespaceType": "Custom",
        "source": {"entityTypeId": source_entity_type_id},
        "target": {"entityTypeId": target_entity_type_id},
    }
    return rt_id, definition


def add_relationship_type(parts: list[dict], rt_id: str, definition: dict) -> list[dict]:
    return parts + [{"path": f"RelationshipTypes/{rt_id}/definition.json", "content": definition}]


def list_relationship_types(parts: list[dict]) -> list[dict]:
    return [
        p["content"] for p in parts
        if "RelationshipTypes/" in p["path"]
        and p["path"].endswith("/definition.json")
    ]


def remove_relationship_type(parts: list[dict], rt_id: str) -> list[dict]:
    prefix = f"RelationshipTypes/{rt_id}/"
    return [p for p in parts if not p["path"].startswith(prefix)]


# -- Data bindings (entity -> lakehouse table) -------------------------------

def make_property_binding(source_column: str, target_property_id: str) -> dict:
    return {"sourceColumnName": source_column, "targetPropertyId": target_property_id}


def make_lakehouse_binding(
    entity_type_id: str,
    property_bindings: list[dict],
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    *,
    binding_type: str = "NonTimeSeries",
    timestamp_column: str | None = None,
    source_schema: str = "dbo",
    binding_id: str | None = None,
) -> tuple[str, dict]:
    bid = binding_id or generate_guid()
    config: dict = {
        "dataBindingType": binding_type,
        "propertyBindings": property_bindings,
        "sourceTableProperties": {
            "sourceType": "LakehouseTable",
            "workspaceId": workspace_id,
            "itemId": lakehouse_id,
            "sourceTableName": table_name,
            "sourceSchema": source_schema,
        },
    }
    if binding_type == "TimeSeries":
        config["timestampColumnName"] = timestamp_column
    return bid, {"id": bid, "dataBindingConfiguration": config}


def add_data_binding(parts: list[dict], entity_type_id: str,
                     binding_id: str, definition: dict) -> list[dict]:
    path = f"EntityTypes/{entity_type_id}/DataBindings/{binding_id}.json"
    return parts + [{"path": path, "content": definition}]


def list_data_bindings(parts: list[dict], entity_type_id: str | None = None) -> list[dict]:
    results = []
    for p in parts:
        if "/DataBindings/" not in p["path"]:
            continue
        if entity_type_id and f"EntityTypes/{entity_type_id}/" not in p["path"]:
            continue
        results.append({"path": p["path"], "content": p["content"]})
    return results


# -- Contextualizations (edge -> junction/context table) ---------------------

def make_key_ref_binding(source_column: str, target_property_id: str) -> dict:
    return {"sourceColumnName": source_column, "targetPropertyId": target_property_id}


def make_contextualization(
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    source_key_bindings: list[dict],
    target_key_bindings: list[dict],
    *,
    source_schema: str = "dbo",
    ctx_id: str | None = None,
) -> tuple[str, dict]:
    cid = ctx_id or generate_guid()
    definition = {
        "id": cid,
        "dataBindingTable": {
            "workspaceId": workspace_id,
            "itemId": lakehouse_id,
            "sourceTableName": table_name,
            "sourceSchema": source_schema,
            "sourceType": "LakehouseTable",
        },
        "sourceKeyRefBindings": source_key_bindings,
        "targetKeyRefBindings": target_key_bindings,
    }
    return cid, definition


def add_contextualization(parts: list[dict], rt_id: str,
                          ctx_id: str, definition: dict) -> list[dict]:
    path = f"RelationshipTypes/{rt_id}/Contextualizations/{ctx_id}.json"
    return parts + [{"path": path, "content": definition}]


# -- Config-driven high-level builder ----------------------------------------

def build_from_config(
    config: dict,
    table_namer: Callable[[str], str] | None = None,
) -> tuple[list[dict], dict, dict]:
    """Build the initial ontology definition from a Fabric-ready config dict.

    Returns
    -------
    parts:
        Decoded parts list (use ``encode_definition`` before pushing).
    entity_map:
        ``{entity_name: {id, key_prop_ids, key_prop_names, prop_ids, table}}``
    relationship_map:
        ``{rel_name: {id, source, target, contextEntity, sourceKeyColumns, targetKeyColumns}}``
    """
    from . import lakehouse_sync as _lhs  # local import to avoid cycle

    prefix = config.get("tablePrefix", "ont")
    namer = table_namer or (lambda n: f"{prefix}_{_lhs.entity_name_to_table(n)}")

    parts: list[dict] = [{"path": "definition.json", "content": {}}]
    entity_map: dict = {}

    def _as_list(v):
        if isinstance(v, list):
            return v
        if v is None:
            return []
        return [v]

    for entity_cfg in config["entities"]:
        name = entity_cfg["name"]
        key_prop_names = _as_list(entity_cfg.get("keyProperty"))
        if not key_prop_names:
            raise ValueError(f"Entity '{name}': keyProperty is required")
        display_prop_name = entity_cfg.get("displayProperty", key_prop_names[0])

        properties = [make_property(p["name"], p["valueType"]) for p in entity_cfg["properties"]]
        prop_ids_by_name = {p["name"]: p["id"] for p in properties}
        display_prop_id = prop_ids_by_name.get(display_prop_name)

        missing_keys = [k for k in key_prop_names if k not in prop_ids_by_name]
        if missing_keys:
            raise ValueError(
                f"Entity '{name}': keyProperty values not in properties: {missing_keys}"
            )
        key_prop_ids = [prop_ids_by_name[k] for k in key_prop_names]

        et_id, et_def = make_entity_type(
            name,
            properties=properties,
            entity_id_parts=key_prop_ids,
            display_name_property_id=display_prop_id,
        )
        parts = add_entity_type(parts, et_id, et_def)

        entity_map[name] = {
            "id": et_id,
            "key_prop_id": key_prop_ids[0],
            "key_prop_name": key_prop_names[0],
            "key_prop_ids": key_prop_ids,
            "key_prop_names": key_prop_names,
            "prop_ids": {p["name"]: p["id"] for p in et_def["properties"]},
            "table": namer(name),
        }

    relationship_map: dict = {}
    for rel_cfg in config["relationships"]:
        rel_name = rel_cfg["name"]
        source_name = rel_cfg["source"]
        target_name = rel_cfg["target"]
        context_entity = rel_cfg.get("contextEntity", target_name)

        rt_id, rt_def = make_relationship_type(
            rel_name,
            entity_map[source_name]["id"],
            entity_map[target_name]["id"],
        )
        parts = add_relationship_type(parts, rt_id, rt_def)

        relationship_map[rel_name] = {
            "id": rt_id,
            "source": source_name,
            "target": target_name,
            "contextEntity": context_entity,
            "contextTable": rel_cfg.get("contextTable"),
            "sourceKeyColumns": rel_cfg.get("sourceKeyColumns"),
            "targetKeyColumns": rel_cfg.get("targetKeyColumns"),
        }

    return parts, entity_map, relationship_map


def add_all_bindings(parts: list, entity_map: dict, entities_config: list,
                     workspace_id: str, lakehouse_id: str) -> list:
    """Add lakehouse data bindings for every entity in one pass."""
    for entity_cfg in entities_config:
        name = entity_cfg["name"]
        info = entity_map[name]

        prop_bindings = [
            make_property_binding(p["name"], info["prop_ids"][p["name"]])
            for p in entity_cfg["properties"]
        ]

        bid, binding_def = make_lakehouse_binding(
            info["id"], prop_bindings,
            workspace_id, lakehouse_id, info["table"],
        )
        parts = add_data_binding(parts, info["id"], bid, binding_def)
    return parts


def add_all_contextualizations(
    parts: list,
    relationship_map: dict,
    entity_map: dict,
    workspace_id: str,
    lakehouse_id: str,
) -> list:
    """Add contextualizations for every relationship in one pass.

    Each relationship's ``contextEntity`` identifies which table carries
    the edge. ``contextTable`` overrides the naming convention when the
    junction table doesn't share the entity name. ``sourceKeyColumns`` /
    ``targetKeyColumns`` override the default column names.
    """

    def _as_list(v):
        if isinstance(v, list):
            return v
        if v is None:
            return []
        return [v]

    for rel_name, rel_info in relationship_map.items():
        source_info = entity_map[rel_info["source"]]
        target_info = entity_map[rel_info["target"]]

        # context table: explicit override > entity_map lookup
        ctx_table: str
        if rel_info.get("contextTable"):
            ctx_table = rel_info["contextTable"]
        else:
            ctx_name = rel_info["contextEntity"]
            if ctx_name in entity_map:
                ctx_table = entity_map[ctx_name]["table"]
            else:
                raise ValueError(
                    f"Relationship '{rel_name}': contextEntity '{ctx_name}' "
                    f"not in entity_map and no contextTable override supplied"
                )

        source_prop_ids = source_info["key_prop_ids"]
        target_prop_ids = target_info["key_prop_ids"]
        source_cols = _as_list(rel_info.get("sourceKeyColumns")) or source_info["key_prop_names"]
        target_cols = _as_list(rel_info.get("targetKeyColumns")) or target_info["key_prop_names"]

        if len(source_cols) != len(source_prop_ids):
            raise ValueError(
                f"Relationship '{rel_name}': sourceKeyColumns count mismatch"
            )
        if len(target_cols) != len(target_prop_ids):
            raise ValueError(
                f"Relationship '{rel_name}': targetKeyColumns count mismatch"
            )

        source_bindings = [
            make_key_ref_binding(col, pid)
            for col, pid in zip(source_cols, source_prop_ids)
        ]
        target_bindings = [
            make_key_ref_binding(col, pid)
            for col, pid in zip(target_cols, target_prop_ids)
        ]

        ctx_id, ctx_def = make_contextualization(
            workspace_id, lakehouse_id, ctx_table,
            source_bindings, target_bindings,
        )
        parts = add_contextualization(parts, rel_info["id"], ctx_id, ctx_def)

    return parts
