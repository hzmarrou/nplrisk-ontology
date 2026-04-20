"""03 — Create the ontology in Fabric, load tables, push bindings.

Inputs:
    outputs/ontology-config.json        (from 02_build_mapping.py)
    input/data/csv/*.csv                (13 NPL CSVs + loan_borrower_link.csv)
    input/data/schema/ddl.sql           (for junction-table column types)
Outputs:
    outputs/_state.json                 (ontologyId, ontologyName, tables)

Behaviour:
    1. Delete any stale NPL artifacts (ontology, graph model, auto-created lakehouse)
    2. Create the new Fabric ontology, push entity + relationship schema
    3. Open a Livy session and drop any pre-existing npl_* tables
    4. Create entity + junction tables from the config + DDL, load CSVs
    5. Add data bindings + contextualizations, push final definition
    6. Write outputs/_state.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from nplrisk_bench.fabric_client import FabricConfig  # noqa: E402
from nplrisk_bench.fabric_client.auth import get_headers  # noqa: E402
from nplrisk_bench.fabric_client.definition_builder import (  # noqa: E402
    add_all_bindings,
    add_all_contextualizations,
    build_from_config,
    decode_definition,
    encode_definition,
)
from nplrisk_bench.fabric_client.graph_api import GraphClient  # noqa: E402
from nplrisk_bench.fabric_client.lakehouse_sync import (  # noqa: E402
    create_tables_from_config,
    entity_name_to_table,
    load_csv_data,
)
from nplrisk_bench.fabric_client.livy_api import LivyClient  # noqa: E402
from nplrisk_bench.fabric_client.ontology_api import OntologyClient  # noqa: E402
from nplrisk_bench.mapping import load_ddl_tables  # noqa: E402


# -- Helpers ----------------------------------------------------------------

def _list_workspace_items(config: FabricConfig) -> list[dict]:
    items: list[dict] = []
    url = f"{config.api_base}/workspaces/{config.workspace_id}/items"
    params: dict[str, str] = {}
    while url:
        r = requests.get(url, headers=get_headers(config), params=params)
        r.raise_for_status()
        body = r.json()
        items.extend(body.get("value", []))
        url = body.get("continuationUri")
        params = {}
    return items


def _cleanup_stale(config: FabricConfig, ontology_names: list[str], extra_lh_prefixes: list[str]) -> None:
    """Delete ontologies / graph models / auto-created lakehouses matching the given names.

    After the deletions, poll ``list_ontologies`` until the removed names no
    longer appear — Fabric sometimes takes a handful of seconds to release
    the displayName, and a second create_ontology call raced against that
    window returns 409 Conflict.
    """
    items = _list_workspace_items(config)
    headers = get_headers(config)
    deleted_names: set[str] = set()

    # 1. Delete ontologies (this usually removes the auto-created graph too, but we also clean below)
    ont_client = OntologyClient(config)
    for o in ont_client.list_ontologies():
        if o["displayName"] in ontology_names:
            print(f"  deleting ontology {o['displayName']} ({o['id']})")
            ont_client.delete_ontology(o["id"])
            deleted_names.add(o["displayName"])

    # 2. Orphan graph models that match our name pattern
    gc = GraphClient(config)
    for g in gc.list_graph_models():
        name = g.get("displayName", "")
        if any(name.startswith(prefix) for prefix in ontology_names) or "_graph_" in name or name == "npl_graph":
            print(f"  deleting graph model {name} ({g['id']})")
            try:
                gc.delete_graph_model(g["id"])
            except Exception as exc:  # noqa: BLE001
                print(f"    WARN: {exc}")

    # 3. Auto-created lakehouses from previous runs
    for it in items:
        if it.get("type") != "Lakehouse":
            continue
        if it.get("id") == config.lakehouse_id:
            continue  # never touch the configured lakehouse
        name = it.get("displayName", "")
        if any(name.startswith(prefix) for prefix in extra_lh_prefixes):
            print(f"  deleting auto-created lakehouse {name} ({it['id']})")
            try:
                r = requests.delete(
                    f"{config.api_base}/workspaces/{config.workspace_id}/lakehouses/{it['id']}",
                    headers=headers,
                )
                r.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                print(f"    WARN: {exc}")

    # 4. Wait until every deleted ontology displayName is actually gone.
    if deleted_names:
        print(f"  waiting for {len(deleted_names)} ontology deletion(s) to propagate...")
        deadline = time.time() + 120
        while time.time() < deadline:
            remaining = {o["displayName"] for o in ont_client.list_ontologies()} & deleted_names
            if not remaining:
                print("    deletions propagated.")
                return
            time.sleep(5)
        print(f"    WARN: still seeing {sorted(remaining)} after 120s; continuing anyway.")


def _create_ontology_with_id(
    ont: OntologyClient,
    name: str,
    description: str,
    *,
    conflict_retries: int = 12,
    conflict_backoff: int = 10,
) -> str:
    """Create an ontology, retrying on 409 Conflict.

    Fabric reports a deleted displayName as gone from ``list_ontologies``
    before its internal name reservation is actually released. A fresh
    POST in that window returns 409. We retry with a modest backoff until
    the name becomes available.
    """
    for attempt in range(1, conflict_retries + 1):
        try:
            result = ont.create_ontology(name, description=description)
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 409 and attempt < conflict_retries:
                print(f"  create returned 409 (attempt {attempt}); waiting {conflict_backoff}s before retry...")
                time.sleep(conflict_backoff)
                continue
            raise

        ontology_id = result.get("id")
        if ontology_id:
            return ontology_id
        time.sleep(3)
        for o in ont.list_ontologies():
            if o["displayName"] == name:
                return o["id"]
        raise RuntimeError(f"Could not resolve newly created ontology '{name}'")

    raise RuntimeError(
        f"Could not create ontology '{name}' after {conflict_retries} attempts; "
        f"Fabric kept returning 409 Conflict."
    )


# -- Junction table handling ------------------------------------------------

_DDL_TO_SPARK = {
    "bigint": "BIGINT",
    "integer": "INT",
    "smallint": "SMALLINT",
    "int": "INT",
    "numeric": "DOUBLE",
    "decimal": "DOUBLE",
    "float": "DOUBLE",
    "double": "DOUBLE",
    "real": "DOUBLE",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMP",
    "datetime": "TIMESTAMP",
    "date": "DATE",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
}


def _sql_to_spark(sql_type: str) -> str:
    s = sql_type.strip().lower()
    for prefix, spark in _DDL_TO_SPARK.items():
        if s.startswith(prefix):
            return spark
    return "STRING"


def _junction_tables_to_load(config_dict: dict, ddl_tables: dict, entity_tables: set[str]) -> list[tuple[str, str, list[tuple[str, str]]]]:
    """Return `[(prefixed_table_name, ddl_table_name, [(col, spark_type), ...]), ...]`
    for every `contextTable` referenced by a relationship that is NOT an entity table.
    """
    prefix = config_dict.get("tablePrefix", "npl")
    out: list[tuple[str, str, list[tuple[str, str]]]] = []
    seen: set[str] = set()

    for rel in config_dict.get("relationships", []):
        ctx = rel.get("contextTable")
        if not ctx or ctx in entity_tables or ctx in seen:
            continue
        raw = ctx[len(prefix) + 1:] if ctx.startswith(prefix + "_") else ctx
        if raw not in ddl_tables:
            continue
        seen.add(ctx)
        cols = [(c.name, _sql_to_spark(c.sql_type)) for c in ddl_tables[raw].columns]
        out.append((ctx, raw, cols))
    return out


def _create_junction_table(livy: LivyClient, table_name: str, columns: list[tuple[str, str]]) -> None:
    col_def = ", ".join(f"{name} {spark_type}" for name, spark_type in columns)
    print(f"  creating junction table {table_name}...")
    livy.sql(f"DROP TABLE IF EXISTS {table_name}")
    livy.sql(f"CREATE TABLE {table_name} ({col_def}) USING DELTA")


def _load_junction_csv(livy: LivyClient, table_name: str, ddl_table_name: str,
                      columns: list[tuple[str, str]], csv_dir: Path, batch_size: int = 200) -> None:
    import csv as csv_mod

    csv_path = csv_dir / f"{ddl_table_name}.csv"
    if not csv_path.exists():
        print(f"  SKIP junction {table_name} — seed file missing: {csv_path}")
        return

    col_types = {name: spark_type for name, spark_type in columns}
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv_mod.DictReader(f))
    if not rows:
        return

    col_names = [c for c, _ in columns if c in rows[0].keys()]
    col_list = ", ".join(col_names)

    value_rows: list[str] = []
    for row in rows:
        values: list[str] = []
        for col in col_names:
            val = (row.get(col) or "").strip()
            spark = col_types[col]
            if val == "":
                values.append("NULL")
            elif spark in ("BIGINT", "INT", "SMALLINT", "DOUBLE"):
                values.append(val)
            elif spark == "BOOLEAN":
                values.append(val.lower())
            elif spark == "TIMESTAMP":
                values.append(f"TIMESTAMP '{val}'")
            elif spark == "DATE":
                values.append(f"DATE '{val}'")
            else:
                values.append("'" + val.replace("'", "''") + "'")
        value_rows.append("(" + ", ".join(values) + ")")

    print(f"  loading {len(rows)} rows into {table_name}...")
    for i in range(0, len(value_rows), batch_size):
        batch = value_rows[i:i + batch_size]
        livy.sql(f"INSERT INTO {table_name} ({col_list}) VALUES {', '.join(batch)}")


# -- Main -------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", default=REPO_ROOT / "outputs" / "ontology-config.json", type=Path)
    parser.add_argument("--ddl", default=REPO_ROOT / "input" / "data" / "schema" / "ddl.sql", type=Path)
    parser.add_argument("--csv-dir", default=REPO_ROOT / "input" / "data" / "csv", type=Path)
    parser.add_argument("--state-out", default=REPO_ROOT / "outputs" / "_state.json", type=Path)
    parser.add_argument("--cleanup-names", nargs="*",
                        default=["NPL_Risk", "npl_ontology", "NPLRisk_Access_Probe"],
                        help="Ontology displayNames to delete before creating the new one.")
    parser.add_argument("--cleanup-lh-prefixes", nargs="*",
                        default=["npl_ontology_lh_", "NPL_Risk_lh_", "NPLRisk_Access_Probe_lh_"],
                        help="Auto-created lakehouse name prefixes to delete.")
    args = parser.parse_args()

    config = FabricConfig.from_env()
    cfg_dict = json.loads(args.config.read_text(encoding="utf-8"))
    ddl_tables = load_ddl_tables(args.ddl)

    print("=" * 60)
    print(f"  SETUP: {cfg_dict['name']}")
    print(f"  workspace={config.workspace_id}  lakehouse={config.lakehouse_id}")
    print("=" * 60)

    # 1. Cleanup
    print("\n[1] Cleaning up stale NPL artifacts...")
    _cleanup_stale(config, args.cleanup_names, args.cleanup_lh_prefixes)

    # 2. Build schema parts, create ontology, push schema
    print("\n[2] Building initial definition...")
    parts, entity_map, relationship_map = build_from_config(cfg_dict)
    print(f"    {len(entity_map)} entities, {len(relationship_map)} relationships")

    print("\n[3] Creating ontology...")
    ont = OntologyClient(config)
    ontology_id = _create_ontology_with_id(ont, cfg_dict["name"], cfg_dict.get("description", ""))
    print(f"    ontologyId: {ontology_id}")

    print("\n[4] Pushing entity + relationship schema...")
    ont.update_definition(ontology_id, encode_definition(parts))

    # 5. Livy: tables + CSV loads
    entity_tables = {info["table"] for info in entity_map.values()}
    junction_specs = _junction_tables_to_load(cfg_dict, ddl_tables, entity_tables)
    all_tables = sorted(entity_tables | {j[0] for j in junction_specs})

    # Any `npl_<raw_ddl_table_name>` name is a legal candidate (including
    # previous-run ghosts like npl_enforcement that the current mapping no
    # longer produces). Drop everything so we start from a clean slate.
    stale_tables = {f"{cfg_dict.get('tablePrefix','npl')}_{raw}" for raw in ddl_tables}
    drop_targets = sorted(set(all_tables) | stale_tables)

    print(f"\n[5] Opening Livy session...")
    with LivyClient(config) as livy:
        print("\n[5a] Dropping any pre-existing NPL tables...")
        for t in drop_targets:
            livy.sql(f"DROP TABLE IF EXISTS {t}")

        print("\n[5b] Creating entity tables...")
        create_tables_from_config(livy, cfg_dict["entities"], entity_map, if_not_exists=False)

        print("\n[5c] Creating junction tables...")
        for table_name, _, cols in junction_specs:
            _create_junction_table(livy, table_name, cols)

        print("\n[5d] Loading CSVs into entity tables...")
        load_csv_data(
            livy,
            args.csv_dir,
            cfg_dict["entities"],
            entity_map,
            filename_resolver=lambda name: f"{entity_name_to_table(name)}.csv",
        )

        print("\n[5e] Loading CSVs into junction tables...")
        for table_name, ddl_table_name, cols in junction_specs:
            _load_junction_csv(livy, table_name, ddl_table_name, cols, args.csv_dir)

    # 6. Re-fetch, add bindings + contextualizations, push final definition
    print("\n[6] Re-fetching definition...")
    raw = ont.get_definition(ontology_id)
    parts = decode_definition(raw)
    print(f"    got {len(parts)} parts")

    print("\n[7] Building data bindings...")
    parts = add_all_bindings(parts, entity_map, cfg_dict["entities"],
                             config.workspace_id, config.lakehouse_id)

    print("\n[8] Building contextualizations...")
    parts = add_all_contextualizations(parts, relationship_map, entity_map,
                                       config.workspace_id, config.lakehouse_id)

    print("\n[9] Pushing final definition (bindings + contextualizations)...")
    ont.update_definition(ontology_id, encode_definition(parts))

    # 10. State
    state = {
        "ontologyId": ontology_id,
        "ontologyName": cfg_dict["name"],
        "workspaceId": config.workspace_id,
        "lakehouseId": config.lakehouse_id,
        "tables": all_tables,
    }
    args.state_out.parent.mkdir(parents=True, exist_ok=True)
    args.state_out.write_text(json.dumps(state, indent=2), encoding="utf-8")

    print("\n" + "=" * 60)
    print(f"  Setup complete. State -> {args.state_out}")
    print(f"  Next: python scripts/04_refresh_and_validate.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
