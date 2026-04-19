"""Create Lakehouse tables and load CSV seed data via the Livy API.

Exposes two entry points used by the pipeline:

- ``create_tables_from_config`` — create Delta tables from the ontology config
- ``load_csv_data`` — batch-insert rows from per-entity CSVs

Column type mapping (ontology ``valueType`` -> Spark SQL):

  ==========  ===========
  String      STRING
  DateTime    TIMESTAMP
  BigInt      BIGINT
  Double      DOUBLE
  Boolean     BOOLEAN
  Object      STRING
  ==========  ===========
"""

from __future__ import annotations

import csv as csv_mod
import re
from pathlib import Path

from .livy_api import LivyClient


_ONTOLOGY_TYPE_TO_SPARK = {
    "String": "STRING",
    "DateTime": "TIMESTAMP",
    "BigInt": "BIGINT",
    "Double": "DOUBLE",
    "Boolean": "BOOLEAN",
    "Object": "STRING",
}


def _spark_type(ontology_type: str) -> str:
    return _ONTOLOGY_TYPE_TO_SPARK.get(ontology_type, "STRING")


def entity_name_to_table(name: str) -> str:
    """Convert a PascalCase entity name to a snake_case table name."""
    s = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name)
    s = re.sub(r"(?<=[A-Z])([A-Z][a-z])", r"_\1", s)
    return s.lower().replace(" ", "_")


def _table_exists(livy: LivyClient, table_name: str) -> bool:
    result = livy.execute(
        f'print(spark.catalog.tableExists("{table_name}"))',
        kind="pyspark",
    )
    return bool(result) and "True" in result


def create_tables_from_config(
    livy: LivyClient,
    entities_config: list[dict],
    entity_map: dict,
    *,
    if_not_exists: bool = True,
) -> None:
    """Create a Delta table per entity, named per ``entity_map[name]["table"]``.

    ``entities_config`` is the ontology config's ``entities`` list.
    ``entity_map`` is what ``build_from_config`` returns.
    """
    for entity_cfg in entities_config:
        name = entity_cfg["name"]
        table = entity_map[name]["table"]
        cols = ", ".join(
            f"{p['name']} {_spark_type(p['valueType'])}"
            for p in entity_cfg["properties"]
        )
        qualifier = "IF NOT EXISTS " if if_not_exists else ""
        print(f"  Creating table {qualifier.lower()}{table}...")
        livy.sql(f"CREATE TABLE {qualifier}{table} ({cols}) USING DELTA")


def load_csv_data(
    livy: LivyClient,
    csv_dir: str | Path,
    entities_config: list[dict],
    entity_map: dict,
    *,
    batch_size: int = 200,
    filename_resolver=None,
) -> None:
    """Load per-entity CSVs into their Lakehouse tables via INSERT statements.

    ``filename_resolver(entity_name)`` returns the path to the CSV
    relative to ``csv_dir``. By default, the file is
    ``{entity_name}.csv``. Callers that use snake_case CSV names should
    pass ``filename_resolver=lambda name: f"{entity_name_to_table(name)}.csv"``.
    """
    csv_dir = Path(csv_dir)
    resolver = filename_resolver or (lambda name: f"{name}.csv")

    for entity_cfg in entities_config:
        name = entity_cfg["name"]
        table = entity_map[name]["table"]
        csv_path = csv_dir / resolver(name)

        if not csv_path.exists():
            print(f"  SKIP {name} - seed file missing: {csv_path}")
            continue

        type_map = {p["name"]: p["valueType"] for p in entity_cfg["properties"]}

        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv_mod.DictReader(f))

        if not rows:
            print(f"  SKIP {name} - empty CSV")
            continue

        # Intersect CSV columns with entity properties so we tolerate CSVs that
        # have extra columns (e.g. created_at we don't model in the ontology).
        entity_cols = [p["name"] for p in entity_cfg["properties"]]
        csv_cols = list(rows[0].keys())
        columns = [c for c in entity_cols if c in csv_cols]

        if not columns:
            print(f"  SKIP {name} - no overlapping columns between CSV and entity")
            continue

        col_list = ", ".join(columns)

        value_rows: list[str] = []
        for row in rows:
            values: list[str] = []
            for col in columns:
                raw = row.get(col, "")
                val = raw.strip() if raw else ""
                vtype = type_map.get(col, "String")
                if val == "":
                    values.append("NULL")
                elif vtype == "String":
                    values.append("'" + val.replace("'", "''") + "'")
                elif vtype in ("BigInt", "Double"):
                    values.append(val)
                elif vtype == "Boolean":
                    values.append(val.lower())
                elif vtype == "DateTime":
                    values.append(f"TIMESTAMP '{val}'")
                else:
                    values.append("'" + val.replace("'", "''") + "'")
            value_rows.append("(" + ", ".join(values) + ")")

        print(f"  Loading {len(rows)} rows into {table}...")
        for i in range(0, len(value_rows), batch_size):
            batch = value_rows[i:i + batch_size]
            sql = f"INSERT INTO {table} ({col_list}) VALUES {', '.join(batch)}"
            livy.sql(sql)


def drop_tables(livy: LivyClient, tables: list[str]) -> None:
    """Drop a list of tables. Safe to call on tables that may not exist."""
    for t in tables:
        try:
            livy.sql(f"DROP TABLE IF EXISTS {t}")
            print(f"  dropped {t}")
        except Exception as e:  # noqa: BLE001
            print(f"  WARN: could not drop {t}: {e}")
