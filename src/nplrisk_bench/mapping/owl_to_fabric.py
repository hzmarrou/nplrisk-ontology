"""Bridge a ``ParsedOntology`` and a physical DDL into a Fabric-ready
ontology config dict.

The output has the same shape consumed by
``fabric_client.definition_builder.build_from_config``:

.. code:: python

    {
        "name": "NPL_Risk",
        "description": "...",
        "tablePrefix": "npl",
        "entities": [
            {"name": "Borrower", "keyProperty": "borrower_id",
             "properties": [{"name": "...", "valueType": "..."}, ...]},
            ...
        ],
        "relationships": [
            {"name": "has_borrowed_loan", "source": "Borrower",
             "target": "Loan", "contextEntity": "LoanBorrowerLink",
             "contextTable": "npl_loan_borrower_link",
             "sourceKeyColumns": "borrower_id",
             "targetKeyColumns": "loan_id"},
            ...
        ],
    }

The mapping strategy is deliberate:

- OWL classes drive the entity names (faithful to the ontology)
- DDL drives the property list, primary keys, and foreign keys (so the
  Fabric side matches the physical tables the CSVs will populate)
- OWL object properties drive relationship names and source/target
  semantics; DDL FKs supply the context table and column names
- Foreign keys in the DDL that have no matching OWL object property are
  optionally exposed as auto-generated relationships (``auto_fks=True``)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from ..owl_parser.model import DatatypeProperty, ObjectProperty, OwlClass, ParsedOntology

# -- SQL type -> Fabric valueType --------------------------------------------

_SQL_TYPE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^\s*(bigint|integer|int|smallint|serial|bigserial)\b", re.I), "BigInt"),
    (re.compile(r"^\s*(numeric|decimal|real|float|double)\b", re.I), "Double"),
    (re.compile(r"^\s*(timestamptz|timestamp|datetime|date)\b", re.I), "DateTime"),
    (re.compile(r"^\s*(boolean|bool)\b", re.I), "Boolean"),
]


def sql_type_to_fabric(sql_type: str) -> str:
    for pat, fabric in _SQL_TYPE_PATTERNS:
        if pat.search(sql_type):
            return fabric
    return "String"


# -- DDL parser (very small, PostgreSQL-flavoured) ---------------------------

@dataclass
class DDLColumn:
    name: str
    sql_type: str
    nullable: bool = True
    is_primary_key: bool = False
    references_table: str | None = None
    references_column: str | None = None

    @property
    def fabric_value_type(self) -> str:
        return sql_type_to_fabric(self.sql_type)


@dataclass
class DDLTable:
    name: str
    columns: list[DDLColumn] = field(default_factory=list)
    composite_primary_key: list[str] = field(default_factory=list)

    def column(self, name: str) -> DDLColumn | None:
        return next((c for c in self.columns if c.name == name), None)

    @property
    def primary_key_columns(self) -> list[str]:
        if self.composite_primary_key:
            return list(self.composite_primary_key)
        return [c.name for c in self.columns if c.is_primary_key]

    @property
    def foreign_keys(self) -> list[DDLColumn]:
        return [c for c in self.columns if c.references_table]


def load_ddl_tables(ddl_path: Path) -> dict[str, DDLTable]:
    """Parse a subset of PostgreSQL DDL into ``DDLTable`` records.

    Handles ``CREATE TABLE [IF NOT EXISTS] name ( ... )`` with inline
    ``PRIMARY KEY``, ``REFERENCES table(col)``, table-level composite
    ``PRIMARY KEY (a, b, ...)``, and multi-line column definitions. Ignores
    ``CHECK``, ``UNIQUE`` (except inline REFERENCES), indexes, and comments.
    """
    text = ddl_path.read_text(encoding="utf-8")
    # Strip line comments
    text = re.sub(r"--[^\n]*", "", text)

    tables: dict[str, DDLTable] = {}

    table_pattern = re.compile(
        r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+(\w+)\s*\((.*?)\);",
        re.IGNORECASE | re.DOTALL,
    )

    for m in table_pattern.finditer(text):
        tname = m.group(1)
        body = m.group(2)
        table = DDLTable(name=tname)

        # Split body into top-level clauses by commas *outside* parentheses
        depth = 0
        buf: list[str] = []
        clauses: list[str] = []
        for ch in body:
            if ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth -= 1
                buf.append(ch)
            elif ch == "," and depth == 0:
                clauses.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)
        if buf:
            clauses.append("".join(buf).strip())

        for clause in clauses:
            if not clause:
                continue
            upper = clause.upper()

            # Table-level composite PK
            cpk = re.match(r"PRIMARY\s+KEY\s*\(([^)]+)\)", clause, re.IGNORECASE)
            if cpk:
                cols = [c.strip() for c in cpk.group(1).split(",")]
                table.composite_primary_key = cols
                continue

            # Skip pure CHECK / UNIQUE / CONSTRAINT clauses (no column being defined)
            if upper.startswith(("CHECK", "UNIQUE", "CONSTRAINT", "FOREIGN KEY")):
                continue

            # Column definition: first token is the name, second onwards the type + modifiers
            col_match = re.match(r"(\w+)\s+([^,]+)", clause)
            if not col_match:
                continue
            col_name = col_match.group(1)
            rest = col_match.group(2).strip()

            # Grab the type up to the first recognized modifier / keyword
            type_match = re.match(
                r"((?:NUMERIC|DECIMAL)\s*\([^)]*\)|\w+(?:\s*\([^)]*\))?)",
                rest,
                re.IGNORECASE,
            )
            sql_type = type_match.group(1) if type_match else rest.split()[0]

            # PK / nullable / REFERENCES
            is_pk = bool(re.search(r"\bPRIMARY\s+KEY\b", rest, re.IGNORECASE))
            nullable = not re.search(r"\bNOT\s+NULL\b", rest, re.IGNORECASE)
            ref = re.search(r"REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)", rest, re.IGNORECASE)

            table.columns.append(DDLColumn(
                name=col_name,
                sql_type=sql_type.strip(),
                nullable=nullable,
                is_primary_key=is_pk,
                references_table=ref.group(1) if ref else None,
                references_column=ref.group(2) if ref else None,
            ))

        tables[tname] = table

    return tables


# -- Class <-> table name resolution -----------------------------------------

def _pascal_to_snake(name: str) -> str:
    s = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name)
    s = re.sub(r"(?<=[A-Z])([A-Z][a-z])", r"_\1", s)
    return s.lower()


def _class_to_table_candidates(class_name: str) -> list[str]:
    """Candidate DDL table names for a given OWL class name.

    Tries the snake_case identity, a ``_event`` suffix (common in NPLO for
    Forbearance/Enforcement/ExternalCollection classes), and a few
    plural variants.
    """
    base = _pascal_to_snake(class_name)
    return [base, f"{base}_event", base + "s", base.replace("external_collection", "external_collection")]


def _match_class_to_table(
    class_name: str,
    tables: dict[str, DDLTable],
    overrides: dict[str, str] | None = None,
) -> str | None:
    if overrides and class_name in overrides:
        return overrides[class_name]
    for cand in _class_to_table_candidates(class_name):
        if cand in tables:
            return cand
    return None


# -- Main entry point --------------------------------------------------------

DEFAULT_CLASS_TABLE_OVERRIDES: dict[str, str] = {
    # NPL-specific: these OWL classes map to *_event tables
    "Forbearance": "forbearance_event",
    "Enforcement": "enforcement_event",
    "ExternalCollection": "external_collection_event",
    # PropertyCollateral already snake_case-matches property_collateral
}

# OWL classes we deliberately skip (abstract or subsumed by subclasses)
DEFAULT_SKIP_CLASSES: set[str] = {"Counterparty"}

# OWL object properties whose range is a meta/enum type (no entity)
_META_RANGE_PROPERTIES: set[str] = {
    "collateral_has_type_of_identifier",
    "enforcement_has_type_of_identifier",
    "forbearance_has_type_of_identifier",
}


def build_ontology_config(
    parsed: ParsedOntology,
    ddl_path: Path,
    *,
    display_name: str = "NPL_Risk",
    description: str = "Non-Performing Loan risk ontology (NPLO)",
    table_prefix: str = "npl",
    flatten_roots: list[str] | None = None,
    class_table_overrides: dict[str, str] | None = None,
    skip_classes: set[str] | None = None,
    auto_fk_relationships: bool = True,
) -> dict:
    """Produce the Fabric-ready ontology config dict.

    Parameters
    ----------
    parsed:
        Output of ``parse_owl()``.
    ddl_path:
        Path to the ``ddl.sql`` that describes the physical tables.
    display_name:
        Fabric ontology display name.
    table_prefix:
        Prefix applied to every Lakehouse table name.
    flatten_roots:
        Pass a list of root class names (e.g. ``["Borrower", "Loan"]``)
        to collapse their subclass hierarchies before mapping.
    auto_fk_relationships:
        When ``True``, emit relationships for FKs that don't have a matching
        OWL object property. Relationship names are derived from
        ``{source_table}_to_{target_class}``.
    """
    class_overrides = {**DEFAULT_CLASS_TABLE_OVERRIDES, **(class_table_overrides or {})}
    skip = DEFAULT_SKIP_CLASSES | (skip_classes or set())

    source = parsed.flatten_hierarchy(flatten_roots) if flatten_roots else parsed

    tables = load_ddl_tables(ddl_path)

    # -- 1. Map OWL classes -> DDL tables ----------------------------------
    class_to_table: dict[str, str] = {}
    for cls in source.classes:
        if cls.name in skip:
            continue
        table = _match_class_to_table(cls.name, tables, class_overrides)
        if table:
            class_to_table[cls.name] = table

    # Reverse index for FK lookup
    table_to_class: dict[str, str] = {t: c for c, t in class_to_table.items()}

    # -- 2. Build entities --------------------------------------------------
    entities: list[dict] = []
    for cls_name, table_name in class_to_table.items():
        table = tables[table_name]
        pk_cols = table.primary_key_columns
        if not pk_cols:
            raise ValueError(f"Table '{table_name}' has no primary key; cannot map class '{cls_name}'.")

        # OWL datatype properties (with inherited), keyed by property name
        owl_props: dict[str, DatatypeProperty] = {}
        for p in source.all_datatype_properties_for_class(cls_name):
            owl_props.setdefault(p.name, p)

        # Physical columns win (they're what actually exists in the lakehouse).
        # Skip audit columns that aren't part of the semantic model.
        skip_cols = {"created_at", "updated_at", "linked_at"}
        properties: list[dict] = []
        for col in table.columns:
            if col.name in skip_cols:
                continue
            properties.append({
                "name": col.name,
                "valueType": col.fabric_value_type,
            })

        entity = {
            "name": cls_name,
            "keyProperty": pk_cols[0] if len(pk_cols) == 1 else pk_cols,
            "properties": properties,
        }
        entities.append(entity)

    # -- 3. Build relationships from OWL object properties -----------------
    relationships: list[dict] = []
    seen_names: set[str] = set()
    seen_edges: set[tuple[str, str, str, str, str]] = set()

    def emit(name: str, source_cls: str, target_cls: str,
             context_table: str, source_col: str, target_col: str,
             context_entity: str | None = None) -> None:
        edge = (source_cls, target_cls, context_table, source_col, target_col)
        if name in seen_names or edge in seen_edges:
            return
        seen_names.add(name)
        seen_edges.add(edge)
        rel: dict = {
            "name": name,
            "source": source_cls,
            "target": target_cls,
            "contextTable": f"{table_prefix}_{context_table}",
            "sourceKeyColumns": source_col,
            "targetKeyColumns": target_col,
        }
        if context_entity:
            rel["contextEntity"] = context_entity
        relationships.append(rel)

    for op in source.object_properties:
        if op.name in _META_RANGE_PROPERTIES:
            continue
        source_cls = op.domain_name
        target_cls = op.range_name
        if source_cls not in class_to_table or target_cls not in class_to_table:
            continue

        ctx_table, source_col, target_col = _resolve_context(
            op, source_cls, target_cls, class_to_table, tables, table_to_class,
        )
        if ctx_table is None:
            # Leave unresolved rels out rather than emitting something broken.
            continue

        emit(
            op.name,
            source_cls,
            target_cls,
            ctx_table,
            source_col,
            target_col,
        )

    # -- 4. Auto-generate relationships for unmatched FKs ------------------
    if auto_fk_relationships:
        for ctx_table_name, table in tables.items():
            # Skip junction tables (no single PK) and tables we don't map
            if not table.primary_key_columns:
                continue
            is_mapped = ctx_table_name in table_to_class

            for fk in table.foreign_keys:
                target_table = fk.references_table
                if target_table not in table_to_class:
                    continue  # FK to an unmapped table, skip
                target_cls = table_to_class[target_table]

                if is_mapped:
                    source_cls = table_to_class[ctx_table_name]
                    source_col = table.primary_key_columns[0]
                    target_col = fk.name
                    rel_name = f"{_pascal_to_snake(source_cls)}_references_{_pascal_to_snake(target_cls)}"
                else:
                    # Junction table: emit an edge between two mapped entities
                    mapped_fks = [c for c in table.foreign_keys if c.references_table in table_to_class]
                    others = [c for c in mapped_fks if c.name != fk.name]
                    if not others:
                        continue
                    other = others[0]
                    source_cls = table_to_class[other.references_table]  # arbitrary pick
                    target_cls_local = table_to_class[fk.references_table]
                    # Only emit once per junction-edge name:
                    pair_key = tuple(sorted([source_cls, target_cls_local]))
                    rel_name = f"{_pascal_to_snake(source_cls)}_links_{_pascal_to_snake(target_cls_local)}"
                    if rel_name in seen_names:
                        continue
                    source_col = other.name
                    target_col = fk.name
                    target_cls = target_cls_local

                # Skip if already covered by an OWL-declared rel
                if rel_name in seen_names:
                    continue

                emit(
                    rel_name,
                    source_cls,
                    target_cls,
                    ctx_table_name,
                    source_col,
                    target_col,
                )

    return {
        "name": display_name,
        "description": description,
        "tablePrefix": table_prefix,
        "entities": entities,
        "relationships": relationships,
    }


def _resolve_context(
    op: ObjectProperty,
    source_cls: str,
    target_cls: str,
    class_to_table: dict[str, str],
    tables: dict[str, DDLTable],
    table_to_class: dict[str, str],
) -> tuple[str | None, str, str]:
    """Pick the context table + source/target columns for an OWL object property.

    Preference order:

    1. The source entity's own table has an FK to the target's table.
    2. The target entity's own table has an FK to the source's table.
    3. A junction table exists whose FKs go to both source and target.
    """
    src_table_name = class_to_table[source_cls]
    tgt_table_name = class_to_table[target_cls]
    src_table = tables[src_table_name]
    tgt_table = tables[tgt_table_name]

    src_pk = src_table.primary_key_columns[0] if src_table.primary_key_columns else None
    tgt_pk = tgt_table.primary_key_columns[0] if tgt_table.primary_key_columns else None

    # 1) FK in source table
    for fk in src_table.foreign_keys:
        if fk.references_table == tgt_table_name and src_pk:
            return src_table_name, src_pk, fk.name

    # 2) FK in target table
    for fk in tgt_table.foreign_keys:
        if fk.references_table == src_table_name and tgt_pk:
            return tgt_table_name, fk.name, tgt_pk

    # 3) Junction table with FKs to both
    for jname, jtable in tables.items():
        src_fk = next(
            (c for c in jtable.foreign_keys if c.references_table == src_table_name),
            None,
        )
        tgt_fk = next(
            (c for c in jtable.foreign_keys if c.references_table == tgt_table_name),
            None,
        )
        if src_fk and tgt_fk:
            return jname, src_fk.name, tgt_fk.name

    return None, "", ""
