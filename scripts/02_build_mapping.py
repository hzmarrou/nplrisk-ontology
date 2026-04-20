"""02 — Build a Fabric-ready ontology config from OWL + DDL.

Inputs:
    input/OWL/ontology.xml
    input/data/schema/ddl.sql
Outputs:
    outputs/ontology-config.json   — {name, description, tablePrefix, entities, relationships}
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from nplrisk_bench.mapping import build_ontology_config  # noqa: E402
from nplrisk_bench.owl_parser import parse_owl  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--owl", default=REPO_ROOT / "input" / "OWL" / "ontology.xml", type=Path)
    parser.add_argument("--ddl", default=REPO_ROOT / "input" / "data" / "schema" / "ddl.sql", type=Path)
    parser.add_argument("--name", default="NPL_Risk", help="Fabric ontology displayName.")
    parser.add_argument("--description", default="Non-Performing Loan risk ontology (NPLO)")
    parser.add_argument("--table-prefix", default="npl")
    parser.add_argument("--flatten", nargs="*", default=["Borrower", "Loan", "Collateral"],
                        help="OWL class names to flatten (drop subclasses).")
    parser.add_argument("--out", default=REPO_ROOT / "outputs" / "ontology-config.json", type=Path)
    parser.add_argument("--strict", action="store_true",
                        help="Fail (non-zero exit) if any OWL class or "
                             "object property cannot be mapped to the DDL. "
                             "Recommended for CI / review runs.")
    args = parser.parse_args()

    print(f"Parsing OWL from {args.owl} ...")
    parsed = parse_owl(args.owl)

    print(f"Loading DDL from {args.ddl} ...")
    cfg = build_ontology_config(
        parsed,
        args.ddl,
        display_name=args.name,
        description=args.description,
        table_prefix=args.table_prefix,
        flatten_roots=args.flatten,
        strict=args.strict,
    )

    report = cfg.pop("_mapping_report", {})

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    print(f"\nOntology: {cfg['name']}  ({len(cfg['entities'])} entities, "
          f"{len(cfg['relationships'])} relationships)")
    for e in cfg["entities"]:
        print(f"  entity: {e['name']:25s} key={e['keyProperty']:35s} properties={len(e['properties'])}")
    print()
    for r in cfg["relationships"]:
        ctx = r.get("contextTable", "")
        print(f"  rel:    {r['name']:45s} {r['source']:20s} -> {r['target']:20s} ctx={ctx}")

    if report:
        skipped = (
            len(report.get("unmapped_classes", []))
            + len(report.get("unmapped_relationships", []))
            + len(report.get("unresolved_contexts", []))
        )
        if skipped:
            print(f"\nMapping report ({skipped} skipped; rerun with --strict to fail):")
            for name in report.get("unmapped_classes", []):
                print(f"  - class not in DDL:      {name}")
            for r in report.get("unmapped_relationships", []):
                print(f"  - endpoint unmapped:     {r['name']} ({r['source']} -> {r['target']})")
            for r in report.get("unresolved_contexts", []):
                print(f"  - no context table:      {r['name']} ({r['source']} -> {r['target']})")

    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
