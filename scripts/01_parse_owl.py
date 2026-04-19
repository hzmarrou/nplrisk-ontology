"""01 — Parse the NPLO OWL file into a neutral ParsedOntology JSON.

Inputs:
    input/OWL/ontology.xml
Outputs:
    outputs/parsed_ontology.json
    outputs/ontology_summary.txt
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from nplrisk_bench.owl_parser import parse_owl  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--owl", default=REPO_ROOT / "input" / "OWL" / "ontology.xml",
                        type=Path, help="Path to the OWL/RDF-XML file.")
    parser.add_argument("--out", default=REPO_ROOT / "outputs" / "parsed_ontology.json",
                        type=Path, help="Where to write the parsed JSON.")
    parser.add_argument("--summary", default=REPO_ROOT / "outputs" / "ontology_summary.txt",
                        type=Path, help="Where to write the human-readable summary.")
    args = parser.parse_args()

    print(f"Parsing {args.owl} ...")
    ontology = parse_owl(args.owl)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    ontology.to_json(args.out)
    args.summary.write_text(ontology.summary, encoding="utf-8")

    print(ontology.summary)
    print()
    print(f"Wrote {args.out}")
    print(f"Wrote {args.summary}")


if __name__ == "__main__":
    main()
