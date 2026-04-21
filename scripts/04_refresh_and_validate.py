"""04 — Refresh the graph model and run GQL competency queries.

Inputs:
    outputs/_state.json               (ontologyName from 03_setup.py)
    gql-queries/*.gql                 (competency queries)
Outputs:
    outputs/_validation.json          (pass/fail per query + raw result rows)

Notes:
    API refresh (jobs/instances?jobType=RefreshGraph) is the preferred path.
    Fabric occasionally auto-cancels overlapping refresh jobs in under a
    second. If the first refresh returns Cancelled, the script retries
    once after a short pause before falling back to a manual UI prompt.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from nplrisk_bench.fabric_client import FabricConfig  # noqa: E402
from nplrisk_bench.fabric_client.graph_api import GraphClient  # noqa: E402


_GRAPH_SUFFIX_RE = re.compile(r"^_graph_[0-9a-f]+$", re.IGNORECASE)


def _find_graph_for_ontology(gc: GraphClient, ontology_name: str) -> dict | None:
    """Resolve the graph model that belongs to ``ontology_name``.

    Fabric auto-provisions the graph with one of two display-name
    shapes:

      * exact match: ``NPL_Risk``
      * suffixed:    ``NPL_Risk_graph_<32-hex-id>``

    Accept both; reject anything else (avoids matching
    ``NPL_Risk_Other`` or similar unrelated items). Multiple matches
    still fail hard and require ``--graph-id``.
    """
    matches = []
    for g in gc.list_graph_models():
        name = g.get("displayName", "")
        if name == ontology_name:
            matches.append(g)
            continue
        if name.startswith(ontology_name):
            tail = name[len(ontology_name):]
            if _GRAPH_SUFFIX_RE.match(tail):
                matches.append(g)
    if len(matches) > 1:
        raise RuntimeError(
            f"Multiple graph models matching {ontology_name!r}: "
            f"{[(g.get('displayName'), g['id']) for g in matches]}. "
            f"Pass --graph-id to pick one."
        )
    return matches[0] if matches else None


def _try_refresh(gc: GraphClient, graph_id: str, attempts: int = 2, pause: int = 60) -> dict:
    last_result: dict = {}
    for i in range(1, attempts + 1):
        print(f"  refresh attempt {i}/{attempts}...")
        try:
            last_result = gc.refresh(graph_id, wait=True, poll_interval=20)
        except RuntimeError as exc:
            print(f"    refresh error: {exc}")
            last_result = {"status": "Failed", "failureReason": str(exc)}
        status = last_result.get("status", "Unknown")
        print(f"  refresh status: {status}")
        if status == "Completed":
            return last_result
        if status == "Cancelled" and i < attempts:
            print(f"  waiting {pause}s before retrying (Fabric may have auto-cancelled overlapping jobs)...")
            time.sleep(pause)
    return last_result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", default=REPO_ROOT / "outputs" / "_state.json", type=Path)
    parser.add_argument("--gql-dir", default=REPO_ROOT / "gql-queries", type=Path)
    parser.add_argument("--out", default=REPO_ROOT / "outputs" / "_validation.json", type=Path)
    parser.add_argument("--skip-refresh", action="store_true",
                        help="Skip the API refresh. Use when you've just refreshed via the UI.")
    parser.add_argument("--graph-id", default=None, help="Override graph model ID.")
    args = parser.parse_args()

    config = FabricConfig.from_env()
    state = json.loads(args.state.read_text(encoding="utf-8"))
    gc = GraphClient(config)

    graph_id = args.graph_id
    if not graph_id:
        gm = _find_graph_for_ontology(gc, state["ontologyName"])
        if not gm:
            print("No graph model matching the ontology name. Available models:")
            for g in gc.list_graph_models():
                print(f"  - {g.get('displayName','?')} ({g['id']})")
            sys.exit(1)
        graph_id = gm["id"]
        print(f"Graph model: {gm['displayName']}  ({graph_id})")

    if not args.skip_refresh:
        print("\nTriggering graph refresh...")
        result = _try_refresh(gc, graph_id)
        if result.get("status") != "Completed":
            print("\nAPI refresh did not complete. Fall back to the Fabric UI:")
            print("  1. Open the graph model in the workspace")
            print("  2. Click 'Refresh now'")
            print("  3. Rerun this script with --skip-refresh once it finishes")
            sys.exit(2)

    # Run GQL files
    gql_files = sorted(args.gql_dir.glob("*.gql"))
    if not gql_files:
        print(f"No .gql files in {args.gql_dir}")
        sys.exit(0)

    print(f"\nExecuting {len(gql_files)} GQL queries...\n")
    per_query: list[dict] = []
    passed = failed = 0

    for gql_file in gql_files:
        content = gql_file.read_text(encoding="utf-8")
        header_lines, body_lines = [], []
        for line in content.strip().split("\n"):
            if line.strip().startswith("//"):
                header_lines.append(line.strip().lstrip("/ ").strip())
            else:
                body_lines.append(line)
        query = "\n".join(body_lines).strip()

        print(f"--- {gql_file.name} ---")
        for h in header_lines:
            print(f"  {h}")
        print()
        record: dict = {"file": gql_file.name, "headers": header_lines, "query": query}
        try:
            result = gc.execute_query(graph_id, query)
            status = result.get("status", {})
            if status.get("code") == "00000":
                data = result.get("result", {})
                columns = data.get("columns", [])
                rows = data.get("data", [])
                col_names = [c["name"] if isinstance(c, dict) else str(c) for c in columns]
                if col_names:
                    header = " | ".join(col_names)
                    print(f"  {header}")
                    print(f"  {'-' * len(header)}")
                for row in rows[:10]:
                    vals = [str(row.get(c, "")) for c in col_names] if isinstance(row, dict) else [str(v) for v in row]
                    print(f"  {' | '.join(vals)}")
                print(f"  ({len(rows)} rows)")
                record.update({"status": "passed", "row_count": len(rows), "columns": col_names})
                passed += 1
            else:
                desc = status.get("description", "unknown error")
                print(f"  ERROR: {desc}")
                record.update({"status": "failed", "error": desc})
                failed += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  EXCEPTION: {exc}")
            record.update({"status": "failed", "error": str(exc)})
            failed += 1
        per_query.append(record)
        print()

    out = {
        "ontologyName": state["ontologyName"],
        "graphId": graph_id,
        "passed": passed,
        "failed": failed,
        "total": len(gql_files),
        "queries": per_query,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")

    print(f"Results: {passed} passed, {failed} failed out of {len(gql_files)}  -> {args.out}")


if __name__ == "__main__":
    main()
