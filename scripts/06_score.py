"""06 — Score the NakedAgent vs OntologyAgent comparison.

Inputs:
    outputs/_agent_comparison.json    (produced by the Fabric notebook)
    outputs/ontology-config.json      (known tables + relationships for extraction)
    scenarios/npl_scenarios.json      (optional override — see scenario locking)
Outputs:
    outputs/scorecard.md
    outputs/scorecard.json

Scenario locking
----------------
Starting with this version, the comparison JSON produced by the notebook
is the source of truth for which questions were asked. That file MUST
include two fields:

    "scenariosSha256": "<hex>"
    "scenariosPayload": [ {scenario_id: ...}, ... ]

The scorer reads ``scenariosPayload`` directly by default and ignores any
local ``scenarios/npl_scenarios.json``. This prevents an operator from
silently re-scoring a notebook run against a mutated local scenario
file. If you want to supply a local override (e.g. to try a scoring
rewrite against a historical run), pass ``--scenarios-from local`` AND
either:

  * ensure the local file's sha256 matches ``scenariosSha256``, or
  * pass ``--override-scenario-hash`` to accept the divergence
    deliberately (a warning is printed and the mismatch is recorded
    under ``scenarioHashOverride`` in scorecard.json).

perQuestion rows are joined back to scenarios by ``scenario_id``. Missing
or duplicate IDs fail hard; joining by question text (the old behaviour)
would silently drop rows if a question was reworded between the notebook
run and the local scenarios file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from nplrisk_bench.scoring import (  # noqa: E402
    AgentResponse,
    generate_scorecard,
    golden_answers_from_scenarios,
    load_scenarios,
    score_all,
)
from nplrisk_bench.scoring.scenarios import Scenario  # noqa: E402
from nplrisk_bench.scoring.evaluator import infer_response_metadata  # noqa: E402


_POSITIVE_VERDICTS = {"yes", "true", "1"}
_NEGATIVE_VERDICTS = {"no", "false", "0"}


def _verdict_of(row: dict, suffix: str) -> str:
    """Return 'yes' / 'no' / 'unclear' / '' based on the critic's judgement.

    Python ``False`` is a legitimate negative verdict but evaluates falsy,
    so we check for ``None`` explicitly rather than using ``... or ""``.
    """
    for col in (f"evaluation_judgement_{suffix}",
                f"evaluation_result_{suffix}",
                f"evaluation_status_{suffix}"):
        raw = row.get(col)
        if raw is None:
            continue
        if isinstance(raw, bool):
            return "yes" if raw else "no"
        s = str(raw).strip().lower()
        if not s:
            continue
        if s in _POSITIVE_VERDICTS:
            return "yes"
        if s in _NEGATIVE_VERDICTS:
            return "no"
        if s == "unclear":
            return "unclear"
    return ""


def _build_response(sid: str, agent_type: str, row: dict) -> AgentResponse:
    suffix = agent_type  # "naked" or "ontology"
    ans = row.get(f"actual_answer_{suffix}", "") or ""
    resp = AgentResponse(
        scenario_id=sid,
        agent_type=agent_type,
        answer=ans,
        reasoning=ans,
        sql_or_gql=ans,
        error=None,
    )
    resp.reasoning = f"__critic_verdict__={_verdict_of(row, suffix)}\n{ans}"
    return resp


def _scenarios_from_payload(payload: list[dict]) -> list[Scenario]:
    return [
        Scenario(**{k: v for k, v in s.items() if k in Scenario.__dataclass_fields__})
        for s in payload
    ]


def _canonical_sha256(payload: list[dict]) -> str:
    """Hash the scenarios payload the same way both sides compute it.

    The comparison JSON's ``scenariosSha256`` is the sha256 of the raw
    bytes of whatever scenarios file the notebook (or 06_score.py) read.
    To let a local-override caller check equivalence even after a
    pretty-print, we also compute a canonical-JSON hash and compare
    against that as a fallback.
    """
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _index_rows_by_scenario_id(per_question: list[dict]) -> dict[str, dict]:
    """Validate the per-question list and index it by scenario_id.

    Fails hard if any row is missing ``scenario_id`` or if the set has
    duplicates — those are the failure modes the reviewer (F03) called
    out as the reason to abandon question-text joins.
    """
    by_sid: dict[str, dict] = {}
    missing_idx: list[int] = []
    duplicates: list[str] = []
    for i, row in enumerate(per_question):
        sid = row.get("scenario_id")
        if not sid:
            missing_idx.append(i)
            continue
        if sid in by_sid:
            duplicates.append(sid)
            continue
        by_sid[sid] = row
    if missing_idx:
        raise RuntimeError(
            f"perQuestion rows missing scenario_id at indices {missing_idx}. "
            "Upgrade the notebook (compare_agents_fabric.ipynb) to emit a "
            "scenario_id on every row."
        )
    if duplicates:
        raise RuntimeError(f"duplicate scenario_id(s) in perQuestion: {duplicates}")
    return by_sid


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--comparison", default=REPO_ROOT / "outputs" / "_agent_comparison.json", type=Path)
    parser.add_argument("--scenarios", default=REPO_ROOT / "scenarios" / "npl_scenarios.json", type=Path,
                        help="Local scenarios file; consulted only when "
                             "--scenarios-from local is passed.")
    parser.add_argument("--scenarios-from", choices=("comparison", "local"),
                        default="comparison",
                        help="Which scenarios payload to score against. "
                             "'comparison' (default) reads the scenariosPayload "
                             "embedded in the comparison JSON. 'local' reads "
                             "--scenarios and verifies its sha against "
                             "scenariosSha256.")
    parser.add_argument("--override-scenario-hash", action="store_true",
                        help="Allow --scenarios-from local to proceed even "
                             "when the local file's sha256 does not match "
                             "scenariosSha256 in the comparison JSON.")
    parser.add_argument("--config", default=REPO_ROOT / "outputs" / "ontology-config.json", type=Path)
    parser.add_argument("--md-out", default=REPO_ROOT / "outputs" / "scorecard.md", type=Path)
    parser.add_argument("--json-out", default=REPO_ROOT / "outputs" / "scorecard.json", type=Path)
    args = parser.parse_args()

    comparison = json.loads(args.comparison.read_text(encoding="utf-8"))
    per_question = comparison.get("perQuestion", [])
    embedded_payload = comparison.get("scenariosPayload")
    embedded_sha = comparison.get("scenariosSha256")

    hash_override_note: str | None = None

    if args.scenarios_from == "comparison":
        if not embedded_payload:
            raise RuntimeError(
                f"{args.comparison} has no scenariosPayload — this file was "
                f"produced by a pre-R04 notebook. Rerun with --scenarios-from "
                f"local --override-scenario-hash to score anyway."
            )
        scenarios_payload = embedded_payload
        scenarios_sha256 = embedded_sha or _canonical_sha256(embedded_payload)
        source_description = f"embedded in {args.comparison}"
    else:
        if not args.scenarios.exists():
            raise RuntimeError(f"--scenarios file not found: {args.scenarios}")
        scenarios_bytes = args.scenarios.read_bytes()
        scenarios_payload = json.loads(scenarios_bytes.decode("utf-8"))
        local_sha = hashlib.sha256(scenarios_bytes).hexdigest()
        local_canonical_sha = _canonical_sha256(scenarios_payload)
        matches = embedded_sha in (None, local_sha, local_canonical_sha)
        if not matches:
            if not args.override_scenario_hash:
                raise RuntimeError(
                    f"Local scenarios sha256 ({local_sha}) does not match "
                    f"scenariosSha256 in the comparison JSON ({embedded_sha}). "
                    f"Pass --override-scenario-hash if this divergence is "
                    f"intentional."
                )
            hash_override_note = (
                f"local sha {local_sha} != embedded sha {embedded_sha}; "
                f"--override-scenario-hash accepted"
            )
            print(f"WARNING: {hash_override_note}")
        scenarios_sha256 = local_sha
        source_description = str(args.scenarios)

    scenarios = _scenarios_from_payload(scenarios_payload)
    if not scenarios:
        raise RuntimeError("resolved scenarios list is empty")
    by_sid = {s.scenario_id: s for s in scenarios}
    if len(by_sid) != len(scenarios):
        seen: dict[str, int] = {}
        for s in scenarios:
            seen[s.scenario_id] = seen.get(s.scenario_id, 0) + 1
        dups = [sid for sid, n in seen.items() if n > 1]
        raise RuntimeError(f"duplicate scenario_id(s) in scenarios payload: {dups}")
    golden_answers = golden_answers_from_scenarios(scenarios)

    rows_by_sid = _index_rows_by_scenario_id(per_question)

    unknown_ids = sorted(set(rows_by_sid) - set(by_sid))
    if unknown_ids:
        raise RuntimeError(
            f"perQuestion contains scenario_id(s) not in the scenarios "
            f"payload: {unknown_ids}"
        )

    cfg = json.loads(args.config.read_text(encoding="utf-8")) if args.config.exists() else {}
    known_tables = [
        e["tableName"] for e in cfg.get("entities", []) if e.get("tableName")
    ]
    known_relationships = [r["name"] for r in cfg.get("relationships", [])]

    naked_responses: list[AgentResponse] = []
    ontology_responses: list[AgentResponse] = []

    for sid, scenario in by_sid.items():
        row = rows_by_sid.get(sid)
        if row is None:
            print(f"  (no agent answer for {sid} in comparison JSON — skipping)")
            continue
        naked_responses.append(
            infer_response_metadata(
                _build_response(sid, "naked", row),
                known_tables=known_tables,
                known_relationships=known_relationships,
            )
        )
        ontology_responses.append(
            infer_response_metadata(
                _build_response(sid, "ontology", row),
                known_tables=known_tables,
                known_relationships=known_relationships,
            )
        )

    naked_scores = score_all(naked_responses, golden_answers)
    onto_scores = score_all(ontology_responses, golden_answers)

    md = generate_scorecard(naked_scores, onto_scores, scenarios)
    args.md_out.parent.mkdir(parents=True, exist_ok=True)
    args.md_out.write_text(md, encoding="utf-8")

    def _dump(results):
        return [
            {k: getattr(r, k) for k in ("scenario_id", "agent_type", "metric_correct",
                                          "tables_correct", "relationships_correct",
                                          "ambiguity_detected", "guardrail_respected",
                                          "signals_correct", "numeric_correct",
                                          "total_score", "max_score", "notes")}
            for r in results
        ]

    output = {
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "scenariosSource": source_description,
        "scenariosSha256": scenarios_sha256,
        "scenariosPayload": scenarios_payload,
        "naked": _dump(naked_scores),
        "ontology": _dump(onto_scores),
    }
    if hash_override_note:
        output["scenarioHashOverride"] = hash_override_note
    args.json_out.write_text(json.dumps(output, indent=2), encoding="utf-8")

    print(md)
    print()
    print(f"Scenarios:  {source_description}  (sha256 {scenarios_sha256[:12]}...)")
    print(f"Wrote {args.md_out}")
    print(f"Wrote {args.json_out}")


if __name__ == "__main__":
    main()
