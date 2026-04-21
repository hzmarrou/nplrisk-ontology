"""06 — Score the NakedAgent vs OntologyAgent comparison.

Inputs:
    outputs/_agent_comparison.json    (produced by the Fabric notebook)
    scenarios/npl_scenarios.json      (golden scenarios)
    outputs/ontology-config.json      (known tables + relationships for extraction)
Outputs:
    outputs/scorecard.md
    outputs/scorecard.json

Input JSON shape:
    {
      "runAtUtc": "...",
      "stage": "sandbox",
      "agents": {
        "naked": {"name": "NakedAgent", "evaluationId": "...", "summary": [...]},
        "ontology": {"name": "OntologyAgent", "evaluationId": "...", "summary": [...]}
      },
      "perQuestion": [
        {"question": "...", "expected_answer": "...",
         "actual_answer_naked": "...", "evaluation_result_naked": "true|false|unclear",
         "actual_answer_ontology": "...", "evaluation_result_ontology": "..."},
        ...
      ]
    }

The scorer maps each perQuestion row back to a Scenario by question text,
extracts tables + relationships + ambiguity + action-policy heuristically,
and produces both a JSON result and a markdown scorecard.
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
    # Keep the raw verdict in a hidden marker so score_response can prefer it
    # over the heuristic pipeline when it is present.
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--comparison", default=REPO_ROOT / "outputs" / "_agent_comparison.json", type=Path)
    parser.add_argument("--scenarios", default=REPO_ROOT / "scenarios" / "npl_scenarios.json", type=Path)
    parser.add_argument("--config", default=REPO_ROOT / "outputs" / "ontology-config.json", type=Path)
    parser.add_argument("--md-out", default=REPO_ROOT / "outputs" / "scorecard.md", type=Path)
    parser.add_argument("--json-out", default=REPO_ROOT / "outputs" / "scorecard.json", type=Path)
    args = parser.parse_args()

    scenarios = load_scenarios(args.scenarios)
    by_question = {s.user_question: s for s in scenarios}
    golden_answers = golden_answers_from_scenarios(scenarios)

    cfg = json.loads(args.config.read_text(encoding="utf-8")) if args.config.exists() else {}
    # Use the canonical tableName recorded on each entity. Recomputing from
    # the entity name would silently miswire any class whose DDL table does
    # not share its snake_case name (e.g. Enforcement -> enforcement_event).
    known_tables = [
        e["tableName"] for e in cfg.get("entities", []) if e.get("tableName")
    ]
    known_relationships = [r["name"] for r in cfg.get("relationships", [])]

    comparison = json.loads(args.comparison.read_text(encoding="utf-8"))
    per_question = comparison.get("perQuestion", [])

    naked_responses: list[AgentResponse] = []
    ontology_responses: list[AgentResponse] = []

    for row in per_question:
        q = row.get("question", "")
        scenario = by_question.get(q)
        if not scenario:
            continue
        naked_responses.append(
            infer_response_metadata(
                _build_response(scenario.scenario_id, "naked", row),
                known_tables=known_tables,
                known_relationships=known_relationships,
            )
        )
        ontology_responses.append(
            infer_response_metadata(
                _build_response(scenario.scenario_id, "ontology", row),
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

    # Version-lock the scorecard to the exact scenarios payload that
    # produced it. Re-running the scorer against a different scenarios
    # file changes the sha256; downstream consumers can detect drift.
    scenarios_bytes = args.scenarios.read_bytes()
    scenarios_sha256 = hashlib.sha256(scenarios_bytes).hexdigest()
    scenarios_payload = json.loads(scenarios_bytes.decode("utf-8"))

    args.json_out.write_text(json.dumps({
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "scenariosPath": str(args.scenarios),
        "scenariosSha256": scenarios_sha256,
        "scenariosPayload": scenarios_payload,
        "naked": _dump(naked_scores),
        "ontology": _dump(onto_scores),
    }, indent=2), encoding="utf-8")

    print(md)
    print()
    print(f"Wrote {args.md_out}")
    print(f"Wrote {args.json_out}")


if __name__ == "__main__":
    main()
