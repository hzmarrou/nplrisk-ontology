"""Multi-dimensional scorer for agent responses.

Each response is scored against a ``GoldenAnswer`` along up to six
dimensions:

1. ``metric_correct`` — the agent picked the right governed metric
2. ``tables_correct`` — every required table is present in the answer
3. ``relationships_correct`` — every required relationship is referenced
4. ``ambiguity_detected`` — if the scenario is ambiguous, did the agent flag it?
5. ``guardrail_respected`` — did the agent recommend rather than claim to act?
6. ``signals_correct`` — (fallback) ontology-signal tokens are present

Dimensions are only counted against the max_score when the golden answer
declares an expectation for that dimension, so single-signal sanity
questions and full multi-hop scenarios can share the same scoring code.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .scenarios import GoldenAnswer, Scenario


@dataclass
class AgentResponse:
    """The result of asking a question to one of the two data agents."""

    scenario_id: str
    agent_type: str  # "naked" | "ontology"
    answer: str = ""
    reasoning: str = ""
    sql_or_gql: str = ""
    metric_selected: str = ""
    tables_used: list[str] = field(default_factory=list)
    relationships_used: list[str] = field(default_factory=list)
    ambiguity_flagged: bool = False
    action_policy: str = "execute"
    error: str | None = None


@dataclass
class ScoreResult:
    """Score for a single (scenario, agent) pair."""

    scenario_id: str
    agent_type: str
    metric_correct: bool = False
    tables_correct: bool = False
    relationships_correct: bool = False
    ambiguity_detected: bool = False
    guardrail_respected: bool = False
    signals_correct: bool = False
    total_score: int = 0
    max_score: int = 0
    notes: str = ""


def score_signals(answer: str, signals: list[str]) -> tuple[bool, list[str], list[str]]:
    """Return (all_matched, matched, missing) for a must-mention token list."""
    if not signals:
        return True, [], []
    lower = answer.lower()
    matched = [s for s in signals if s.lower() in lower]
    missing = [s for s in signals if s.lower() not in lower]
    return len(missing) == 0, matched, missing


def _extract_critic_verdict(response: AgentResponse) -> str | None:
    """Parse ``__critic_verdict__=yes/no/unclear`` out of response.reasoning.

    Callers that know the upstream critic's judgement (e.g. ``06_score.py``
    reading a notebook JSON) can stash it in ``response.reasoning`` so the
    scorer prefers it over heuristic extraction.
    """
    text = response.reasoning or ""
    marker = "__critic_verdict__="
    if marker not in text:
        return None
    line = text.split(marker, 1)[1].splitlines()[0].strip().lower()
    return line if line in {"yes", "no", "unclear"} else None


def score_response(response: AgentResponse, golden: GoldenAnswer) -> ScoreResult:
    """Score one ``AgentResponse`` against its ``GoldenAnswer``.

    The critic verdict (when available) is ONE independent dimension, not
    a full override. Every dimension the golden answer declares an
    expectation for is scored and contributes 1 point to ``max_score`` /
    ``total_score``.
    """
    result = ScoreResult(scenario_id=response.scenario_id, agent_type=response.agent_type)
    notes: list[str] = []

    if response.error:
        notes.append(f"error: {response.error}")

    # 0. Critic verdict as one of N dimensions
    verdict = _extract_critic_verdict(response)
    if verdict is not None:
        result.max_score += 1
        if verdict == "yes":
            result.total_score += 1
            notes.append("critic: yes")
        elif verdict == "unclear":
            notes.append("critic: unclear")
        else:
            notes.append("critic: no")

    # 1. Metric selection
    if golden.gold_label and golden.gold_label != "graph_traversal":
        result.max_score += 1
        if response.metric_selected == golden.gold_label:
            result.metric_correct = True
            result.total_score += 1
        else:
            notes.append(
                f"metric: expected {golden.gold_label!r} got {response.metric_selected!r}"
            )

    # 2. Tables used
    if golden.required_scope_tables:
        result.max_score += 1
        required = set(golden.required_scope_tables)
        used = set(response.tables_used)
        if required.issubset(used):
            result.tables_correct = True
            result.total_score += 1
        else:
            missing = required - used
            notes.append(f"tables missing: {sorted(missing)}")

    # 3. Relationships used (graph scenarios)
    if golden.required_relationships:
        result.max_score += 1
        required_rels = set(golden.required_relationships)
        used_rels = set(response.relationships_used)
        if required_rels.issubset(used_rels):
            result.relationships_correct = True
            result.total_score += 1
        else:
            missing = required_rels - used_rels
            notes.append(f"relationships missing: {sorted(missing)}")

    # 4. Ambiguity detection
    if golden.ambiguity_expected:
        result.max_score += 1
        if response.ambiguity_flagged:
            result.ambiguity_detected = True
            result.total_score += 1
        else:
            notes.append("ambiguity not detected")

    # 5. Action guardrail
    if golden.action_policy == "recommend_only":
        result.max_score += 1
        if response.action_policy == "recommend_only":
            result.guardrail_respected = True
            result.total_score += 1
        else:
            notes.append(f"guardrail violated: {response.action_policy}")

    # 6. Signal-token lexical coverage — independent dimension, always
    # scored when the golden answer declares signals. This is LEXICAL
    # COVERAGE only (did the agent mention the expected terms), not
    # numeric correctness.
    if golden.ontology_signals:
        ok, _matched, missing = score_signals(response.answer, golden.ontology_signals)
        result.max_score += 1
        if ok:
            result.signals_correct = True
            result.total_score += 1
        else:
            notes.append(f"signals missing: {missing}")

    result.notes = "; ".join(notes) if notes else "all correct"
    return result


def score_all(
    responses: list[AgentResponse],
    golden_answers: dict[str, GoldenAnswer],
) -> list[ScoreResult]:
    return [
        score_response(r, golden_answers[r.scenario_id])
        for r in responses
        if r.scenario_id in golden_answers
    ]


# -- Heuristic extraction from a raw agent answer ----------------------------

_TABLE_NAME_PATTERN = re.compile(r"\b([a-z_]+_(?:id|event|link|group|agent|collateral|borrower|loan|rating_agency|practitioner|receiver|provider))\b")
_FROM_PATTERN = re.compile(r"\bfrom\s+([a-z0-9_.]+)", re.IGNORECASE)
_JOIN_PATTERN = re.compile(r"\bjoin\s+([a-z0-9_.]+)", re.IGNORECASE)
_GQL_REL_PATTERN = re.compile(r"\[\s*:`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s*\]")

_AMBIGUITY_HINTS = (
    "ambigu", "clarif", "unclear", "could mean", "two interpretations",
    "which definition", "please specify", "depends on which",
)
_RECOMMEND_HINTS = (
    "recommend", "you may want to", "consider ", "suggest",
    "the option", "the options", "decision is yours",
)
_EXECUTE_HINTS = ("i will ", "i've ", "i have ", "executed", "initiated", "started the")


def infer_response_metadata(
    response: AgentResponse,
    known_tables: list[str] | None = None,
    known_relationships: list[str] | None = None,
) -> AgentResponse:
    """Fill in ``tables_used``, ``relationships_used``, ``ambiguity_flagged``
    and ``action_policy`` on an ``AgentResponse`` by scanning its text.

    This is deliberately lightweight: the full corpus of reasoning text
    (``answer + reasoning + sql_or_gql``) is searched for table and
    relationship names. Callers can pass the canonical lists from the
    ontology config to anchor the extraction.
    """
    text = "\n".join(x for x in (response.answer, response.reasoning, response.sql_or_gql) if x)
    lower = text.lower()

    if not response.tables_used:
        used: set[str] = set()
        for m in _FROM_PATTERN.finditer(text):
            used.add(m.group(1).split(".")[-1])
        for m in _JOIN_PATTERN.finditer(text):
            used.add(m.group(1).split(".")[-1])
        if known_tables:
            for t in known_tables:
                if re.search(rf"\b{re.escape(t)}\b", text, re.IGNORECASE):
                    used.add(t)
        response.tables_used = sorted(used)

    if not response.relationships_used:
        rels: set[str] = set()
        for m in _GQL_REL_PATTERN.finditer(text):
            rels.add(m.group(1))
        if known_relationships:
            for r in known_relationships:
                if re.search(rf"\b{re.escape(r)}\b", text):
                    rels.add(r)
        response.relationships_used = sorted(rels)

    if not response.ambiguity_flagged:
        response.ambiguity_flagged = any(h in lower for h in _AMBIGUITY_HINTS)

    if response.action_policy == "execute":
        if any(h in lower for h in _EXECUTE_HINTS):
            response.action_policy = "execute"
        elif any(h in lower for h in _RECOMMEND_HINTS):
            response.action_policy = "recommend_only"

    return response


# -- Scorecard rendering -----------------------------------------------------

def generate_scorecard(
    naked_results: list[ScoreResult],
    ontology_results: list[ScoreResult],
    scenarios: list[Scenario] | None = None,
) -> str:
    """Render a markdown scorecard comparing the two agents."""
    naked_by = {r.scenario_id: r for r in naked_results}
    onto_by = {r.scenario_id: r for r in ontology_results}
    scen_by = {s.scenario_id: s for s in (scenarios or [])}
    ids = sorted(set(naked_by) | set(onto_by))

    lines = [
        "# NakedAgent vs OntologyAgent — scorecard",
        "",
        "| Scenario | Domain | Naked | Ontology | Winner | Notes |",
        "|----------|--------|-------|----------|--------|-------|",
    ]

    n_tot = n_max = o_tot = o_max = 0
    for sid in ids:
        n = naked_by.get(sid)
        o = onto_by.get(sid)
        dom = scen_by[sid].domain if sid in scen_by else ""
        n_str = f"{n.total_score}/{n.max_score}" if n else "-"
        o_str = f"{o.total_score}/{o.max_score}" if o else "-"
        if n and o:
            n_tot += n.total_score; n_max += n.max_score
            o_tot += o.total_score; o_max += o.max_score
            if n.total_score > o.total_score:
                winner = "Naked"
            elif o.total_score > n.total_score:
                winner = "Ontology"
            else:
                winner = "tie"
        else:
            winner = "-"
        note_pieces = []
        if n and n.notes != "all correct":
            note_pieces.append(f"N: {n.notes}")
        if o and o.notes != "all correct":
            note_pieces.append(f"O: {o.notes}")
        note = " | ".join(note_pieces)
        lines.append(f"| {sid} | {dom} | {n_str} | {o_str} | {winner} | {note} |")

    def pct(num: int, denom: int) -> str:
        return f"{round(100 * num / denom)}%" if denom else "-"

    lines.extend([
        "",
        "## Summary",
        "",
        "| Agent | Score | Max | Accuracy |",
        "|-------|-------|-----|----------|",
        f"| Naked | {n_tot} | {n_max} | {pct(n_tot, n_max)} |",
        f"| Ontology | {o_tot} | {o_max} | {pct(o_tot, o_max)} |",
    ])
    return "\n".join(lines)
