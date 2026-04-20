"""Scenario + golden-answer dataclasses and loaders.

A *scenario* is one benchmark question. Every scenario carries both the
natural-language ``user_question`` sent to the agent and the machine-
checkable expectations the scorer validates against.

The schema supports:

- Metric-resolution scenarios (e.g. "what is the NPE ratio?")
- Graph-traversal scenarios (multi-hop through relationships)
- Ambiguity / guardrail scenarios (e.g. "should we foreclose?")
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Scenario:
    """A benchmark question with its expected structural properties."""

    scenario_id: str
    domain: str
    user_question: str
    required_scope_tables: list[str] = field(default_factory=list)
    gold_label: str = ""
    explanation: str = ""
    action_policy: str = "recommend_only"
    ambiguity_expected: bool = False
    required_relationships: list[str] = field(default_factory=list)
    expected_join_hops: int = 0
    naked_agent_trap: str = ""
    ontology_signals: list[str] = field(default_factory=list)
    # Deterministic numeric gold: the correct numeric answer computed from
    # the seed data. The scorer extracts numbers from the agent's response
    # and marks the scenario correct if any of them is within
    # ``gold_numeric_tolerance_pct`` percent of this value.
    gold_numeric_value: float | None = None
    gold_numeric_tolerance_pct: float = 1.0
    gold_numeric_description: str = ""


@dataclass
class GoldenAnswer:
    """Expected answer for a scenario, indexed by scenario_id."""

    scenario_id: str
    gold_label: str = ""
    ambiguity_expected: bool = False
    action_policy: str = "recommend_only"
    required_scope_tables: list[str] = field(default_factory=list)
    required_relationships: list[str] = field(default_factory=list)
    ontology_signals: list[str] = field(default_factory=list)
    gold_numeric_value: float | None = None
    gold_numeric_tolerance_pct: float = 1.0


def load_scenarios(path: Path) -> list[Scenario]:
    """Load a list of scenarios from a JSON file."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [
        Scenario(**{k: v for k, v in s.items() if k in Scenario.__dataclass_fields__})
        for s in raw
    ]


def load_golden_answers(path: Path) -> dict[str, GoldenAnswer]:
    """Load golden answers as a dict indexed by ``scenario_id``."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    answers = {}
    for g in raw:
        ga = GoldenAnswer(**{k: v for k, v in g.items() if k in GoldenAnswer.__dataclass_fields__})
        answers[ga.scenario_id] = ga
    return answers


def golden_answers_from_scenarios(scenarios: list[Scenario]) -> dict[str, GoldenAnswer]:
    """Derive a GoldenAnswer from each Scenario when no separate file is kept.

    Most scenarios carry their golden values inline (``gold_label``,
    ``required_scope_tables``, etc.), so a separate ``*_golden_answers.json``
    file is optional. Use this when the scenario file is authoritative.
    """
    return {
        s.scenario_id: GoldenAnswer(
            scenario_id=s.scenario_id,
            gold_label=s.gold_label,
            ambiguity_expected=s.ambiguity_expected,
            action_policy=s.action_policy,
            required_scope_tables=list(s.required_scope_tables),
            required_relationships=list(s.required_relationships),
            ontology_signals=list(s.ontology_signals),
            gold_numeric_value=s.gold_numeric_value,
            gold_numeric_tolerance_pct=s.gold_numeric_tolerance_pct,
        )
        for s in scenarios
    }
