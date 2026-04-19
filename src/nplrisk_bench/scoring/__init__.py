"""Scenario-based scoring for the NakedAgent vs OntologyAgent benchmark."""

from .evaluator import (
    AgentResponse,
    ScoreResult,
    generate_scorecard,
    score_all,
    score_response,
    score_signals,
)
from .scenarios import (
    GoldenAnswer,
    Scenario,
    golden_answers_from_scenarios,
    load_golden_answers,
    load_scenarios,
)

__all__ = [
    "AgentResponse",
    "GoldenAnswer",
    "Scenario",
    "ScoreResult",
    "generate_scorecard",
    "golden_answers_from_scenarios",
    "load_golden_answers",
    "load_scenarios",
    "score_all",
    "score_response",
    "score_signals",
]
