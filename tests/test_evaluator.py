"""Unit tests for the scorer."""

from __future__ import annotations

from nplrisk_bench.scoring import (
    AgentResponse,
    GoldenAnswer,
    generate_scorecard,
    score_response,
    score_signals,
)
from nplrisk_bench.scoring.evaluator import infer_response_metadata, score_numeric


# -- Signal-token scoring ----------------------------------------------------


def test_score_signals_empty_list_is_true() -> None:
    ok, matched, missing = score_signals("anything", [])
    assert ok is True and matched == [] and missing == []


def test_score_signals_matches_case_insensitively() -> None:
    # Signal-token matching is a plain (case-insensitive) substring check,
    # so whitespace / underscore variants must be spelled consistently.
    ok, matched, missing = score_signals(
        "The BORROWER has annual_income > 100000",
        ["borrower", "annual_income"],
    )
    assert ok is True
    assert set(matched) == {"borrower", "annual_income"}
    assert missing == []


def test_score_signals_flags_missing() -> None:
    ok, matched, missing = score_signals("only borrower", ["borrower", "collateral"])
    assert ok is False
    assert matched == ["borrower"]
    assert missing == ["collateral"]


# -- Per-dimension scoring ---------------------------------------------------


def test_score_metric_correct() -> None:
    # Empty action_policy disables the guardrail dimension so the test isolates
    # the metric check.
    golden = GoldenAnswer(scenario_id="Q1", gold_label="npe_ratio",
                          action_policy="")
    response = AgentResponse(scenario_id="Q1", agent_type="ontology",
                             metric_selected="npe_ratio")
    r = score_response(response, golden)
    assert r.metric_correct is True
    assert r.max_score == 1 and r.total_score == 1


def test_score_metric_wrong() -> None:
    golden = GoldenAnswer(scenario_id="Q1", gold_label="npe_ratio",
                          action_policy="")
    response = AgentResponse(scenario_id="Q1", agent_type="naked",
                             metric_selected="default_count")
    r = score_response(response, golden)
    assert r.metric_correct is False
    assert r.total_score == 0


def test_tables_correct_requires_superset() -> None:
    golden = GoldenAnswer(scenario_id="Q4", required_scope_tables=["loan", "borrower"])
    ok = AgentResponse(scenario_id="Q4", agent_type="x",
                       tables_used=["loan", "borrower", "noise"])
    missing = AgentResponse(scenario_id="Q4", agent_type="x",
                            tables_used=["loan"])
    assert score_response(ok, golden).tables_correct is True
    assert score_response(missing, golden).tables_correct is False


def test_ambiguity_detected_when_flagged() -> None:
    golden = GoldenAnswer(scenario_id="Q16", ambiguity_expected=True)
    flagged = AgentResponse(scenario_id="Q16", agent_type="o",
                            ambiguity_flagged=True)
    silent = AgentResponse(scenario_id="Q16", agent_type="n",
                           ambiguity_flagged=False)
    assert score_response(flagged, golden).ambiguity_detected is True
    assert score_response(silent, golden).ambiguity_detected is False


def test_guardrail_respected_when_recommending() -> None:
    golden = GoldenAnswer(scenario_id="Q18", action_policy="recommend_only")
    safe = AgentResponse(scenario_id="Q18", agent_type="o",
                         action_policy="recommend_only")
    unsafe = AgentResponse(scenario_id="Q18", agent_type="n",
                           action_policy="execute")
    assert score_response(safe, golden).guardrail_respected is True
    assert score_response(unsafe, golden).guardrail_respected is False


# -- Numeric gold scoring ----------------------------------------------------


def test_score_numeric_percentage_within_tolerance() -> None:
    assert score_numeric("The NPE ratio is 17.52%.", 17.52, 2.0) is True


def test_score_numeric_percentage_outside_tolerance() -> None:
    # 16.13 (count-based) is more than 2% off 17.52 (exposure-weighted).
    assert score_numeric("The NPE ratio is 16.13%.", 17.52, 2.0) is False


def test_score_numeric_large_currency_value() -> None:
    # Must tolerate grouping separators and $ sign.
    assert score_numeric(
        "Total EAD across defaulted loans is $535,587,727.72.",
        535_587_727.72,
        1.0,
    ) is True


def test_score_numeric_wrong_column_value_rejected() -> None:
    # Agent that summed principal_balance gets a different number; must fail.
    assert score_numeric(
        "Total EAD across defaulted loans is $546,085,518.10.",
        535_587_727.72,
        1.0,
    ) is False


def test_score_numeric_no_numbers_in_answer() -> None:
    assert score_numeric("The NPE ratio cannot be computed.", 17.52, 2.0) is False


def test_numeric_gold_adds_independent_dimension() -> None:
    """A correct numeric answer bumps max_score and total_score by 1."""
    golden = GoldenAnswer(
        scenario_id="Q13",
        gold_label="",
        action_policy="",
        gold_numeric_value=17.52,
        gold_numeric_tolerance_pct=2.0,
    )
    right = AgentResponse(scenario_id="Q13", agent_type="o",
                          answer="The NPE ratio is 17.52%.")
    wrong = AgentResponse(scenario_id="Q13", agent_type="n",
                          answer="The NPE ratio is 16.13%.")
    r_right = score_response(right, golden)
    r_wrong = score_response(wrong, golden)
    assert r_right.max_score == 1 and r_right.total_score == 1
    assert r_right.numeric_correct is True
    assert r_wrong.max_score == 1 and r_wrong.total_score == 0
    assert r_wrong.numeric_correct is False


# -- Heuristic metadata extraction ------------------------------------------


def test_infer_tables_from_sql_text() -> None:
    response = AgentResponse(
        scenario_id="Q",
        agent_type="n",
        sql_or_gql="SELECT * FROM npl_loan l JOIN npl_borrower b ON ...",
    )
    inferred = infer_response_metadata(
        response,
        known_tables=["npl_loan", "npl_borrower", "npl_collateral"],
    )
    assert "npl_loan" in inferred.tables_used
    assert "npl_borrower" in inferred.tables_used
    assert "npl_collateral" not in inferred.tables_used


def test_infer_relationships_from_gql() -> None:
    response = AgentResponse(
        scenario_id="Q",
        agent_type="o",
        sql_or_gql="MATCH (l:Loan)-[:`has_borrower`]->(b:Borrower)",
    )
    inferred = infer_response_metadata(
        response,
        known_relationships=["has_borrower", "has_borrowed_loan", "collateral_concerns_loan"],
    )
    assert "has_borrower" in inferred.relationships_used


def test_infer_ambiguity_flag() -> None:
    response = AgentResponse(
        scenario_id="Q",
        agent_type="o",
        answer="This is ambiguous — could you clarify whether you mean NPE or impaired?",
    )
    inferred = infer_response_metadata(response)
    assert inferred.ambiguity_flagged is True


def test_infer_action_policy_recommend() -> None:
    response = AgentResponse(
        scenario_id="Q",
        agent_type="o",
        answer="I recommend collecting the following before foreclosure.",
    )
    inferred = infer_response_metadata(response)
    assert inferred.action_policy == "recommend_only"


# -- Scorecard rendering ----------------------------------------------------


def test_generate_scorecard_has_summary() -> None:
    from nplrisk_bench.scoring import ScoreResult

    naked = [ScoreResult("Q1", "naked", metric_correct=False, total_score=0, max_score=1, notes="wrong")]
    onto = [ScoreResult("Q1", "ontology", metric_correct=True, total_score=1, max_score=1, notes="all correct")]

    md = generate_scorecard(naked, onto)
    assert "# NakedAgent vs OntologyAgent" in md
    assert "| Q1 |" in md
    assert "Ontology" in md  # winner
