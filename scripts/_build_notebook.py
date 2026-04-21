"""Utility: regenerate the Fabric comparison notebook.

Run:
    python scripts/_build_notebook.py

Rewrites ``notebooks/compare_agents_fabric.ipynb`` from the cell
definitions below so the notebook stays in sync with the scenario set.

Design notes
------------
The notebook deliberately does **not** use ``fabric.dataagent.evaluation``.
During development we hit repeated issues with its Delta write path
(`CANNOT_MERGE_TYPE BooleanType vs StructType`, Arrow `BufferHolder`
errors, silently missing rows, unresolvable table names). Instead we use
``FabricOpenAI`` to chat with each agent directly. That produces the
same answers, bypasses all the Spark write gymnastics, and gives us a
reproducible, deterministic benchmark:

* Each agent is asked each question via OpenAI-compatible Assistants API
* The answer is scored with a strict token-match: every ``ontology_signals``
  token of the scenario must appear (case-insensitive substring) for the
  answer to count as correct
* Results land in a plain pandas DataFrame and a JSON file on the
  attached lakehouse; no Delta tables, no schema inference, no SDK quirks

This makes the evaluation auditable end-to-end and lets
``scripts/06_score.py`` score the same output locally without needing a
Fabric runtime.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "notebooks" / "compare_agents_fabric.ipynb"

SCENARIOS_JSON = (REPO / "scenarios" / "npl_scenarios.json").read_text(encoding="utf-8")


def md(text: str) -> dict:
    return {
        "cell_type": "markdown",
        "id": str(uuid.uuid4()),
        "metadata": {},
        "source": text.splitlines(keepends=True),
    }


def code(text: str) -> dict:
    return {
        "cell_type": "code",
        "id": str(uuid.uuid4()),
        "metadata": {},
        "outputs": [],
        "execution_count": None,
        "source": text.splitlines(keepends=True),
    }


cells: list[dict] = []

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
cells.append(md("""# NakedAgent vs OntologyAgent — NPL risk benchmark

Side-by-side evaluation of two Fabric Data Agents on 18 NPL-risk scenarios (sanity, multi-hop traversal, graph, governed metrics, ambiguity, action guardrails).

## Architecture — two different query engines

`scripts/05_setup_agents.py` wires the two agents so they each go through exactly one data surface:

| Agent | Data source | Query engine | What it "sees" |
|---|---|---|---|
| `NakedAgent` | Lakehouse Delta tables | Spark SQL | 13 raw tables with `_id` FK columns |
| `OntologyAgent` | Ontology (graph model) | GQL | 13 entity types + 17 typed relationships |

The ontology's graph model is populated by `scripts/04_refresh_and_validate.py`, which materialises nodes and edges from the Lakehouse tables via the bindings + contextualizations. Both agents see the *same underlying data*; they just traverse it through different engines.

## How the notebook works

For each scenario the notebook:

1. Sends the question to `NakedAgent` via `FabricOpenAI` and captures the final text reply
2. Does the same for `OntologyAgent`
3. Scores each answer with a deterministic token check — every `ontology_signals` token in the scenario must appear in the response (case + separator insensitive) for the answer to count as correct
4. Emits a side-by-side DataFrame and a JSON report to `Files/npl/_agent_comparison.json` on the attached lakehouse

Scoring is reproducible: the same agents + scenarios produce the same scorecard every time.

## Prerequisites

- **Default lakehouse must be attached** — left sidebar → Lakehouses → + Add → star it. The notebook writes the report under `Files/npl/` on this lakehouse.
- `NakedAgent` and `OntologyAgent` already provisioned in this workspace (`scripts/05_setup_agents.py` does this outside of Fabric).
- **The graph model must be refreshed since the last lakehouse change.** `OntologyAgent` queries the graph, not the lakehouse directly — stale graph = stale answers. If you just loaded data, run `scripts/04_refresh_and_validate.py` first or click **Refresh now** on the graph model in the Fabric UI.
- The notebook is self-contained — if `Files/npl/agent-comparison-questions.json` is not present in the lakehouse, an inline copy of the 18 scenarios is used as a fallback.
"""))

# ---------------------------------------------------------------------------
# 1. Install
# ---------------------------------------------------------------------------
cells.append(md("""## 1. Install the SDK

`Jinja2==3.1.6` is pinned because the Fabric runtime ships a newer Jinja2 that breaks the Data Agent SDK's template rendering."""))
cells.append(code("%pip install -U fabric-data-agent-sdk pandas Jinja2==3.1.6"))

# ---------------------------------------------------------------------------
# 2. Configure
# ---------------------------------------------------------------------------
cells.append(md("## 2. Configure the run"))
cells.append(code("""# -- Agent configuration ----------------------------------------------------
NAKED_AGENT_NAME = "NakedAgent"
ONTOLOGY_AGENT_NAME = "OntologyAgent"
DATA_AGENT_STAGE = "sandbox"   # use "production" after you have published the agents

# -- Output --------------------------------------------------------------------
OUTPUT_DIR = "/lakehouse/default/Files/npl"
OUTPUT_FILE = f"{OUTPUT_DIR}/_agent_comparison.json"

# -- Reliability knobs ---------------------------------------------------------
MAX_ANSWER_WAIT_SECONDS = 300     # per question, across the agent thread + run
RETRIES_PER_QUESTION = 2          # retry a failing question before giving up
"""))

# ---------------------------------------------------------------------------
# 3. Load scenarios
# ---------------------------------------------------------------------------
cells.append(md("""## 3. Load the 18-scenario NPL benchmark

The scenarios ship with the repo at `scenarios/npl_scenarios.json`. Upload a copy to `Files/npl/agent-comparison-questions.json` in the lakehouse if you want to edit between runs without touching the notebook; otherwise the inline fallback is used."""))
cells.append(code(f"""import json
from pathlib import Path

import pandas as pd

LAKEHOUSE_QUESTIONS_PATH = "/lakehouse/default/Files/npl/agent-comparison-questions.json"

# Raw JSON string so Python does not mis-parse true/false/null as identifiers.
INLINE_SCENARIOS_JSON = r\"\"\"{SCENARIOS_JSON}\"\"\"

def load_scenarios() -> list[dict]:
    path = Path(LAKEHOUSE_QUESTIONS_PATH)
    if path.exists():
        print(f"Loaded scenarios from lakehouse: {{path}}")
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    print("Lakehouse file not found; using inline fallback.")
    return json.loads(INLINE_SCENARIOS_JSON)

scenarios: list[dict] = load_scenarios()
print(f"Loaded {{len(scenarios)}} scenarios")
pd.DataFrame([
    {{"scenario_id": s["scenario_id"], "domain": s["domain"], "question": s["user_question"]}}
    for s in scenarios
])
"""))

# ---------------------------------------------------------------------------
# 4. Agent wrapper
# ---------------------------------------------------------------------------
cells.append(md("""## 4. Agent wrapper

`ask_agent(agent_name, question)` creates a short-lived thread against the agent, posts the question, waits for the run to complete, and returns the agent's final text reply. It wraps each call in a timeout plus a retry so a single flaky question does not kill the loop."""))
cells.append(code("""import time
from fabric.dataagent.client import FabricOpenAI


def _make_client(agent_name: str) -> "FabricOpenAI":
    \"\"\"Construct a FabricOpenAI client across known SDK signatures.

    Newer SDK versions dropped the ``data_agent_stage`` constructor kwarg.
    We try both forms so the notebook works against either.
    \"\"\"
    try:
        return FabricOpenAI(artifact_name=agent_name, data_agent_stage=DATA_AGENT_STAGE)
    except TypeError:
        return FabricOpenAI(artifact_name=agent_name)


_ACTIVE_RUN_STATES = {"queued", "in_progress", "requires_action", "cancelling"}


def _call_once(agent_name: str, question: str, max_wait: int) -> str:
    \"\"\"Ask one question; enforce an explicit wall-clock deadline.

    ``create_and_poll`` (SDK helper) has its own internal poll loop and
    ignores our ``max_wait``. A stuck question would block the whole
    benchmark. Poll manually so we can cancel and return a ``<timeout>``
    marker when the deadline is hit.
    \"\"\"
    client = _make_client(agent_name)
    assistant = client.beta.assistants.create(model="not-used")
    thread = client.beta.threads.create()
    client.beta.threads.messages.create(
        thread_id=thread.id, role="user", content=question
    )
    run = client.beta.threads.runs.create(
        thread_id=thread.id, assistant_id=assistant.id
    )

    deadline = time.time() + max_wait
    while run.status in _ACTIVE_RUN_STATES:
        if time.time() >= deadline:
            try:
                client.beta.threads.runs.cancel(thread_id=thread.id, run_id=run.id)
            except Exception:
                pass
            return f"<timeout after {max_wait}s>"
        time.sleep(2)
        run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)

    if run.status != "completed":
        return f"<run {run.status}>"

    # Return only the LATEST assistant message. The Fabric SDK does not
    # always hand back a pristine thread for each ``threads.create()``
    # call, so earlier answers can linger; picking by max(created_at) is
    # robust even when the list order is undefined.
    msgs = client.beta.threads.messages.list(thread_id=thread.id)
    assistant_msgs = [m for m in msgs.data if m.role == "assistant"]
    if not assistant_msgs:
        return "<empty>"
    latest = max(assistant_msgs, key=lambda m: getattr(m, "created_at", 0))
    pieces = [c.text.value for c in latest.content if c.type == "text"]
    return "\\n".join(pieces).strip() or "<empty>"


# Exceptions that will fail the same way every time — no point retrying.
_NON_RETRYABLE = (TypeError, ImportError, AttributeError)


def ask_agent(agent_name: str, question: str,
              max_wait: int = MAX_ANSWER_WAIT_SECONDS,
              retries: int = RETRIES_PER_QUESTION) -> str:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return _call_once(agent_name, question, max_wait)
        except _NON_RETRYABLE as exc:
            return f"<error: {exc}>"
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(5 * attempt)
    return f"<error: {last_exc}>"
"""))

# ---------------------------------------------------------------------------
# 5. Scoring helper
# ---------------------------------------------------------------------------
cells.append(md("""## 5. Scoring helper

An answer is marked correct when every token in the scenario's `ontology_signals` list appears in the answer as a case-insensitive substring. Empty signal lists evaluate to `None` (treated as "no lexical verdict" — the downstream scorecard uses the critic / ambiguity / numeric-gold dimensions for those scenarios; forcing a False here would double-penalise the OntologyAgent)."""))
cells.append(code("""import re


def _normalize(s: str) -> str:
    \"\"\"Fold separators + whitespace so 'write_off_flag' matches 'write-off flag'.\"\"\"
    s = (s or "").lower()
    # underscores / hyphens / slashes -> space
    s = re.sub(r"[_\\-/]+", " ", s)
    # collapse runs of whitespace
    s = re.sub(r"\\s+", " ", s)
    return s.strip()


def score_answer(answer: str, signals: list[str]) -> dict:
    \"\"\"Token-match verdict for one agent answer.

    Semantics:
      * ``signals`` non-empty -> ``correct`` is True iff every token
        appears in the answer (case + separator insensitive); False
        otherwise.
      * ``signals`` empty     -> ``correct`` is None (N/A); no lexical
        verdict is possible. The downstream scorecard
        (scripts/06_score.py) is the authoritative judge for those
        scenarios (ambiguity / guardrail), so marking them False here
        would double-penalise the OntologyAgent.
    \"\"\"
    if not signals:
        return {"correct": None, "matched": [], "missing": [], "signal_count": 0}
    answer_norm = _normalize(answer)
    matched: list[str] = []
    missing: list[str] = []
    for s in signals:
        if _normalize(s) in answer_norm:
            matched.append(s)
        else:
            missing.append(s)
    return {
        "correct": len(missing) == 0,
        "matched": matched,
        "missing": missing,
        "signal_count": len(signals),
    }
"""))

# ---------------------------------------------------------------------------
# 6. Run the benchmark
# ---------------------------------------------------------------------------
cells.append(md("""## 6. Run the benchmark

Sends all 18 questions to each agent and scores the answers. Expect ~3 minutes per agent on F16 capacity. If a question fails past the retry budget the cell records the error in `actual_answer_<agent>` and treats it as incorrect — the loop never aborts early."""))
cells.append(code("""from datetime import datetime

per_question: list[dict] = []
for i, scenario in enumerate(scenarios, 1):
    qid = scenario["scenario_id"]
    question = scenario["user_question"]
    signals = scenario.get("ontology_signals", [])
    print(f"[{i}/{len(scenarios)}] {qid} — {question[:70]}")

    naked_answer = ask_agent(NAKED_AGENT_NAME, question)
    ontology_answer = ask_agent(ONTOLOGY_AGENT_NAME, question)

    naked_score = score_answer(naked_answer, signals)
    ontology_score = score_answer(ontology_answer, signals)

    per_question.append({
        "scenario_id": qid,
        "domain": scenario.get("domain", ""),
        "question": question,
        "expected_answer": scenario.get("gold_label", ""),
        "ontology_signals": signals,

        "actual_answer_naked": naked_answer,
        "evaluation_judgement_naked": naked_score["correct"],
        "matched_signals_naked": naked_score["matched"],
        "missing_signals_naked": naked_score["missing"],

        "actual_answer_ontology": ontology_answer,
        "evaluation_judgement_ontology": ontology_score["correct"],
        "matched_signals_ontology": ontology_score["matched"],
        "missing_signals_ontology": ontology_score["missing"],
    })

df_results = pd.DataFrame(per_question)


def _count_verdicts(col: str) -> tuple[int, int, int]:
    \"\"\"Return (correct, scored, na) for the verdict column.

    ``scored`` excludes rows where the verdict is None (no signals);
    those are reported separately as N/A. A True verdict counts as
    correct.
    \"\"\"
    vals = list(df_results[col])
    correct = sum(1 for v in vals if v is True)
    na = sum(1 for v in vals if v is None)
    scored = len(vals) - na
    return correct, scored, na


naked_correct, naked_scored, naked_na = _count_verdicts("evaluation_judgement_naked")
onto_correct, onto_scored, onto_na = _count_verdicts("evaluation_judgement_ontology")

print(f"\\nCompleted {len(df_results)} scenarios.")
print(
    f"NakedAgent correct:    {naked_correct}/{naked_scored} "
    f"(+{naked_na} N/A)"
)
print(
    f"OntologyAgent correct: {onto_correct}/{onto_scored} "
    f"(+{onto_na} N/A)"
)
"""))

# ---------------------------------------------------------------------------
# 7. Side-by-side view
# ---------------------------------------------------------------------------
cells.append(md("""## 7. Side-by-side view"""))
cells.append(code("""display_cols = [
    "scenario_id", "domain", "question",
    "evaluation_judgement_naked",
    "evaluation_judgement_ontology",
    "actual_answer_naked",
    "actual_answer_ontology",
]
df_results[display_cols]
"""))

# ---------------------------------------------------------------------------
# 8. Summary
# ---------------------------------------------------------------------------
cells.append(md("""## 8. Summary"""))
cells.append(code("""def _summary(df: pd.DataFrame, suffix: str) -> dict:
    col = f"evaluation_judgement_{suffix}"
    vals = list(df[col])
    correct = sum(1 for v in vals if v is True)
    na = sum(1 for v in vals if v is None)
    scored = len(vals) - na
    return {
        "correctCount": correct,
        "scoredQuestions": scored,
        "naQuestions": na,
        "totalQuestions": len(vals),
        "accuracyPct": round(100 * correct / scored, 1) if scored else 0.0,
    }

naked_summary = _summary(df_results, "naked")
ontology_summary = _summary(df_results, "ontology")

pd.DataFrame({
    "NakedAgent": naked_summary,
    "OntologyAgent": ontology_summary,
})
"""))

# ---------------------------------------------------------------------------
# 9. Save JSON report
# ---------------------------------------------------------------------------
cells.append(md("""## 9. Save the JSON report

Produces `Files/npl/_agent_comparison.json` on the attached lakehouse. Download it to your local `nplrisk-ontology/outputs/_agent_comparison.json` and run `python scripts/06_score.py` to render the markdown scorecard."""))
cells.append(code("""import os
import hashlib

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Canonical-form hash of the scenarios payload so the local scorer can
# detect drift against a mutated local scenarios file.
def _canonical_sha(payload: list[dict]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

scenarios_sha256 = _canonical_sha(scenarios)

report = {
    "runAtUtc": datetime.utcnow().isoformat() + "Z",
    "stage": DATA_AGENT_STAGE,
    "scoringMethod": "ontology_signals token match (all tokens must appear)",
    "scenariosSha256": scenarios_sha256,
    "scenariosPayload": scenarios,
    "agents": {
        "naked": {"name": NAKED_AGENT_NAME, **naked_summary},
        "ontology": {"name": ONTOLOGY_AGENT_NAME, **ontology_summary},
    },
    "perQuestion": per_question,
}

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2, default=str)

print(f"Saved: {OUTPUT_FILE}")
print(f"Rows:  {len(report['perQuestion'])}")
print(f"Scenarios sha256: {scenarios_sha256}")
print(f"Naked  {naked_summary['correctCount']}/{naked_summary['scoredQuestions']} ({naked_summary['accuracyPct']}%)")
print(f"Ontology {ontology_summary['correctCount']}/{ontology_summary['scoredQuestions']} ({ontology_summary['accuracyPct']}%)")
"""))

# ---------------------------------------------------------------------------
# 10. What to look for
# ---------------------------------------------------------------------------
cells.append(md("""## 10. What to look for

Because `OntologyAgent` now speaks GQL against the graph and `NakedAgent` speaks SQL against Delta tables, they are two genuinely different query engines. Expect the deltas to reflect that:

**Where OntologyAgent should win**

- **Multi-hop traversals** — Q04 (loan/borrower fanout), Q06 (property-backed valuations), Q08 (enforcement + practitioner), Q09 (group rollup), Q10 (cross-country collateral). GQL expresses multi-hop edge traversal naturally; SQL has to chain joins and tends to drop a side.
- **Governed metrics with a semantic trap** — Q13 (NPE ratio), Q14 (EAD), Q15 (IFRS stage 3). A schema-only agent commonly picks the wrong balance column (`principal_balance` vs `balance_at_default`) or lumps `ifrs_stage_3_impaired` with `other_impaired`.
- **Negation / anti-joins** — Q12 (loans with no collateral). Graph `MATCH NOT` patterns are clearer than SQL `LEFT JOIN ... IS NULL`.
- **Ambiguity & guardrails** — Q16 (bad loans), Q17 (exposure), Q18 (foreclose). The ontology agent has richer semantic context to flag multiple valid definitions; the schema-only agent usually picks one silently.

**Where NakedAgent may win or tie**

- **Sanity questions (Q01–Q03)** — single-table SQL aggregations are trivial. OntologyAgent should tie here; if it loses, the GQL group-by workaround in the instructions is worth checking.
- **Heavy numeric aggregation** — GQL aggregations are a documented weak spot in Fabric ontology; the agent instructions include the "Support group by in GQL" nudge from the Fabric tutorial to mitigate, but edge cases persist.

**Operational reminders**

- If `OntologyAgent` returns counts that don't match `NakedAgent`'s, first check whether the graph was refreshed since the last lakehouse write. The graph is not live-bound.
- Both agents answer from the *same data*, so a gap from knowing which column to use is a genuine ontology win; a gap from the engine's query capability is a platform artefact, not a semantic win.

Once you are happy with the results, download `Files/npl/_agent_comparison.json` to your local `nplrisk-ontology/outputs/` folder and run `python scripts/06_score.py` for the markdown scorecard.
"""))


notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Synapse PySpark", "language": "Python", "name": "synapse_pyspark"},
        "language_info": {"name": "python"},
        "microsoft": {
            "language": "python",
            "language_group": "synapse_pyspark",
            "ms_spell_check": {"ms_spell_check_language": "en"},
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(notebook, indent=2), encoding="utf-8", newline="\n")
print(f"Wrote {OUT}  ({len(cells)} cells)")
