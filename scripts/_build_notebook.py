"""Utility: regenerate the Fabric comparison notebook.

Standalone so the notebook cell structure lives next to the rest of the
project and can be rebuilt reproducibly. Run:

    python scripts/_build_notebook.py

This writes ``notebooks/compare_agents_fabric.ipynb`` from the cell
definitions below. It is not part of the main pipeline.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "notebooks" / "compare_agents_fabric.ipynb"


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


SCENARIOS_JSON = (REPO / "scenarios" / "npl_scenarios.json").read_text(encoding="utf-8")


cells = []

cells.append(md("""# NakedAgent vs OntologyAgent — NPL risk benchmark

Run this notebook **inside Fabric in user context** (not via service principal). The AISkill `/assistants` chat endpoint used by Data Agents does not accept SP tokens. Agent provisioning is done outside Fabric by `scripts/05_setup_agents.py`; agent evaluation runs here.

**Before running:**

1. Attach `NPLLakehouse` (or the lakehouse referenced in `.env`) to this notebook as the **default lakehouse**. The evaluation SDK fails with `Missing required Fabric context parameters` without it.
2. Upload `scenarios/npl_scenarios.json` into the lakehouse at `Files/npl/agent-comparison-questions.json`. If that file is missing the notebook falls back to an inline copy.

**What the notebook does:**

- Loads the 18 NPL scenarios
- Calls `evaluate_data_agent` for `NakedAgent` and `OntologyAgent`
- Shows summary + per-question detail DataFrames
- Merges a side-by-side table and saves a JSON report to `Files/npl/_agent_comparison.json`
"""))

cells.append(md("## 1. Install the SDK"))
cells.append(code("%pip install -U fabric-data-agent-sdk pandas"))

cells.append(md("## 2. Load the scenario set"))
# Embed the scenarios file as a raw JSON string and parse at runtime so the
# JSON literals (true/false/null) don't collide with Python's True/False/None.
cells.append(code(f"""import json
from pathlib import Path

import pandas as pd

LAKEHOUSE_PATH = "/lakehouse/default/Files/npl/agent-comparison-questions.json"

INLINE_SCENARIOS_JSON = r\"\"\"{SCENARIOS_JSON}\"\"\"

def load_scenarios():
    path = Path(LAKEHOUSE_PATH)
    if path.exists():
        print(f"Loaded scenarios from lakehouse: {{path}}")
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    print("Using inline scenario fallback.")
    return json.loads(INLINE_SCENARIOS_JSON)

scenarios = load_scenarios()

df_scenarios = pd.DataFrame({{
    "question": [s["user_question"] for s in scenarios],
    "expected_answer": [
        "Expected metric: " + (s.get("gold_label") or "flag-ambiguity")
        + ". Tables: " + ", ".join(s.get("required_scope_tables", []))
        + (". Must mention: " + ", ".join(s.get("ontology_signals", []))
           if s.get("ontology_signals") else "")
        for s in scenarios
    ],
}})
print(f"Loaded {{len(df_scenarios)}} scenarios")
df_scenarios.head(20)
"""))

cells.append(md("""## 3. Configure the evaluation

The critic prompt only receives `{query}` and `{expected_answer}` from the SDK. It is posted as a follow-up message in the same thread as the agent's prior answer; `{actual_answer}` is **not** a substituted placeholder and would raise `KeyError`."""))
cells.append(code("""NAKED_AGENT_NAME = "NakedAgent"
ONTOLOGY_AGENT_NAME = "OntologyAgent"
DATA_AGENT_STAGE = "sandbox"
WORKSPACE_NAME = None

NAKED_TABLE = "npl_agent_compare_naked"
ONTOLOGY_TABLE = "npl_agent_compare_ontology"

CRITIC_PROMPT = '''
You judge whether YOUR PREVIOUS ANSWER in this thread satisfies the expected answer for an NPL-portfolio question.

Rules:
- Respond 'Yes' if your previous answer cites the expected metric / tables / tokens and avoids the traps listed (e.g. principal_balance vs balance_at_default, all impaired vs ifrs_stage_3_impaired).
- Respond 'No' if your answer is missing a required element, chose the wrong metric, refused to answer, or returned an error.
- Respond 'Unclear' only if your answer is plausibly correct but partial and you cannot verify it from the tokens alone.

Return one word: Yes, No, or Unclear.

Query: {query}

Expected Answer (criteria):
{expected_answer}
'''.strip()
"""))

cells.append(md("## 4. Evaluate NakedAgent"))
cells.append(code("""from fabric.dataagent.evaluation import evaluate_data_agent

naked_eval_id = evaluate_data_agent(
    df_scenarios,
    NAKED_AGENT_NAME,
    workspace_name=WORKSPACE_NAME,
    table_name=NAKED_TABLE,
    data_agent_stage=DATA_AGENT_STAGE,
    critic_prompt=CRITIC_PROMPT,
)
print(f"NakedAgent evaluation_id: {naked_eval_id}")
"""))

cells.append(md("## 5. Evaluate OntologyAgent"))
cells.append(code("""ontology_eval_id = evaluate_data_agent(
    df_scenarios,
    ONTOLOGY_AGENT_NAME,
    workspace_name=WORKSPACE_NAME,
    table_name=ONTOLOGY_TABLE,
    data_agent_stage=DATA_AGENT_STAGE,
    critic_prompt=CRITIC_PROMPT,
)
print(f"OntologyAgent evaluation_id: {ontology_eval_id}")
"""))

cells.append(md("## 6. Summary metrics"))
cells.append(code("""from fabric.dataagent.evaluation import get_evaluation_summary

naked_summary = get_evaluation_summary(NAKED_TABLE, verbose=True)
ontology_summary = get_evaluation_summary(ONTOLOGY_TABLE, verbose=True)

print("\\n=== NakedAgent summary ===")
display(naked_summary)
print("\\n=== OntologyAgent summary ===")
display(ontology_summary)
"""))

cells.append(md("## 7. Per-question details"))
cells.append(code("""from fabric.dataagent.evaluation import get_evaluation_details

naked_details = get_evaluation_details(naked_eval_id, NAKED_TABLE, get_all_rows=True, verbose=False)
ontology_details = get_evaluation_details(ontology_eval_id, ONTOLOGY_TABLE, get_all_rows=True, verbose=False)

print("NakedAgent details:")
display(naked_details)
print("\\nOntologyAgent details:")
display(ontology_details)
"""))

cells.append(md("## 8. Side-by-side merge"))
cells.append(code("""def normalize(df, suffix):
    keep = [c for c in ["question", "expected_answer", "actual_answer", "evaluation_result", "thread_url"] if c in df.columns]
    out = df[keep].copy()
    rename = {c: f"{c}_{suffix}" for c in out.columns if c not in ("question", "expected_answer")}
    return out.rename(columns=rename)

naked_norm = normalize(naked_details, "naked")
ontology_norm = normalize(ontology_details, "ontology")

side_by_side = pd.merge(naked_norm, ontology_norm, on=["question", "expected_answer"], how="outer")
display(side_by_side)
"""))

cells.append(md("## 9. Persist the comparison JSON for scripts/06_score.py"))
cells.append(code("""import os
from datetime import datetime

OUTPUT_DIR = "/lakehouse/default/Files/npl"
os.makedirs(OUTPUT_DIR, exist_ok=True)

report = {
    "runAtUtc": datetime.utcnow().isoformat() + "Z",
    "stage": DATA_AGENT_STAGE,
    "agents": {
        "naked": {
            "name": NAKED_AGENT_NAME,
            "evaluationId": str(naked_eval_id),
            "summary": naked_summary.to_dict(orient="records"),
        },
        "ontology": {
            "name": ONTOLOGY_AGENT_NAME,
            "evaluationId": str(ontology_eval_id),
            "summary": ontology_summary.to_dict(orient="records"),
        },
    },
    "perQuestion": side_by_side.to_dict(orient="records"),
}

out_path = f"{OUTPUT_DIR}/_agent_comparison.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2, default=str)
print(f"Saved: {out_path}")
"""))

cells.append(md("""## Where OntologyAgent should win

- **Graph traversal** (Q04, Q06, Q09, Q10, Q11) — NakedAgent tends to lose join direction
- **Governed metrics** (Q13, Q14, Q15) — the EBA-defined metric (e.g. `balance_at_default`, `ifrs_stage_3_impaired`) must be chosen, not a proxy like `principal_balance` or `all_impaired`
- **Negation** (Q12) — "loans with NO collateral" via anti-join
- **Ambiguity & guardrail** (Q16, Q17, Q18) — must flag ambiguity or refuse to execute action-oriented requests

Sanity-check questions (Q01–Q03) should be a tie.
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
