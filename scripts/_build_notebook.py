"""Utility: regenerate the Fabric comparison notebook.

Run:
    python scripts/_build_notebook.py

This rewrites ``notebooks/compare_agents_fabric.ipynb`` from the cell
definitions below so the notebook stays in sync with the scenario set.
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

cells.append(md("""# NakedAgent vs OntologyAgent — NPL risk benchmark

Run this notebook **inside Fabric in user context** (not via service principal). Agent provisioning is done by `scripts/05_setup_agents.py` outside Fabric; agent evaluation runs here.

**Prerequisites**

1. Attach the NPL lakehouse (the one referenced in your repo's `.env`) to this notebook as the **default lakehouse**. Without it, the evaluation SDK fails with `Missing required Fabric context parameters`.
2. (Optional) Upload `scenarios/npl_scenarios.json` into the lakehouse at `Files/npl/agent-comparison-questions.json`. If that file is missing the notebook falls back to an embedded copy below.

**What the notebook does**

1. Installs the Fabric Data Agent SDK
2. Drops any stale `npl_agent_compare_*` tables so a schema drift from a previous run can't corrupt this one
3. Loads the 18 NPL scenarios
4. Calls `evaluate_data_agent` for `NakedAgent` and `OntologyAgent`
5. Shows summary + per-question detail DataFrames
6. Merges a side-by-side comparison table
7. Saves a JSON report to `Files/npl/_agent_comparison.json` for `scripts/06_score.py`
"""))

cells.append(md("## 1. Install the SDK"))
cells.append(code("%pip install -U fabric-data-agent-sdk pandas"))

cells.append(md("""## 2. Fresh-start: drop any stale evaluation tables

The Fabric `evaluate_data_agent` helper appends to a Delta table. If a previous
run left a table with a slightly different schema (e.g. evolving SDK versions),
the next append can fail with `CANNOT_MERGE_TYPE`. We drop any stale tables
here so every run starts from a clean slate.
"""))
cells.append(code("""# Explicit table names — change these if you want to keep a previous run's tables around.
NAKED_TABLE = "npl_agent_compare_naked_v2"
ONTOLOGY_TABLE = "npl_agent_compare_ontology_v2"

for tbl in (NAKED_TABLE, ONTOLOGY_TABLE, f"{NAKED_TABLE}_steps", f"{ONTOLOGY_TABLE}_steps"):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
        print(f"  dropped (if present): {tbl}")
    except Exception as e:
        print(f"  WARN dropping {tbl}: {e}")
"""))

cells.append(md("""## 3. Load the 18-scenario NPL benchmark

The scenario set ships with the repo as `scenarios/npl_scenarios.json`. We
prefer a lakehouse-uploaded copy at `Files/npl/agent-comparison-questions.json`
so you can edit it between runs without touching the notebook, and fall back
to an embedded copy so the notebook is always self-contained.
"""))
cells.append(code(f"""import json
from pathlib import Path

import pandas as pd

LAKEHOUSE_PATH = "/lakehouse/default/Files/npl/agent-comparison-questions.json"

# Raw JSON string so Python does not mis-parse true/false/null as identifiers.
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

cells.append(md("""## 4. Configure the evaluation

The critic prompt only receives `{query}` and `{expected_answer}` from the SDK
(it is posted as a follow-up message in the same thread as the agent's prior
answer). `{actual_answer}` is **not** a substituted placeholder and would raise
`KeyError`.
"""))
cells.append(code("""NAKED_AGENT_NAME = "NakedAgent"
ONTOLOGY_AGENT_NAME = "OntologyAgent"
DATA_AGENT_STAGE = "sandbox"
WORKSPACE_NAME = None   # keep None when this notebook runs in the agents' own workspace

CRITIC_PROMPT = '''
You judge whether YOUR PREVIOUS ANSWER in this thread satisfies the expected answer for an NPL-portfolio question.

Rules:
- Respond 'Yes' if your previous answer cites the expected metric / tables / tokens and avoids the traps listed (e.g. principal_balance vs balance_at_default, all-impaired vs ifrs_stage_3_impaired).
- Respond 'No' if your answer is missing a required element, chose the wrong metric, refused to answer, or returned an error.
- Respond 'Unclear' only if your answer is plausibly correct but partial and you cannot verify it from the tokens alone.

Return one word: Yes, No, or Unclear.

Query: {query}

Expected Answer (criteria):
{expected_answer}
'''.strip()

print("NAKED_TABLE   =", NAKED_TABLE)
print("ONTOLOGY_TABLE=", ONTOLOGY_TABLE)
print("NAKED_AGENT   =", NAKED_AGENT_NAME)
print("ONTOLOGY_AGENT=", ONTOLOGY_AGENT_NAME)
print("STAGE         =", DATA_AGENT_STAGE)
"""))

cells.append(md("""## 5. Evaluate NakedAgent

This writes results into the fresh `NAKED_TABLE` (and its `_steps` companion).
Expect ~3 min on F16: each of the 18 questions goes through the agent thread + critic.
"""))
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

cells.append(md("## 6. Evaluate OntologyAgent"))
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

cells.append(md("""## 7. Summary metrics (per agent)

If a summary is empty but the corresponding eval_id printed above, wait 20-30 s
and re-run this cell — the Delta table can take a moment to become visible to
the Spark catalog.
"""))
cells.append(code("""from fabric.dataagent.evaluation import get_evaluation_summary

naked_summary = get_evaluation_summary(NAKED_TABLE, verbose=True)
ontology_summary = get_evaluation_summary(ONTOLOGY_TABLE, verbose=True)

print("\\n=== NakedAgent summary ===")
display(naked_summary)
print("\\n=== OntologyAgent summary ===")
display(ontology_summary)
"""))

cells.append(md("## 8. Per-question details"))
cells.append(code("""from fabric.dataagent.evaluation import get_evaluation_details

naked_details = get_evaluation_details(
    naked_eval_id, NAKED_TABLE, get_all_rows=True, verbose=False
)
ontology_details = get_evaluation_details(
    ontology_eval_id, ONTOLOGY_TABLE, get_all_rows=True, verbose=False
)

def _describe(df, label):
    if df is None:
        return f"{label}: None (evaluation probably did not complete or the table is missing)"
    return f"{label}: {df.shape[0]} rows x {df.shape[1]} cols"

print(_describe(naked_details, "naked_details"))
print(_describe(ontology_details, "ontology_details"))

if naked_details is not None:
    print("\\nNakedAgent details:")
    display(naked_details)
if ontology_details is not None:
    print("\\nOntologyAgent details:")
    display(ontology_details)
"""))

cells.append(md("""## 9. Side-by-side merge

Joins the two details frames on the question text so each row lines up both
agents' answers.
"""))
cells.append(code("""KEEP_COLUMNS = [
    "question", "expected_answer",
    "actual_answer",
    "evaluation_judgement",   # Yes/No/true/false/1/0 verdict from the critic
    "evaluation_result",      # alternate name in some SDK versions
    "evaluation_status",
    "evaluation_message",
    "thread_url",
]

def normalize(df, suffix):
    if df is None:
        print(f"WARNING: {suffix} details are None. That agent's evaluation didn't produce rows.")
        return pd.DataFrame(columns=["question", "expected_answer"])
    keep = [c for c in KEEP_COLUMNS if c in df.columns]
    out = df[keep].copy()
    rename = {c: f"{c}_{suffix}" for c in out.columns if c not in ("question", "expected_answer")}
    return out.rename(columns=rename)

naked_norm = normalize(naked_details, "naked")
ontology_norm = normalize(ontology_details, "ontology")

print(f"naked rows: {len(naked_norm)}, ontology rows: {len(ontology_norm)}")

if naked_norm.empty and ontology_norm.empty:
    raise RuntimeError(
        "Both detail frames are empty. Re-run cells 5 and 6 — "
        "either the eval IDs expired or no rows landed in the Delta tables."
    )

side_by_side = pd.merge(naked_norm, ontology_norm, on=["question", "expected_answer"], how="outer")
display(side_by_side)
"""))

cells.append(md("""## 10. Persist the comparison JSON for `scripts/06_score.py`

Downloads/save the resulting `_agent_comparison.json` to your local
`nplrisk-ontology/outputs/` folder, then run `python scripts/06_score.py`.
"""))
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
            "summary": naked_summary.to_dict(orient="records") if naked_summary is not None else [],
        },
        "ontology": {
            "name": ONTOLOGY_AGENT_NAME,
            "evaluationId": str(ontology_eval_id),
            "summary": ontology_summary.to_dict(orient="records") if ontology_summary is not None else [],
        },
    },
    "perQuestion": side_by_side.to_dict(orient="records"),
}

out_path = f"{OUTPUT_DIR}/_agent_comparison.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2, default=str)
print(f"Saved: {out_path}")
print(f"Rows: {len(report['perQuestion'])}")
"""))

cells.append(md("""## What to look for

- **Aggregate accuracy** — OntologyAgent should clear NakedAgent on overall `Yes` rate, especially past Q05.
- **Biggest ontology wins** — Q08 (negation), Q09 (counterparty-group rollup), Q13/Q14/Q15 (governed metrics where NakedAgent picks the wrong column), Q16/Q17 (ambiguity), Q18 (action guardrail).
- **Expected ties** — Q01, Q02, Q03 (single-table sanity). If OntologyAgent loses any of these, investigate the prompt.

When you're happy with the results, download `Files/npl/_agent_comparison.json` to your local `nplrisk-ontology/outputs/` folder and run `python scripts/06_score.py` for the final scorecard.
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
