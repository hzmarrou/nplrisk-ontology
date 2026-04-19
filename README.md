# nplrisk-bench

End-to-end benchmark that stands up a Non-Performing Loan (NPL) risk
ontology on Microsoft Fabric, wires it to a Lakehouse full of EBA-aligned
domain data, provisions two Data Agents — one schema-only (`NakedAgent`)
and one ontology-grounded (`OntologyAgent`) — and scores them side-by-side
on a curated scenario benchmark.

> This README walks you through installing, configuring, and running the
> full pipeline. More detailed authoring notes are in [docs/](./docs).

## What you get

- A real OWL/RDF ontology (NPLO — Non Performing Loan Ontology, aligned
  to EBA NPL Data Templates) parsed into a Fabric ontology
- 12 entities, 10–14 relationships, 14 Lakehouse Delta tables, populated
  from ~7.6 k rows of realistic sample data
- Live GQL competency-query validation
- Two provisioned Fabric Data Agents: `NakedAgent` (lakehouse only) and
  `OntologyAgent` (lakehouse + ontology)
- A 18-scenario benchmark with multi-dimensional scoring (metric
  correctness, table coverage, relationship usage, ambiguity detection,
  action-guardrail compliance) producing a markdown + JSON scorecard

## Prerequisites

- Python 3.11+ and [`uv`](https://docs.astral.sh/uv/) (or plain `pip`)
- A Microsoft Fabric capacity (F2+ or P1+) with the
  [Fabric Data Agent tenant settings](https://learn.microsoft.com/en-us/fabric/data-science/data-agent-tenant-settings)
  enabled, including "Capacities can be designated as Fabric Copilot
  capacities" and cross-geo processing/storing for AI
- An Entra application (service principal) that is:
  - granted `Tenant.ReadWrite.All` with admin consent, and
  - added to the target Fabric workspace as **Admin**

## Setup

```bash
# 1. Clone and install
git clone <this-repo>
cd nplrisk-ontology
uv venv && uv pip install -e .[dev]

# 2. Fill in credentials
cp .env.example .env   # then edit .env with your five values
```

## Pipeline

Each numbered script has a single job. Run them in order.

```bash
# 1. Parse the OWL file -> outputs/parsed_ontology.json (+ summary printed)
python scripts/01_parse_owl.py

# 2. Merge OWL + DDL + CSV headers -> outputs/ontology-config.json
python scripts/02_build_mapping.py

# 3. Create the ontology in Fabric, create + load the 14 Delta tables,
#    push bindings and contextualizations. Writes outputs/_state.json.
python scripts/03_setup.py

# 4. Trigger a graph refresh, wait for Completed, then run every
#    gql-queries/*.gql. Prints pass/fail table, writes outputs/_validation.json.
python scripts/04_refresh_and_validate.py

# 5. Provision NakedAgent + OntologyAgent, write outputs/_agents.json and
#    outputs/agent-comparison-questions.json.
python scripts/05_setup_agents.py
```

### Run the agent comparison (Fabric notebook, user context)

Provisioning runs under the service principal, but chatting with a Data
Agent requires a user identity. Open Fabric and run the notebook:

1. Upload `notebooks/compare_agents_fabric.ipynb` to your workspace
2. Attach `NPLLakehouse` (or whichever lakehouse your `.env` points to) as
   **default** lakehouse on the notebook
3. Run all cells. The notebook writes
   `Files/npl/_agent_comparison.json` back into the lakehouse.

### Score the comparison

Pull the comparison JSON down (via OneLake Explorer or the Fabric UI),
drop it into `outputs/_agent_comparison.json`, and run:

```bash
python scripts/06_score.py
```

This produces `outputs/scorecard.md` and `outputs/scorecard.json`.
`OntologyAgent` should clearly beat `NakedAgent` on multi-hop traversals,
governed-metric scenarios, negation, and ambiguity/guardrail cases.

## Repository layout

```
nplrisk-ontology/
├── input/                 NPLO OWL + 14 CSVs + ddl.sql (committed)
├── scenarios/             benchmark scenarios + golden answers (JSON)
├── gql-queries/           competency queries (.gql)
├── scripts/               numbered pipeline entry points (01..06)
├── notebooks/             Fabric notebook for the agent comparison
├── src/nplrisk_bench/     Python package
│   ├── fabric_client/     Fabric REST API clients (ontology, graph, Livy, data agent)
│   ├── owl_parser/        OWL parsing into a neutral dataclass model
│   ├── mapping/           OWL + DDL -> Fabric ontology config
│   ├── agents/            Data Agent provisioning + default instructions
│   └── scoring/           scenario + golden answer + scorecard evaluator
├── tests/                 pytest unit tests
└── outputs/               generated at runtime; gitignored
```

## Troubleshooting

- **403 on the first `POST /ontologies`**: the service principal is not
  Admin on the target workspace. Fix it in *Workspace settings → Manage
  access*.
- **Graph refresh stuck on `Cancelled`**: Fabric auto-cancels overlapping
  refresh jobs. Wait ~60 s and retry a single clean refresh. If the
  cancellation persists, click "Refresh now" in the Fabric UI.
- **Notebook evaluate_data_agent call fails with "Missing required Fabric
  context parameters"**: the notebook has no default lakehouse attached.
  Attach one and rerun from the evaluation cell.
- **`KeyError: 'actual_answer'` inside `evaluate_data_agent`**: the critic
  prompt may only reference `{query}` and `{expected_answer}`
  placeholders. `{actual_answer}` is not substituted by the SDK — the
  agent sees its own prior answer from the thread context.

## License

MIT
