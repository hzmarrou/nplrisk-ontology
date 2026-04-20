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
- 12 entities, 13 relationships, 14 Lakehouse Delta tables, populated
  from ~7.6 k rows of realistic sample data
- 11 GQL competency queries run through
  `scripts/04_refresh_and_validate.py`
- Two provisioned Fabric Data Agents:
  - **`NakedAgent`** — wired to the Lakehouse tables only (Spark SQL
    engine; no ontology hints)
  - **`OntologyAgent`** — wired to the ontology **only** (GQL engine;
    the ontology runtime resolves the graph against the same Lakehouse
    through the bindings / contextualizations, but the agent has no
    direct SQL access)
- An 18-scenario benchmark with multi-dimensional scoring (critic
  verdict, table coverage, relationship usage, ambiguity detection,
  action-guardrail compliance, signal coverage, and a deterministic
  numeric-gold check for governed metrics) producing a Markdown + JSON
  scorecard that hash-locks the scenario payload it was produced from

## Prerequisites

- Python 3.11+ and [`uv`](https://docs.astral.sh/uv/) (or plain `pip`)
- A Microsoft Fabric capacity (F2+ or P1+) with the
  [Fabric Data Agent tenant settings](https://learn.microsoft.com/en-us/fabric/data-science/data-agent-tenant-settings)
  enabled, including "Capacities can be designated as Fabric Copilot
  capacities" and cross-geo processing/storing for AI

### Service-principal permissions (least privilege)

The SP is used only by the numbered scripts (`01`..`05`). The
user-context notebook runs as a user, so the SP does NOT need any
OpenAI / Assistants API permissions.

| Resource | Level needed | Why |
|---|---|---|
| Target Fabric workspace | **Admin** | Create/update ontologies, graph models, data agents; run Livy Spark sessions against the lakehouse |
| Target Lakehouse (in that workspace) | inherits from workspace Admin | `scripts/03_setup.py` creates tables + bindings |
| Entra app Graph scopes | *none* | The scripts talk only to the Fabric API (`api.fabric.microsoft.com`); no MS Graph calls |
| Tenant-level app roles | *none required* | `Tenant.ReadWrite.All` is not used. Fabric scopes come from the workspace Admin assignment, not from a tenant-wide app role |

Fabric workspace role mappings are documented in
[Microsoft's workspace-role reference](https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces).
Keep the SP scoped to the benchmark workspace — creating a fresh
workspace per evaluation keeps the blast radius minimal.

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
