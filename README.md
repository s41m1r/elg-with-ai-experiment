# Replication Package — AI-Assisted Event Log Extraction for Process Mining in Healthcare

> **Paper:** *AAI-Assisted Event Log Extraction for Process Mining in Healthcare*
> **Venue:** BPM 2026 Engineering Track

---

## Overview

This repository contains the complete replication package for our empirical study evaluating large language models (LLMs) on the task of generating SQL queries that extract event logs from healthcare databases structured according to the **OMOP Common Data Model (CDM)**. The target dataset is **MIMIC-IV**, a large publicly available ICU database from Beth Israel Deaconess Medical Center.

The experiment covers **6 clinical process tasks**, **3 LLMs** (GPT-4o, Claude Sonnet, Llama 3 70B), **4 prompting strategies** (Naive, Zero-Shot, Schema-Aware, Few-Shot), and **3 repetitions per configuration** — totalling **216 LLM API calls** and **135 generated SQL queries** (some tasks overlap). All generated SQL is evaluated against expert-written ground-truth queries on a stratified sample of 888 MIMIC-IV patients.

---

## What This Package Reproduces

The pre-computed results in `results/` directly correspond to the following paper artefacts:

| Paper artefact | File in this package |
|---|---|
| Table: M1 Executability by LLM × Strategy | `results/latex_tables.tex` (Tab M1 Grid) |
| Table: M1 Executability by Task | `results/latex_tables.tex` (Tab M1 Task) |
| Table: M3/M4 Completeness & Fidelity | `results/latex_tables.tex` (Tab M3/M4) |
| Table: M5 Prompt Sensitivity | `results/latex_tables.tex` (Tab M5) |
| Table: Per-Task Best Performance | `results/latex_tables.tex` (Tab Per-Task) |
| Key findings text (Section 5) | `results/key_findings.txt` |
| Error breakdown (Section 5.1) | `results/error_analysis.md` |
| Process mining figures (Section 6) | `figures/` + `mine_process_models.ipynb` |

---

## Repository Structure

```
elg-with-ai-experiment/
│
├── ground_truth/              # Expert-written reference SQL queries (by task)
│   ├── t1_gt.sql              # T1: ICU Patient Pathway
│   ├── t2_gt.sql              # T2: Medication Administration
│   ├── t3_gt.sql              # T3: Sepsis Treatment Trajectory
│   ├── t4_gt.sql              # T4: Lab-Order-to-Result Cycle
│   ├── t5_gt.sql              # T5: Emergency Department Flow
│   └── t6_gt.sql              # T6: Inpatient Diagnosis Pathway
│
├── schemas/                   # OMOP CDM DDL schemas per task (used in prompts)
│   ├── t1_icu_pathway.sql
│   ├── t2_medication.sql
│   ├── t3_sepsis.sql
│   ├── t4_lab_cycle.sql
│   ├── t5_ed_flow.sql
│   └── t6_diagnosis_pathway.sql
│
├── prompts/                   # Generated prompt files (6 tasks × 4 strategies)
│   ├── t1/ … t6/
│   │   ├── naive.txt          # Minimal prompt, no schema
│   │   ├── zero_shot.txt      # Task description, no schema
│   │   ├── schema_aware.txt   # Task description + full OMOP CDM DDL
│   │   └── few_shot.txt       # Task description + worked example
│
├── outputs/                   # LLM-generated SQL files (135 total)
│   ├── t1/ … t6/
│   │   ├── claude/            # Claude Sonnet outputs
│   │   ├── gpt4o/             # GPT-4o outputs
│   │   └── llama3/            # Llama 3 70B outputs
│   │       ├── <strategy>_r<N>.sql      # Extracted SQL
│   │       └── <strategy>_r<N>_raw.txt  # Raw LLM response
│
├── results/                   # All evaluation metrics (pre-computed)
│   ├── executability.csv      # M1 (SQL executability) + M2 (schema correctness)
│   ├── executability_duckdb.csv # M1/M2 on local DuckDB demo data
│   ├── pg_executability.csv   # M1/M2 on full PostgreSQL MIMIC-IV
│   ├── completeness.csv       # M3a (case coverage), M3b (activity coverage)
│   ├── fidelity.csv           # M4a (activity Jaccard similarity)
│   ├── sensitivity.csv        # M5 (prompt sensitivity, std across strategies)
│   ├── summary.csv            # All metrics joined (one row per SQL file)
│   ├── summary_agg.csv        # Aggregated by (task, LLM, strategy)
│   ├── api_log.csv            # Record of all LLM API calls (tokens, latency)
│   ├── gt_results.csv         # Ground-truth query execution results
│   ├── normalisation_t5_results.csv  # Activity normalisation experiment (T5)
│   ├── normalisation_t5_summary.txt  # Summary of normalisation experiment
│   ├── error_analysis.md      # Breakdown of SQL error types by LLM/strategy
│   ├── key_findings.txt       # Auto-generated summary of key results
│   └── latex_tables.tex       # Publication-ready LaTeX tables (copy-paste into paper)
│
├── figures/                   # Process mining visualisations (22 PNG files)
│   ├── overview_cases_events.png
│   ├── t1_dfg.png, t1_petri_net.png, …
│   └── (per-task DFGs, Petri nets, top-activity charts)
│
├── scripts/                   # Experiment pipeline (run in order)
│   ├── build_prompts.py       # Step 1: Generate all prompt files
│   ├── run_llms.py            # Step 2: Call LLM APIs, collect SQL
│   ├── execute_sql.py         # Step 3: Execute SQL, measure M1 & M2
│   ├── evaluate_metrics.py    # Step 4: Compute M3, M4, M5
│   ├── generate_tables.py     # Step 5: Generate LaTeX tables & findings
│   ├── generate_event_logs.py # (Optional) Generate XES event logs from GT SQL
│   ├── create_sample.py       # (Optional) Extract stratified patient sample
│   └── normalisation_experiment.py  # (Optional) Activity normalisation experiment
│
├── mine_process_models.ipynb  # Jupyter notebook: process mining on GT event logs
│                              # (figures pre-computed, no database required)
│
├── .env.example               # Template for API key configuration
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

---

## Data Availability

> **Important:** MIMIC-IV is a controlled-access dataset. Access requires completion of a CITI Data or Specimens Only Research training course and credentialing via [PhysioNet](https://physionet.org/content/mimiciv/). The raw data is **not included** in this repository and cannot be redistributed.

There are two ways to engage with this replication package:

### Option A — Inspect Pre-Computed Results (No Data Access Required)

All evaluation results are **already computed and committed** to this repository. Reviewers can immediately inspect:

- `results/latex_tables.tex` — the five LaTeX tables from the paper
- `results/key_findings.txt` — auto-generated summary of key numerical findings
- `results/error_analysis.md` — detailed breakdown of SQL error categories
- `results/*.csv` — all raw metric values in machine-readable form
- `mine_process_models.ipynb` — pre-rendered Jupyter notebook with process mining figures

No database access, API keys, or Python environment is needed for this level of inspection.

### Option B — Full Reproduction (Requires MIMIC-IV Access + API Keys)

To reproduce the experiment from scratch, you will need:

1. **MIMIC-IV** mapped to OMOP CDM, accessible via PostgreSQL (see [OHDSI/mimic-iv-demo-omop](https://github.com/OHDSI/mimic-iv-demo-omop) for the ETL)
2. **API keys** for OpenAI (GPT-4o), Anthropic (Claude Sonnet), and Groq (Llama 3 70B)
3. A Python 3.10+ environment

---

## Setup

```bash
# 1. Clone the repository
git clone https://github.com/s41m1r/elg-with-ai-experiment.git
cd elg-with-ai-experiment

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate          # On Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment variables
cp .env.example .env
# Edit .env and fill in your API keys and database connection details
```

### Environment Variables (`.env`)

```ini
# LLM API Keys
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
GROQ_API_KEY=gsk_...

# PostgreSQL connection (full MIMIC-IV OMOP CDM)
PG_HOST=<host>
PG_PORT=5432
PG_DB=mimic
PG_USER=<user>
PG_PASSWORD=<password>
PG_SCHEMA=cdm
```

---

## Running the Pipeline (Full Reproduction)

The pipeline consists of five sequential steps. Each script can be run independently once its dependencies are available.

### Step 1 — Build Prompts

Generates the 24 prompt files (6 tasks × 4 strategies) from the OMOP CDM schemas.

```bash
python scripts/build_prompts.py
```

Output: `prompts/t1/` … `prompts/t6/`, each containing `naive.txt`, `zero_shot.txt`, `schema_aware.txt`, `few_shot.txt`.

> **Note on data privacy:** Prompts contain only synthetic example rows (fabricated, not from the database) to comply with the MIMIC-IV Data Use Agreement. No real patient data is sent to external LLM APIs.

### Step 2 — Run LLMs

Calls the three LLM APIs and saves generated SQL. This step makes ~216 API calls and may take 30–60 minutes depending on rate limits.

```bash
python scripts/run_llms.py
# Optional: restrict to specific tasks, LLMs, or strategies
python scripts/run_llms.py --tasks t1 t2 --llms claude gpt4o --strategies schema_aware few_shot
```

Output: `outputs/t*/claude/`, `outputs/t*/gpt4o/`, `outputs/t*/llama3/`

### Step 3 — Execute SQL (M1 & M2)

Executes all generated SQL files against the database and records executability (M1) and schema correctness (M2).

```bash
# Against PostgreSQL (full MIMIC-IV OMOP CDM)
python scripts/execute_sql.py --db postgres

# Against DuckDB using local sample CSVs (if available)
python scripts/execute_sql.py --db duckdb --data-dir sample_data/
```

Output: `results/executability.csv`, `results/executability_duckdb.csv`

### Step 4 — Evaluate Metrics (M3, M4, M5)

Computes event log completeness (M3), process fidelity (M4), and prompt sensitivity (M5) by comparing LLM-generated outputs to ground-truth SQL results.

```bash
python scripts/evaluate_metrics.py --db postgres
```

Output: `results/completeness.csv`, `results/fidelity.csv`, `results/sensitivity.csv`, `results/summary.csv`

### Step 5 — Generate Tables & Findings

Produces publication-ready LaTeX tables and auto-generates a plain-text summary of key findings.

```bash
python scripts/generate_tables.py
```

Output: `results/latex_tables.tex`, `results/key_findings.txt`, `results/summary_agg.csv`

---

## Evaluation Metrics

| Metric | Symbol | Definition |
|---|---|---|
| Executability | M1 | 1 if the SQL runs without error; 0 otherwise |
| Schema Correctness | M2 | 1 if output contains `case_id`, `activity`, `timestamp` columns |
| Case Coverage | M3a | \|cases\_llm ∩ cases\_gt\| / \|cases\_gt\| |
| Activity Coverage | M3b | \|activities\_llm ∩ activities\_gt\| / \|activities\_gt\| |
| Activity Jaccard | M4a | \|A\_llm ∩ A\_gt\| / \|A\_llm ∪ A\_gt\| |
| Prompt Sensitivity | M5 | Std. deviation of M1/M3a/M3b/M4a across 4 prompt strategies |

M3 and M4 are only computed for queries that pass M1 (executable) and have a corresponding ground-truth query.

---

## Clinical Process Tasks

All tasks target the **OMOP Common Data Model**. The framework is designed to be OMOP-portable: the same prompts, schemas, and evaluation scripts can run on any OMOP-compliant database, not just MIMIC-IV.

| Task | Clinical Process | Key OMOP Tables |
|---|---|---|
| T1 | ICU Patient Pathway | `visit_occurrence`, `visit_detail`, `procedure_occurrence` |
| T2 | Medication Administration | `drug_exposure`, `drug_era`, `concept` |
| T3 | Sepsis Treatment Trajectory | `condition_occurrence`, `measurement`, `drug_exposure`, `observation` |
| T4 | Lab-Order-to-Result Cycle | `measurement`, `specimen` |
| T5 | Emergency Department Flow | `visit_occurrence`, `visit_detail`, `condition_occurrence` |
| T6 | Inpatient Diagnosis Pathway | `visit_occurrence`, `condition_occurrence` |

---

## Process Mining Demonstration

A Jupyter notebook is provided with pre-computed process mining results on the ground-truth event logs:

```bash
jupyter notebook mine_process_models.ipynb
```

The notebook demonstrates how the extracted event logs can be used for process discovery (Directly-Follows Graphs, Petri nets) and process analysis across all five clinical tasks. **All figures are pre-rendered** — no database connection is required to view the notebook.

---

## LLMs Used

| LLM | Provider | Model ID | API |
|---|---|---|---|
| GPT-4o | OpenAI | `gpt-4o-2024-08-06` | OpenAI API |
| Claude Sonnet | Anthropic | `claude-sonnet-4-5` | Anthropic API |
| Llama 3 70B | Meta (via Groq) | `llama3-70b-8192` | Groq API |

---

## Key Results Summary

> Full results are in `results/key_findings.txt` and `results/latex_tables.tex`.

- **M1 Executability:** Claude Sonnet achieved 100% executability; GPT-4o 91%; Llama 3 71% overall.
- **M3a Case Coverage:** Mean 78.7% — LLMs identify the correct patient cohorts effectively.
- **M3b Activity Coverage / M4a Jaccard:** Near-zero (~0.17%) — LLMs and ground truth use different concept naming conventions, causing a terminology mismatch rather than a structural failure.
- **M5 Prompt Sensitivity:** Claude Sonnet is the most stable across prompt strategies (σ=0.012); GPT-4o is the most sensitive (σ=0.116).

---

## Repository Size Note

The following items are excluded from this repository due to data access restrictions or size:

- `sample_data/` — stratified OMOP CDM CSV export (888 patients); available to researchers with PhysioNet credentials
- `.env` — API keys and database credentials (see `.env.example`)

---

## Citation

If you use this replication package, please cite:

```bibtex
@inproceedings{anonymous2026llmelg,
  title     = {LLM-Driven SQL Generation for Healthcare Event Log Extraction:
               Evaluating Large Language Models on MIMIC-IV for Process Mining},
  author    = {Anonymous Authors},
  booktitle = {Proceedings of the International Conference on Business Process Management
               (BPM 2026) -- Engineering Track},
  year      = {2026}
}
```

---

## License

The code and prompt templates in this repository are released under the **MIT License**.
The MIMIC-IV data underlying the experiments is governed by the [PhysioNet Credentialed Health Data License](https://physionet.org/content/mimiciv/view-license/2.2/).
