# ELG with AI Experiment

This repository contains only the experiment artifacts and pipeline for LLM-driven SQL event log extraction from OMOP CDM (MIMIC-IV).

## Experiment contents
- `experiment/ground_truth/`
- `experiment/prompts/`
- `experiment/outputs/`
- `experiment/results/`
- `experiment/schemas/`
- `experiment/scripts/`

## Excluded from GitHub
- `experiment/sample_data/` (local sample data, ignored)
- API keys and `.env` secrets

## Run instructions
```bash
cd experiment
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python scripts/build_prompts.py
python scripts/run_llms.py
python scripts/execute_sql.py
python scripts/evaluate_metrics.py
```

## Key results to inspect
- `experiment/results/executability.csv`
- `experiment/results/completeness.csv`
- `experiment/results/fidelity.csv`
- `experiment/results/sensitivity.csv`
- `experiment/results/summary.csv`
