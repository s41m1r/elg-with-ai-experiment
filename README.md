# ELG with AI Experiment

This repository contains only the experiment artifacts and pipeline for LLM-driven SQL event log extraction from OMOP CDM.

## Experiment contents
- `experiment/ground_truth/`
- `experiment/prompts/`
- `experiment/outputs/`
- `experiment/results/`
- `experiment/schemas/`
- `experiment/scripts/`

## Excluded from GitHub
- `experiment/sample_data/` (local sample data: MIMIC IV in the OMOP CDM format)
- API keys and `.env` secrets (e.g., API keys)

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

## Initial XES and Process Mining results
We provide the a [Jupyter notebook](mine_process_models.ipynb) that shows a demonstration of the utility of our approach to analyze the various treatment processes with process mining. Results and figures are already included in the notebook.


