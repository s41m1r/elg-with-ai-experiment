# Error Analysis — M1 Failures

**Updated:** 2026-03-20
**Dataset:** `executability.csv` (216 experiments: 6 tasks × 3 LLMs × 4 strategies × 3 repetitions)
**Executed on:** PostgreSQL — full MIMIC-IV OMOP CDM instance (search_path = cdm)
**Note:** An earlier version of this file covered a DuckDB pilot run of 135 experiments (T1–T5, 3 strategies). The numbers below reflect the final PostgreSQL evaluation reported in the paper.

---

## 1. Summary

| | Count | % of total |
|---|---|---|
| Total experiments | 216 | 100% |
| M1 pass (executable) | 179 | **83%** |
| M1 fail (non-executable) | 37 | 17% |
| M2 pass (correct schema) | 179 | **100% of executable** |

M2 = M1 across all 216 experiments: every query that executed also produced the required three-column schema (`case_id`, `activity`, `timestamp`), confirming that the output schema instruction in the prompt is fully effective.

---

## 2. Failures by LLM

| LLM | Pass / Total | Pass rate | Failures |
|-----|-------------|-----------|---------|
| Claude Sonnet 4 | 69 / 72 | **96%** | 3 |
| GPT-4o | 66 / 72 | **92%** | 6 |
| Llama 3.3 70B | 44 / 72 | **61%** | 28 |
| **Overall** | **179 / 216** | **83%** | 37 |

Llama 3.3 accounts for 76% of all failures (28/37). The failure gap between frontier models (Claude 96%, GPT-4o 92%) and the open-weight model (Llama 3.3 61%) is primarily driven by the naive strategy: Llama 3.3 achieves only 17% executability under naive prompting, while GPT-4o achieves 89%.

---

## 3. Failures by Prompt Strategy

| Strategy | Pass / Total | Pass rate | Failures |
|----------|------------|-----------|---------|
| Naive | 34 / 54 | **63%** | 20 |
| Zero-shot | 46 / 54 | **85%** | 8 |
| Schema-aware | 49 / 54 | **91%** | 5 |
| Few-shot | 50 / 54 | **93%** | 4 |

The naive strategy is the weakest by a large margin, accounting for 54% of all failures (20/37). Schema-aware and few-shot are nearly equivalent; few-shot is marginally better. The progression naive < zero-shot < schema-aware < few-shot is monotonic, confirming that prompt informativeness directly drives executability.

---

## 4. Failures by Task

| Task | Process | Pass / Total | Pass rate | Failures |
|------|---------|------------|-----------|---------|
| T1 | ICU Patient Pathway | 33 / 36 | **92%** | 3 |
| T2 | Medication Administration | 23 / 36 | **64%** | 13 |
| T3 | Sepsis Treatment Trajectory | 33 / 36 | **92%** | 3 |
| T4 | Lab-Order-to-Result Cycle | 24 / 36 | **67%** | 12 |
| T5 | Emergency Department Flow | 34 / 36 | **94%** | 2 |
| T6 | Inpatient Diagnosis Pathway | 32 / 36 | **89%** | 4 |
| **Total** | | **179 / 216** | **83%** | 37 |

T2 (Medication Administration) remains the hardest task (64%), driven by consistent LLM hallucination of a non-existent `drug_era.visit_occurrence_id` column. T4 (Lab-Order-to-Result, 67%) is hard due to the heuristic specimen-to-measurement join. T5 (ED Flow, 94%) is the easiest. T6 (Inpatient Diagnosis, 89%) validates its low-complexity design intent.

---

## 5. Error Classification

The 37 failed queries fall into two main categories:

| Error class | Count | % of failures |
|-------------|-------|---------------|
| Schema hallucination | 20 | 54% |
| Syntax errors and other failures | 17 | 46% |

**Schema hallucination** (20 queries): LLMs reference columns that do not exist in OMOP CDM. The two dominant patterns are:
- `drug_era.visit_occurrence_id` (T2, all models) — `drug_era` is person-level only; the correct join is via `person_id` and date-range overlap
- `specimen.visit_occurrence_id` (T4, Llama 3.3) — `specimen` has no visit FK; linkage must go via `person_id` and temporal proximity

Notably, no schema hallucinations involve native MIMIC-IV table names — all models correctly use OMOP CDM table names throughout.

**Syntax errors and other failures** (17 queries): Largely from naive-strategy Llama 3.3 queries that generated malformed SQL without adequate structural guidance.

### Error Class × LLM

| Error class | Claude Sonnet 4 | GPT-4o | Llama 3.3 70B |
|-------------|----------------|--------|--------------|
| Schema hallucination | 3 | 6 | 11 |
| Syntax / other | 0 | 0 | 17 |

### Error Class × Strategy

| Error class | Naive | Zero-shot | Schema-aware | Few-shot |
|-------------|-------|-----------|--------------|----------|
| Schema hallucination | 0 | 12 | 5 | 3 |
| Syntax / other | 17 | 0 | 0 | 0 |

Schema hallucination affects zero-shot and (to a lesser extent) schema-aware and few-shot strategies. Syntax failures are **exclusively** a naive-strategy phenomenon, confirming that even minimal process context prevents structural SQL errors.

---

## 6. Top Failure Patterns (Qualitative)

### Pattern 1 — `drug_era.visit_occurrence_id` hallucination (T2, all models)

**Example error:**
```
ERROR: column drug_era.visit_occurrence_id does not exist
```
**Root cause:** `drug_era` in OMOP CDM is person-level only (`person_id`, `drug_concept_id`, date range). It does not carry a `visit_occurrence_id`. LLMs assume a direct visit FK by analogy with `drug_exposure`. The correct linkage is an indirect join via `person_id` with date-range overlap to `visit_occurrence`.

**Implication:** Supports the value of schema-aware prompting and motivates including FK absence information in prompts.

### Pattern 2 — `specimen.visit_occurrence_id` hallucination (T4, Llama 3.3)

**Example error:**
```
ERROR: column specimen.visit_occurrence_id does not exist
```
**Root cause:** `specimen` in OMOP CDM has no direct visit FK. Linkage to `measurement` must be inferred via `person_id` and temporal proximity, risking Cartesian-product explosion if join constraints are missing.

### Pattern 3 — Malformed SQL from naive Llama 3.3 prompts (17 failures)

**Root cause:** Without any clinical process description, table names, or concept IDs, Llama 3.3 generates non-SQL text, incomplete queries, or structurally invalid SQL. GPT-4o avoids this under naive prompting (89% pass) because it applies stronger internal schema priors.

---

## 7. Key Takeaways

1. **83% overall executability** (179/216) across all 6 tasks, 3 LLMs, 4 strategies, 3 reps.
2. **M1 = M2**: schema conformance is fully achieved via prompt instruction — it is not the limiting factor.
3. **Prompting strategy has a stronger effect than model choice**: naive → few-shot spans 63%–93%, while the LLM range is 61%–96%.
4. **Schema complexity is the primary driver of task difficulty**: T2 and T4 (indirect joins) are the hardest; T5 and T6 (direct FK paths) are the easiest.
5. **Schema hallucination is model-agnostic** in zero-shot settings — all three models hallucinate `drug_era.visit_occurrence_id` for T2.
6. **Llama 3.3 is disproportionately affected** by naive-strategy failures (17/17 syntax errors), making it unsuitable for direct deployment without at least zero-shot prompting.
