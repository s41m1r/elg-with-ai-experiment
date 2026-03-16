#!/usr/bin/env python3
"""
build_prompts.py — Generate 15 prompt files (5 tasks × 3 strategies)
for the LLM event log extraction benchmark.

Output: experiment/prompts/{task}/{strategy}.txt
Usage:  python build_prompts.py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️  DATA PROTECTION — MIMIC-IV DUA COMPLIANCE (HARD RULE)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MIMIC-IV data is governed by the PhysioNet Data Use Agreement.
Real patient data rows MUST NEVER be included in any prompt sent
to an external LLM API (OpenAI, Anthropic, Groq, Together, etc.).

The ONLY content permitted to reach an external LLM is:
  1. Natural language task descriptions (no patient data)
  2. OMOP CDM DDL / CREATE TABLE statements (schema metadata only)
  3. SYNTHETIC (fabricated) example output rows — see T4_EXAMPLE_OUTPUT

DO NOT replace T4_EXAMPLE_OUTPUT with rows queried from the actual
MIMIC-IV database. Keep all example rows synthetic/fabricated.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_DIR  = os.path.join(BASE_DIR, "schemas")
PROMPTS_DIR = os.path.join(BASE_DIR, "prompts")

# ── Task descriptions ──────────────────────────────────────────────────────
TASK_DESCRIPTIONS = {
    "t1": (
        "ICU Patient Pathway: Track a patient's journey from hospital admission "
        "through ICU transfer(s) to discharge. The case_id should be the "
        "visit_occurrence_id (hospital admission). Activities include: Hospital "
        "Admission, ICU Admission, ICU Procedure, ICU Discharge, Hospital Discharge. "
        "Use the visit_occurrence table for the admission/discharge events, "
        "visit_detail for ICU sub-stays (filter by visit_detail_concept_id for ICU), "
        "and procedure_occurrence for procedures performed during the ICU stay. "
        "Join all tables via visit_occurrence_id. Filter out NULL timestamps."
    ),
    "t2": (
        "Medication Administration: Track the lifecycle of medications for a hospital "
        "admission. The case_id should be the visit_occurrence_id. Activities include: "
        "Drug Exposure Started, Drug Exposure Ended, Drug Era Started, Drug Era Ended. "
        "Use drug_exposure for individual administration events "
        "(drug_exposure_start_datetime, drug_exposure_end_datetime) and drug_era for "
        "aggregated drug periods (drug_era_start_date, drug_era_end_date). "
        "Join concept table on drug_concept_id to include the medication name in the "
        "activity label. Filter out NULL timestamps."
    ),
    "t3": (
        "Sepsis Treatment Trajectory: Track diagnostic and treatment steps for patients "
        "with sepsis. The case_id should be the visit_occurrence_id. Activities include: "
        "Sepsis Diagnosed, Lab Result Recorded, Antibiotic Started, Antibiotic Ended, "
        "Vital Sign Measured. Use condition_occurrence (filter by condition_concept_id "
        "for sepsis SNOMED codes, e.g. 132797, 4103023, 40479642) for the diagnosis "
        "event, measurement for lab results and vital signs (measurement_datetime), "
        "drug_exposure for antibiotic/vasopressor administration "
        "(drug_exposure_start_datetime, drug_exposure_end_datetime), and observation "
        "for clinical observations (observation_datetime). All tables join via "
        "visit_occurrence_id. Use the concept table to resolve concept_ids into "
        "human-readable names. Filter out NULL timestamps."
    ),
    "t4": (
        "Lab-Order-to-Result Cycle: Track the lifecycle of a laboratory test from "
        "specimen collection to result recording. The case_id should be the "
        "visit_occurrence_id. Activities include: Specimen Collected, Lab Result "
        "Recorded, Abnormal Result Flagged. Use specimen for collection events "
        "(specimen_datetime, joined via person_id) and measurement for result events "
        "(measurement_datetime, value_as_number, range_low, range_high). "
        "Flag a result as abnormal when value_as_number is outside range_low–range_high. "
        "Use the concept table to resolve measurement_concept_id and specimen_concept_id "
        "to human-readable names. Filter out NULL timestamps."
    ),
    "t5": (
        "Emergency Department Flow: Track a patient's flow through the emergency "
        "department from arrival to disposition. The case_id should be the "
        "visit_occurrence_id (ED visit). Activities include: ED Arrival, ED Procedure, "
        "ED Diagnosis Recorded, ED Discharge. Filter visit_occurrence to ED visits using "
        "visit_concept_id = 9203 (Emergency Room Visit) or 262 (Emergency Room and "
        "Inpatient Visit). Use visit_detail (filter by visit_detail_concept_id = 8870 "
        "for Emergency Room) for sub-stay events, condition_occurrence for diagnoses "
        "(condition_start_datetime), and procedure_occurrence for procedures "
        "(procedure_datetime). All tables join via visit_occurrence_id. "
        "Filter out NULL timestamps."
    ),
}

# ── T4 worked example (written by Saimir; used as few-shot example) ────────
T4_WORKED_EXAMPLE_SQL = """\
SELECT
    m.visit_occurrence_id AS case_id,
    CASE
        WHEN m.value_as_number < m.range_low OR m.value_as_number > m.range_high
            THEN 'Abnormal Result: ' || c.concept_name
        ELSE 'Lab Result: ' || c.concept_name
    END AS activity,
    m.measurement_datetime AS timestamp
FROM measurement m
JOIN concept c ON m.measurement_concept_id = c.concept_id
WHERE m.measurement_datetime IS NOT NULL
  AND m.visit_occurrence_id IS NOT NULL

UNION ALL

SELECT
    m.visit_occurrence_id AS case_id,
    'Specimen Collected: ' || cs.concept_name AS activity,
    s.specimen_datetime AS timestamp
FROM specimen s
JOIN measurement m
    ON s.person_id = m.person_id
    AND m.measurement_datetime >= s.specimen_datetime
    AND m.measurement_datetime <= s.specimen_datetime + INTERVAL '4 hours'
JOIN concept cs ON s.specimen_concept_id = cs.concept_id
WHERE s.specimen_datetime IS NOT NULL
  AND m.visit_occurrence_id IS NOT NULL

ORDER BY case_id, timestamp;"""

# ⚠️ SYNTHETIC ROWS — These are fabricated for illustration only.
# They do NOT come from the MIMIC-IV database and contain NO real patient data.
# Per the PhysioNet DUA, real MIMIC-IV rows must NEVER be sent to any LLM API.
T4_EXAMPLE_OUTPUT = """\
case_id | activity                           | timestamp
--------+------------------------------------+---------------------
10010   | Specimen Collected: Venous blood   | 2154-06-12 06:15:00
10010   | Lab Result: Creatinine             | 2154-06-12 07:30:00
10010   | Abnormal Result: Potassium         | 2154-06-12 07:32:00
[NOTE: These rows are synthetic examples only — not real patient data]"""

# ── Few-shot example assignment (Section 3.2 of research plan) ─────────────
# Target task → (example_task_id, example_task_description, example_sql, example_output)
FEW_SHOT_EXAMPLES = {
    "t1": "t4",  # ICU pathway ← Lab cycle example
    "t2": "t4",  # Medication  ← Lab cycle example
    "t3": "t1",  # Sepsis      ← ICU pathway example (requires t1 GT, use placeholder)
    "t4": "t5",  # Lab cycle   ← ED flow example   (requires t5 GT, use placeholder)
    "t5": "t1",  # ED flow     ← ICU pathway example
}

# Placeholder for tasks where ground truth is not yet available
PLACEHOLDER_SQL = """\
-- [Ground-truth SQL for this example task will be inserted here]
-- Placeholder: this few-shot example will be completed once
-- Zeinab's ground-truth SQL for this task is available."""

PLACEHOLDER_OUTPUT = """\
case_id | activity              | timestamp
--------+-----------------------+---------------------
[Sample rows will be added when ground-truth SQL is available]"""

def load_schema(task_id):
    path = os.path.join(SCHEMA_DIR, f"{task_id}_*.sql")
    import glob
    files = glob.glob(path)
    if not files:
        raise FileNotFoundError(f"No schema file found for {task_id} in {SCHEMA_DIR}")
    with open(files[0]) as f:
        return f.read()

def get_task_label(task_id):
    labels = {
        "t1": "T1: ICU Patient Pathway",
        "t2": "T2: Medication Administration",
        "t3": "T3: Sepsis Treatment Trajectory",
        "t4": "T4: Lab-Order-to-Result Cycle",
        "t5": "T5: Emergency Department Flow",
    }
    return labels[task_id]

def make_zero_shot(task_id):
    return f"""\
You are a database expert specialising in healthcare data and process mining.
Generate a PostgreSQL SQL query that extracts an event log for the following
clinical process from a healthcare database structured according to the
OMOP Common Data Model (CDM). The underlying data originates from MIMIC-IV.

Process: {TASK_DESCRIPTIONS[task_id]}

The output query MUST produce exactly three columns:
  - case_id   : a unique identifier for each process instance (BIGINT)
  - activity  : a human-readable name for the clinical event (VARCHAR)
  - timestamp : when the event occurred (TIMESTAMP)

Requirements:
  - Use ONLY OMOP CDM table names (e.g. visit_occurrence, drug_exposure,
    measurement). Do NOT use native MIMIC-IV table names (e.g. admissions,
    icustays, labevents).
  - Use UNION ALL to combine events from multiple source tables.
  - Filter out any rows where timestamp IS NULL.
  - Order results by case_id, timestamp.
  - Return ONLY the SQL query — no explanation, no markdown fences.
"""

def make_schema_aware(task_id, schema_ddl):
    return f"""\
You are a database expert specialising in healthcare data and process mining.
Generate a PostgreSQL SQL query that extracts an event log for the following
clinical process from a healthcare database structured according to the
OMOP Common Data Model (CDM). The underlying data originates from MIMIC-IV.

Process: {TASK_DESCRIPTIONS[task_id]}

The relevant OMOP CDM database schema (CREATE TABLE statements) is provided
below. Use ONLY these tables and columns — do not reference any other tables.

{schema_ddl}

The output query MUST produce exactly three columns:
  - case_id   : a unique identifier for each process instance (BIGINT)
  - activity  : a human-readable name for the clinical event (VARCHAR)
  - timestamp : when the event occurred (TIMESTAMP)

Requirements:
  - Use ONLY the tables defined in the schema above.
  - Use UNION ALL to combine events from multiple source tables.
  - Filter out any rows where timestamp IS NULL.
  - Order results by case_id, timestamp.
  - Return ONLY the SQL query — no explanation, no markdown fences.
"""

def make_few_shot(task_id, task_schema_ddl, example_task_id, example_schema_ddl,
                  example_sql, example_output):
    return f"""\
You are a database expert specialising in healthcare data and process mining.
Generate a PostgreSQL SQL query that extracts an event log for the following
clinical process from a healthcare database structured according to the
OMOP Common Data Model (CDM). The underlying data originates from MIMIC-IV.

Below is a complete worked example for a DIFFERENT clinical process, showing
the expected format and approach.

────────────────────────────────────────────
EXAMPLE ({get_task_label(example_task_id)})
────────────────────────────────────────────

Example process: {TASK_DESCRIPTIONS[example_task_id]}

Example OMOP CDM schema:
{example_schema_ddl}

Example SQL:
{example_sql}

Example output (first 3 rows):
{example_output}
────────────────────────────────────────────

Now generate the SQL for THIS process:

Process: {TASK_DESCRIPTIONS[task_id]}

Relevant OMOP CDM schema:
{task_schema_ddl}

The output query MUST produce exactly three columns:
  - case_id   : a unique identifier for each process instance (BIGINT)
  - activity  : a human-readable name for the clinical event (VARCHAR)
  - timestamp : when the event occurred (TIMESTAMP)

Requirements:
  - Use ONLY the tables defined in the schema above.
  - Use UNION ALL to combine events from multiple source tables.
  - Filter out any rows where timestamp IS NULL.
  - Order results by case_id, timestamp.
  - Return ONLY the SQL query — no explanation, no markdown fences.
"""

def main():
    tasks = ["t1", "t2", "t3", "t4", "t5"]

    # Pre-load all schemas
    schemas = {t: load_schema(t) for t in tasks}

    # Resolve few-shot examples
    few_shot_sql = {
        "t4": (T4_WORKED_EXAMPLE_SQL, T4_EXAMPLE_OUTPUT),
    }
    # For tasks whose example is not yet available, use placeholder
    for t in tasks:
        ex = FEW_SHOT_EXAMPLES[t]
        if ex not in few_shot_sql:
            few_shot_sql[ex] = (PLACEHOLDER_SQL, PLACEHOLDER_OUTPUT)

    generated = []
    for task_id in tasks:
        out_dir = os.path.join(PROMPTS_DIR, task_id)
        os.makedirs(out_dir, exist_ok=True)

        schema_ddl = schemas[task_id]
        ex_task_id = FEW_SHOT_EXAMPLES[task_id]
        ex_schema   = schemas[ex_task_id]
        ex_sql, ex_out = few_shot_sql.get(ex_task_id, (PLACEHOLDER_SQL, PLACEHOLDER_OUTPUT))

        prompts = {
            "zero_shot":    make_zero_shot(task_id),
            "schema_aware": make_schema_aware(task_id, schema_ddl),
            "few_shot":     make_few_shot(task_id, schema_ddl, ex_task_id,
                                          ex_schema, ex_sql, ex_out),
        }

        for strategy, content in prompts.items():
            fpath = os.path.join(out_dir, f"{strategy}.txt")
            with open(fpath, "w") as f:
                f.write(content)
            generated.append(fpath)
            print(f"  ✓ {task_id}/{strategy}.txt  ({len(content):,} chars)")

    print(f"\nDone. {len(generated)} prompt files generated in {PROMPTS_DIR}/")

if __name__ == "__main__":
    main()
