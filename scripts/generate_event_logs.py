#!/usr/bin/env python3
"""
generate_event_logs.py  —  ELG × AI × Healthcare | BPM 2026
============================================================

Generates XES-format event logs for the five clinical process tasks (T1–T5)
from the MIMIC-IV OMOP CDM 888-patient sample data stored as local CSV files.

The SQL logic is a faithful port of the ground-truth queries in
    experiment/ground_truth/t{N}_gt.sql
executed in-memory via DuckDB over the CSV files in experiment/sample_data/.

Usage
-----
    cd experiment/
    python scripts/generate_event_logs.py                  # all tasks
    python scripts/generate_event_logs.py --tasks t1 t5    # subset
    python scripts/generate_event_logs.py --max-cases 100  # quick test

Output
------
    experiment/event_logs/
        t1_icu_pathway.xes
        t2_medication_admin.xes
        t3_sepsis_trajectory.xes   (sampled to MAX_CASES_T3 by default)
        t4_lab_cycle.xes           (sampled to MAX_CASES_T4 by default)
        t5_ed_flow.xes

Notes
-----
* T3 (Sepsis) and T4 (Lab Cycle) can produce millions of events for all 888
  patients because the measurement table has ~2.9M rows.  By default the
  scripts retains only the first MAX_CASES cases (sorted ascending by
  case/visit id) to keep XES files manageable for process mining.
  Pass --max-cases 0 to disable sampling.

* DUA compliance: this script reads only local CSV files; no patient data
  is transmitted to any external service.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

import duckdb
import pandas as pd
import pm4py

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Default paths  (relative to experiment/)
# ─────────────────────────────────────────────────────────────────────────────
_SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
_EXPERIMENT_DIR = os.path.dirname(_SCRIPT_DIR)

DEFAULT_SAMPLE_DIR = os.path.join(_EXPERIMENT_DIR, "sample_data")
DEFAULT_OUTPUT_DIR = os.path.join(_EXPERIMENT_DIR, "event_logs")

# ─────────────────────────────────────────────────────────────────────────────
# OMOP CDM tables present in sample_data/
# ─────────────────────────────────────────────────────────────────────────────
_TABLES = [
    "person",
    "visit_occurrence",
    "visit_detail",
    "condition_occurrence",
    "drug_exposure",
    "drug_era",
    "measurement",
    "observation",
    "procedure_occurrence",
    "specimen",
    "concept",
    "concept_ancestor",
]

# ─────────────────────────────────────────────────────────────────────────────
# Ground-truth SQL (mirrors experiment/ground_truth/t{N}_gt.sql)
# ─────────────────────────────────────────────────────────────────────────────

T1_SQL = """
-- ══════════════════════════════════════════════════════════════════════════
-- T1: ICU Patient Pathway
-- Activities: Hospital Admission → ICU Admission → ICU Discharge
--             → ICU Procedure → Hospital Discharge
-- Case ID:    visit_occurrence_id
-- ══════════════════════════════════════════════════════════════════════════

SELECT vo.visit_occurrence_id  AS case_id,
       'Hospital Admission'    AS activity,
       vo.visit_start_datetime AS timestamp
FROM   visit_occurrence vo
WHERE  vo.visit_start_datetime IS NOT NULL
  AND  vo.visit_concept_id IN (9201, 262)      -- Inpatient Visit, ER+Inpatient

UNION ALL

SELECT vd.visit_occurrence_id       AS case_id,
       'ICU Admission: ' || c.concept_name AS activity,
       vd.visit_detail_start_datetime      AS timestamp
FROM   visit_detail vd
JOIN   concept c ON vd.visit_detail_concept_id = c.concept_id
WHERE  vd.visit_detail_start_datetime IS NOT NULL
  AND  vd.visit_detail_concept_id = 32037       -- Intensive Care

UNION ALL

SELECT vd.visit_occurrence_id       AS case_id,
       'ICU Discharge'              AS activity,
       vd.visit_detail_end_datetime AS timestamp
FROM   visit_detail vd
WHERE  vd.visit_detail_end_datetime IS NOT NULL
  AND  vd.visit_detail_concept_id = 32037

UNION ALL

SELECT po.visit_occurrence_id                AS case_id,
       'ICU Procedure: ' || c.concept_name  AS activity,
       po.procedure_datetime                AS timestamp
FROM   procedure_occurrence po
JOIN   concept c ON po.procedure_concept_id = c.concept_id
WHERE  po.procedure_datetime    IS NOT NULL
  AND  po.visit_occurrence_id   IS NOT NULL
  AND  po.visit_occurrence_id IN (
           SELECT DISTINCT vd2.visit_occurrence_id
           FROM   visit_detail vd2
           WHERE  vd2.visit_detail_concept_id = 32037
       )

UNION ALL

SELECT vo.visit_occurrence_id AS case_id,
       'Hospital Discharge'   AS activity,
       vo.visit_end_datetime  AS timestamp
FROM   visit_occurrence vo
WHERE  vo.visit_end_datetime IS NOT NULL
  AND  vo.visit_concept_id IN (9201, 262)

ORDER BY case_id, timestamp
"""

# ─────────────────────────────────────────────────────────────────────────────

T2_SQL = """
-- ══════════════════════════════════════════════════════════════════════════
-- T2: Medication Administration
-- Activities: Drug Started → Drug Ended  (labelled with concept name)
-- Case ID:    visit_occurrence_id
-- ══════════════════════════════════════════════════════════════════════════

SELECT de.visit_occurrence_id              AS case_id,
       'Drug Started: ' || c.concept_name AS activity,
       de.drug_exposure_start_datetime     AS timestamp
FROM   drug_exposure de
JOIN   concept c ON de.drug_concept_id = c.concept_id
WHERE  de.drug_exposure_start_datetime IS NOT NULL
  AND  de.visit_occurrence_id          IS NOT NULL

UNION ALL

SELECT de.visit_occurrence_id            AS case_id,
       'Drug Ended: ' || c.concept_name AS activity,
       de.drug_exposure_end_datetime     AS timestamp
FROM   drug_exposure de
JOIN   concept c ON de.drug_concept_id = c.concept_id
WHERE  de.drug_exposure_end_datetime IS NOT NULL
  AND  de.visit_occurrence_id        IS NOT NULL

ORDER BY case_id, timestamp
"""

# ─────────────────────────────────────────────────────────────────────────────
# T3 — Sepsis concept list (129 SNOMED codes, expanded by a domain expert via
#       concept_ancestor traversal from core sepsis codes)
# ─────────────────────────────────────────────────────────────────────────────

_SEPSIS_CONCEPT_IDS = (
    132797, 40487101, 40484176, 37017557, 44784136, 44782822, 44805136,
    37394658, 46284901, 4000938, 4102318, 4029281, 4285746, 37163133,
    42690418, 46284902, 35622880, 35622881, 4111261, 133594, 37163131,
    4009954, 44784138, 36674642, 4071063, 4029251, 40486058, 44782631,
    37164410, 40487062, 40487063, 40486629, 607321, 40489980, 37018498,
    607325, 44782630, 37312594, 607320, 37167232, 37167199, 37163129,
    37151377, 37163132, 40491961, 40493039, 40493415, 40487059, 37116435,
    46269944, 46269946, 37016131, 46270051, 40489908, 37163128, 37162933,
    40489979, 40486685, 42539372, 36715806, 36715567, 602996, 40489913,
    36717263, 42538750, 37018499, 40487064, 40489907, 46269807, 40491523,
    4154698, 37163134, 45757198, 40489912, 40487616, 37163130, 40486631,
    40486059, 40489909, 761851, 761852, 46284320, 46287153, 37163135,
    44784137, 37168933, 45768767, 1244226, 4048275, 37019087, 37162984,
    36716312, 37017566, 40493038, 40489910, 40487617, 603033, 763165,
    37395591, 36715430, 36684427, 1075272, 4073090, 4071727, 4048594,
    42536689, 42536690, 40487662, 40491960, 763027, 3655975, 46270052,
    46270041, 1076394, 36716754, 760987, 4197963, 760981, 760984,
    37018755, 37395517, 37395520, 37175321, 4103655, 37164448, 4124677,
    4121450, 3655135, 4028062,
)
_SEPSIS_IDS_SQL = ", ".join(str(x) for x in _SEPSIS_CONCEPT_IDS)

T3_SQL = f"""
-- ══════════════════════════════════════════════════════════════════════════
-- T3: Sepsis Treatment Trajectory
-- Activities: Sepsis Diagnosed → Lab Result → Drug Started → Observation
-- Case ID:    visit_occurrence_id  (sepsis visits only)
-- ══════════════════════════════════════════════════════════════════════════

WITH sepsis_visits AS (
    SELECT DISTINCT co.visit_occurrence_id
    FROM   condition_occurrence co
    WHERE  co.condition_concept_id IN ({_SEPSIS_IDS_SQL})
      AND  co.visit_occurrence_id IS NOT NULL
)

SELECT co.visit_occurrence_id                  AS case_id,
       'Sepsis Diagnosed: ' || c.concept_name AS activity,
       co.condition_start_datetime             AS timestamp
FROM   condition_occurrence co
JOIN   concept c ON co.condition_concept_id = c.concept_id
WHERE  co.condition_concept_id IN ({_SEPSIS_IDS_SQL})
  AND  co.condition_start_datetime IS NOT NULL
  AND  co.visit_occurrence_id      IS NOT NULL

UNION ALL

-- Lab results: annotate abnormal values (value outside reference range)
SELECT m.visit_occurrence_id AS case_id,
       'Lab Result: ' || c.concept_name
           || CASE
                WHEN m.value_as_number IS NOT NULL
                 AND m.range_low        IS NOT NULL
                 AND m.range_high       IS NOT NULL
                 AND (   m.value_as_number < m.range_low
                      OR m.value_as_number > m.range_high)
                THEN ' [ABNORMAL]'
                ELSE ''
              END             AS activity,
       m.measurement_datetime AS timestamp
FROM   measurement m
JOIN   concept c ON m.measurement_concept_id = c.concept_id
WHERE  m.visit_occurrence_id IN (SELECT visit_occurrence_id FROM sepsis_visits)
  AND  m.measurement_datetime IS NOT NULL

UNION ALL

SELECT de.visit_occurrence_id              AS case_id,
       'Drug Started: ' || c.concept_name AS activity,
       de.drug_exposure_start_datetime     AS timestamp
FROM   drug_exposure de
JOIN   concept c ON de.drug_concept_id = c.concept_id
WHERE  de.visit_occurrence_id IN (SELECT visit_occurrence_id FROM sepsis_visits)
  AND  de.drug_exposure_start_datetime IS NOT NULL

UNION ALL

SELECT o.visit_occurrence_id              AS case_id,
       'Observation: ' || c.concept_name AS activity,
       o.observation_datetime             AS timestamp
FROM   observation o
JOIN   concept c ON o.observation_concept_id = c.concept_id
WHERE  o.visit_occurrence_id IN (SELECT visit_occurrence_id FROM sepsis_visits)
  AND  o.observation_datetime IS NOT NULL

ORDER BY case_id, timestamp
"""

# ─────────────────────────────────────────────────────────────────────────────

T4_SQL = """
-- ══════════════════════════════════════════════════════════════════════════
-- T4: Lab-Order-to-Result Cycle
-- Activities: Specimen Collected → Lab Result (normal) → Abnormal Result
-- Case ID:    visit_occurrence_id
-- Note:       specimen has no visit_occurrence_id FK; linked via
--             person_id + temporal window (specimen within visit dates)
-- ══════════════════════════════════════════════════════════════════════════

SELECT vo.visit_occurrence_id                    AS case_id,
       'Specimen Collected: ' || c.concept_name AS activity,
       s.specimen_datetime                       AS timestamp
FROM   specimen s
JOIN   visit_occurrence vo
         ON  s.person_id         = vo.person_id
         AND s.specimen_datetime >= vo.visit_start_datetime
         AND s.specimen_datetime <= vo.visit_end_datetime
JOIN   concept c ON s.specimen_concept_id = c.concept_id
WHERE  s.specimen_datetime IS NOT NULL

UNION ALL

-- Normal lab results (value within reference range, or range not available)
SELECT m.visit_occurrence_id             AS case_id,
       'Lab Result: ' || c.concept_name AS activity,
       m.measurement_datetime            AS timestamp
FROM   measurement m
JOIN   concept c ON m.measurement_concept_id = c.concept_id
WHERE  m.measurement_datetime IS NOT NULL
  AND  m.visit_occurrence_id  IS NOT NULL
  AND  (   m.value_as_number IS NULL
        OR m.range_low        IS NULL
        OR m.range_high       IS NULL
        OR (m.value_as_number >= m.range_low
            AND m.value_as_number <= m.range_high))

UNION ALL

-- Abnormal lab results (value outside reference range)
SELECT m.visit_occurrence_id                  AS case_id,
       'Abnormal Result: ' || c.concept_name AS activity,
       m.measurement_datetime                 AS timestamp
FROM   measurement m
JOIN   concept c ON m.measurement_concept_id = c.concept_id
WHERE  m.measurement_datetime IS NOT NULL
  AND  m.visit_occurrence_id  IS NOT NULL
  AND  m.value_as_number IS NOT NULL
  AND  m.range_low        IS NOT NULL
  AND  m.range_high       IS NOT NULL
  AND  (m.value_as_number < m.range_low OR m.value_as_number > m.range_high)

ORDER BY case_id, timestamp
"""

# ─────────────────────────────────────────────────────────────────────────────

T5_SQL = """
-- ══════════════════════════════════════════════════════════════════════════
-- T5: Emergency Department Flow
-- Activities: ED Arrival → ED Sub-Stay → ED Diagnosis → ED Procedure
--             → ED Departure
-- Case ID:    visit_occurrence_id  (ED visits only)
-- ══════════════════════════════════════════════════════════════════════════

SELECT vo.visit_occurrence_id  AS case_id,
       'ED Arrival'            AS activity,
       vo.visit_start_datetime AS timestamp
FROM   visit_occurrence vo
WHERE  vo.visit_concept_id IN (9203, 262)    -- Emergency Room Visit, ER+Inpatient
  AND  vo.visit_start_datetime IS NOT NULL

UNION ALL

SELECT vd.visit_occurrence_id        AS case_id,
       'ED Sub-Stay: ' || c.concept_name AS activity,
       vd.visit_detail_start_datetime    AS timestamp
FROM   visit_detail vd
JOIN   concept c ON vd.visit_detail_concept_id = c.concept_id
WHERE  vd.visit_detail_start_datetime IS NOT NULL
  AND  vd.visit_occurrence_id IN (
           SELECT vo2.visit_occurrence_id
           FROM   visit_occurrence vo2
           WHERE  vo2.visit_concept_id IN (9203, 262)
       )

UNION ALL

SELECT co.visit_occurrence_id                AS case_id,
       'ED Diagnosis: ' || c.concept_name  AS activity,
       co.condition_start_datetime          AS timestamp
FROM   condition_occurrence co
JOIN   concept c ON co.condition_concept_id = c.concept_id
WHERE  co.condition_start_datetime IS NOT NULL
  AND  co.visit_occurrence_id IN (
           SELECT vo2.visit_occurrence_id
           FROM   visit_occurrence vo2
           WHERE  vo2.visit_concept_id IN (9203, 262)
       )

UNION ALL

SELECT po.visit_occurrence_id                AS case_id,
       'ED Procedure: ' || c.concept_name  AS activity,
       po.procedure_datetime               AS timestamp
FROM   procedure_occurrence po
JOIN   concept c ON po.procedure_concept_id = c.concept_id
WHERE  po.procedure_datetime IS NOT NULL
  AND  po.visit_occurrence_id IN (
           SELECT vo2.visit_occurrence_id
           FROM   visit_occurrence vo2
           WHERE  vo2.visit_concept_id IN (9203, 262)
       )

UNION ALL

SELECT vo.visit_occurrence_id AS case_id,
       'ED Departure'         AS activity,
       vo.visit_end_datetime  AS timestamp
FROM   visit_occurrence vo
WHERE  vo.visit_concept_id IN (9203, 262)
  AND  vo.visit_end_datetime IS NOT NULL

ORDER BY case_id, timestamp
"""

# ─────────────────────────────────────────────────────────────────────────────
# Task registry — default max_cases for large tasks keeps XES manageable
# Set max_cases=None to export the full sample
# ─────────────────────────────────────────────────────────────────────────────
TASKS: dict[str, dict] = {
    "t1": dict(name="icu_pathway",        sql=T1_SQL, max_cases=None),
    "t2": dict(name="medication_admin",   sql=T2_SQL, max_cases=None),
    "t3": dict(name="sepsis_trajectory",  sql=T3_SQL, max_cases=500),
    "t4": dict(name="lab_cycle",          sql=T4_SQL, max_cases=500),
    "t5": dict(name="ed_flow",            sql=T5_SQL, max_cases=None),
}


# ─────────────────────────────────────────────────────────────────────────────
# DuckDB helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_duckdb_connection(sample_dir: str) -> duckdb.DuckDBPyConnection:
    """
    Load all OMOP CDM CSV files from *sample_dir* into an in-memory DuckDB
    instance and return the open connection.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("SET memory_limit='4GB'")
    conn.execute("SET threads=4")

    logger.info("Loading CSV tables from: %s", sample_dir)
    for table in _TABLES:
        csv_path = os.path.join(sample_dir, f"{table}.csv")
        if not os.path.exists(csv_path):
            logger.warning("  %-28s  ⚠  file not found, skipping", table)
            continue
        conn.execute(
            f"CREATE TABLE {table} AS "
            f"SELECT * FROM read_csv_auto('{csv_path}', header=True)"
        )
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info("  %-28s  %10s rows", table, f"{n:,}")

    return conn


def run_task_sql(
    conn: duckdb.DuckDBPyConnection,
    task_id: str,
    sql: str,
    max_cases: Optional[int],
) -> pd.DataFrame:
    """
    Execute the ground-truth SQL for *task_id* via *conn*, apply optional
    deterministic case sampling, and return a clean DataFrame with columns
    (case_id: str, activity: str, timestamp: datetime[UTC]).
    """
    logger.info("  Executing SQL …")
    df = conn.execute(sql).df()

    if df.empty:
        logger.warning("  No rows returned for %s", task_id.upper())
        return df

    # Coerce types
    df["case_id"]   = df["case_id"].astype(str)
    df["activity"]  = df["activity"].fillna("(unknown)").astype(str)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    logger.info("  Raw result: %s rows", f"{len(df):,}")

    # Deterministic case-level sampling: retain the first max_cases case IDs
    # (sorted ascending) so that results are reproducible across runs.
    if max_cases and max_cases > 0:
        all_cases = sorted(df["case_id"].unique())
        if len(all_cases) > max_cases:
            keep = set(all_cases[:max_cases])
            df   = df[df["case_id"].isin(keep)].copy()
            logger.info(
                "  Sampled to first %d cases → %s rows",
                max_cases, f"{len(df):,}",
            )

    df = df.sort_values(["case_id", "timestamp"]).reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# XES export
# ─────────────────────────────────────────────────────────────────────────────

def export_xes(df: pd.DataFrame, xes_path: str) -> "pm4py.objects.log.obj.EventLog":
    """
    Convert a (case_id, activity, timestamp) DataFrame to a PM4PY EventLog
    object and write it as a standards-compliant XES file.

    Uses pm4py.format_dataframe (PM4PY ≥ 2.7) so that case grouping is
    applied correctly before conversion.

    Returns the in-memory EventLog for immediate downstream use (e.g. process
    mining in the same Python session).
    """
    # Step 1 — rename to PM4PY canonical column names
    pm_df = df.rename(columns={
        "case_id":   "case:concept:name",
        "activity":  "concept:name",
        "timestamp": "time:timestamp",
    }).copy()

    # Step 2 — ensure timestamps are timezone-aware UTC
    pm_df["time:timestamp"] = pd.to_datetime(
        pm_df["time:timestamp"], utc=True
    )

    # Step 3 — format + convert (format_dataframe registers the case/activity/
    #           timestamp roles so that the converter groups events correctly)
    pm_df = pm4py.format_dataframe(
        pm_df,
        case_id="case:concept:name",
        activity_key="concept:name",
        timestamp_key="time:timestamp",
    )
    event_log = pm4py.convert_to_event_log(pm_df)

    os.makedirs(os.path.dirname(xes_path), exist_ok=True)
    pm4py.write_xes(event_log, xes_path)

    n_cases      = pm_df["case:concept:name"].nunique()
    n_events     = len(pm_df)
    n_activities = pm_df["concept:name"].nunique()

    logger.info(
        "  ✓ Saved  %-42s  cases: %6s  events: %9s  activities: %4s",
        os.path.basename(xes_path),
        f"{n_cases:,}", f"{n_events:,}", f"{n_activities:,}",
    )
    return event_log


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def generate_all(
    sample_dir: str = DEFAULT_SAMPLE_DIR,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    tasks: Optional[list[str]] = None,
    max_cases_override: Optional[int] = None,
) -> dict[str, dict]:
    """
    Generate XES event logs for the specified tasks and return a summary dict.

    Parameters
    ----------
    sample_dir : str
        Directory containing the 12 OMOP CDM CSV files.
    output_dir : str
        Directory where XES files will be written.
    tasks : list[str] or None
        Task IDs to process (e.g. ['t1', 't3']).  Defaults to all five.
    max_cases_override : int or None
        If set, overrides the per-task max_cases default for every task.
        Pass 0 to disable sampling entirely.

    Returns
    -------
    dict  keyed by task_id, values are dicts with keys:
          xes_path, n_cases, n_events, n_activities, event_log
    """
    tasks = tasks or list(TASKS.keys())
    os.makedirs(output_dir, exist_ok=True)

    logger.info("═" * 65)
    logger.info("  ELG × AI — XES Event Log Generation (BPM 2026)")
    logger.info("═" * 65)
    logger.info("  Sample dir  : %s", sample_dir)
    logger.info("  Output dir  : %s", output_dir)
    logger.info("  Tasks       : %s", ", ".join(tasks))

    conn    = build_duckdb_connection(sample_dir)
    results = {}

    for task_id in tasks:
        if task_id not in TASKS:
            logger.warning("Unknown task '%s', skipping.", task_id)
            continue

        cfg = TASKS[task_id]

        if max_cases_override is not None:
            max_c = max_cases_override if max_cases_override > 0 else None
        else:
            max_c = cfg["max_cases"]

        logger.info("")
        logger.info("─── %s: %s %s",
                    task_id.upper(), cfg["name"],
                    f"(max_cases={max_c})" if max_c else "(no sampling)")

        df = run_task_sql(conn, task_id, cfg["sql"], max_c)
        if df.empty:
            continue

        xes_path = os.path.join(output_dir, f"{task_id}_{cfg['name']}.xes")
        el = export_xes(df, xes_path)

        results[task_id] = {
            "xes_path":     xes_path,
            "n_cases":      df["case_id"].nunique(),
            "n_events":     len(df),
            "n_activities": df["activity"].nunique(),
            "event_log":    el,
        }

    conn.close()

    # ── Summary table ──────────────────────────────────────────────────────
    logger.info("")
    logger.info("═" * 65)
    logger.info("  Summary")
    logger.info("═" * 65)
    logger.info("  %-4s  %-22s  %8s  %10s  %10s",
                "Task", "Name", "Cases", "Events", "Activities")
    logger.info("  " + "-" * 60)
    for tid, r in results.items():
        logger.info(
            "  %-4s  %-22s  %8s  %10s  %10s",
            tid.upper(), TASKS[tid]["name"],
            f"{r['n_cases']:,}", f"{r['n_events']:,}", f"{r['n_activities']:,}",
        )
    logger.info("═" * 65)
    logger.info("  XES files written to: %s", output_dir)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate XES event logs from MIMIC-IV OMOP CDM sample CSVs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--sample-dir",
        default=DEFAULT_SAMPLE_DIR,
        help="CSV directory (default: experiment/sample_data/)",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="XES output directory (default: experiment/event_logs/)",
    )
    parser.add_argument(
        "--tasks",
        nargs="+",
        choices=list(TASKS.keys()),
        default=list(TASKS.keys()),
        metavar="TASK",
        help="Tasks to generate (t1 t2 t3 t4 t5; default: all)",
    )
    parser.add_argument(
        "--max-cases",
        type=int,
        default=None,
        metavar="N",
        help="Override max_cases for all tasks (0 = no limit; default: per-task)",
    )
    args = parser.parse_args()

    generate_all(
        sample_dir=args.sample_dir,
        output_dir=args.output_dir,
        tasks=args.tasks,
        max_cases_override=args.max_cases,
    )


if __name__ == "__main__":
    main()
