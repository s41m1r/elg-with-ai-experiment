#!/usr/bin/env python3
"""
evaluate_metrics.py — M3/M4/M5 Evaluation for LLM-Generated OMOP CDM SQL

Computes:
  M3 — Event Log Completeness
       M3a: case coverage       = |cases_llm ∩ cases_gt| / |cases_gt|
       M3b: activity coverage   = |activities_llm ∩ activities_gt| / |activities_gt|
       M3c: row count ratio     = |rows_llm| / |rows_gt|
  M4 — Process Fidelity
       M4a: activity Jaccard    = |A_llm ∩ A_gt| / |A_llm ∪ A_gt|
  M5 — Prompt Sensitivity
       For each (LLM, task): std of M1, M3a, M3b, M4a across S1/S2/S3

Prerequisite: M1/M2 results must exist in results/executability.csv
              Ground-truth SQL files must be in ground_truth/

⚠️  DUA COMPLIANCE: All execution is LOCAL. No patient data leaves the
    environment. Only SQL structure is sent to LLM APIs (done separately).

Usage:
    # Run against DuckDB demo data (default)
    python evaluate_metrics.py --db duckdb \\
        --duckdb-csv-dir ../../mimic-iv-demo-data-in-the-omop-common-data-model-0.9/1_omop_data_csv

    # Run against PostgreSQL (full MIMIC-IV OMOP CDM)
    python evaluate_metrics.py --db postgres \\
        --pg-conn "host=localhost dbname=mimic_omop user=postgres"

    # Dry run (show what would be evaluated, no DB needed)
    python evaluate_metrics.py --dry-run

    # Skip M4b trace similarity (faster)
    python evaluate_metrics.py --db duckdb \\
        --duckdb-csv-dir ../../mimic-iv-demo-data-in-the-omop-common-data-model-0.9/1_omop_data_csv \\
        --no-m4b

Outputs (written to results/):
    completeness.csv   — M3a, M3b, M3c per (task, llm, strategy, repetition)
    fidelity.csv       — M4a, M4b per (task, llm, strategy, repetition)
    sensitivity.csv    — M5 per (task, llm)
    summary.csv        — All metrics joined in one table
"""

import argparse
import csv
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
EXPERIMENT_DIR = SCRIPT_DIR.parent
OUTPUTS_DIR = EXPERIMENT_DIR / "outputs"
RESULTS_DIR = EXPERIMENT_DIR / "results"
GT_DIR = EXPERIMENT_DIR / "ground_truth"

TASKS = ["t1", "t2", "t3", "t4", "t5"]
LLMS = ["gpt4o", "claude", "llama3"]
STRATEGIES = ["zero_shot", "schema_aware", "few_shot"]
REPETITIONS = [1, 2, 3]

SQL_TIMEOUT = 600  # seconds (full MIMIC-IV — T3 can take 7+ min)

# ---------------------------------------------------------------------------
# Database connection helpers (mirrors execute_sql.py)
# ---------------------------------------------------------------------------

def connect_duckdb(csv_dir: str):
    """Load OMOP demo CSVs into an in-memory DuckDB instance."""
    try:
        import duckdb
    except ImportError:
        print("Error: duckdb not installed. Run: pip install duckdb --break-system-packages")
        sys.exit(1)

    conn = duckdb.connect(":memory:")
    # Cap memory so Cartesian-product queries throw an error instead of OOM-killing
    # the process. 3 GB is generous for 888-patient sample data.
    conn.execute("SET memory_limit='3GB'")
    conn.execute("SET threads=4")
    csv_path = Path(csv_dir)
    if not csv_path.exists():
        print(f"Error: CSV directory not found: {csv_dir}")
        sys.exit(1)

    loaded = []
    for csv_file in sorted(csv_path.glob("*.csv")):
        # Derive table name: strip leading "2b_" prefix (concept files)
        table = csv_file.stem
        if table.startswith("2b_"):
            table = table[3:]
        try:
            conn.execute(
                f"CREATE OR REPLACE VIEW {table} AS "
                f"SELECT * FROM read_csv_auto('{str(csv_file)}', sample_size=-1)"
            )
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            loaded.append(f"{table} ({count:,} rows)")
        except Exception as e:
            print(f"  [warn] Could not load {csv_file.name}: {e}")

    print(f"DuckDB: loaded {len(loaded)} tables from {csv_dir}")
    return conn, "duckdb"


def connect_postgres(conn_string: str):
    """Connect to a PostgreSQL instance."""
    try:
        import psycopg2
    except ImportError:
        print("Error: psycopg2 not installed. Run: pip install psycopg2-binary --break-system-packages")
        sys.exit(1)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = '{SQL_TIMEOUT * 1000}'")  # ms
        cur.execute("SET search_path TO cdm, public")  # OMOP tables in 'cdm' schema
    print(f"PostgreSQL: connected ({conn_string[:60]}...)")
    print(f"  search_path=cdm,public | statement_timeout={SQL_TIMEOUT}s")
    return conn, "postgres"


# ---------------------------------------------------------------------------
# SQL execution helper
# ---------------------------------------------------------------------------

def run_sql(conn, db_type: str, sql: str, label: str = "",
            m4b_sample: int = 50) -> dict:
    """
    Execute SQL efficiently without fetching all rows into memory.

    Strategy: wrap the SQL as a CTE and run targeted sub-queries:
      1. COUNT(*)                         → row_count (M3c)
      2. SELECT DISTINCT case_id          → case_ids  (M3a)
      3. SELECT DISTINCT activity         → activities (M3b, M4a)
      4. Sample m4b_sample cases + traces → for M4b (optional)

    This avoids loading millions of rows for large result sets.
    """
    # Wrap in a CTE to avoid re-executing the SQL multiple times
    # DuckDB and PostgreSQL both support this pattern.
    cte = f"WITH _log AS (\n{sql}\n)"

    def execute(query):
        if db_type == "duckdb":
            return conn.execute(query).fetchall()
        else:
            cur = conn.cursor()
            cur.execute(query)
            rows = cur.fetchall()
            cur.close()
            return rows

    try:
        if db_type == "postgres":
            # For PostgreSQL: materialize into a TEMP TABLE to execute the SQL
            # only once. This is critical for large queries (T3: 44M rows,
            # T2: 30M rows) to avoid 4x re-execution via CTE.
            import re as _re
            sql_clean = sql.rstrip().rstrip(";")
            # Strip trailing ORDER BY — unneeded for aggregation and slows things down
            sql_no_order = _re.sub(
                r'\bORDER\s+BY\b[^;]*$', '', sql_clean,
                flags=_re.IGNORECASE | _re.DOTALL
            ).strip()
            cur = conn.cursor()
            # Create temp table (unique name per call to avoid collisions)
            import time as _time
            tname = f"_elg_tmp_{int(_time.time() * 1000) % 100000}"
            cur.execute(f"CREATE TEMP TABLE {tname} AS {sql_no_order}")
            cur.close()

            def execute_tmp(query):
                cur2 = conn.cursor()
                cur2.execute(query.replace("_log", tname))
                rows = cur2.fetchall()
                cur2.close()
                return rows

            count_rows = execute_tmp("SELECT COUNT(*) FROM _log")
            row_count = count_rows[0][0] if count_rows else 0
            case_rows = execute_tmp(
                "SELECT DISTINCT case_id FROM _log WHERE case_id IS NOT NULL"
            )
            case_ids = set(r[0] for r in case_rows)
            act_rows = execute_tmp(
                "SELECT DISTINCT activity FROM _log WHERE activity IS NOT NULL"
            )
            activities = set(str(r[0]).strip() for r in act_rows)

            sample_rows = []
            if m4b_sample > 0 and case_ids:
                sample_case_ids = list(case_ids)[:m4b_sample]
                if sample_case_ids:
                    id_list = ", ".join(
                        str(c) if isinstance(c, (int, float)) else f"'{c}'"
                        for c in sample_case_ids
                    )
                    try:
                        sample_rows = execute_tmp(
                            f"SELECT case_id, activity, timestamp FROM _log "
                            f"WHERE case_id IN ({id_list}) "
                            f"ORDER BY case_id, timestamp"
                        )
                    except Exception:
                        sample_rows = []

            # Clean up temp table
            try:
                cur3 = conn.cursor()
                cur3.execute(f"DROP TABLE IF EXISTS {tname}")
                cur3.close()
            except Exception:
                pass

        else:
            # DuckDB: materialize into a temp table to avoid re-executing SQL 4×.
            # Critical for large queries (T3: 1.4M rows, T4: 3M rows on sample data).
            import re as _re
            import time as _time
            tname = f"_elg_tmp_{int(_time.time() * 1000) % 100000}"
            # Strip trailing ORDER BY before materialization (irrelevant for aggregates)
            sql_no_order = _re.sub(
                r'\bORDER\s+BY\b[^;]*$', '', sql.rstrip().rstrip(";"),
                flags=_re.IGNORECASE | _re.DOTALL
            ).strip()
            conn.execute(f"CREATE OR REPLACE TABLE {tname} AS ({sql_no_order})")

            def execute_tmp(q):
                return conn.execute(q.replace("_log", tname)).fetchall()

            # 1. Row count
            count_rows = execute_tmp("SELECT COUNT(*) FROM _log")
            row_count = count_rows[0][0] if count_rows else 0

            # 2. Distinct case_ids
            case_rows = execute_tmp(
                "SELECT DISTINCT case_id FROM _log WHERE case_id IS NOT NULL"
            )
            case_ids = set(r[0] for r in case_rows)

            # 3. Distinct activity labels
            act_rows = execute_tmp(
                "SELECT DISTINCT activity FROM _log WHERE activity IS NOT NULL"
            )
            activities = set(str(r[0]).strip() for r in act_rows)

            # 4. Sample traces for M4b (small number of cases only)
            sample_rows = []
            if m4b_sample > 0 and case_ids:
                sample_case_ids = list(case_ids)[:m4b_sample]
                if sample_case_ids:
                    id_list = ", ".join(
                        str(c) if isinstance(c, (int, float)) else f"'{c}'"
                        for c in sample_case_ids
                    )
                    try:
                        sample_rows = execute_tmp(
                            f"SELECT case_id, activity, timestamp FROM _log "
                            f"WHERE case_id IN ({id_list}) "
                            f"ORDER BY case_id, timestamp"
                        )
                    except Exception:
                        sample_rows = []  # M4b is optional; don't fail on this

            # Clean up temp table
            try:
                conn.execute(f"DROP TABLE IF EXISTS {tname}")
            except Exception:
                pass

        return {
            "success": True,
            "rows": sample_rows,   # only sample rows (for M4b)
            "case_ids": case_ids,
            "activities": activities,
            "row_count": row_count,
            "error_msg": "",
        }
    except Exception as e:
        return {
            "success": False,
            "rows": [],
            "case_ids": set(),
            "activities": set(),
            "row_count": 0,
            "error_msg": str(e),
        }


def read_sql_file(path: Path) -> str:
    """Read a .sql file, stripping markdown fences and trailing semicolons."""
    text = path.read_text(encoding="utf-8").strip()
    # Strip ```sql ... ``` fences
    if text.startswith("```"):
        lines = text.splitlines()
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    # Strip trailing semicolons (they break CTE wrapping)
    text = text.rstrip(";").rstrip()
    return text


# ---------------------------------------------------------------------------
# Load M1 results from executability.csv
# ---------------------------------------------------------------------------

def load_m1_results(csv_path: Path) -> dict:
    """
    Returns a dict keyed by (task, llm, strategy, repetition) -> {m1, row_count}.
    Only includes LLM-sourced rows (source == 'llm').
    """
    results = {}
    if not csv_path.exists():
        print(f"Warning: {csv_path} not found. M1 data unavailable.")
        return results
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("source", "llm") != "llm":
                continue
            key = (
                row["task"],
                row["llm"],
                row["strategy"],
                int(row["repetition"]),
            )
            results[key] = {
                "m1": int(row["m1_executability"]),
                "row_count": int(row["row_count"]) if row["row_count"] else 0,
                "sql_file": row["sql_file"],
            }
    return results


# ---------------------------------------------------------------------------
# M3: Event Log Completeness
# ---------------------------------------------------------------------------

def compute_m3(llm_result: dict, gt_result: dict) -> dict:
    """
    Compute M3 sub-metrics given LLM and GT execution results.
    Returns dict with m3a, m3b, m3c (all floats, None if GT unavailable).
    """
    if not gt_result["success"] or gt_result["row_count"] == 0:
        return {"m3a": None, "m3b": None, "m3c": None}

    if not llm_result["success"]:
        return {"m3a": 0.0, "m3b": 0.0, "m3c": 0.0}

    # M3a: case coverage
    gt_cases = gt_result["case_ids"]
    llm_cases = llm_result["case_ids"]
    m3a = len(llm_cases & gt_cases) / len(gt_cases) if gt_cases else None

    # M3b: activity type coverage
    gt_acts = gt_result["activities"]
    llm_acts = llm_result["activities"]
    m3b = len(llm_acts & gt_acts) / len(gt_acts) if gt_acts else None

    # M3c: row count ratio
    m3c = llm_result["row_count"] / gt_result["row_count"] if gt_result["row_count"] > 0 else None

    return {"m3a": round(m3a, 4) if m3a is not None else None,
            "m3b": round(m3b, 4) if m3b is not None else None,
            "m3c": round(m3c, 4) if m3c is not None else None}


# ---------------------------------------------------------------------------
# M4: Process Fidelity
# ---------------------------------------------------------------------------

def compute_m4a(llm_result: dict, gt_result: dict) -> float | None:
    """
    M4a: Activity Jaccard similarity = |A_llm ∩ A_gt| / |A_llm ∪ A_gt|
    """
    if not gt_result["success"] or not llm_result["success"]:
        return None
    a_llm = llm_result["activities"]
    a_gt = gt_result["activities"]
    union = a_llm | a_gt
    if not union:
        return None
    return round(len(a_llm & a_gt) / len(union), 4)


def compute_m4b_trace_similarity(llm_rows: list, gt_rows: list,
                                  sample_cases: int = 50) -> float | None:
    """
    M4b: Normalised trace edit distance (Levenshtein) over a sample of cases.
    Requires editdistance package (optional).
    Returns mean normalised edit distance, or None if package unavailable.
    """
    try:
        import editdistance
    except ImportError:
        return None  # optional metric

    # Build traces: {case_id: [activity1, activity2, ...]} ordered by timestamp
    def build_traces(rows):
        traces = defaultdict(list)
        for row in rows:
            case_id, activity = row[0], str(row[1])
            traces[case_id].append(activity)
        return traces

    llm_traces = build_traces(llm_rows)
    gt_traces = build_traces(gt_rows)

    # Sample cases that appear in both
    common_cases = list(set(llm_traces.keys()) & set(gt_traces.keys()))
    if not common_cases:
        return None

    sample = common_cases[:sample_cases]
    distances = []
    for case_id in sample:
        t_llm = llm_traces[case_id]
        t_gt = gt_traces[case_id]
        max_len = max(len(t_llm), len(t_gt))
        if max_len == 0:
            continue
        dist = editdistance.eval(t_llm, t_gt)
        distances.append(dist / max_len)

    return round(sum(distances) / len(distances), 4) if distances else None


# ---------------------------------------------------------------------------
# M5: Prompt Sensitivity
# ---------------------------------------------------------------------------

def compute_m5(completeness_rows: list, fidelity_rows: list, m1_data: dict) -> list:
    """
    M5: For each (task, llm), compute std of M1, M3a, M3b, M4a across strategies.
    Returns list of dicts with task, llm, m5_m1, m5_m3a, m5_m3b, m5_m4a, m5_mean.
    """
    import statistics

    # Index completeness and fidelity by (task, llm, strategy)
    comp_idx = {}
    for row in completeness_rows:
        key = (row["task"], row["llm"], row["strategy"])
        if key not in comp_idx:
            comp_idx[key] = []
        comp_idx[key].append(row)

    fid_idx = {}
    for row in fidelity_rows:
        key = (row["task"], row["llm"], row["strategy"])
        if key not in fid_idx:
            fid_idx[key] = []
        fid_idx[key].append(row)

    results = []
    for task in TASKS:
        for llm in LLMS:
            per_strategy = {}
            for strategy in STRATEGIES:
                key = (task, llm, strategy)
                # Average across repetitions for this (task, llm, strategy)
                m1_vals = [
                    m1_data.get((task, llm, strategy, rep), {}).get("m1", None)
                    for rep in REPETITIONS
                ]
                m1_vals = [v for v in m1_vals if v is not None]

                comp_vals = comp_idx.get(key, [])
                m3a_vals = [r["m3a_case_coverage"] for r in comp_vals
                            if r.get("m3a_case_coverage") is not None]
                m3b_vals = [r["m3b_activity_coverage"] for r in comp_vals
                            if r.get("m3b_activity_coverage") is not None]

                fid_vals = fid_idx.get(key, [])
                m4a_vals = [r["m4a_jaccard"] for r in fid_vals
                            if r.get("m4a_jaccard") is not None]

                per_strategy[strategy] = {
                    "m1":  sum(m1_vals) / len(m1_vals) if m1_vals else None,
                    "m3a": sum(m3a_vals) / len(m3a_vals) if m3a_vals else None,
                    "m3b": sum(m3b_vals) / len(m3b_vals) if m3b_vals else None,
                    "m4a": sum(m4a_vals) / len(m4a_vals) if m4a_vals else None,
                }

            def safe_std(metric):
                vals = [per_strategy[s][metric] for s in STRATEGIES
                        if per_strategy[s][metric] is not None]
                if len(vals) < 2:
                    return None
                return round(statistics.stdev(vals), 4)

            m5_m1  = safe_std("m1")
            m5_m3a = safe_std("m3a")
            m5_m3b = safe_std("m3b")
            m5_m4a = safe_std("m4a")

            # Mean sensitivity across available metrics
            m5_vals = [v for v in [m5_m1, m5_m3a, m5_m3b, m5_m4a] if v is not None]
            m5_mean = round(sum(m5_vals) / len(m5_vals), 4) if m5_vals else None

            results.append({
                "task": task,
                "llm": llm,
                "m5_m1":  m5_m1,
                "m5_m3a": m5_m3a,
                "m5_m3b": m5_m3b,
                "m5_m4a": m5_m4a,
                "m5_mean": m5_mean,
            })

    return results


# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------

def write_completeness_csv(rows: list, path: Path):
    fieldnames = ["run_at", "task", "llm", "strategy", "repetition",
                  "m3a_case_coverage", "m3b_activity_coverage", "m3c_row_ratio",
                  "llm_row_count", "gt_row_count",
                  "llm_cases", "gt_cases", "llm_activities", "gt_activities"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote {len(rows)} rows → {path}")


def write_fidelity_csv(rows: list, path: Path):
    fieldnames = ["run_at", "task", "llm", "strategy", "repetition",
                  "m4a_jaccard", "m4b_trace_sim",
                  "llm_activity_types", "gt_activity_types",
                  "intersection_size", "union_size"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote {len(rows)} rows → {path}")


def write_sensitivity_csv(rows: list, path: Path):
    fieldnames = ["task", "llm", "m5_m1_std", "m5_m3a_std", "m5_m3b_std",
                  "m5_m4a_std", "m5_mean_std"]
    renamed = []
    for r in rows:
        renamed.append({
            "task": r["task"], "llm": r["llm"],
            "m5_m1_std":  r["m5_m1"],
            "m5_m3a_std": r["m5_m3a"],
            "m5_m3b_std": r["m5_m3b"],
            "m5_m4a_std": r["m5_m4a"],
            "m5_mean_std": r["m5_mean"],
        })
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(renamed)
    print(f"  Wrote {len(renamed)} rows → {path}")


def write_summary_csv(completeness: list, fidelity: list,
                      sensitivity: list, m1_data: dict, path: Path):
    """
    One row per (task, llm, strategy, repetition) with all M1–M5 metrics.
    """
    # Index fidelity
    fid_idx = {(r["task"], r["llm"], r["strategy"], r["repetition"]): r
               for r in fidelity}
    sens_idx = {(r["task"], r["llm"]): r for r in sensitivity}

    fieldnames = [
        "task", "llm", "strategy", "repetition",
        "m1_executability",
        "m3a_case_coverage", "m3b_activity_coverage", "m3c_row_ratio",
        "m4a_jaccard", "m4b_trace_sim",
        "m5_mean_std",
    ]

    rows = []
    for comp_row in completeness:
        key = (comp_row["task"], comp_row["llm"],
               comp_row["strategy"], comp_row["repetition"])
        fid = fid_idx.get(key, {})
        sens = sens_idx.get((comp_row["task"], comp_row["llm"]), {})
        m1 = m1_data.get(key, {}).get("m1", None)
        rows.append({
            "task":       comp_row["task"],
            "llm":        comp_row["llm"],
            "strategy":   comp_row["strategy"],
            "repetition": comp_row["repetition"],
            "m1_executability":       m1,
            "m3a_case_coverage":      comp_row["m3a_case_coverage"],
            "m3b_activity_coverage":  comp_row["m3b_activity_coverage"],
            "m3c_row_ratio":          comp_row["m3c_row_ratio"],
            "m4a_jaccard":            fid.get("m4a_jaccard"),
            "m4b_trace_sim":          fid.get("m4b_trace_sim"),
            "m5_mean_std":            sens.get("m5_mean"),
        })

    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote {len(rows)} rows → {path}")


# ---------------------------------------------------------------------------
# Main evaluation loop
# ---------------------------------------------------------------------------

def run_evaluation(conn, db_type: str, m1_data: dict,
                   dry_run: bool = False, no_m4b: bool = False):
    """
    Main loop: for each task, execute GT SQL, then compare each passing
    LLM SQL against it to compute M3/M4.
    """
    run_ts = datetime.now(timezone.utc).isoformat()
    completeness_rows = []
    fidelity_rows = []

    for task in TASKS:
        # ── Ground truth ──────────────────────────────────────────────────
        gt_sql_file = GT_DIR / f"{task}_gt.sql"
        # Also accept legacy naming from author_packet
        if not gt_sql_file.exists():
            gt_sql_file = GT_DIR / f"{task}_icu_pathway_gt.sql"  # fallback
            # Try all _gt.sql files for this task
            candidates = list(GT_DIR.glob(f"{task}_*_gt.sql")) + \
                         list(GT_DIR.glob(f"{task}_gt.sql"))
            gt_sql_file = candidates[0] if candidates else None

        if dry_run:
            print(f"\n[DRY RUN] Task {task.upper()}: GT file = {gt_sql_file}")
            for llm in LLMS:
                for strategy in STRATEGIES:
                    for rep in REPETITIONS:
                        sql_path = OUTPUTS_DIR / task / llm / f"{strategy}_r{rep}.sql"
                        m1 = m1_data.get((task, llm, strategy, rep), {}).get("m1", "?")
                        print(f"  {llm}/{strategy}/r{rep}: M1={m1}, SQL exists={sql_path.exists()}")
            continue

        if gt_sql_file is None or not gt_sql_file.exists():
            print(f"\n[SKIP] Task {task.upper()}: no ground-truth SQL found in {GT_DIR}/")
            print(f"       Expected: {GT_DIR}/{task}_gt.sql  (or {task}_*_gt.sql)")
            # Still emit rows with None for M3/M4 so summary CSV is complete
            for llm in LLMS:
                for strategy in STRATEGIES:
                    for rep in REPETITIONS:
                        m1_entry = m1_data.get((task, llm, strategy, rep), {})
                        if not m1_entry:
                            continue
                        base = {"run_at": run_ts, "task": task, "llm": llm,
                                "strategy": strategy, "repetition": rep}
                        completeness_rows.append({**base,
                            "m3a_case_coverage": None, "m3b_activity_coverage": None,
                            "m3c_row_ratio": None, "llm_row_count": m1_entry.get("row_count"),
                            "gt_row_count": None, "llm_cases": None, "gt_cases": None,
                            "llm_activities": None, "gt_activities": None})
                        fidelity_rows.append({**base,
                            "m4a_jaccard": None, "m4b_trace_sim": None,
                            "llm_activity_types": None, "gt_activity_types": None,
                            "intersection_size": None, "union_size": None})
            continue

        print(f"\nTask {task.upper()} — GT: {gt_sql_file.name}")
        gt_sql = read_sql_file(gt_sql_file)
        gt_result = run_sql(conn, db_type, gt_sql, label=f"{task}/ground_truth")
        if not gt_result["success"]:
            print(f"  [ERROR] GT SQL failed: {gt_result['error_msg'][:120]}")
        else:
            print(f"  GT: {gt_result['row_count']:,} rows, "
                  f"{len(gt_result['case_ids'])} cases, "
                  f"{len(gt_result['activities'])} activity types")

        # ── LLM outputs ───────────────────────────────────────────────────
        for llm in LLMS:
            for strategy in STRATEGIES:
                for rep in REPETITIONS:
                    m1_entry = m1_data.get((task, llm, strategy, rep), {})
                    base = {"run_at": run_ts, "task": task, "llm": llm,
                            "strategy": strategy, "repetition": rep}

                    # If M1 = 0 (failed), record zeros
                    if not m1_entry or m1_entry.get("m1", 0) == 0:
                        completeness_rows.append({**base,
                            "m3a_case_coverage": 0.0, "m3b_activity_coverage": 0.0,
                            "m3c_row_ratio": 0.0, "llm_row_count": 0,
                            "gt_row_count": gt_result.get("row_count"),
                            "llm_cases": 0, "gt_cases": len(gt_result.get("case_ids", set())),
                            "llm_activities": 0,
                            "gt_activities": len(gt_result.get("activities", set()))})
                        fidelity_rows.append({**base,
                            "m4a_jaccard": 0.0, "m4b_trace_sim": None,
                            "llm_activity_types": 0,
                            "gt_activity_types": len(gt_result.get("activities", set())),
                            "intersection_size": 0, "union_size": None})
                        continue

                    # Re-execute LLM SQL to get full result set
                    sql_path = OUTPUTS_DIR / task / llm / f"{strategy}_r{rep}.sql"
                    if not sql_path.exists():
                        print(f"  [warn] SQL file missing: {sql_path}")
                        continue

                    llm_sql = read_sql_file(sql_path)
                    llm_result = run_sql(conn, db_type, llm_sql,
                                         label=f"{task}/{llm}/{strategy}_r{rep}")

                    # M3
                    m3 = compute_m3(llm_result, gt_result)
                    completeness_rows.append({**base,
                        "m3a_case_coverage":     m3["m3a"],
                        "m3b_activity_coverage": m3["m3b"],
                        "m3c_row_ratio":         m3["m3c"],
                        "llm_row_count":         llm_result["row_count"],
                        "gt_row_count":          gt_result["row_count"],
                        "llm_cases":             len(llm_result["case_ids"]),
                        "gt_cases":              len(gt_result["case_ids"]),
                        "llm_activities":        len(llm_result["activities"]),
                        "gt_activities":         len(gt_result["activities"]),
                    })

                    # M4a
                    m4a = compute_m4a(llm_result, gt_result)
                    a_llm = llm_result["activities"]
                    a_gt  = gt_result["activities"]
                    intersection = a_llm & a_gt
                    union        = a_llm | a_gt

                    # M4b (optional)
                    m4b = None
                    if not no_m4b:
                        m4b = compute_m4b_trace_similarity(
                            llm_result["rows"], gt_result["rows"])

                    fidelity_rows.append({**base,
                        "m4a_jaccard":        m4a,
                        "m4b_trace_sim":      m4b,
                        "llm_activity_types": len(a_llm),
                        "gt_activity_types":  len(a_gt),
                        "intersection_size":  len(intersection),
                        "union_size":         len(union),
                    })

                    status = "✓" if llm_result["success"] else "✗"
                    print(f"  {status} {llm}/{strategy}/r{rep}: "
                          f"rows={llm_result['row_count']:,}, "
                          f"M3a={m3['m3a']}, M3b={m3['m3b']}, M4a={m4a}")

    return completeness_rows, fidelity_rows


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Evaluate M3/M4/M5 metrics for LLM-generated OMOP CDM SQL"
    )
    db_group = parser.add_mutually_exclusive_group()
    db_group.add_argument("--db", choices=["duckdb", "postgres"],
                          help="Database backend")
    parser.add_argument("--duckdb-csv-dir", type=str,
                        default=str(EXPERIMENT_DIR.parent /
                                    "mimic-iv-demo-data-in-the-omop-common-data-model-0.9" /
                                    "1_omop_data_csv"),
                        help="Path to OMOP demo CSV directory (for DuckDB)")
    parser.add_argument("--pg-conn", type=str,
                        help="PostgreSQL connection string")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be evaluated without connecting to DB")
    parser.add_argument("--no-m4b", action="store_true",
                        help="Skip M4b trace similarity (faster, no editdistance needed)")
    parser.add_argument("--tasks", nargs="+", default=TASKS,
                        choices=TASKS, help="Restrict to specific tasks")
    parser.add_argument("--append", action="store_true",
                        help="Append rows to existing completeness/fidelity CSVs "
                             "instead of overwriting (use with --tasks for incremental runs)")
    parser.add_argument("--m1-csv", type=str,
                        default=str(RESULTS_DIR / "executability.csv"),
                        help="Path to M1/M2 executability CSV")
    args = parser.parse_args()

    if not args.dry_run and args.db is None:
        parser.error("--db is required unless --dry-run is set")
    if not args.dry_run and args.db == "postgres" and not args.pg_conn:
        parser.error("--pg-conn required when --db=postgres")

    RESULTS_DIR.mkdir(exist_ok=True)

    # Load M1 results
    print(f"\nLoading M1 results from {args.m1_csv} ...")
    m1_data = load_m1_results(Path(args.m1_csv))
    m1_pass = sum(1 for v in m1_data.values() if v["m1"] == 1)
    print(f"  {len(m1_data)} records, {m1_pass} passing (M1=1)")

    # Connect to DB
    conn, db_type = None, None
    if not args.dry_run:
        if args.db == "duckdb":
            conn, db_type = connect_duckdb(args.duckdb_csv_dir)
        else:
            conn, db_type = connect_postgres(args.pg_conn)

    # Override TASKS global if restricted
    if args.tasks != TASKS:
        TASKS[:] = args.tasks

    # Run evaluation
    print(f"\nEvaluating M3/M4 for tasks: {TASKS}")
    t_start = time.time()
    completeness_rows, fidelity_rows = run_evaluation(
        conn, db_type, m1_data,
        dry_run=args.dry_run,
        no_m4b=args.no_m4b,
    )
    elapsed = time.time() - t_start

    if args.dry_run:
        print("\n[DRY RUN complete — no DB queries executed]")
        return

    # In append mode: merge new rows with existing CSV data (other tasks)
    if args.append:
        def load_existing_csv(path, fieldnames):
            rows = []
            if path.exists():
                with open(path, newline="") as f:
                    for r in csv.DictReader(f):
                        # Convert numeric strings back
                        rows.append(r)
            return rows

        current_tasks = set(args.tasks)
        # Load completeness — keep rows for tasks NOT in current run
        existing_comp = [r for r in load_existing_csv(
                            RESULTS_DIR / "completeness.csv", [])
                         if r.get("task") not in current_tasks]
        completeness_rows = existing_comp + completeness_rows

        existing_fid = [r for r in load_existing_csv(
                            RESULTS_DIR / "fidelity.csv", [])
                        if r.get("task") not in current_tasks]
        fidelity_rows = existing_fid + fidelity_rows

    # Compute M5 (uses full completeness + fidelity data)
    print("\nComputing M5 (prompt sensitivity) ...")
    sensitivity_rows = compute_m5(completeness_rows, fidelity_rows, m1_data)

    # Write outputs
    print("\nWriting results ...")
    write_completeness_csv(completeness_rows, RESULTS_DIR / "completeness.csv")
    write_fidelity_csv(fidelity_rows, RESULTS_DIR / "fidelity.csv")
    write_sensitivity_csv(sensitivity_rows, RESULTS_DIR / "sensitivity.csv")
    write_summary_csv(completeness_rows, fidelity_rows,
                      sensitivity_rows, m1_data, RESULTS_DIR / "summary.csv")

    # Close DB
    if conn:
        try:
            conn.close()
        except Exception:
            pass

    print(f"\nDone in {elapsed:.1f}s.")
    print(f"Results written to {RESULTS_DIR}/")

    # Quick summary to stdout
    if completeness_rows:
        def _to_float(v):
            try: return float(v)
            except (TypeError, ValueError): return None
        m3a_vals = [_to_float(r["m3a_case_coverage"]) for r in completeness_rows
                    if _to_float(r.get("m3a_case_coverage")) is not None]
        m3b_vals = [_to_float(r["m3b_activity_coverage"]) for r in completeness_rows
                    if _to_float(r.get("m3b_activity_coverage")) is not None]
        m4a_vals = [_to_float(r["m4a_jaccard"]) for r in fidelity_rows
                    if _to_float(r.get("m4a_jaccard")) is not None]
        if m3a_vals:
            print(f"\nOverall averages (executable queries with GT):")
            print(f"  M3a case coverage:      {sum(m3a_vals)/len(m3a_vals):.3f}")
            print(f"  M3b activity coverage:  {sum(m3b_vals)/len(m3b_vals):.3f}" if m3b_vals else "")
            print(f"  M4a activity Jaccard:   {sum(m4a_vals)/len(m4a_vals):.3f}" if m4a_vals else "")


if __name__ == "__main__":
    main()
