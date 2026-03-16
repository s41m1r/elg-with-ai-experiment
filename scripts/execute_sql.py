#!/usr/bin/env python3
"""
execute_sql.py — SQL Execution & Evaluation Harness

Executes SQL files against an OMOP CDM database (DuckDB or PostgreSQL),
records executability (M1) and schema correctness (M2), and saves results.

Supports:
  - Single file execution
  - Batch mode (all outputs from run_llms.py)
  - Ground truth execution (author's queries)
  - Both DuckDB (demo/prototyping) and PostgreSQL (full MIMIC-IV OMOP CDM)

⚠️  DUA COMPLIANCE: All SQL execution happens LOCALLY. No patient data
    leaves the local environment. This script only reads from the database
    and writes results to local CSV files.

Usage:
    # Single file on DuckDB demo data
    python execute_sql.py --file outputs/t1/gpt4o/zero_shot_r1.sql \\
                          --db duckdb --duckdb-csv-dir ../path/to/omop_csvs

    # Single file on PostgreSQL
    python execute_sql.py --file outputs/t1/gpt4o/zero_shot_r1.sql \\
                          --db postgres --pg-conn "host=localhost dbname=mimic_omop"

    # Batch mode: execute all LLM outputs
    python execute_sql.py --batch --db duckdb --duckdb-csv-dir ../path/to/omop_csvs

    # Batch mode: execute ground truth files
    python execute_sql.py --batch-gt --gt-dir ../author_packet \\
                          --db postgres --pg-conn "host=localhost dbname=mimic_omop"

    # Dry run
    python execute_sql.py --batch --db duckdb --dry-run
"""

import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

EXPERIMENT_DIR = Path(__file__).resolve().parent.parent  # experiment/
OUTPUTS_DIR = EXPERIMENT_DIR / "outputs"
RESULTS_DIR = EXPERIMENT_DIR / "results"
GT_DIR = EXPERIMENT_DIR / "ground_truth"

EXPECTED_COLUMNS = {"case_id", "activity", "timestamp"}
SQL_TIMEOUT = 300  # seconds (increased for full MIMIC-IV — T3 measurement scans can be slow)

# Error classification patterns
ERROR_PATTERNS = [
    (r"syntax error", "syntax_error"),
    (r"relation .+ does not exist|Table .+ not found|table .+ does not exist", "missing_table"),
    (r"column .+ does not exist|column .+ not found|Referenced column", "missing_column"),
    (r"type mismatch|cannot cast|invalid input syntax", "type_error"),
    (r"timeout|cancel|statement timeout", "timeout"),
    (r"permission denied", "permission_error"),
    (r"ambiguous column", "ambiguous_column"),
    (r"division by zero", "division_by_zero"),
]


def classify_error(error_msg: str) -> str:
    """Classify a SQL error into a standard category."""
    lower = error_msg.lower()
    for pattern, category in ERROR_PATTERNS:
        if re.search(pattern, lower):
            return category
    return "other"


# ---------------------------------------------------------------------------
# Database Connections
# ---------------------------------------------------------------------------

def connect_duckdb(csv_dir: str):
    """
    Connect to DuckDB and load OMOP CDM tables from CSV files.
    Returns a connection object.
    """
    try:
        import duckdb
    except ImportError:
        print("Error: duckdb not installed. Run: pip install duckdb --break-system-packages")
        sys.exit(1)

    conn = duckdb.connect(":memory:")
    csv_path = Path(csv_dir)

    if not csv_path.exists():
        print(f"Error: CSV directory not found: {csv_dir}")
        sys.exit(1)

    # Load each CSV as a table
    omop_tables = [
        "visit_occurrence", "visit_detail", "procedure_occurrence",
        "drug_exposure", "drug_era", "condition_occurrence",
        "measurement", "specimen", "observation", "concept",
        "person", "concept_relationship", "concept_ancestor",
    ]

    # Some OMOP demo datasets prefix CSVs (e.g., "2b_concept.csv")
    # Build a lookup of available CSV files
    csv_files_available = {f.name: f for f in csv_path.glob("*.csv")}

    def find_csv(table_name: str) -> Path | None:
        """Find CSV file for a table, handling prefixed names."""
        # Try exact match first
        exact = f"{table_name}.csv"
        if exact in csv_files_available:
            return csv_files_available[exact]
        # Try prefixed match (e.g., "2b_concept.csv" for "concept")
        for fname, fpath in csv_files_available.items():
            if fname.endswith(f"_{table_name}.csv") or fname == f"{table_name}.csv":
                return fpath
        return None

    loaded = []
    for table in omop_tables:
        csv_file = find_csv(table)
        if csv_file:
            try:
                conn.execute(f"""
                    CREATE TABLE {table} AS
                    SELECT * FROM read_csv_auto('{str(csv_file)}', sample_size=-1)
                """)
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                loaded.append(f"{table} ({count:,} rows)")
            except Exception as e:
                print(f"  Warning: Could not load {table}.csv: {e}")

    print(f"  DuckDB loaded {len(loaded)} tables: {', '.join(loaded[:5])}{'...' if len(loaded) > 5 else ''}")
    return conn, "duckdb"


def connect_postgres(conn_string: str):
    """
    Connect to PostgreSQL with MIMIC-IV OMOP CDM.
    Returns a connection object.
    """
    try:
        import psycopg2
    except ImportError:
        print("Error: psycopg2 not installed. Run: pip install psycopg2-binary --break-system-packages")
        sys.exit(1)

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True  # Don't leave transactions open

    # Set session-level defaults: timeout + OMOP CDM schema
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = '{SQL_TIMEOUT * 1000}'")  # ms
        cur.execute("SET search_path TO cdm, public")  # OMOP tables live in 'cdm' schema

    print(f"  PostgreSQL connected: {conn_string[:50]}...")
    print(f"  search_path=cdm,public | statement_timeout={SQL_TIMEOUT}s")
    return conn, "postgres"


# ---------------------------------------------------------------------------
# SQL Execution
# ---------------------------------------------------------------------------

def execute_sql_file(sql_path: Path, conn, db_type: str, **kwargs) -> dict:
    """
    Execute a SQL file and return results dict.

    Returns:
        {
            "success": bool,
            "error_msg": str or None,
            "error_class": str or None,
            "columns": list of str,
            "column_types": list of str,
            "row_count": int,
            "sample_rows": list of tuples (first 5 rows),
            "latency_s": float,
            "m1_executability": 0 or 1,
            "m2_schema_correct": 0 or 1,
            "m2_has_case_id": bool,
            "m2_has_activity": bool,
            "m2_has_timestamp": bool,
        }
    """
    sql_text = sql_path.read_text(encoding="utf-8").strip()

    if not sql_text:
        return {
            "success": False,
            "error_msg": "Empty SQL file",
            "error_class": "empty_file",
            "columns": [],
            "column_types": [],
            "row_count": 0,
            "sample_rows": [],
            "latency_s": 0,
            "m1_executability": 0,
            "m2_schema_correct": 0,
            "m2_has_case_id": False,
            "m2_has_activity": False,
            "m2_has_timestamp": False,
        }

    start = time.time()

    try:
        if db_type == "duckdb":
            result = conn.execute(sql_text)
            columns = [desc[0] for desc in result.description]
            # DuckDB: get types from description
            column_types = [str(desc[1]) for desc in result.description]
            rows = result.fetchall()
            row_count = len(rows)
            sample_rows = rows[:5]
        else:
            # PostgreSQL — wrap in subquery to avoid fetching all rows from
            # large tables (e.g., T3 measurement can return 200M+ rows).
            # We get column info + 5 sample rows from a LIMIT query, then
            # get the true row count via a separate COUNT(*) subquery.
            # Pass skip_count=True to skip the COUNT (fast M1/M2-only mode).
            wrapped_sql = sql_text.rstrip().rstrip(";")
            # Strip trailing ORDER BY for the LIMIT 5 sample — this lets PostgreSQL
            # short-circuit after finding 5 rows instead of fully sorting the result.
            # The ORDER BY is preserved for the COUNT query (correctness check only).
            import re as _re
            sql_for_sample = _re.sub(
                r'\bORDER\s+BY\b[^;]*$', '', wrapped_sql,
                flags=_re.IGNORECASE | _re.DOTALL
            ).strip()
            with conn.cursor() as cur:
                # 1. Sample rows + column metadata (ORDER BY stripped for speed)
                cur.execute(f"SELECT * FROM ({sql_for_sample}) AS _q LIMIT 5")
                columns = [desc[0] for desc in cur.description]
                column_types = [str(desc.type_code) for desc in cur.description]
                sample_rows = cur.fetchall()
            if kwargs.get("skip_count", False):
                row_count = -1  # sentinel: count not computed
            else:
                with conn.cursor() as cur:
                    # 2. Count (may be slow for very large results — timeout applies)
                    cur.execute(f"SELECT COUNT(*) FROM ({wrapped_sql}) AS _q")
                    row_count = cur.fetchone()[0]

        latency = time.time() - start

        # Normalize column names for comparison
        col_lower = {c.lower() for c in columns}
        has_case_id = "case_id" in col_lower
        has_activity = "activity" in col_lower
        has_timestamp = "timestamp" in col_lower

        m2 = 1 if (has_case_id and has_activity and has_timestamp) else 0

        return {
            "success": True,
            "error_msg": None,
            "error_class": None,
            "columns": columns,
            "column_types": column_types,
            "row_count": row_count,
            "sample_rows": sample_rows,
            "latency_s": round(latency, 3),
            "m1_executability": 1,
            "m2_schema_correct": m2,
            "m2_has_case_id": has_case_id,
            "m2_has_activity": has_activity,
            "m2_has_timestamp": has_timestamp,
        }

    except Exception as e:
        latency = time.time() - start
        error_msg = str(e).replace("\n", " ")[:500]
        return {
            "success": False,
            "error_msg": error_msg,
            "error_class": classify_error(error_msg),
            "columns": [],
            "column_types": [],
            "row_count": 0,
            "sample_rows": [],
            "latency_s": round(latency, 3),
            "m1_executability": 0,
            "m2_schema_correct": 0,
            "m2_has_case_id": False,
            "m2_has_activity": False,
            "m2_has_timestamp": False,
        }


# ---------------------------------------------------------------------------
# Results Logging
# ---------------------------------------------------------------------------

def init_results_csv(filename: str = "executability.csv") -> Path:
    """Initialize results CSV with headers."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RESULTS_DIR / filename
    if not csv_path.exists():
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "sql_file", "source", "task", "llm", "strategy",
                "repetition", "db_type",
                "m1_executability", "m2_schema_correct",
                "m2_has_case_id", "m2_has_activity", "m2_has_timestamp",
                "columns", "column_types", "row_count", "latency_s",
                "error_class", "error_msg",
            ])
    return csv_path


def append_result(csv_path: Path, meta: dict, result: dict):
    """Append a result row to the CSV."""
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now(timezone.utc).isoformat(),
            meta.get("sql_file", ""),
            meta.get("source", "llm"),
            meta.get("task", ""),
            meta.get("llm", ""),
            meta.get("strategy", ""),
            meta.get("repetition", ""),
            meta.get("db_type", ""),
            result["m1_executability"],
            result["m2_schema_correct"],
            result["m2_has_case_id"],
            result["m2_has_activity"],
            result["m2_has_timestamp"],
            "|".join(result["columns"]),
            "|".join(result["column_types"]),
            result["row_count"],
            result["latency_s"],
            result.get("error_class", ""),
            result.get("error_msg", ""),
        ])


# ---------------------------------------------------------------------------
# File Discovery
# ---------------------------------------------------------------------------

def parse_output_filename(path: Path) -> dict:
    """
    Parse metadata from an output file path.
    Expected: outputs/{task}/{llm}/{strategy}_r{N}.sql
    """
    parts = path.parts
    filename = path.stem  # e.g., "zero_shot_r1"

    # Find 'outputs' in path
    try:
        idx = list(parts).index("outputs")
        task = parts[idx + 1]  # t1, t2, ...
        llm = parts[idx + 2]  # gpt4o, claude, llama3
    except (ValueError, IndexError):
        task = "unknown"
        llm = "unknown"

    # Parse strategy and repetition from filename
    match = re.match(r"(.+)_r(\d+)", filename)
    if match:
        strategy = match.group(1)
        repetition = int(match.group(2))
    else:
        strategy = filename
        repetition = 0

    return {
        "task": task,
        "llm": llm,
        "strategy": strategy,
        "repetition": repetition,
    }


def find_output_files() -> list:
    """Find all LLM output SQL files."""
    files = sorted(OUTPUTS_DIR.rglob("*.sql"))
    # Exclude raw response files
    files = [f for f in files if "_raw" not in f.stem]
    return files


def find_gt_files(gt_dir: Path) -> list:
    """Find all ground truth SQL files."""
    patterns = ["*_gt.sql", "*_complete.sql"]
    files = []
    for p in patterns:
        files.extend(sorted(gt_dir.glob(p)))
    return files


def parse_gt_filename(path: Path) -> dict:
    """Parse metadata from a ground truth filename."""
    stem = path.stem  # e.g., "t1_icu_pathway_gt"
    task_match = re.match(r"(t\d)", stem)
    task = task_match.group(1) if task_match else "unknown"
    return {
        "task": task,
        "llm": "ground_truth",
        "strategy": "expert",
        "repetition": 0,
    }


# ---------------------------------------------------------------------------
# Main Execution Modes
# ---------------------------------------------------------------------------

def run_single(sql_path: str, conn, db_type: str, csv_path: Path, skip_count: bool = False):
    """Execute a single SQL file."""
    path = Path(sql_path)
    if not path.exists():
        print(f"Error: File not found: {path}")
        sys.exit(1)

    meta = parse_output_filename(path)
    meta["sql_file"] = str(path)
    meta["source"] = "llm"
    meta["db_type"] = db_type

    print(f"\n  Executing: {path.name}")
    result = execute_sql_file(path, conn, db_type, skip_count=skip_count)
    append_result(csv_path, meta, result)

    _print_result(path.name, result)


def run_batch(conn, db_type: str, csv_path: Path, dry_run: bool, skip_count: bool = False,
              resume: bool = False):
    """Execute all LLM output files."""
    files = find_output_files()
    if not files:
        print(f"\n  No output files found in {OUTPUTS_DIR}")
        print(f"  Run run_llms.py first to generate LLM outputs.")
        return

    # Resume: skip files already in the output CSV
    if resume and csv_path.exists():
        import csv as _csv
        already_done = set()
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                already_done.add(row.get("sql_file", ""))
        before = len(files)
        files = [f for f in files if str(f) not in already_done]
        print(f"\n  Resume mode: skipping {before - len(files)} already-done files.")

    total_all = len(find_output_files())
    total = len(files)
    print(f"\n  Found {total} SQL files to execute ({total_all} total).\n")

    success = 0
    schema_ok = 0
    errors_by_class = {}

    for i, path in enumerate(files, 1):
        meta = parse_output_filename(path)
        meta["sql_file"] = str(path)
        meta["source"] = "llm"
        meta["db_type"] = db_type

        label = f"[{i}/{total}] {meta['task']}/{meta['llm']}/{meta['strategy']}_r{meta['repetition']}"

        if dry_run:
            print(f"  {label}: {path}")
            continue

        print(f"  {label} ...", end="", flush=True)
        result = execute_sql_file(path, conn, db_type, skip_count=skip_count)
        append_result(csv_path, meta, result)

        if result["m1_executability"]:
            success += 1
            if result["m2_schema_correct"]:
                schema_ok += 1
            row_display = "count skipped" if result["row_count"] == -1 else f"{result['row_count']} rows"
            print(f" ✓ M1=1 M2={result['m2_schema_correct']} "
                  f"({row_display}, {result['latency_s']}s)")
        else:
            ec = result["error_class"]
            errors_by_class[ec] = errors_by_class.get(ec, 0) + 1
            print(f" ✗ {ec}: {(result['error_msg'] or '')[:80]}")

    if not dry_run:
        print(f"\n  {'=' * 50}")
        print(f"  Summary: {success}/{total} executable (M1), "
              f"{schema_ok}/{total} schema-correct (M2)")
        if errors_by_class:
            print(f"  Error breakdown:")
            for ec, count in sorted(errors_by_class.items(), key=lambda x: -x[1]):
                print(f"    {ec}: {count}")
        print(f"  Results saved to: {csv_path}")


def run_batch_gt(gt_dir: str, conn, db_type: str, csv_path: Path, dry_run: bool, skip_count: bool = False):
    """Execute all ground truth SQL files."""
    gt_path = Path(gt_dir)
    files = find_gt_files(gt_path)

    if not files:
        print(f"\n  No ground truth files found in {gt_path}")
        print(f"  Expected filenames: t*_gt.sql or t*_complete.sql")
        return

    total = len(files)
    print(f"\n  Found {total} ground truth SQL files.\n")

    for i, path in enumerate(files, 1):
        meta = parse_gt_filename(path)
        meta["sql_file"] = str(path)
        meta["source"] = "ground_truth"
        meta["db_type"] = db_type

        label = f"[{i}/{total}] GT {meta['task']}: {path.name}"

        if dry_run:
            print(f"  {label}")
            continue

        print(f"  {label} ...", end="", flush=True)
        result = execute_sql_file(path, conn, db_type, skip_count=skip_count)
        append_result(csv_path, meta, result)
        _print_result(path.name, result)


def _print_result(name: str, result: dict):
    """Print a single execution result."""
    if result["m1_executability"]:
        cols = ", ".join(result["columns"])
        print(f" ✓ M1=1 M2={result['m2_schema_correct']} | "
              f"{result['row_count']} rows | cols: [{cols}] | {result['latency_s']}s")
        if result["sample_rows"]:
            print(f"    Sample: {result['sample_rows'][0]}")
    else:
        print(f" ✗ {result['error_class']}: {(result['error_msg'] or '')[:100]}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="SQL Execution & Evaluation Harness (M1 + M2)"
    )

    # Execution mode
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--file", type=str,
                      help="Execute a single SQL file")
    mode.add_argument("--batch", action="store_true",
                      help="Execute all LLM output files in outputs/")
    mode.add_argument("--batch-gt", action="store_true",
                      help="Execute all ground truth files")

    # Database connection
    parser.add_argument("--db", required=True, choices=["duckdb", "postgres"],
                        help="Database type")
    parser.add_argument("--duckdb-csv-dir", type=str,
                        help="Path to OMOP CDM CSV directory (for DuckDB)")
    parser.add_argument("--pg-conn", type=str,
                        help="PostgreSQL connection string")

    # Ground truth
    parser.add_argument("--gt-dir", type=str, default=str(GT_DIR),
                        help="Directory containing ground truth SQL files")

    # Options
    parser.add_argument("--dry-run", action="store_true",
                        help="List files without executing")
    parser.add_argument("--resume", action="store_true",
                        help="Skip SQL files already recorded in the output CSV")
    parser.add_argument("--skip-count", action="store_true",
                        help="Skip COUNT(*) subquery for PostgreSQL (fast M1/M2-only mode). "
                             "row_count will be -1 for successful queries.")
    parser.add_argument("--output-csv", type=str, default="executability.csv",
                        help="Output CSV filename (default: executability.csv)")

    args = parser.parse_args()

    # Validate DB args
    if args.db == "duckdb" and not args.duckdb_csv_dir and not args.dry_run:
        parser.error("--duckdb-csv-dir required when --db=duckdb")
    if args.db == "postgres" and not args.pg_conn and not args.dry_run:
        parser.error("--pg-conn required when --db=postgres")

    print(f"\n{'=' * 60}")
    print(f"  SQL Execution Harness — Metrics M1 (Executability) & M2 (Schema)")
    print(f"{'=' * 60}")

    # Connect to database
    conn = None
    db_type = args.db
    if not args.dry_run:
        if args.db == "duckdb":
            conn, db_type = connect_duckdb(args.duckdb_csv_dir)
        else:
            conn, db_type = connect_postgres(args.pg_conn)

    csv_path = init_results_csv(args.output_csv)

    skip_count = getattr(args, "skip_count", False)

    # Execute
    if args.file:
        if args.dry_run:
            print(f"\n  Would execute: {args.file}")
        else:
            run_single(args.file, conn, db_type, csv_path, skip_count=skip_count)
    elif args.batch:
        run_batch(conn, db_type, csv_path, args.dry_run, skip_count=skip_count,
                  resume=getattr(args, "resume", False))
    elif args.batch_gt:
        run_batch_gt(args.gt_dir, conn, db_type, csv_path, args.dry_run, skip_count=skip_count)

    # Cleanup
    if conn and args.db == "duckdb":
        conn.close()
    elif conn and args.db == "postgres":
        conn.close()

    print()


if __name__ == "__main__":
    main()
