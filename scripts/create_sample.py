#!/usr/bin/env python3
"""
create_sample.py — Extract a stratified patient sample from full MIMIC-IV PostgreSQL
                   and export as CSVs compatible with the DuckDB evaluation pipeline.

Strategy: ~1 000 patients, stratified to guarantee all five clinical processes:
  - 400 ICU patients           (T1 ICU Pathway + likely T2 Medication + T4 Lab Cycle)
  - 350 Sepsis patients        (T3 Sepsis Trajectory, some overlap with above)
  - 250 ED-only patients       (T5 ED Flow — people with ED visit but not in ICU set)
  Unique union → ~800–1 000 patients total

Output: CSV files in experiment/sample_data/ named after OMOP tables:
  visit_occurrence.csv, visit_detail.csv, condition_occurrence.csv,
  drug_exposure.csv, drug_era.csv, measurement.csv, specimen.csv,
  observation.csv, procedure_occurrence.csv, person.csv, concept.csv,
  concept_ancestor.csv

These CSVs are consumed by evaluate_metrics.py --duckdb-csv-dir sample_data/

Usage:
    python create_sample.py --pg-conn "host=10.224.188.122 port=5432 dbname=mimic user=saimir"
    python create_sample.py --pg-conn "..." --n-icu 400 --n-sepsis 350 --n-ed 250
"""

import argparse
import csv
import sys
import time
from pathlib import Path

SCRIPT_DIR   = Path(__file__).resolve().parent
EXPERIMENT_DIR = SCRIPT_DIR.parent
SAMPLE_DIR   = EXPERIMENT_DIR / "sample_data"

# ── Sepsis concept IDs from t3_gt.sql (129 unique) ─────────────────────────
SEPSIS_CONCEPTS = list({
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
})


def connect(pg_conn: str):
    try:
        import psycopg2
    except ImportError:
        print("Error: psycopg2 not installed. Run: pip install psycopg2-binary --break-system-packages")
        sys.exit(1)
    conn = psycopg2.connect(pg_conn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET search_path TO cdm, public")
        cur.execute("SET statement_timeout = '600000'")  # 10 min per statement
    return conn


def connect_streaming(pg_conn: str):
    """Separate connection with autocommit=False for server-side cursors."""
    try:
        import psycopg2
    except ImportError:
        sys.exit(1)
    conn = psycopg2.connect(pg_conn)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET search_path TO cdm, public")
        cur.execute("SET statement_timeout = '600000'")
    return conn


def fetch_all(conn, sql: str, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return cols, rows


def write_csv(path: Path, cols, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print(f"  ✓ {path.name}: {len(rows):,} rows")


def stream_csv(conn, sql: str, path: Path, batch_size: int = 5000):
    """Stream a large query to CSV using a server-side cursor (avoids OOM)."""
    import uuid
    path.parent.mkdir(parents=True, exist_ok=True)
    cursor_name = f"stream_{uuid.uuid4().hex[:8]}"
    total = 0
    with conn.cursor(cursor_name) as cur:
        cur.itersize = batch_size
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                w.writerows(rows)
                total += len(rows)
    print(f"  ✓ {path.name}: {total:,} rows")


def build_id_list(ids) -> str:
    """Build a safe SQL IN-list string from a collection of integers."""
    return ", ".join(str(int(i)) for i in ids)


def select_patients(conn, n_icu: int, n_sepsis: int, n_ed: int) -> list:
    """
    Returns a list of person_ids covering all five clinical processes.
    Strategy:
      1. ICU patients   → T1 (+ T2, T4 coverage by default)
      2. Sepsis patients → T3  (many overlap with ICU)
      3. ED patients    → T5  (prioritise those NOT already in set)
    """
    print(f"\nSelecting patients (ICU≤{n_icu}, sepsis≤{n_sepsis}, ED≤{n_ed}) …")
    person_ids = set()

    # 1. ICU patients (visit_detail concept 32037 = Intensive Care)
    t0 = time.time()
    _, rows = fetch_all(conn, f"""
        SELECT DISTINCT person_id
        FROM visit_detail
        WHERE visit_detail_concept_id = 32037
        ORDER BY person_id
        LIMIT {n_icu}
    """)
    icu_ids = {r[0] for r in rows}
    person_ids |= icu_ids
    print(f"  ICU patients:     {len(icu_ids):,}  ({time.time()-t0:.1f}s)")

    # 2. Sepsis patients
    sepsis_in = build_id_list(SEPSIS_CONCEPTS)
    t0 = time.time()
    _, rows = fetch_all(conn, f"""
        SELECT DISTINCT person_id
        FROM condition_occurrence
        WHERE condition_concept_id IN ({sepsis_in})
        ORDER BY person_id
        LIMIT {n_sepsis}
    """)
    sepsis_ids = {r[0] for r in rows}
    person_ids |= sepsis_ids
    print(f"  Sepsis patients:  {len(sepsis_ids):,}  ({time.time()-t0:.1f}s)")

    # 3. ED patients not already included — guarantees T5 coverage
    n_ed_needed = max(n_ed, 250)
    t0 = time.time()
    _, rows = fetch_all(conn, f"""
        SELECT DISTINCT person_id
        FROM visit_occurrence
        WHERE visit_concept_id IN (9203, 262)
        ORDER BY person_id
        LIMIT {n_ed_needed * 3}
    """)
    ed_ids = {r[0] for r in rows}
    # Prefer new IDs first, then fill from existing overlap
    new_ed = ed_ids - person_ids
    ed_sample = list(new_ed)[:n_ed_needed]
    if len(ed_sample) < n_ed_needed:
        ed_sample += list(ed_ids & person_ids)[:n_ed_needed - len(ed_sample)]
    person_ids |= set(ed_sample)
    print(f"  ED patients:      {len(ed_sample):,}  ({time.time()-t0:.1f}s)")

    result = sorted(person_ids)
    print(f"  → Total unique patients: {len(result):,}")
    return result


def export_table(conn, table: str, pid_col: str, person_ids: list,
                 out_dir: Path, extra_where: str = "") -> int:
    """Export a single person-filtered OMOP table to CSV."""
    id_list = build_id_list(person_ids)
    where = f"{pid_col} IN ({id_list})"
    if extra_where:
        where = f"({where}) AND ({extra_where})"
    t0 = time.time()
    cols, rows = fetch_all(conn, f"SELECT * FROM {table} WHERE {where}")
    write_csv(out_dir / f"{table}.csv", cols, rows)
    print(f"     ({time.time()-t0:.1f}s)")
    return len(rows)


def export_concept(conn, person_ids: list, out_dir: Path):
    """Export only the concept IDs actually referenced in the sample data."""
    id_list = build_id_list(person_ids)
    t0 = time.time()
    print(f"  concept (referenced only) …", end="", flush=True)
    cols, rows = fetch_all(conn, f"""
        SELECT DISTINCT c.*
        FROM concept c
        WHERE c.concept_id IN (
            SELECT DISTINCT visit_concept_id          FROM visit_occurrence        WHERE person_id IN ({id_list}) AND visit_concept_id IS NOT NULL
            UNION
            SELECT DISTINCT visit_detail_concept_id   FROM visit_detail            WHERE person_id IN ({id_list}) AND visit_detail_concept_id IS NOT NULL
            UNION
            SELECT DISTINCT condition_concept_id      FROM condition_occurrence    WHERE person_id IN ({id_list}) AND condition_concept_id IS NOT NULL
            UNION
            SELECT DISTINCT drug_concept_id           FROM drug_exposure           WHERE person_id IN ({id_list}) AND drug_concept_id IS NOT NULL
            UNION
            SELECT DISTINCT drug_concept_id           FROM drug_era                WHERE person_id IN ({id_list}) AND drug_concept_id IS NOT NULL
            UNION
            SELECT DISTINCT measurement_concept_id    FROM measurement             WHERE person_id IN ({id_list}) AND measurement_concept_id IS NOT NULL
            UNION
            SELECT DISTINCT specimen_concept_id       FROM specimen                WHERE person_id IN ({id_list}) AND specimen_concept_id IS NOT NULL
            UNION
            SELECT DISTINCT observation_concept_id    FROM observation             WHERE person_id IN ({id_list}) AND observation_concept_id IS NOT NULL
            UNION
            SELECT DISTINCT procedure_concept_id      FROM procedure_occurrence    WHERE person_id IN ({id_list}) AND procedure_concept_id IS NOT NULL
        )
    """)
    write_csv(out_dir / "concept.csv", cols, rows)
    print(f"     ({time.time()-t0:.1f}s)")


def export_concept_ancestor(conn, person_ids: list, out_dir: Path):
    """Export concept_ancestor rows for concepts used in the sample (for T6 / concept_ancestor joins)."""
    id_list = build_id_list(person_ids)
    t0 = time.time()
    print(f"  concept_ancestor (for sample concepts) …", end="", flush=True)
    # Only need rows where ancestor/descendant concepts appear in the sample
    cols, rows = fetch_all(conn, f"""
        SELECT DISTINCT ca.*
        FROM concept_ancestor ca
        WHERE ca.descendant_concept_id IN (
            SELECT DISTINCT drug_concept_id FROM drug_exposure WHERE person_id IN ({id_list}) AND drug_concept_id IS NOT NULL
            UNION
            SELECT DISTINCT condition_concept_id FROM condition_occurrence WHERE person_id IN ({id_list}) AND condition_concept_id IS NOT NULL
        )
        LIMIT 500000
    """)
    write_csv(out_dir / "concept_ancestor.csv", cols, rows)
    print(f"     ({time.time()-t0:.1f}s)")


def main():
    parser = argparse.ArgumentParser(
        description="Extract a stratified MIMIC-IV sample for M3/M4/M5 evaluation"
    )
    parser.add_argument("--pg-conn", required=True,
                        help="PostgreSQL connection string")
    parser.add_argument("--n-icu",    type=int, default=400,
                        help="Max ICU patients to include (default 400)")
    parser.add_argument("--n-sepsis", type=int, default=350,
                        help="Max sepsis patients to include (default 350)")
    parser.add_argument("--n-ed",     type=int, default=250,
                        help="Min ED patients to include (default 250)")
    parser.add_argument("--out-dir",  type=str, default=str(SAMPLE_DIR),
                        help=f"Output directory for CSVs (default: {SAMPLE_DIR})")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  MIMIC-IV Sample Extractor")
    print(f"  Output: {out_dir}")
    print(f"{'='*60}")

    conn = connect(args.pg_conn)

    # ── 1. Select patients ────────────────────────────────────────────────
    person_ids = select_patients(conn, args.n_icu, args.n_sepsis, args.n_ed)
    id_list    = build_id_list(person_ids)

    # ── 2. Export person-filtered OMOP tables ─────────────────────────────
    print("\nExporting OMOP tables …")

    tables_by_pidfk = [
        ("person",               "person_id"),
        ("visit_occurrence",     "person_id"),
        ("visit_detail",         "person_id"),
        ("condition_occurrence", "person_id"),
        ("drug_exposure",        "person_id"),
        ("drug_era",             "person_id"),
        ("observation",          "person_id"),
        ("procedure_occurrence", "person_id"),
        ("specimen",             "person_id"),
    ]

    total_rows = 0
    for table, pid_col in tables_by_pidfk:
        print(f"  {table} …", end="", flush=True)
        n = export_table(conn, table, pid_col, person_ids, out_dir)
        total_rows += n

    # measurement — very large: stream via server-side cursor to avoid OOM
    print(f"  measurement (streaming) …", flush=True)
    id_list_m = build_id_list(person_ids)
    t0 = time.time()
    conn_stream = connect_streaming(args.pg_conn)
    try:
        stream_csv(
            conn_stream,
            f"SELECT * FROM measurement WHERE person_id IN ({id_list_m})",
            out_dir / "measurement.csv",
        )
    finally:
        conn_stream.close()
    print(f"     ({time.time()-t0:.1f}s)")
    # Count separately
    _, rows_m = fetch_all(conn, f"SELECT COUNT(*) FROM measurement WHERE person_id IN ({id_list_m})")
    total_rows += rows_m[0][0]

    # concept — export only referenced IDs
    export_concept(conn, person_ids, out_dir)

    # concept_ancestor — subset relevant to sample
    export_concept_ancestor(conn, person_ids, out_dir)

    # ── 3. Summary ────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  Done.")
    print(f"  Patients:    {len(person_ids):,}")
    print(f"  Total rows:  {total_rows:,}  (excl. concept tables)")
    print(f"  Output dir:  {out_dir}")
    print(f"\n  Now run:")
    print(f"    python evaluate_metrics.py \\")
    print(f"      --db duckdb \\")
    print(f"      --duckdb-csv-dir {out_dir} \\")
    print(f"      --no-m4b")
    print(f"{'='*60}\n")

    conn.close()


if __name__ == "__main__":
    main()
