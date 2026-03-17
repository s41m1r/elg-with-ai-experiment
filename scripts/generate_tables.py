#!/usr/bin/env python3
"""
generate_tables.py — LaTeX Table & Summary Generator

Reads the results CSVs (executability.csv, completeness.csv, fidelity.csv,
sensitivity.csv) and produces:

  1. latex_tables.tex   — Ready-to-paste LaTeX snippets for the paper:
       Tab 1: M1 executability by LLM × strategy (already in paper as tab:m1grid)
       Tab 2: M1 executability by task (already in paper as tab:m1task)
       Tab 3: M3/M4 completeness + fidelity — main results table
       Tab 4: M5 prompt sensitivity per (LLM, task)
       Tab 5: Per-task M3a/M3b/M4a heatmap (coloured cells)

  2. summary.csv        — One row per (task, llm, strategy) with aggregated metrics

  3. key_findings.txt   — Auto-generated bullet points for the Discussion section

Usage:
    python generate_tables.py                  # reads from results/ (defaults)
    python generate_tables.py --results-dir /path/to/results
    python generate_tables.py --no-color       # disable LaTeX cell colouring
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
EXPERIMENT_DIR = SCRIPT_DIR.parent
RESULTS_DIR = EXPERIMENT_DIR / "results"

TASKS = ["t1", "t2", "t3", "t4", "t5", "t6"]
TASK_LABELS = {
    "t1": "ICU Pathway",
    "t2": "Medication Admin.",
    "t3": "Sepsis Trajectory",
    "t4": "Lab-Order Cycle",
    "t5": "ED Flow",
    "t6": "Diagnosis Pathway",
}
LLM_LABELS = {
    "gpt4o":  "GPT-4o",
    "claude": "Claude Sonnet",
    "llama3": "Llama 3 70B",
}
STRATEGY_LABELS = {
    "naive":        "Naive",
    "zero_shot":    "Zero-shot",
    "schema_aware": "Schema-aware",
    "few_shot":     "Few-shot",
}
LLMS = ["gpt4o", "claude", "llama3"]
STRATEGIES = ["naive", "zero_shot", "schema_aware", "few_shot"]
REPETITIONS = [1, 2, 3]

# ---------------------------------------------------------------------------
# CSV loaders
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> list:
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def safe_float(val) -> float | None:
    try:
        return float(val) if val not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


def safe_int(val) -> int | None:
    try:
        return int(float(val)) if val not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def avg(vals):
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else None


def fmt(val, decimals=3, pct=False, bold=False):
    """Format a float for LaTeX."""
    if val is None:
        return "---"
    if pct:
        s = f"{val * 100:.1f}\\%"
    else:
        s = f"{val:.{decimals}f}"
    return f"\\textbf{{{s}}}" if bold else s


def color_cell(val, thresholds=(0.33, 0.66), metric_type="ratio"):
    """
    Return a LaTeX \\cellcolor command based on value.
    Green for high, yellow for medium, red for low.
    """
    if val is None:
        return "\\cellcolor{gray!15}"
    lo, hi = thresholds
    if val >= hi:
        return "\\cellcolor{green!20}"
    elif val >= lo:
        return "\\cellcolor{yellow!30}"
    else:
        return "\\cellcolor{red!20}"


# ---------------------------------------------------------------------------
# Build aggregated index
# ---------------------------------------------------------------------------

def build_index(exec_rows, comp_rows, fid_rows, sens_rows):
    """
    Returns a dict:
      agg[(task, llm, strategy)] = {
          m1, m1_pass, m1_total,
          m3a, m3b, m3c, m4a, m4b, m5_mean
      }
    where values are averages across repetitions.
    """
    # Index raw rows by (task, llm, strategy, rep)
    def idx_by_key(rows, *fields):
        d = defaultdict(list)
        for row in rows:
            key = tuple(row.get(f) for f in fields)
            d[key].append(row)
        return d

    exec_idx  = idx_by_key(exec_rows,  "task", "llm", "strategy", "repetition")
    comp_idx  = idx_by_key(comp_rows,  "task", "llm", "strategy", "repetition")
    fid_idx   = idx_by_key(fid_rows,   "task", "llm", "strategy", "repetition")
    sens_map  = {(r["task"], r["llm"]): r for r in sens_rows}

    result = {}
    for task in TASKS:
        for llm in LLMS:
            for strategy in STRATEGIES:
                m1_vals, m3a_vals, m3b_vals, m3c_vals, m4a_vals, m4b_vals = \
                    [], [], [], [], [], []

                for rep in REPETITIONS:
                    rep_s = str(rep)
                    e = exec_idx.get((task, llm, strategy, rep_s), [])
                    c = comp_idx.get((task, llm, strategy, rep_s), [])
                    fi = fid_idx.get((task, llm, strategy, rep_s), [])

                    for row in e:
                        v = safe_int(row.get("m1_executability"))
                        if v is not None:
                            m1_vals.append(v)
                    for row in c:
                        for field, lst in [("m3a_case_coverage", m3a_vals),
                                           ("m3b_activity_coverage", m3b_vals),
                                           ("m3c_row_ratio", m3c_vals)]:
                            v = safe_float(row.get(field))
                            if v is not None:
                                lst.append(v)
                    for row in fi:
                        v = safe_float(row.get("m4a_jaccard"))
                        if v is not None:
                            m4a_vals.append(v)
                        v = safe_float(row.get("m4b_trace_sim"))
                        if v is not None:
                            m4b_vals.append(v)

                sens = sens_map.get((task, llm), {})
                result[(task, llm, strategy)] = {
                    "m1":       avg(m1_vals),
                    "m1_pass":  sum(m1_vals),
                    "m1_total": len(m1_vals),
                    "m3a":  avg(m3a_vals),
                    "m3b":  avg(m3b_vals),
                    "m3c":  avg(m3c_vals),
                    "m4a":  avg(m4a_vals),
                    "m4b":  avg(m4b_vals),
                    "m5":   safe_float(sens.get("m5_mean_std")),
                }
    return result


# ---------------------------------------------------------------------------
# Table generators
# ---------------------------------------------------------------------------

def table_m1_by_llm_strategy(agg: dict) -> str:
    """Regenerate Table 1: M1 executability (%) by LLM × strategy."""
    lines = [
        "% === Table: M1 Executability by LLM and Prompt Strategy ===",
        "\\begin{table}[t]",
        "  \\caption{M1 Executability (\\%) by LLM and prompt strategy. "
        "Each cell: pass/total (18 runs).}\\label{tab:m1grid}",
        "  \\centering",
        "  \\begin{tabular}{@{}lccccc@{}}",
        "    \\toprule",
        "    \\textbf{LLM} & \\textbf{Naive} & \\textbf{Zero-shot} & \\textbf{Schema-aware} "
        "& \\textbf{Few-shot} & \\textbf{Overall} \\\\",
        "    \\midrule",
    ]

    for llm in LLMS:
        row_vals = []
        total_pass, total_total = 0, 0
        for strategy in STRATEGIES:
            pass_count = sum(
                agg.get((task, llm, strategy), {}).get("m1_pass", 0) or 0
                for task in TASKS
            )
            total_count = sum(
                agg.get((task, llm, strategy), {}).get("m1_total", 0) or 0
                for task in TASKS
            )
            pct = int(round(pass_count / total_count * 100)) if total_count else 0
            row_vals.append(f"{pct}\\% ({pass_count}/{total_count})")
            total_pass  += pass_count
            total_total += total_count

        overall_pct = int(round(total_pass / total_total * 100)) if total_total else 0
        row_vals.append(f"{overall_pct}\\% ({total_pass}/{total_total})")
        cells = " & ".join(row_vals)
        lines.append(f"    {LLM_LABELS[llm]} & {cells} \\\\")

    # Overall row
    lines.append("    \\midrule")
    overall_cells = []
    grand_pass, grand_total = 0, 0
    for strategy in STRATEGIES:
        p = sum(agg.get((t, l, strategy), {}).get("m1_pass", 0) or 0
                for t in TASKS for l in LLMS)
        n = sum(agg.get((t, l, strategy), {}).get("m1_total", 0) or 0
                for t in TASKS for l in LLMS)
        pct = int(round(p / n * 100)) if n else 0
        overall_cells.append(f"\\textbf{{{pct}\\%}} ({p}/{n})")
        grand_pass  += p
        grand_total += n
    gp = int(round(grand_pass / grand_total * 100)) if grand_total else 0
    overall_cells.append(f"\\textbf{{{gp}\\%}} ({grand_pass}/{grand_total})")
    lines.append(f"    \\textbf{{Overall}} & {' & '.join(overall_cells)} \\\\")
    lines += [
        "    \\bottomrule",
        "  \\end{tabular}",
        "\\end{table}",
        "",
    ]
    return "\n".join(lines)


def table_m1_by_task(agg: dict) -> str:
    """Regenerate Table 2: M1 executability (%) by task."""
    lines = [
        "% === Table: M1 Executability by Task ===",
        "\\begin{table}[t]",
        "  \\caption{M1 Executability (\\%) by clinical process task "
        "(36 runs each).}\\label{tab:m1task}",
        "  \\centering",
        "  \\begin{tabular}{@{}llcc@{}}",
        "    \\toprule",
        "    \\textbf{Task} & \\textbf{Process} & \\textbf{M1 Pass} "
        "& \\textbf{Rate} \\\\",
        "    \\midrule",
    ]

    grand_pass, grand_total = 0, 0
    for task in TASKS:
        pass_count = sum(
            agg.get((task, llm, strategy), {}).get("m1_pass", 0) or 0
            for llm in LLMS for strategy in STRATEGIES
        )
        total_count = sum(
            agg.get((task, llm, strategy), {}).get("m1_total", 0) or 0
            for llm in LLMS for strategy in STRATEGIES
        )
        pct = int(round(pass_count / total_count * 100)) if total_count else 0
        bold = "\\textbf{" if pct == 100 else ""
        endb = "}" if pct == 100 else ""
        lines.append(
            f"    {task.upper()} & {TASK_LABELS[task]} & "
            f"{pass_count}/{total_count} & {bold}{pct}\\%{endb} \\\\"
        )
        grand_pass  += pass_count
        grand_total += total_count

    gp = int(round(grand_pass / grand_total * 100)) if grand_total else 0
    lines += [
        "    \\midrule",
        f"    \\textbf{{Total}} & & {grand_pass}/{grand_total} & {gp}\\% \\\\",
        "    \\bottomrule",
        "  \\end{tabular}",
        "\\end{table}",
        "",
    ]
    return "\n".join(lines)


def table_m3_m4_main(agg: dict, use_color: bool = True) -> str:
    """
    Table 3: Main results — M3a, M3b, M4a per (LLM, strategy), averaged across tasks.
    Highlights best value per column.
    """
    # Compute per-(llm, strategy) averages across all tasks
    summary = {}
    for llm in LLMS:
        for strategy in STRATEGIES:
            m3a = avg([agg.get((t, llm, strategy), {}).get("m3a") for t in TASKS])
            m3b = avg([agg.get((t, llm, strategy), {}).get("m3b") for t in TASKS])
            m4a = avg([agg.get((t, llm, strategy), {}).get("m4a") for t in TASKS])
            summary[(llm, strategy)] = {"m3a": m3a, "m3b": m3b, "m4a": m4a}

    # Find best per column for bolding
    def best(metric):
        vals = {k: v[metric] for k, v in summary.items() if v[metric] is not None}
        return max(vals, key=lambda k: vals[k]) if vals else None

    best_m3a = best("m3a")
    best_m3b = best("m3b")
    best_m4a = best("m4a")

    lines = [
        "% === Table: M3/M4 Main Results (averaged across tasks) ===",
        "\\begin{table}[t]",
        "  \\caption{M3 Completeness and M4 Fidelity results averaged across all six tasks. "
        "Bold = best in column. --- = ground-truth SQL not yet available.}\\label{tab:m3m4}",
        "  \\centering",
        "  \\begin{tabular}{@{}llccc@{}}",
        "    \\toprule",
        "    \\textbf{LLM} & \\textbf{Strategy} & "
        "\\textbf{M3a (case cov.)} & \\textbf{M3b (act.~cov.)} & \\textbf{M4a (Jaccard)} \\\\",
        "    \\midrule",
    ]

    for llm in LLMS:
        first = True
        for strategy in STRATEGIES:
            s = summary[(llm, strategy)]
            c_m3a = color_cell(s["m3a"]) if use_color else ""
            c_m3b = color_cell(s["m3b"]) if use_color else ""
            c_m4a = color_cell(s["m4a"]) if use_color else ""

            bold_m3a = (llm, strategy) == best_m3a
            bold_m3b = (llm, strategy) == best_m3b
            bold_m4a = (llm, strategy) == best_m4a

            llm_label = LLM_LABELS[llm] if first else ""
            first = False

            lines.append(
                f"    {llm_label} & {STRATEGY_LABELS[strategy]} & "
                f"{c_m3a}{fmt(s['m3a'], pct=True, bold=bold_m3a)} & "
                f"{c_m3b}{fmt(s['m3b'], pct=True, bold=bold_m3b)} & "
                f"{c_m4a}{fmt(s['m4a'], decimals=3, bold=bold_m4a)} \\\\"
            )
        lines.append("    \\midrule")

    lines[-1] = lines[-1]  # keep last midrule before bottomrule

    lines += [
        "    \\bottomrule",
        "  \\end{tabular}",
        "\\end{table}",
        "",
    ]
    return "\n".join(lines)


def table_m5_sensitivity(agg: dict, sens_rows: list) -> str:
    """
    Table 4: M5 Prompt Sensitivity — std of M1, M3a, M3b, M4a across strategies,
    per (LLM, task).
    """
    sens_map = {(r["task"], r["llm"]): r for r in sens_rows}

    lines = [
        "% === Table: M5 Prompt Sensitivity ===",
        "\\begin{table}[t]",
        "  \\caption{M5 Prompt Sensitivity: standard deviation of M1, M3a, M3b, M4a "
        "across the four prompt strategies, per LLM and clinical process task. "
        "Higher = more sensitive to prompt choice.}\\label{tab:m5}",
        "  \\centering",
        "  \\begin{tabular}{@{}llcccc@{}}",
        "    \\toprule",
        "    \\textbf{LLM} & \\textbf{Task} & "
        "\\textbf{$\\sigma$(M1)} & \\textbf{$\\sigma$(M3a)} & "
        "\\textbf{$\\sigma$(M3b)} & \\textbf{$\\sigma$(M4a)} \\\\",
        "    \\midrule",
    ]

    for llm in LLMS:
        first = True
        for task in TASKS:
            s = sens_map.get((task, llm), {})
            m5_m1  = safe_float(s.get("m5_m1_std"))
            m5_m3a = safe_float(s.get("m5_m3a_std"))
            m5_m3b = safe_float(s.get("m5_m3b_std"))
            m5_m4a = safe_float(s.get("m5_m4a_std"))

            llm_label = LLM_LABELS[llm] if first else ""
            first = False
            lines.append(
                f"    {llm_label} & {TASK_LABELS[task]} & "
                f"{fmt(m5_m1, 3)} & {fmt(m5_m3a, 3)} & "
                f"{fmt(m5_m3b, 3)} & {fmt(m5_m4a, 3)} \\\\"
            )
        lines.append("    \\midrule")

    lines += [
        "    \\bottomrule",
        "  \\end{tabular}",
        "\\end{table}",
        "",
    ]
    return "\n".join(lines)


def table_per_task_heatmap(agg: dict, use_color: bool = True) -> str:
    """
    Table 5: Per-task M3a/M3b/M4a breakdown — one row per task,
    best LLM+strategy shown with colour coding.
    """
    lines = [
        "% === Table: Per-Task Completeness & Fidelity (best LLM/strategy) ===",
        "\\begin{table}[t]",
        "  \\caption{Best M3a, M3b, M4a per clinical process task "
        "(best LLM--strategy combination shown).}\\label{tab:pertask}",
        "  \\centering",
        "  \\begin{tabular}{@{}llllccc@{}}",
        "    \\toprule",
        "    \\textbf{Task} & \\textbf{Best LLM} & \\textbf{Best Strategy} & "
        "\\textbf{M3a} & \\textbf{M3b} & \\textbf{M4a} \\\\",
        "    \\midrule",
    ]

    for task in TASKS:
        best_key, best_score = None, -1
        for llm in LLMS:
            for strategy in STRATEGIES:
                d = agg.get((task, llm, strategy), {})
                vals = [d.get("m3a"), d.get("m3b"), d.get("m4a")]
                available = [v for v in vals if v is not None]
                score = avg(available) if available else -1
                if score is not None and score > best_score:
                    best_score = score
                    best_key = (task, llm, strategy)

        if best_key:
            t, l, s = best_key
            d = agg[(t, l, s)]
            c_m3a = color_cell(d["m3a"]) if use_color else ""
            c_m3b = color_cell(d["m3b"]) if use_color else ""
            c_m4a = color_cell(d["m4a"]) if use_color else ""
            lines.append(
                f"    {t.upper()} & {LLM_LABELS[l]} & {STRATEGY_LABELS[s]} & "
                f"{c_m3a}{fmt(d['m3a'], pct=True)} & "
                f"{c_m3b}{fmt(d['m3b'], pct=True)} & "
                f"{c_m4a}{fmt(d['m4a'], decimals=3)} \\\\"
            )
        else:
            lines.append(
                f"    {task.upper()} & --- & --- & --- & --- & --- \\\\"
            )

    lines += [
        "    \\bottomrule",
        "  \\end{tabular}",
        "\\end{table}",
        "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Key findings generator
# ---------------------------------------------------------------------------

def generate_key_findings(agg: dict, sens_rows: list) -> str:
    """Auto-generate bullet point findings for the Discussion section."""
    lines = ["KEY FINDINGS (auto-generated from results)", "=" * 50, ""]

    # Best overall LLM
    llm_avgs = {}
    for llm in LLMS:
        m1_vals = [agg.get((t, llm, s), {}).get("m1") for t in TASKS for s in STRATEGIES]
        llm_avgs[llm] = avg([v for v in m1_vals if v is not None])
    best_llm = max(llm_avgs, key=lambda l: llm_avgs[l] or 0)
    lines.append(f"RQ1/RQ3 — Best LLM by M1:")
    for llm in LLMS:
        v = llm_avgs[llm]
        lines.append(f"  {LLM_LABELS[llm]}: M1 avg = {v*100:.1f}%" if v is not None else
                     f"  {LLM_LABELS[llm]}: M1 avg = ---")
    lines.append(f"  → Best: {LLM_LABELS[best_llm]}")
    lines.append("")

    # Best strategy
    strat_avgs = {}
    for strategy in STRATEGIES:
        m1_vals = [agg.get((t, l, strategy), {}).get("m1") for t in TASKS for l in LLMS]
        strat_avgs[strategy] = avg([v for v in m1_vals if v is not None])
    best_strat = max(strat_avgs, key=lambda s: strat_avgs[s] or 0)
    lines.append("RQ2 — Best strategy by M1:")
    for s in STRATEGIES:
        v = strat_avgs[s]
        lines.append(f"  {STRATEGY_LABELS[s]}: M1 avg = {v*100:.1f}%" if v is not None else
                     f"  {STRATEGY_LABELS[s]}: M1 avg = ---")
    lines.append(f"  → Best: {STRATEGY_LABELS[best_strat]}")
    lines.append("")

    # Hardest task
    task_m1 = {}
    for task in TASKS:
        m1_vals = [agg.get((task, l, s), {}).get("m1") for l in LLMS for s in STRATEGIES]
        task_m1[task] = avg([v for v in m1_vals if v is not None])
    hardest = min(task_m1, key=lambda t: task_m1[t] or 1)
    easiest = max(task_m1, key=lambda t: task_m1[t] or 0)
    lines.append("RQ3 — Task difficulty (M1):")
    for task in TASKS:
        v = task_m1[task]
        lines.append(f"  {task.upper()} ({TASK_LABELS[task]}): {v*100:.1f}%" if v is not None
                     else f"  {task.upper()}: ---")
    lines.append(f"  → Hardest: {hardest.upper()} ({TASK_LABELS[hardest]})")
    lines.append(f"  → Easiest: {easiest.upper()} ({TASK_LABELS[easiest]})")
    lines.append("")

    # M3/M4 summary (if available)
    all_m3a = [agg.get((t, l, s), {}).get("m3a")
               for t in TASKS for l in LLMS for s in STRATEGIES]
    all_m3a = [v for v in all_m3a if v is not None]
    all_m4a = [agg.get((t, l, s), {}).get("m4a")
               for t in TASKS for l in LLMS for s in STRATEGIES]
    all_m4a = [v for v in all_m4a if v is not None]

    if all_m3a:
        lines.append("M3/M4 Summary (executable queries with GT):")
        lines.append(f"  M3a case coverage (mean):     {avg(all_m3a)*100:.1f}%")
        lines.append(f"  M4a activity Jaccard (mean):  {avg(all_m4a)*100:.1f}%"
                     if all_m4a else "  M4a: ---")
        lines.append("")

    # M5: most sensitive LLM
    sens_map = {(r["task"], r["llm"]): r for r in sens_rows}
    llm_sens = {}
    for llm in LLMS:
        vals = [safe_float(sens_map.get((t, llm), {}).get("m5_mean_std")) for t in TASKS]
        llm_sens[llm] = avg([v for v in vals if v is not None])
    has_sens = any(v is not None for v in llm_sens.values())
    if has_sens:
        most_sensitive = max((l for l in LLMS if llm_sens[l] is not None),
                             key=lambda l: llm_sens[l])
        lines.append("M5 Prompt Sensitivity:")
        for llm in LLMS:
            v = llm_sens[llm]
            lines.append(f"  {LLM_LABELS[llm]}: σ(mean) = {v:.4f}" if v is not None
                         else f"  {LLM_LABELS[llm]}: ---")
        lines.append(f"  → Most sensitive: {LLM_LABELS[most_sensitive]}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Summary CSV
# ---------------------------------------------------------------------------

def write_aggregated_summary_csv(agg: dict, path: Path):
    """Write one row per (task, llm, strategy) with all averaged metrics."""
    fieldnames = ["task", "task_label", "llm", "llm_label", "strategy", "strategy_label",
                  "m1_avg", "m1_pass", "m1_total",
                  "m3a_avg", "m3b_avg", "m3c_avg", "m4a_avg", "m4b_avg", "m5"]
    rows = []
    for task in TASKS:
        for llm in LLMS:
            for strategy in STRATEGIES:
                d = agg.get((task, llm, strategy), {})
                rows.append({
                    "task": task, "task_label": TASK_LABELS[task],
                    "llm": llm, "llm_label": LLM_LABELS[llm],
                    "strategy": strategy, "strategy_label": STRATEGY_LABELS[strategy],
                    "m1_avg":   round(d["m1"],  4) if d.get("m1")  is not None else "",
                    "m1_pass":  d.get("m1_pass", ""),
                    "m1_total": d.get("m1_total", ""),
                    "m3a_avg":  round(d["m3a"], 4) if d.get("m3a") is not None else "",
                    "m3b_avg":  round(d["m3b"], 4) if d.get("m3b") is not None else "",
                    "m3c_avg":  round(d["m3c"], 4) if d.get("m3c") is not None else "",
                    "m4a_avg":  round(d["m4a"], 4) if d.get("m4a") is not None else "",
                    "m4b_avg":  round(d["m4b"], 4) if d.get("m4b") is not None else "",
                    "m5":       round(d["m5"],  4) if d.get("m5")  is not None else "",
                })
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote {len(rows)} rows → {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate LaTeX tables and summary CSV from M1–M5 results"
    )
    parser.add_argument("--results-dir", type=str, default=str(RESULTS_DIR),
                        help="Directory containing results CSVs")
    parser.add_argument("--no-color", action="store_true",
                        help="Disable \\cellcolor in tables")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Output directory (defaults to results-dir)")
    args = parser.parse_args()

    res_dir = Path(args.results_dir)
    out_dir = Path(args.output_dir) if args.output_dir else res_dir
    out_dir.mkdir(exist_ok=True)
    use_color = not args.no_color

    # Load CSVs
    print(f"Loading results from {res_dir} ...")
    exec_rows = load_csv(res_dir / "executability.csv")
    comp_rows = load_csv(res_dir / "completeness.csv")
    fid_rows  = load_csv(res_dir / "fidelity.csv")
    sens_rows = load_csv(res_dir / "sensitivity.csv")

    print(f"  executability.csv : {len(exec_rows)} rows")
    print(f"  completeness.csv  : {len(comp_rows)} rows")
    print(f"  fidelity.csv      : {len(fid_rows)} rows")
    print(f"  sensitivity.csv   : {len(sens_rows)} rows")

    if not exec_rows:
        print("Error: executability.csv is empty or missing. Run execute_sql.py first.")
        sys.exit(1)

    # Build aggregated index
    print("\nAggregating metrics ...")
    agg = build_index(exec_rows, comp_rows, fid_rows, sens_rows)

    # Generate LaTeX tables
    print("Generating LaTeX tables ...")
    use_color_latex = use_color
    # If coloring is requested, add required package reminder
    preamble = (
        "% ================================================================\n"
        "% LaTeX tables for: AI-Assisted Event Log Extraction (BPM 2026)\n"
        "% Generated by generate_tables.py\n"
        "% Required packages: booktabs, xcolor, colortbl (for cell colours)\n"
        "% Add to preamble: \\usepackage{colortbl}\n"
        "% ================================================================\n\n"
    ) if use_color else (
        "% ================================================================\n"
        "% LaTeX tables for: AI-Assisted Event Log Extraction (BPM 2026)\n"
        "% Generated by generate_tables.py\n"
        "% Required packages: booktabs\n"
        "% ================================================================\n\n"
    )

    tables = preamble
    tables += table_m1_by_llm_strategy(agg) + "\n"
    tables += table_m1_by_task(agg) + "\n"
    tables += table_m3_m4_main(agg, use_color=use_color_latex) + "\n"
    tables += table_m5_sensitivity(agg, sens_rows) + "\n"
    tables += table_per_task_heatmap(agg, use_color=use_color_latex) + "\n"

    tex_path = out_dir / "latex_tables.tex"
    tex_path.write_text(tables, encoding="utf-8")
    print(f"  Wrote LaTeX tables → {tex_path}")

    # Summary CSV
    print("Writing aggregated summary CSV ...")
    write_aggregated_summary_csv(agg, out_dir / "summary_agg.csv")

    # Key findings
    print("Generating key findings ...")
    findings = generate_key_findings(agg, sens_rows)
    findings_path = out_dir / "key_findings.txt"
    findings_path.write_text(findings, encoding="utf-8")
    print(f"  Wrote findings → {findings_path}")
    print("\n" + findings)


if __name__ == "__main__":
    main()
