#!/usr/bin/env python3
"""
run_llms.py — LLM API Harness for Event Log Extraction Experiments

Loads prompt files from experiment/prompts/, sends them to 3 LLMs
(GPT-4o, Claude Sonnet, Llama 3 70B), extracts SQL from responses,
and saves outputs + logs.

Experiment matrix: 5 tasks × 3 strategies × 3 LLMs × 3 repetitions = 135 calls.

⚠️  DUA COMPLIANCE: This script sends ONLY prompt text (task descriptions,
    OMOP CDM DDL schemas, synthetic example rows) to LLM APIs. No real
    MIMIC-IV patient data is ever transmitted. All prompts have been
    pre-validated to contain zero patient data.

Usage:
    python run_llms.py --dry-run          # Print plan without calling APIs
    python run_llms.py                     # Run all 135 experiments
    python run_llms.py --tasks t1 t3       # Run only tasks t1 and t3
    python run_llms.py --llms gpt4o claude # Run only GPT-4o and Claude
    python run_llms.py --strategies zero_shot schema_aware  # Subset strategies
    python run_llms.py --reps 1            # Single repetition (for testing)
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def _load_env():
    """Load .env file from project root (two levels up from scripts/).
    Overwrites any existing empty environment variables (e.g. pre-set blanks)."""
    env_file = Path(__file__).resolve().parent.parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                key, value = key.strip(), value.strip()
                # Always overwrite if current value is empty or unset
                if not os.environ.get(key):
                    os.environ[key] = value


_load_env()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

EXPERIMENT_DIR = Path(__file__).resolve().parent.parent  # experiment/
PROMPTS_DIR = EXPERIMENT_DIR / "prompts"
OUTPUTS_DIR = EXPERIMENT_DIR / "outputs"
RESULTS_DIR = EXPERIMENT_DIR / "results"

TASKS = ["t1", "t2", "t3", "t4", "t5"]
STRATEGIES = ["zero_shot", "schema_aware", "few_shot"]

# LLM configurations
LLMS = {
    "gpt4o": {
        "display_name": "GPT-4o",
        "provider": "openai",
        "model": "gpt-4o",
        "env_key": "OPENAI_API_KEY",
        "max_tokens": 4096,
        "temperature": 0.0,  # deterministic for reproducibility
    },
    "claude": {
        "display_name": "Claude Sonnet",
        "provider": "anthropic",
        "model": "claude-sonnet-4-20250514",
        "env_key": "ANTHROPIC_API_KEY",
        "max_tokens": 4096,
        "temperature": 0.0,
    },
    "llama3": {
        "display_name": "Llama 3 70B",
        "provider": "groq",
        "model": "llama-3.3-70b-versatile",  # Groq's current Llama 3 70B model
        "env_key": "TOGETHER_API_KEY",  # reusing env var name; holds Groq key (gsk_...)
        "max_tokens": 4096,
        "temperature": 0.0,
    },
}

# Retry configuration
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2  # seconds, exponential backoff: 2, 4, 8

# ---------------------------------------------------------------------------
# SQL Extraction
# ---------------------------------------------------------------------------

def extract_sql(response_text: str) -> str:
    """
    Extract SQL query from LLM response text.

    Handles common formats:
    1. ```sql ... ``` code blocks
    2. ``` ... ``` generic code blocks
    3. Raw SQL (starts with SELECT/WITH/CREATE)
    4. SQL embedded in explanation text
    """
    if not response_text:
        return ""

    # Strategy 1: Extract from ```sql ... ``` blocks
    sql_blocks = re.findall(r"```sql\s*\n?(.*?)```", response_text, re.DOTALL | re.IGNORECASE)
    if sql_blocks:
        # Take the longest block (most likely the full query)
        return max(sql_blocks, key=len).strip()

    # Strategy 2: Extract from ``` ... ``` generic blocks
    code_blocks = re.findall(r"```\s*\n?(.*?)```", response_text, re.DOTALL)
    if code_blocks:
        # Filter for blocks that look like SQL
        sql_like = [b for b in code_blocks if _looks_like_sql(b)]
        if sql_like:
            return max(sql_like, key=len).strip()

    # Strategy 3: Try to find SQL starting with SELECT/WITH
    lines = response_text.split("\n")
    sql_start = None
    for i, line in enumerate(lines):
        stripped = line.strip().upper()
        if stripped.startswith(("SELECT", "WITH", "-- ")):
            if sql_start is None:
                sql_start = i
        elif sql_start is not None and stripped == "":
            # Allow blank lines within SQL
            continue
        elif sql_start is not None and not _looks_like_sql_line(line):
            # End of SQL block
            break

    if sql_start is not None:
        sql_text = "\n".join(lines[sql_start:]).strip()
        if _looks_like_sql(sql_text):
            return sql_text

    # Strategy 4: Return entire response if it looks like SQL
    if _looks_like_sql(response_text):
        return response_text.strip()

    # Fallback: return empty (extraction failed)
    return ""


def _looks_like_sql(text: str) -> bool:
    """Heuristic: does this text look like SQL?"""
    upper = text.upper()
    sql_keywords = ["SELECT", "FROM", "WHERE", "JOIN", "UNION"]
    matches = sum(1 for kw in sql_keywords if kw in upper)
    return matches >= 2


def _looks_like_sql_line(line: str) -> bool:
    """Heuristic: does this line look like part of a SQL query?"""
    stripped = line.strip()
    if not stripped:
        return True  # blank lines are OK within SQL
    upper = stripped.upper()
    sql_starts = (
        "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
        "ON", "AND", "OR", "NOT", "IN", "IS", "AS", "UNION", "ORDER", "GROUP",
        "HAVING", "LIMIT", "OFFSET", "WITH", "CASE", "WHEN", "THEN", "ELSE",
        "END", "COALESCE", "CAST", "NULL", "--", ")", "(", "'", "||",
    )
    return any(upper.startswith(s) for s in sql_starts) or upper.startswith("--")


# ---------------------------------------------------------------------------
# API Callers
# ---------------------------------------------------------------------------

def call_openai(prompt: str, config: dict) -> dict:
    """Call OpenAI API. Returns dict with response_text, tokens, latency."""
    from openai import OpenAI

    client = OpenAI(api_key=os.environ[config["env_key"]])

    start = time.time()
    response = client.chat.completions.create(
        model=config["model"],
        messages=[
            {"role": "system", "content": "You are a SQL expert. Return ONLY the SQL query, no explanation."},
            {"role": "user", "content": prompt},
        ],
        max_tokens=config["max_tokens"],
        temperature=config["temperature"],
    )
    latency = time.time() - start

    msg = response.choices[0].message.content or ""
    usage = response.usage

    return {
        "response_text": msg,
        "input_tokens": usage.prompt_tokens if usage else 0,
        "output_tokens": usage.completion_tokens if usage else 0,
        "latency_s": round(latency, 2),
        "model_used": response.model,
    }


def call_anthropic(prompt: str, config: dict) -> dict:
    """Call Anthropic API. Returns dict with response_text, tokens, latency."""
    import anthropic

    client = anthropic.Anthropic(api_key=os.environ[config["env_key"]])

    start = time.time()
    response = client.messages.create(
        model=config["model"],
        max_tokens=config["max_tokens"],
        temperature=config["temperature"],
        system="You are a SQL expert. Return ONLY the SQL query, no explanation.",
        messages=[{"role": "user", "content": prompt}],
    )
    latency = time.time() - start

    msg = response.content[0].text if response.content else ""

    return {
        "response_text": msg,
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
        "latency_s": round(latency, 2),
        "model_used": config["model"],
    }


def _call_openai_compatible(prompt: str, config: dict, base_url: str) -> dict:
    """Shared helper for OpenAI-compatible APIs (Together, Groq, etc.)."""
    from openai import OpenAI

    client = OpenAI(
        api_key=os.environ[config["env_key"]],
        base_url=base_url,
    )

    start = time.time()
    response = client.chat.completions.create(
        model=config["model"],
        messages=[
            {"role": "system", "content": "You are a SQL expert. Return ONLY the SQL query, no explanation."},
            {"role": "user", "content": prompt},
        ],
        max_tokens=config["max_tokens"],
        temperature=config["temperature"],
    )
    latency = time.time() - start

    msg = response.choices[0].message.content or ""
    usage = response.usage

    return {
        "response_text": msg,
        "input_tokens": usage.prompt_tokens if usage else 0,
        "output_tokens": usage.completion_tokens if usage else 0,
        "latency_s": round(latency, 2),
        "model_used": config["model"],
    }


def call_together(prompt: str, config: dict) -> dict:
    """Call Together AI API."""
    return _call_openai_compatible(prompt, config, "https://api.together.xyz/v1")


def call_groq(prompt: str, config: dict) -> dict:
    """Call Groq API (free tier, OpenAI-compatible)."""
    return _call_openai_compatible(prompt, config, "https://api.groq.com/openai/v1")


PROVIDER_CALLERS = {
    "openai": call_openai,
    "anthropic": call_anthropic,
    "together": call_together,
    "groq": call_groq,
}


def call_llm(prompt: str, llm_key: str) -> dict:
    """
    Call an LLM with retry logic. Returns result dict or raises after max retries.
    """
    config = LLMS[llm_key]
    caller = PROVIDER_CALLERS[config["provider"]]

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = caller(prompt, config)
            result["attempt"] = attempt
            result["error"] = None
            return result
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY ** attempt
                print(f"    ⚠ Attempt {attempt} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"    ✗ All {MAX_RETRIES} attempts failed: {e}")

    return {
        "response_text": "",
        "input_tokens": 0,
        "output_tokens": 0,
        "latency_s": 0,
        "model_used": LLMS[llm_key]["model"],
        "attempt": MAX_RETRIES,
        "error": str(last_error),
    }


# ---------------------------------------------------------------------------
# Main Experiment Loop
# ---------------------------------------------------------------------------

def load_prompt(task: str, strategy: str) -> str:
    """Load a prompt file."""
    prompt_file = PROMPTS_DIR / task / f"{strategy}.txt"
    if not prompt_file.exists():
        raise FileNotFoundError(f"Prompt not found: {prompt_file}")
    return prompt_file.read_text(encoding="utf-8")


def save_output(task: str, llm_key: str, strategy: str, rep: int,
                raw_response: str, extracted_sql: str):
    """Save raw response and extracted SQL."""
    out_dir = OUTPUTS_DIR / task / llm_key
    out_dir.mkdir(parents=True, exist_ok=True)

    # Save extracted SQL
    sql_file = out_dir / f"{strategy}_r{rep}.sql"
    sql_file.write_text(extracted_sql, encoding="utf-8")

    # Save raw response (for debugging / manual inspection)
    raw_file = out_dir / f"{strategy}_r{rep}_raw.txt"
    raw_file.write_text(raw_response, encoding="utf-8")


def init_log():
    """Initialize the API log CSV."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = RESULTS_DIR / "api_log.csv"
    if not log_file.exists():
        with open(log_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "task", "llm", "strategy", "repetition",
                "model_used", "input_tokens", "output_tokens", "latency_s",
                "attempt", "error", "sql_extracted", "sql_length",
                "output_file",
            ])
    return log_file


def append_log(log_file: Path, row: dict):
    """Append a row to the API log."""
    with open(log_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            row["timestamp"], row["task"], row["llm"], row["strategy"],
            row["repetition"], row["model_used"], row["input_tokens"],
            row["output_tokens"], row["latency_s"], row["attempt"],
            row["error"], row["sql_extracted"], row["sql_length"],
            row["output_file"],
        ])


def run_experiments(tasks: list, llms: list, strategies: list,
                    reps: int, dry_run: bool):
    """Run the full experiment matrix."""
    total = len(tasks) * len(strategies) * len(llms) * reps
    print(f"\n{'=' * 60}")
    print(f"  LLM API Harness — Event Log Extraction Experiments")
    print(f"{'=' * 60}")
    print(f"  Tasks:      {', '.join(tasks)}")
    print(f"  LLMs:       {', '.join(LLMS[l]['display_name'] for l in llms)}")
    print(f"  Strategies: {', '.join(strategies)}")
    print(f"  Repetitions: {reps}")
    print(f"  Total calls: {total}")
    print(f"  Mode:       {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"{'=' * 60}\n")

    if not dry_run:
        # Check API keys
        missing_keys = []
        for llm_key in llms:
            env_key = LLMS[llm_key]["env_key"]
            if not os.environ.get(env_key):
                missing_keys.append(f"  {env_key} (for {LLMS[llm_key]['display_name']})")
        if missing_keys:
            print("✗ Missing API keys:")
            for mk in missing_keys:
                print(mk)
            print("\nSet these environment variables and retry.")
            sys.exit(1)

    log_file = init_log()
    completed = 0
    errors = 0

    for task in tasks:
        for strategy in strategies:
            prompt = load_prompt(task, strategy)
            prompt_preview = prompt[:80].replace("\n", " ") + "..."

            for llm_key in llms:
                for rep in range(1, reps + 1):
                    completed += 1
                    label = f"[{completed}/{total}] {task}/{strategy} → {LLMS[llm_key]['display_name']} r{rep}"

                    if dry_run:
                        out_path = OUTPUTS_DIR / task / llm_key / f"{strategy}_r{rep}.sql"
                        print(f"  {label}")
                        print(f"    Prompt: {prompt_preview}")
                        print(f"    Output: {out_path}")
                        print()
                        continue

                    print(f"  {label} ...", end="", flush=True)

                    result = call_llm(prompt, llm_key)
                    extracted = extract_sql(result["response_text"])
                    sql_ok = bool(extracted)

                    save_output(task, llm_key, strategy, rep,
                                result["response_text"], extracted)

                    out_path = OUTPUTS_DIR / task / llm_key / f"{strategy}_r{rep}.sql"

                    # Log
                    append_log(log_file, {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "task": task,
                        "llm": llm_key,
                        "strategy": strategy,
                        "repetition": rep,
                        "model_used": result.get("model_used", ""),
                        "input_tokens": result.get("input_tokens", 0),
                        "output_tokens": result.get("output_tokens", 0),
                        "latency_s": result.get("latency_s", 0),
                        "attempt": result.get("attempt", 0),
                        "error": result.get("error", ""),
                        "sql_extracted": sql_ok,
                        "sql_length": len(extracted),
                        "output_file": str(out_path),
                    })

                    if result.get("error"):
                        errors += 1
                        print(f" ✗ ERROR: {result['error']}")
                    elif not sql_ok:
                        print(f" ⚠ No SQL extracted ({len(result['response_text'])} chars response)")
                    else:
                        print(f" ✓ {len(extracted)} chars, {result['latency_s']}s, "
                              f"{result.get('input_tokens', '?')}+{result.get('output_tokens', '?')} tokens")

    print(f"\n{'=' * 60}")
    if dry_run:
        print(f"  DRY RUN complete. {total} calls planned.")
        print(f"  Prompts dir:  {PROMPTS_DIR}")
        print(f"  Outputs dir:  {OUTPUTS_DIR}")
    else:
        print(f"  Done. {completed} calls, {errors} errors.")
        print(f"  Outputs: {OUTPUTS_DIR}")
        print(f"  Log:     {log_file}")
    print(f"{'=' * 60}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="LLM API Harness — run event log extraction experiments"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print experiment plan without calling APIs")
    parser.add_argument("--tasks", nargs="+", default=TASKS,
                        choices=TASKS, help="Tasks to run (default: all)")
    parser.add_argument("--llms", nargs="+", default=list(LLMS.keys()),
                        choices=list(LLMS.keys()),
                        help="LLMs to use (default: all)")
    parser.add_argument("--strategies", nargs="+", default=STRATEGIES,
                        choices=STRATEGIES, help="Prompt strategies (default: all)")
    parser.add_argument("--reps", type=int, default=3,
                        help="Number of repetitions per experiment (default: 3)")

    args = parser.parse_args()
    run_experiments(args.tasks, args.llms, args.strategies, args.reps, args.dry_run)


if __name__ == "__main__":
    main()
