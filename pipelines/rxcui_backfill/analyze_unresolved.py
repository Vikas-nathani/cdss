#!/usr/bin/env python3
"""
analyze_unresolved.py — Single-query analysis of all unresolved ingredient names.
Fetches everything in one shot, categorizes in Python, writes report.
"""

import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

_env_path = Path(__file__).parent.parent.parent / ".env"
if _env_path.exists():
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k = k.strip(); v = v.strip().strip('"').strip("'")
                if k not in os.environ:
                    os.environ[k] = v

from utils import connect_db, fmt_ist
from config import LOGS_DIR

REPORT_FILE = LOGS_DIR / "unresolved_analysis.log"


def main():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    conn = connect_db()
    t0 = time.time()

    print("Step 1: fetching unresolved names + row counts...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ingredient_name_norm, COUNT(*) AS cnt
            FROM drugdb.indian_brand_ingredient
            WHERE rxcui_in IS NULL
            GROUP BY ingredient_name_norm
            ORDER BY cnt DESC
        """)
        unresolved = cur.fetchall()  # [(name, cnt), ...]

    total_names = len(unresolved)
    total_rows  = sum(r[1] for r in unresolved)
    keys        = [r[0].strip().lower() for r in unresolved]
    print(f"  {total_names} distinct names, {total_rows} rows")

    print("Step 2: fetching all rxnconso hits for these names in one query...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT LOWER(str) AS key, sab, tty, suppress, rxcui, str
            FROM public.rxnconso
            WHERE LOWER(str) = ANY(%s)
        """, (keys,))
        hits = cur.fetchall()

    print(f"  {len(hits)} rxnconso rows returned")

    # Build lookup: key → list of (sab, tty, suppress, rxcui, str)
    rxn_map = defaultdict(list)
    for key, sab, tty, suppress, rxcui, str_ in hits:
        rxn_map[key].append((sab, tty, suppress, rxcui, str_))

    print("Step 3: categorizing each name...")

    results = []
    for name, cnt in unresolved:
        key = name.strip().lower()
        rows = rxn_map.get(key, [])

        if not rows:
            reason = "NOT_IN_RXNCONSO"
            detail = "Absent from rxnconso entirely — India/Japan-specific drug not in RxNorm."
        else:
            rxnorm_rows   = [r for r in rows if r[0] == "RXNORM"]
            in_pin_rows   = [r for r in rxnorm_rows if r[1] in ("IN", "PIN")]
            unsuppressed  = [r for r in in_pin_rows if r[2] == "N"]

            if unsuppressed:
                reason = "SHOULD_HAVE_MATCHED"
                detail = ("Found valid RXNORM IN/PIN suppress=N rows — pipeline should have caught this. "
                          "Entries: " + "; ".join(f"rxcui={r[3]} tty={r[1]}" for r in unsuppressed[:3]))
            elif in_pin_rows:
                reason = "SUPPRESSED"
                suppress_vals = list({r[2] for r in in_pin_rows})
                detail = (f"Exists as IN/PIN in RXNORM but marked suppressed (suppress={suppress_vals}). "
                          "RxNorm considers this concept obsolete/retired. "
                          + "; ".join(f"rxcui={r[3]}" for r in in_pin_rows[:3]))
            elif rxnorm_rows:
                ttys = sorted({r[1] for r in rxnorm_rows})
                reason = "WRONG_TTY"
                detail = (f"In RXNORM but only as tty={ttys} — not an ingredient (IN/PIN). "
                          "Likely a brand name (BN), synonym (SY), or dose form. "
                          + "; ".join(f"'{r[4]}' tty={r[1]} rxcui={r[3]}" for r in rxnorm_rows[:3]))
            else:
                sabs = sorted({r[0] for r in rows})
                ttys = sorted({r[1] for r in rows})
                reason = "WRONG_SAB"
                detail = (f"Found in rxnconso but only under non-RXNORM sources: sab={sabs}, tty={ttys}. "
                          "These sources are not used by this pipeline. "
                          + "; ".join(f"'{r[4]}' sab={r[0]} tty={r[1]}" for r in rows[:3]))

        results.append({"name": name, "cnt": cnt, "reason": reason, "detail": detail})

    conn.close()

    # ── Tally ──────────────────────────────────────────────────────────────────
    reason_name_counts = Counter(r["reason"] for r in results)
    reason_row_counts  = Counter()
    for r in results:
        reason_row_counts[r["reason"]] += r["cnt"]

    # ── Write report ───────────────────────────────────────────────────────────
    descriptions = {
        "NOT_IN_RXNCONSO"    : "Drug is completely absent from RxNorm — India/Japan-specific or not FDA-approved",
        "WRONG_TTY"          : "In RxNorm but listed as brand name / synonym / dose form, not as ingredient (IN/PIN)",
        "WRONG_SAB"          : "In rxnconso but only under non-RXNORM sources (DrugBank, MMX, VANDF, etc.)",
        "SUPPRESSED"         : "In RxNorm as IN/PIN but marked obsolete/suppressed",
        "SHOULD_HAVE_MATCHED": "BUG — pipeline should have resolved this, investigate",
    }

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        def w(line=""):
            f.write(line + "\n")

        w("UNRESOLVED INGREDIENT ANALYSIS REPORT")
        w(f"Generated : {fmt_ist()}")
        w("=" * 80)
        w()
        w(f"Total unresolved distinct names : {total_names}")
        w(f"Total unresolved rows           : {total_rows}")
        w()
        w("REASON SUMMARY")
        w("-" * 60)
        w(f"  {'Reason':<30} {'Names':>6}   {'Rows':>8}   {'% of rows':>10}")
        w(f"  {'-'*30} {'-'*6}   {'-'*8}   {'-'*10}")
        for reason, name_cnt in reason_name_counts.most_common():
            row_cnt = reason_row_counts[reason]
            pct = row_cnt / total_rows * 100
            w(f"  {reason:<30} {name_cnt:>6}   {row_cnt:>8}   {pct:>9.1f}%")
        w()
        w("REASON DESCRIPTIONS")
        w("-" * 60)
        for reason, desc in descriptions.items():
            if reason in reason_name_counts:
                w(f"  {reason}")
                w(f"    {desc}")
        w()
        w("=" * 80)
        w("DETAILED LIST — sorted by row count descending")
        w("=" * 80)
        w()
        w(f"  {'Ingredient Name':<45} {'Rows':>6}  Reason / Detail")
        w(f"  {'-'*45} {'-'*6}  {'-'*40}")
        for r in results:
            w(f"  {r['name']:<45} {r['cnt']:>6}  [{r['reason']}]")
            w(f"  {'':>53}  {r['detail']}")
            w()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")
    print()
    print("REASON SUMMARY")
    print(f"  {'Reason':<30} {'Names':>6}   {'Rows':>8}   {'% of rows':>10}")
    print(f"  {'-'*30} {'-'*6}   {'-'*8}   {'-'*10}")
    for reason, name_cnt in reason_name_counts.most_common():
        row_cnt = reason_row_counts[reason]
        pct = row_cnt / total_rows * 100
        print(f"  {reason:<30} {name_cnt:>6}   {row_cnt:>8}   {pct:>9.1f}%")
    print(f"\nFull report: {REPORT_FILE}")


if __name__ == "__main__":
    main()
