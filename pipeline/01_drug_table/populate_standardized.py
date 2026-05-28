#!/usr/bin/env python3
"""
Phase 2 – Full population of standardized_records column.

Source columns:
  clean_record  → openfda, dailymed, drugbank
  record        → rxnorm  (no clean_record exists)

Target column:  standardized_records  (JSONB, never touches other columns)
Primary key:    id  (uuid)
Batch size:     1000
"""

import copy
import json
import logging
import os
import sys
import time
from datetime import timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras

# ── DB config ─────────────────────────────────────────────────────────────────
DB = dict(host=os.environ.get("DB_HOST", "localhost"), port=5432,
          user="postgres", password=os.environ.get("DB_PASSWORD", ""), database="postgres")
TABLE      = "DrugSourceMaster"
DST_COL    = "standardized_records"
PK         = "id"
BATCH_SIZE = 1000

logging.basicConfig(
    filename="transformation_errors.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s [%(funcName)s] %(message)s",
)

# ═══════════════════════════════════════════════════════════════════════════════
# OPENFDA FIELD MAP  (clean_record key → (category, field, sub))
# sub = "text" | "table" | None
# ═══════════════════════════════════════════════════════════════════════════════
OPENFDA_FLAT_MAP = {
    # ── Labeling content ──────────────────────────────────────────────────────
    "indications_and_usage":             ("labeling_content", "indications_and_usage",             "text"),
    "indications_and_usage_table":       ("labeling_content", "indications_and_usage",             "table"),
    "dosage_and_administration":         ("labeling_content", "dosage_and_administration",         "text"),
    "dosage_tables":                     ("labeling_content", "dosage_and_administration",         "table"),
    "dosage_forms_and_strengths":        ("labeling_content", "dosage_forms_and_strengths",        "text"),
    "dosage_forms_and_strengths_table":  ("labeling_content", "dosage_forms_and_strengths",        "table"),
    "drug_description":                  ("labeling_content", "drug_description",                  None),
    "description_table":                 ("labeling_content", "description_table",                 None),
    "purpose":                           ("labeling_content", "purpose",                           "text"),
    "purpose_table":                     ("labeling_content", "purpose",                           "table"),
    "active_ingredient":                 ("labeling_content", "active_ingredient",                 "text"),
    "active_ingredient_table":           ("labeling_content", "active_ingredient",                 "table"),
    "inactive_ingredient":               ("labeling_content", "inactive_ingredient",               "text"),
    "recent_major_changes":              ("labeling_content", "recent_major_changes",              "text"),
    "recent_major_changes_table":        ("labeling_content", "recent_major_changes",              "table"),
    "unclassified":                      ("labeling_content", "unclassified",                      "text"),
    "spl_unclassified_section_table":    ("labeling_content", "unclassified",                      "table"),
    "health_claim":                      ("labeling_content", "health_claim",                      "text"),
    "statement_of_identity":             ("labeling_content", "statement_of_identity",             "text"),
    "intended_use_of_the_device":        ("labeling_content", "intended_use_of_the_device",        "text"),  # ← fixed

    # ── Safety ────────────────────────────────────────────────────────────────
    "warnings":                          ("safety", "warnings",               "text"),
    "warnings_table":                    ("safety", "warnings",               "table"),
    "warnings_and_cautions":             ("safety", "warnings_and_cautions",  "text"),
    "warnings_and_cautions_table":       ("safety", "warnings_and_cautions",  "table"),
    "boxed_warning":                     ("safety", "boxed_warning",          "text"),
    "boxed_warning_table":               ("safety", "boxed_warning",          "table"),  # ← fixed
    "contraindications":                 ("safety", "contraindications",      "text"),
    "contraindications_table":           ("safety", "contraindications",      "table"),
    "precautions":                       ("safety", "precautions",            "text"),
    "precautions_table":                 ("safety", "precautions",            "table"),
    "general_precautions":               ("safety", "general_precautions",    "text"),
    "general_precautions_table":         ("safety", "general_precautions",    "table"),
    "do_not_use":                        ("safety", "do_not_use",             "text"),
    "stop_use":                          ("safety", "stop_use",               "text"),
    "when_using":                        ("safety", "when_using",             "text"),
    "other_safety_information":          ("safety", "other_safety_information","text"),
    "safe_handling_warning":             ("safety", "safe_handling_warning",  "text"),
    "risks":                             ("safety", "risks",                  "text"),
    "user_safety_warnings":              ("safety", "user_safety_warnings",   None),   # ← fixed

    # ── Adverse events ────────────────────────────────────────────────────────
    "adverse_reactions":                 ("adverse_events", "adverse_reactions", "text"),
    "adverse_reactions_tables":          ("adverse_events", "adverse_reactions", "table"),
    "overdosage":                        ("adverse_events", "overdosage",         "text"),
    "overdosage_table":                  ("adverse_events", "overdosage",         "table"),  # ← fixed

    # ── Clinical ──────────────────────────────────────────────────────────────
    "clinical_pharmacology":             ("clinical", "clinical_pharmacology",                    "text"),
    "clinical_pharmacology_table":       ("clinical", "clinical_pharmacology",                    "table"),
    "clinical_studies":                  ("clinical", "clinical_studies",                         "text"),
    "clinical_studies_table":            ("clinical", "clinical_studies",                         "table"),
    "mechanism_of_action":               ("clinical", "mechanism_of_action",                      "text"),
    "pharmacodynamics":                  ("clinical", "pharmacodynamics",                         "text"),
    "pharmacodynamics_table":            ("clinical", "pharmacodynamics",                         "table"),
    "pharmacokinetics":                  ("clinical", "pharmacokinetics",                         "text"),
    "pharmacokinetics_table":            ("clinical", "pharmacokinetics",                         "table"),
    "microbiology":                      ("clinical", "microbiology",                             "text"),
    "microbiology_table":                ("clinical", "microbiology",                             "table"),
    "nonclinical_toxicology":            ("clinical", "nonclinical_toxicology",                   "text"),
    "nonclinical_toxicology_table":      ("clinical", "nonclinical_toxicology",                   "table"),  # ← fixed
    "animal_pharmacology_and_or_toxicology":       ("clinical", "animal_pharmacology_and_or_toxicology", "text"),
    "animal_pharmacology_and_or_toxicology_table": ("clinical", "animal_pharmacology_and_or_toxicology", "table"),
    "pharmacogenomics":                  ("clinical", "pharmacogenomics",                         "text"),
    "references":                        ("clinical", "references",                               "text"),
    "references_table":                  ("clinical", "references",                               "table"),

    # ── Patient info ──────────────────────────────────────────────────────────
    "information_for_patients":          ("patient_info", "information_for_patients",          "text"),
    "information_for_patients_table":    ("patient_info", "information_for_patients",          "table"),
    "patient_medication_information":    ("patient_info", "patient_medication_information",    "text"),
    "patient_medication_information_table": ("patient_info", "patient_medication_information", "table"),
    "medication_guide":                  ("patient_info", "medication_guide",                  "text"),
    "spl_medguide_table":                ("patient_info", "medication_guide",                  "table"),
    "instructions_for_use":              ("patient_info", "instructions_for_use",              "text"),
    "instructions_for_use_table":        ("patient_info", "instructions_for_use",              "table"),
    "ask_doctor":                        ("patient_info", "ask_doctor",                        "text"),
    "ask_doctor_or_pharmacist":          ("patient_info", "ask_doctor_or_pharmacist",          "text"),
    "spl_patient_package_insert":        ("patient_info", "spl_patient_package_insert",        "text"),
    "spl_patient_package_insert_table":  ("patient_info", "spl_patient_package_insert",        "table"),
    "pregnancy_or_breast_feeding":       ("patient_info", "pregnancy_or_breast_feeding",       "text"),
    "keep_out_of_reach":                 ("patient_info", "keep_out_of_reach",                 "text"),
    "keep_out_of_reach_of_children_table": ("patient_info", "keep_out_of_reach",              "table"),  # ← fixed
    "questions":                         ("patient_info", "questions",                         "text"),
    "information_for_owners_or_caregivers": ("patient_info", "information_for_owners_or_caregivers", "text"),  # ← fixed

    # ── Population specific ───────────────────────────────────────────────────
    "geriatric_use":                     ("population_specific", "geriatric_use",              "text"),
    "geriatric_use_table":               ("population_specific", "geriatric_use",              "table"),
    "pediatric_use":                     ("population_specific", "pediatric_use",              "text"),
    "pediatric_use_table":               ("population_specific", "pediatric_use",              "table"),
    "use_in_pregnancy":                  ("population_specific", "use_in_pregnancy",           "text"),
    "nursing_mothers":                   ("population_specific", "nursing_mothers",            "text"),
    "labor_and_delivery":                ("population_specific", "labor_and_delivery",         "text"),
    "use_in_specific_populations":       ("population_specific", "use_in_specific_populations","text"),
    "use_in_specific_populations_table": ("population_specific", "use_in_specific_populations","table"),
    "carcinogenesis_and_mutagenesis_and_impairment_of_fertility":
                                         ("population_specific", "carcinogenesis_and_mutagenesis_and_impairment_of_fertility", "text"),
    "carcinogenesis_and_mutagenesis_and_impairment_of_fertility_table":
                                         ("population_specific", "carcinogenesis_and_mutagenesis_and_impairment_of_fertility", "table"),  # ← fixed
    "teratogenic_effects":               ("population_specific", "teratogenic_effects",        "text"),
    "nonteratogenic_effects":            ("population_specific", "nonteratogenic_effects",     "text"),

    # ── Drug interactions ─────────────────────────────────────────────────────
    "drug_interactions":                 ("drug_interactions", "drug_interactions",                        "text"),
    "drug_interactions_table":           ("drug_interactions", "drug_interactions",                        "table"),
    "drug_and_or_laboratory_test_interactions":
                                         ("drug_interactions", "drug_and_or_laboratory_test_interactions", "text"),
    "drug_and_or_laboratory_test_interactions_table":
                                         ("drug_interactions", "drug_and_or_laboratory_test_interactions", "table"),  # ← fixed
    "laboratory_tests":                  ("drug_interactions", "laboratory_tests",                        "text"),

    # ── Supply / storage ──────────────────────────────────────────────────────
    "how_supplied_storage":              ("supply_storage", "how_supplied_storage", "text"),
    "how_supplied_table":                ("supply_storage", "how_supplied_storage", "table"),
    "storage_and_handling":              ("supply_storage", "storage_and_handling", "text"),
    "storage_and_handling_table":        ("supply_storage", "storage_and_handling", "table"),
    "package_label":                     ("supply_storage", "package_label",        "text"),
    "package_label_principal_display_panel_table": ("supply_storage", "package_label", "table"),
    "components":                        ("supply_storage", "components",           "text"),

    # ── Abuse / dependence ────────────────────────────────────────────────────
    "abuse":                             ("abuse_dependence", "abuse",                    "text"),
    "dependence":                        ("abuse_dependence", "dependence",               "text"),
    "controlled_substance":              ("abuse_dependence", "controlled_substance",     "text"),
    "drug_abuse_and_dependence":         ("abuse_dependence", "drug_abuse_and_dependence","text"),
    "drug_abuse_and_dependence_table":   ("abuse_dependence", "drug_abuse_and_dependence","table"),  # ← fixed
}

OPENFDA_SPECIAL = {
    "drug_info", "openfda_metadata",
    "set_id", "document_id", "version", "effective_date", "spl_product_data_elements",
}

# ═══════════════════════════════════════════════════════════════════════════════
# DAILYMED SECTION → CATEGORY MAP
# ═══════════════════════════════════════════════════════════════════════════════
DAILYMED_SECTION_TO_CAT = {
    sec: cat
    for cat, secs in {
        "labeling_content":   ["indications_and_usage", "dosage_and_administration", "unclassified", "other_sections"],
        "safety":             ["warnings", "precautions", "contraindications", "boxed_warning", "warnings_and_precautions", "general_precautions"],
        "adverse_events":     ["adverse_reactions", "overdosage"],
        "clinical":           ["clinical_pharmacology", "clinical_studies", "pharmacokinetics", "mechanism_of_action", "carcinogenesis", "references"],
        "patient_info":       ["information_for_patients"],
        "population_specific":["geriatric_use", "pediatric_use", "nursing_mothers", "labor_and_delivery", "teratogenic_effects", "nonteratogenic_effects"],
        "drug_interactions":  ["drug_interactions", "laboratory_tests"],
        "supply_storage":     ["how_supplied", "storage_and_handling", "package_label"],
    }.items()
    for sec in secs
}

# ═══════════════════════════════════════════════════════════════════════════════
# TRANSFORMATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def transform_openfda(rec: dict) -> dict:
    out = {}
    unmapped = {}

    # identification
    ident = {tgt: rec[src]
             for src, tgt in [("set_id","set_id"),("document_id","document_id"),
                              ("version","version"),("effective_date","effective_date"),
                              ("spl_product_data_elements","spl_product_data_elements")]
             if src in rec}
    if ident:
        out["identification"] = ident

    # drug_info and openfda_metadata (already structured)
    for k in ("drug_info", "openfda_metadata"):
        if k in rec:
            out[k] = copy.deepcopy(rec[k])

    # flat section fields
    for src_k, (cat, tgt_k, sub) in OPENFDA_FLAT_MAP.items():
        if src_k not in rec or rec[src_k] is None:
            continue
        val = rec[src_k]
        out.setdefault(cat, {})
        if sub is None:
            out[cat][tgt_k] = val
        else:
            out[cat].setdefault(tgt_k, {})
            out[cat][tgt_k][sub] = val

    # unmapped fields (zero data loss)
    known = OPENFDA_SPECIAL | set(OPENFDA_FLAT_MAP)
    for k, v in rec.items():
        if k not in known:
            unmapped[k] = v
    if unmapped:
        out["unmapped_fields"] = unmapped

    return out


def transform_dailymed(rec: dict) -> dict:
    out = {}
    unmapped = {}

    # drug_info
    di = {}
    if "products" in rec:
        di["products"] = copy.deepcopy(rec["products"])
    if "manufacturer" in rec:
        di["manufacturer"] = copy.deepcopy(rec["manufacturer"])
    if di:
        out["drug_info"] = di

    # identification
    if "drug_label" in rec:
        out["identification"] = {"drug_label": copy.deepcopy(rec["drug_label"])}

    # label_sections → semantic categories
    for sec_k, sec_v in rec.get("label_sections", {}).items():
        cat = DAILYMED_SECTION_TO_CAT.get(sec_k)
        if cat:
            out.setdefault(cat, {})[sec_k] = copy.deepcopy(sec_v)
        else:
            unmapped[f"label_sections.{sec_k}"] = sec_v

    # extra top-level keys
    for k, v in rec.items():
        if k not in {"products", "manufacturer", "drug_label", "label_sections"}:
            unmapped[k] = v
    if unmapped:
        out["unmapped_fields"] = unmapped

    return out


# ═══════════════════════════════════════════════════════════════════════════════
# PROGRESS BAR
# ═══════════════════════════════════════════════════════════════════════════════

def progress_bar(done, total, t0, width=38):
    filled  = int(width * done / total) if total else 0
    bar     = "=" * filled + ">" + " " * max(0, width - filled - 1)
    pct     = 100 * done / total if total else 0
    elapsed = time.time() - t0
    speed   = done / elapsed if elapsed > 0 else 0
    rem     = (total - done) / speed if speed > 0 else 0
    eta     = str(timedelta(seconds=int(rem)))
    print(f"\r  [{bar}] {done:>9,}/{total:,} ({pct:5.1f}%) | "
          f"{speed:6.0f} rec/s | ETA {eta}   ", end="", flush=True)


# ═══════════════════════════════════════════════════════════════════════════════
# CORE BATCH PROCESSOR
# ═══════════════════════════════════════════════════════════════════════════════

def process_source(write_conn, source, src_col, transform_fn, total):
    """
    Stream all rows via a dedicated read connection (autocommit=True so
    the server-side cursor survives write-connection commits).
    """
    processed = errors = 0
    t0 = time.time()

    # Dedicated read-only connection: autocommit=False (default) so named
    # cursor stays alive; we never commit it, avoiding cursor invalidation.
    read_conn = psycopg2.connect(**DB)
    read_conn.autocommit = False

    try:
        with read_conn.cursor(name=f"stream_{source}") as cur:
            cur.execute(
                f'SELECT {PK}, {src_col} FROM "{TABLE}" '
                f'WHERE source = %s AND {src_col} IS NOT NULL ORDER BY {PK}',
                (source,)
            )

            batch = []
            while True:
                chunk = cur.fetchmany(BATCH_SIZE)
                if not chunk:
                    break

                for pk, rec in chunk:
                    try:
                        if isinstance(rec, str):
                            rec = json.loads(rec)
                        out = transform_fn(rec)
                        batch.append((json.dumps(out, default=str), str(pk)))
                        processed += 1
                    except Exception as exc:
                        errors += 1
                        logging.error("[%s] id=%s: %s", source, pk, exc)

                if batch:
                    with write_conn.cursor() as upd:
                        psycopg2.extras.execute_batch(
                            upd,
                            f'UPDATE "{TABLE}" SET {DST_COL} = %s::jsonb '
                            f'WHERE {PK} = %s::uuid',
                            batch,
                            page_size=BATCH_SIZE,
                        )
                    write_conn.commit()
                    batch = []

                progress_bar(processed, total, t0)
    finally:
        read_conn.close()

    elapsed = time.time() - t0
    print()  # newline after progress bar
    return processed, errors, round(elapsed, 1)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("Connecting to database…")
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    print(f"✓ Connected to {DB['host']} / {DB['database']}\n")

    # ── Get counts ────────────────────────────────────────────────────────────
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT source,
                   COUNT(*)                    AS total,
                   COUNT(clean_record)         AS has_clean,
                   COUNT(record)               AS has_record
            FROM "{TABLE}"
            GROUP BY source ORDER BY source
        """)
        counts = {r[0]: {"total": r[1], "has_clean": r[2], "has_record": r[3]}
                  for r in cur.fetchall()}

    print("=" * 60)
    print("PHASE 2: FULL POPULATION")
    print("=" * 60)
    print(f"\n{'Source':<12} {'Total':>10}  {'Src col':<14}  {'Strategy'}")
    print("-" * 60)
    for src, c in counts.items():
        col = "clean_record" if c["has_clean"] > 0 else "record"
        strat = "transform" if src in ("openfda","dailymed") else "copy as-is"
        print(f"  {src:<10} {c['total']:>10,}  {col:<14}  {strat}")
    grand_total = sum(c["total"] for c in counts.values())
    print(f"\n  TOTAL: {grand_total:,} records\n")

    wall_start = time.time()
    all_stats  = {}

    # ── Processing plan ───────────────────────────────────────────────────────
    # (source, src_col, transform_fn)
    plan = [
        ("openfda",  "clean_record", transform_openfda),
        ("dailymed", "clean_record", transform_dailymed),
        ("drugbank", "clean_record", lambda r: copy.deepcopy(r)),
        ("rxnorm",   "record",       lambda r: copy.deepcopy(r)),
    ]

    for source, src_col, fn in plan:
        total = counts.get(source, {}).get("total", 0)
        if total == 0:
            print(f"\n[{source.upper()}] No records — skipping.")
            continue

        verb = "Transforming" if source in ("openfda", "dailymed") else "Copying"
        print(f"\n{verb} {source.upper()} ({total:,} records from {src_col})…")

        done, errs, elapsed = process_source(conn, source, src_col, fn, total)

        all_stats[source] = {
            "total_records": total,
            "processed": done,
            "errors": errs,
            "processing_time_seconds": elapsed,
            "source_column": src_col,
            "strategy": "transform" if source in ("openfda", "dailymed") else "copy",
        }
        print(f"  ✓ {done:,}/{total:,} done | {errs} errors | {elapsed}s")

    wall_elapsed = round(time.time() - wall_start, 1)
    all_stats["total_processing_time_seconds"] = wall_elapsed

    # ── Save stats ────────────────────────────────────────────────────────────
    with open("population_stats.json", "w") as f:
        json.dump(all_stats, f, indent=2)

    # ── Verification queries ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    with conn.cursor() as cur:
        # Count populated vs total
        cur.execute(f"""
            SELECT source,
                   COUNT(*)               AS total,
                   COUNT({DST_COL})       AS populated,
                   COUNT(*) - COUNT({DST_COL}) AS missing
            FROM "{TABLE}"
            GROUP BY source
            ORDER BY source
        """)
        rows = cur.fetchall()
        print(f"\n  {'Source':<12} {'Total':>10}  {'Populated':>10}  {'Missing':>8}")
        print(f"  {'-'*12} {'-'*10}  {'-'*10}  {'-'*8}")
        for src, tot, pop, miss in rows:
            ok = "✓" if miss == 0 else "✗"
            print(f"  {ok} {(src or 'NULL'):<10} {tot:>10,}  {pop:>10,}  {miss:>8,}")

        # Sample top-level keys per source
        print("\n  Top-level keys in standardized_records (1 sample per source):")
        for src in ["openfda", "dailymed", "rxnorm", "drugbank"]:
            cur.execute(f"""
                SELECT jsonb_object_keys({DST_COL})
                FROM "{TABLE}"
                WHERE source = %s AND {DST_COL} IS NOT NULL
                LIMIT 1
            """, (src,))
            keys = [r[0] for r in cur.fetchall()]
            print(f"    {src:<10}: {sorted(keys)}")

        # Check for unmapped_fields in openfda/dailymed
        print("\n  Records with unmapped_fields:")
        for src in ["openfda", "dailymed"]:
            cur.execute(f"""
                SELECT COUNT(*) FROM "{TABLE}"
                WHERE source = %s
                  AND {DST_COL} ? 'unmapped_fields'
                  AND {DST_COL}->'unmapped_fields' != '{{}}'::jsonb
            """, (src,))
            cnt = cur.fetchone()[0]
            print(f"    {src:<10}: {cnt:,} records have unmapped_fields")

    conn.close()

    # ── Final summary ─────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("COMPLETION SUMMARY")
    print("=" * 60)
    total_errors = sum(s.get("errors", 0) for s in all_stats.values() if isinstance(s, dict))
    for src, s in all_stats.items():
        if not isinstance(s, dict):
            continue
        done  = s.get("processed", 0)
        total = s.get("total_records", 0)
        errs  = s.get("errors", 0)
        verb  = "copied" if s["strategy"] == "copy" else "transformed"
        print(f"  ✓ {src.upper():<12}: {done:>10,}/{total:,} {verb} | {errs} errors")
    print(f"\n  Total time   : {timedelta(seconds=int(wall_elapsed))}")
    print(f"  Total errors : {total_errors}  (see transformation_errors.log)")
    print(f"  Stats saved  : population_stats.json")


if __name__ == "__main__":
    main()
