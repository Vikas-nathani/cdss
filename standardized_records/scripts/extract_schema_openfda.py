#!/usr/bin/env python3
"""
Phase 1 – Data structure analysis + field mapping design for all sources.

Reads from: clean_record  (pre-processed column)
Writes to:  standardized_records  (target column, JSONB)

Outputs:
  field_mapping_openfda.json
  field_mapping_dailymed.json
  transformation_samples.json  (10 before/after per source)
  phase1_analysis_report.txt
"""

import copy
import json
import os
from pathlib import Path
import psycopg2

DB = dict(host=os.environ.get("DB_HOST", "localhost"), port=5432, user="postgres",
          password=os.environ.get("DB_PASSWORD", ""), database="postgres")
TABLE    = "DrugSourceMaster"
SRC_COL  = "clean_record"
DATA_DIR = Path("/home/nathanivikas890_gmail_com/cdss/data")

# ═══════════════════════════════════════════════════════════════════════════
# OPENFDA MAPPING RULES
#
# Each entry:  source_key  →  (category, target_key, sub_key)
#   sub_key = "text"  → standardized[cat][tgt]["text"]  = record[src]
#   sub_key = "table" → standardized[cat][tgt]["table"] = record[src]
#   sub_key = None    → standardized[cat][tgt]          = record[src]
#
# Special entries handled separately:
#   "drug_info"       → copy drug_info object directly
#   "openfda_metadata"→ copy openfda_metadata object directly
#   "identification"  → built from set_id / document_id / version / effective_date / spl_product_data_elements
# ═══════════════════════════════════════════════════════════════════════════

OPENFDA_FLAT_MAP = {
    # ── Labeling content ─────────────────────────────────────────────────
    "indications_and_usage":             ("labeling_content", "indications_and_usage",              "text"),
    "indications_and_usage_table":       ("labeling_content", "indications_and_usage",              "table"),
    "dosage_and_administration":         ("labeling_content", "dosage_and_administration",          "text"),
    "dosage_tables":                     ("labeling_content", "dosage_and_administration",          "table"),
    "dosage_forms_and_strengths":        ("labeling_content", "dosage_forms_and_strengths",         "text"),
    "dosage_forms_and_strengths_table":  ("labeling_content", "dosage_forms_and_strengths",         "table"),
    "drug_description":                  ("labeling_content", "drug_description",                   None),
    "description_table":                 ("labeling_content", "description_table",                  None),
    "purpose":                           ("labeling_content", "purpose",                            "text"),
    "purpose_table":                     ("labeling_content", "purpose",                            "table"),
    "active_ingredient":                 ("labeling_content", "active_ingredient",                  "text"),
    "active_ingredient_table":           ("labeling_content", "active_ingredient",                  "table"),
    "inactive_ingredient":               ("labeling_content", "inactive_ingredient",                "text"),
    "recent_major_changes":              ("labeling_content", "recent_major_changes",               "text"),
    "recent_major_changes_table":        ("labeling_content", "recent_major_changes",               "table"),
    "unclassified":                      ("labeling_content", "unclassified",                       "text"),
    "spl_unclassified_section_table":    ("labeling_content", "unclassified",                       "table"),
    "health_claim":                      ("labeling_content", "health_claim",                       "text"),
    "statement_of_identity":             ("labeling_content", "statement_of_identity",              "text"),

    # ── Safety ───────────────────────────────────────────────────────────
    "warnings":                          ("safety", "warnings",              "text"),
    "warnings_table":                    ("safety", "warnings",              "table"),
    "warnings_and_cautions":             ("safety", "warnings_and_cautions", "text"),
    "warnings_and_cautions_table":       ("safety", "warnings_and_cautions", "table"),
    "boxed_warning":                     ("safety", "boxed_warning",         "text"),
    "contraindications":                 ("safety", "contraindications",     "text"),
    "contraindications_table":           ("safety", "contraindications",     "table"),
    "precautions":                       ("safety", "precautions",           "text"),
    "precautions_table":                 ("safety", "precautions",           "table"),
    "general_precautions":               ("safety", "general_precautions",   "text"),
    "general_precautions_table":         ("safety", "general_precautions",   "table"),
    "do_not_use":                        ("safety", "do_not_use",            "text"),
    "stop_use":                          ("safety", "stop_use",              "text"),
    "when_using":                        ("safety", "when_using",            "text"),
    "other_safety_information":          ("safety", "other_safety_information", "text"),
    "safe_handling_warning":             ("safety", "safe_handling_warning", "text"),
    "risks":                             ("safety", "risks",                 "text"),

    # ── Adverse events ───────────────────────────────────────────────────
    "adverse_reactions":                 ("adverse_events", "adverse_reactions", "text"),
    "adverse_reactions_tables":          ("adverse_events", "adverse_reactions", "table"),
    "overdosage":                        ("adverse_events", "overdosage",         "text"),

    # ── Clinical ─────────────────────────────────────────────────────────
    "clinical_pharmacology":             ("clinical", "clinical_pharmacology",                   "text"),
    "clinical_pharmacology_table":       ("clinical", "clinical_pharmacology",                   "table"),
    "clinical_studies":                  ("clinical", "clinical_studies",                        "text"),
    "clinical_studies_table":            ("clinical", "clinical_studies",                        "table"),
    "mechanism_of_action":               ("clinical", "mechanism_of_action",                     "text"),
    "pharmacodynamics":                  ("clinical", "pharmacodynamics",                        "text"),
    "pharmacodynamics_table":            ("clinical", "pharmacodynamics",                        "table"),
    "pharmacokinetics":                  ("clinical", "pharmacokinetics",                        "text"),
    "pharmacokinetics_table":            ("clinical", "pharmacokinetics",                        "table"),
    "microbiology":                      ("clinical", "microbiology",                            "text"),
    "microbiology_table":                ("clinical", "microbiology",                            "table"),
    "nonclinical_toxicology":            ("clinical", "nonclinical_toxicology",                  "text"),
    "animal_pharmacology_and_or_toxicology": ("clinical", "animal_pharmacology_and_or_toxicology", "text"),
    "animal_pharmacology_and_or_toxicology_table": ("clinical", "animal_pharmacology_and_or_toxicology", "table"),
    "pharmacogenomics":                  ("clinical", "pharmacogenomics",                        "text"),
    "references":                        ("clinical", "references",                              "text"),
    "references_table":                  ("clinical", "references",                              "table"),

    # ── Patient info ─────────────────────────────────────────────────────
    "information_for_patients":          ("patient_info", "information_for_patients",         "text"),
    "information_for_patients_table":    ("patient_info", "information_for_patients",         "table"),
    "patient_medication_information":    ("patient_info", "patient_medication_information",   "text"),
    "patient_medication_information_table": ("patient_info", "patient_medication_information","table"),
    "medication_guide":                  ("patient_info", "medication_guide",                 "text"),
    "spl_medguide_table":                ("patient_info", "medication_guide",                 "table"),
    "instructions_for_use":              ("patient_info", "instructions_for_use",             "text"),
    "instructions_for_use_table":        ("patient_info", "instructions_for_use",             "table"),
    "ask_doctor":                        ("patient_info", "ask_doctor",                       "text"),
    "ask_doctor_or_pharmacist":          ("patient_info", "ask_doctor_or_pharmacist",         "text"),
    "spl_patient_package_insert":        ("patient_info", "spl_patient_package_insert",       "text"),
    "spl_patient_package_insert_table":  ("patient_info", "spl_patient_package_insert",       "table"),
    "pregnancy_or_breast_feeding":       ("patient_info", "pregnancy_or_breast_feeding",      "text"),
    "keep_out_of_reach":                 ("patient_info", "keep_out_of_reach",                "text"),
    "questions":                         ("patient_info", "questions",                        "text"),

    # ── Population specific ───────────────────────────────────────────────
    "geriatric_use":                     ("population_specific", "geriatric_use",             "text"),
    "geriatric_use_table":               ("population_specific", "geriatric_use",             "table"),
    "pediatric_use":                     ("population_specific", "pediatric_use",             "text"),
    "pediatric_use_table":               ("population_specific", "pediatric_use",             "table"),
    "use_in_pregnancy":                  ("population_specific", "use_in_pregnancy",          "text"),
    "nursing_mothers":                   ("population_specific", "nursing_mothers",           "text"),
    "labor_and_delivery":                ("population_specific", "labor_and_delivery",        "text"),
    "use_in_specific_populations":       ("population_specific", "use_in_specific_populations", "text"),
    "use_in_specific_populations_table": ("population_specific", "use_in_specific_populations", "table"),
    "carcinogenesis_and_mutagenesis_and_impairment_of_fertility":
                                         ("population_specific", "carcinogenesis_and_mutagenesis_and_impairment_of_fertility", "text"),
    "teratogenic_effects":               ("population_specific", "teratogenic_effects",       "text"),
    "nonteratogenic_effects":            ("population_specific", "nonteratogenic_effects",    "text"),

    # ── Drug interactions ─────────────────────────────────────────────────
    "drug_interactions":                 ("drug_interactions", "drug_interactions",               "text"),
    "drug_interactions_table":           ("drug_interactions", "drug_interactions",               "table"),
    "drug_and_or_laboratory_test_interactions":
                                         ("drug_interactions", "drug_and_or_laboratory_test_interactions", "text"),
    "laboratory_tests":                  ("drug_interactions", "laboratory_tests",               "text"),

    # ── Supply / storage ──────────────────────────────────────────────────
    "how_supplied_storage":              ("supply_storage", "how_supplied_storage",    "text"),
    "how_supplied_table":                ("supply_storage", "how_supplied_storage",    "table"),
    "storage_and_handling":              ("supply_storage", "storage_and_handling",    "text"),
    "storage_and_handling_table":        ("supply_storage", "storage_and_handling",    "table"),
    "package_label":                     ("supply_storage", "package_label",           "text"),
    "package_label_principal_display_panel_table": ("supply_storage", "package_label", "table"),
    "components":                        ("supply_storage", "components",              "text"),

    # ── Abuse / dependence ────────────────────────────────────────────────
    "abuse":                             ("abuse_dependence", "abuse",                   "text"),
    "dependence":                        ("abuse_dependence", "dependence",              "text"),
    "controlled_substance":              ("abuse_dependence", "controlled_substance",    "text"),
    "drug_abuse_and_dependence":         ("abuse_dependence", "drug_abuse_and_dependence","text"),
}

# Fields handled via dedicated logic (not flat map)
OPENFDA_SPECIAL = {
    "drug_info",            # copy entire sub-object → drug_info
    "openfda_metadata",     # copy entire sub-object → openfda_metadata
    "set_id",               # → identification.set_id
    "document_id",          # → identification.document_id
    "version",              # → identification.version
    "effective_date",       # → identification.effective_date
    "spl_product_data_elements",  # → identification.spl_product_data_elements
}

# ═══════════════════════════════════════════════════════════════════════════
# DAILYMED MAPPING RULES
#
# DailyMed clean_record already matches master_schema_dailymed.json:
#   {products, drug_label, manufacturer, label_sections.*}
#
# We apply the SAME semantic normalization as the schema-level normalization:
#   products   → drug_info.products
#   manufacturer → drug_info.manufacturer
#   drug_label → identification.drug_label
#   label_sections.warnings → safety.warnings
#   label_sections.precautions → safety.precautions
#   etc.
# ═══════════════════════════════════════════════════════════════════════════

DAILYMED_LABEL_SECTION_CATEGORIES = {
    "labeling_content": [
        "indications_and_usage", "dosage_and_administration",
        "unclassified", "other_sections",
    ],
    "safety": [
        "warnings", "precautions", "contraindications",
        "boxed_warning", "warnings_and_precautions", "general_precautions",
    ],
    "adverse_events": [
        "adverse_reactions", "overdosage",
    ],
    "clinical": [
        "clinical_pharmacology", "clinical_studies", "pharmacokinetics",
        "mechanism_of_action", "carcinogenesis", "references",
    ],
    "patient_info": [
        "information_for_patients",
    ],
    "population_specific": [
        "geriatric_use", "pediatric_use", "nursing_mothers",
        "labor_and_delivery", "teratogenic_effects", "nonteratogenic_effects",
    ],
    "drug_interactions": [
        "drug_interactions", "laboratory_tests",
    ],
    "supply_storage": [
        "how_supplied", "storage_and_handling", "package_label",
    ],
}

DAILYMED_SECTION_TO_CAT = {
    sec: cat
    for cat, secs in DAILYMED_LABEL_SECTION_CATEGORIES.items()
    for sec in secs
}

# ═══════════════════════════════════════════════════════════════════════════
# TRANSFORMATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def transform_openfda(rec: dict) -> dict:
    """Map OpenFDA clean_record → normalized standardized_records."""
    out = {}
    unmapped = {}

    # 1. identification
    ident = {}
    for src_k, tgt_k in [("set_id", "set_id"), ("document_id", "document_id"),
                          ("version", "version"), ("effective_date", "effective_date"),
                          ("spl_product_data_elements", "spl_product_data_elements")]:
        if src_k in rec:
            ident[tgt_k] = rec[src_k]
    if ident:
        out["identification"] = ident

    # 2. drug_info (already structured in clean_record)
    if "drug_info" in rec:
        out["drug_info"] = copy.deepcopy(rec["drug_info"])

    # 3. openfda_metadata (already structured)
    if "openfda_metadata" in rec:
        out["openfda_metadata"] = copy.deepcopy(rec["openfda_metadata"])

    # 4. Flat section fields → category.field.{text|table}
    for src_k, (cat, tgt_k, sub) in OPENFDA_FLAT_MAP.items():
        if src_k not in rec:
            continue
        val = rec[src_k]
        if val is None:
            continue
        if cat not in out:
            out[cat] = {}
        if sub is None:
            # direct value (e.g. drug_description, description_table)
            out[cat][tgt_k] = val
        else:
            if tgt_k not in out[cat]:
                out[cat][tgt_k] = {}
            out[cat][tgt_k][sub] = val

    # 5. Collect unmapped fields (zero data loss)
    known = OPENFDA_SPECIAL | set(OPENFDA_FLAT_MAP.keys())
    for k, v in rec.items():
        if k not in known:
            unmapped[k] = v

    if unmapped:
        out["unmapped_fields"] = unmapped

    return out


def transform_dailymed(rec: dict) -> dict:
    """Map DailyMed clean_record → normalized standardized_records."""
    out = {}
    unmapped = {}

    # 1. drug_info: products + manufacturer
    drug_info = {}
    if "products" in rec:
        drug_info["products"] = copy.deepcopy(rec["products"])
    if "manufacturer" in rec:
        drug_info["manufacturer"] = copy.deepcopy(rec["manufacturer"])
    if drug_info:
        out["drug_info"] = drug_info

    # 2. identification: drug_label
    if "drug_label" in rec:
        out["identification"] = {"drug_label": copy.deepcopy(rec["drug_label"])}

    # 3. label_sections → semantic categories
    label_sections = rec.get("label_sections", {})
    if isinstance(label_sections, dict):
        for sec_key, sec_val in label_sections.items():
            cat = DAILYMED_SECTION_TO_CAT.get(sec_key)
            if cat:
                if cat not in out:
                    out[cat] = {}
                out[cat][sec_key] = copy.deepcopy(sec_val)
            else:
                unmapped[f"label_sections.{sec_key}"] = sec_val

    # 4. Unmapped top-level keys
    known_top = {"products", "manufacturer", "drug_label", "label_sections"}
    for k, v in rec.items():
        if k not in known_top:
            unmapped[k] = v

    if unmapped:
        out["unmapped_fields"] = unmapped

    return out


def transform_rxnorm(rec: dict) -> dict:
    """RxNorm: copy record as-is (no clean_record available)."""
    return copy.deepcopy(rec)


def transform_drugbank(rec: dict) -> dict:
    """DrugBank: copy clean_record as-is."""
    return copy.deepcopy(rec)


# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS RUNNER
# ═══════════════════════════════════════════════════════════════════════════

def collect_all_paths(obj, prefix=""):
    """Collect all dot-paths in a JSON object."""
    paths = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else k
            paths.append(p)
            paths.extend(collect_all_paths(v, p))
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                paths.extend(collect_all_paths(item, prefix))
    return paths


def run_phase1():
    print("Connecting to DB…")
    conn = psycopg2.connect(**DB)
    cur  = conn.cursor()
    print("✓ Connected\n")

    samples_output = {}
    report_lines   = []

    def section(title):
        line = f"\n{'='*70}\n{title}\n{'='*70}"
        print(line)
        report_lines.append(line)

    def log(msg):
        print(msg)
        report_lines.append(msg)

    # ── OpenFDA ───────────────────────────────────────────────────────────
    section("OPENFDA ANALYSIS (clean_record)")

    cur.execute(f"""SELECT clean_record FROM "{TABLE}"
                   WHERE source='openfda' AND {SRC_COL} IS NOT NULL LIMIT 10""")
    rows = cur.fetchall()
    samples_output["openfda"] = []

    all_source_paths = set()
    all_out_paths    = set()

    for i, (rec,) in enumerate(rows):
        out = transform_openfda(rec)
        src_paths = set(collect_all_paths(rec))
        dst_paths = set(collect_all_paths(out))
        all_source_paths |= src_paths
        all_out_paths    |= dst_paths
        if i < 3:
            log(f"\n  Record {i+1}:")
            log(f"    source top-level keys : {sorted(rec.keys())}")
            log(f"    output top-level keys : {sorted(out.keys())}")
            uf = out.get("unmapped_fields", {})
            if uf:
                log(f"    unmapped_fields       : {list(uf.keys())}")
        samples_output["openfda"].append({
            "id": i + 1,
            "source_top_keys": sorted(rec.keys()),
            "output_top_keys": sorted(out.keys()),
            "unmapped_fields": list(out.get("unmapped_fields", {}).keys()),
            "before": rec,
            "after":  out,
        })

    # Scan 500 records for unmapped coverage
    cur.execute(f"""SELECT clean_record FROM "{TABLE}"
                   WHERE source='openfda' AND {SRC_COL} IS NOT NULL LIMIT 500""")
    unmapped_keys = set()
    for (rec,) in cur.fetchall():
        out = transform_openfda(rec)
        unmapped_keys |= set(out.get("unmapped_fields", {}).keys())

    log(f"\n  Total mapped source keys     : {len(OPENFDA_FLAT_MAP) + len(OPENFDA_SPECIAL)}")
    log(f"  Unmapped fields (500 rec)    : {sorted(unmapped_keys) or 'NONE'}")

    # ── DailyMed ──────────────────────────────────────────────────────────
    section("DAILYMED ANALYSIS (clean_record)")

    cur.execute(f"""SELECT clean_record FROM "{TABLE}"
                   WHERE source='dailymed' AND {SRC_COL} IS NOT NULL LIMIT 10""")
    rows = cur.fetchall()
    samples_output["dailymed"] = []

    for i, (rec,) in enumerate(rows):
        out = transform_dailymed(rec)
        if i < 3:
            log(f"\n  Record {i+1}:")
            log(f"    source top-level keys : {sorted(rec.keys())}")
            log(f"    output top-level keys : {sorted(out.keys())}")
            uf = out.get("unmapped_fields", {})
            if uf:
                log(f"    unmapped_fields       : {list(uf.keys())}")
        samples_output["dailymed"].append({
            "id": i + 1,
            "source_top_keys": sorted(rec.keys()),
            "output_top_keys": sorted(out.keys()),
            "unmapped_fields": list(out.get("unmapped_fields", {}).keys()),
            "before": rec,
            "after":  out,
        })

    cur.execute(f"""SELECT clean_record FROM "{TABLE}"
                   WHERE source='dailymed' AND {SRC_COL} IS NOT NULL LIMIT 500""")
    dm_unmapped = set()
    for (rec,) in cur.fetchall():
        out = transform_dailymed(rec)
        dm_unmapped |= set(out.get("unmapped_fields", {}).keys())

    log(f"\n  Total label_sections sections : {sum(len(v) for v in DAILYMED_LABEL_SECTION_CATEGORIES.values())}")
    log(f"  Semantic categories           : {len(DAILYMED_LABEL_SECTION_CATEGORIES)}")
    log(f"  Unmapped fields (500 rec)     : {sorted(dm_unmapped) or 'NONE'}")

    # ── RxNorm ────────────────────────────────────────────────────────────
    section("RXNORM ANALYSIS (record — no clean_record)")
    cur.execute(f"""SELECT record FROM "{TABLE}"
                   WHERE source='rxnorm' AND record IS NOT NULL LIMIT 3""")
    rows = cur.fetchall()
    samples_output["rxnorm"] = []
    for i, (rec,) in enumerate(rows):
        log(f"  Record {i+1} keys: {sorted(rec.keys())}")
        samples_output["rxnorm"].append({
            "id": i + 1,
            "source_top_keys": sorted(rec.keys()),
            "note": "copied as-is from record column",
            "before": rec,
            "after": transform_rxnorm(rec),
        })
    log("  → Strategy: exact copy from record column (no clean_record exists)")

    # ── DrugBank ──────────────────────────────────────────────────────────
    section("DRUGBANK ANALYSIS (clean_record)")
    cur.execute(f"""SELECT clean_record FROM "{TABLE}"
                   WHERE source='drugbank' AND {SRC_COL} IS NOT NULL LIMIT 3""")
    rows = cur.fetchall()
    samples_output["drugbank"] = []
    for i, (rec,) in enumerate(rows):
        log(f"  Record {i+1} keys: {sorted(rec.keys())}")
        samples_output["drugbank"].append({
            "id": i + 1,
            "source_top_keys": sorted(rec.keys()),
            "note": "copied as-is",
            "before": rec,
            "after": transform_drugbank(rec),
        })
    log("  → Strategy: exact copy of clean_record")

    # ── Summary ───────────────────────────────────────────────────────────
    section("TEMPLATE COMPARISON SUMMARY")
    with open(DATA_DIR / "normalized_schema.json") as f:
        openfda_tpl = json.load(f)
    with open(DATA_DIR / "master_schema_dailymed_normalized.json") as f:
        dailymed_tpl = json.load(f)

    log(f"\n  OpenFDA template top-level categories : {list(openfda_tpl.keys())}")
    log(f"  DailyMed template top-level categories: {list(dailymed_tpl.keys())}")

    log("\n  OpenFDA field mapping coverage:")
    log(f"    Flat section fields mapped  : {len(OPENFDA_FLAT_MAP)}")
    log(f"    Special fields (drug_info, metadata, identification): {len(OPENFDA_SPECIAL)}")
    log(f"    Total rules                 : {len(OPENFDA_FLAT_MAP) + len(OPENFDA_SPECIAL)}")
    log(f"    Unmapped in 500-rec scan    : {sorted(unmapped_keys) or 'NONE'}")

    log("\n  DailyMed field mapping coverage:")
    log(f"    label_sections keys mapped  : {len(DAILYMED_SECTION_TO_CAT)}")
    log(f"    Structural keys (products, manufacturer, drug_label): 3")
    log(f"    Unmapped in 500-rec scan    : {sorted(dm_unmapped) or 'NONE'}")

    conn.close()

    # ── Save files ────────────────────────────────────────────────────────
    openfda_mapping = {
        "description": "OpenFDA clean_record field → standardized_records path mapping",
        "source_column": "clean_record",
        "target_column": "standardized_records",
        "special_fields": {
            "drug_info": "Copied directly to standardized_records.drug_info",
            "openfda_metadata": "Copied directly to standardized_records.openfda_metadata",
            "set_id": "standardized_records.identification.set_id",
            "document_id": "standardized_records.identification.document_id",
            "version": "standardized_records.identification.version",
            "effective_date": "standardized_records.identification.effective_date",
            "spl_product_data_elements": "standardized_records.identification.spl_product_data_elements",
        },
        "flat_map": {
            src: f"standardized_records.{cat}.{tgt}" + (f".{sub}" if sub else "")
            for src, (cat, tgt, sub) in OPENFDA_FLAT_MAP.items()
        },
        "unmapped_fields_found": sorted(unmapped_keys),
        "total_mapped_fields": len(OPENFDA_FLAT_MAP) + len(OPENFDA_SPECIAL),
    }

    dailymed_mapping = {
        "description": "DailyMed clean_record field → standardized_records path mapping",
        "source_column": "clean_record",
        "target_column": "standardized_records",
        "structural_mappings": {
            "products":      "standardized_records.drug_info.products",
            "manufacturer":  "standardized_records.drug_info.manufacturer",
            "drug_label":    "standardized_records.identification.drug_label",
        },
        "label_sections_map": {
            sec: f"standardized_records.{cat}.{sec}"
            for sec, cat in DAILYMED_SECTION_TO_CAT.items()
        },
        "unmapped_fields_found": sorted(dm_unmapped),
        "total_sections_mapped": len(DAILYMED_SECTION_TO_CAT),
    }

    with open("field_mapping_openfda.json", "w") as f:
        json.dump(openfda_mapping, f, indent=2)
    with open("field_mapping_dailymed.json", "w") as f:
        json.dump(dailymed_mapping, f, indent=2)
    with open("transformation_samples.json", "w") as f:
        json.dump(samples_output, f, indent=2, default=str)
    with open("phase1_analysis_report.txt", "w") as f:
        f.write("\n".join(report_lines))

    print("\n✓ Saved: field_mapping_openfda.json")
    print("✓ Saved: field_mapping_dailymed.json")
    print("✓ Saved: transformation_samples.json")
    print("✓ Saved: phase1_analysis_report.txt")
    print("\nReview above + files, then confirm Phase 2 (full population).")


if __name__ == "__main__":
    run_phase1()
