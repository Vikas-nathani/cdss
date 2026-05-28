#!/usr/bin/env python3
"""
Deep field-by-field comparison: clean_record vs standardized_records
for 10 random rows each from openfda and dailymed.
"""
import json
import os
import psycopg2

DB = dict(host=os.environ.get("DB_HOST", "localhost"), port=5432,
          user="postgres", password=os.environ.get("DB_PASSWORD", ""), database="postgres")

# ── Same mapping tables used during population ────────────────────────────────

OPENFDA_FLAT_MAP = {
    "indications_and_usage":             ("labeling_content","indications_and_usage","text"),
    "indications_and_usage_table":       ("labeling_content","indications_and_usage","table"),
    "dosage_and_administration":         ("labeling_content","dosage_and_administration","text"),
    "dosage_tables":                     ("labeling_content","dosage_and_administration","table"),
    "dosage_forms_and_strengths":        ("labeling_content","dosage_forms_and_strengths","text"),
    "dosage_forms_and_strengths_table":  ("labeling_content","dosage_forms_and_strengths","table"),
    "drug_description":                  ("labeling_content","drug_description",None),
    "description_table":                 ("labeling_content","description_table",None),
    "purpose":                           ("labeling_content","purpose","text"),
    "purpose_table":                     ("labeling_content","purpose","table"),
    "active_ingredient":                 ("labeling_content","active_ingredient","text"),
    "active_ingredient_table":           ("labeling_content","active_ingredient","table"),
    "inactive_ingredient":               ("labeling_content","inactive_ingredient","text"),
    "recent_major_changes":              ("labeling_content","recent_major_changes","text"),
    "recent_major_changes_table":        ("labeling_content","recent_major_changes","table"),
    "unclassified":                      ("labeling_content","unclassified","text"),
    "spl_unclassified_section_table":    ("labeling_content","unclassified","table"),
    "health_claim":                      ("labeling_content","health_claim","text"),
    "statement_of_identity":             ("labeling_content","statement_of_identity","text"),
    "intended_use_of_the_device":        ("labeling_content","intended_use_of_the_device","text"),
    "warnings":                          ("safety","warnings","text"),
    "warnings_table":                    ("safety","warnings","table"),
    "warnings_and_cautions":             ("safety","warnings_and_cautions","text"),
    "warnings_and_cautions_table":       ("safety","warnings_and_cautions","table"),
    "boxed_warning":                     ("safety","boxed_warning","text"),
    "boxed_warning_table":               ("safety","boxed_warning","table"),
    "contraindications":                 ("safety","contraindications","text"),
    "contraindications_table":           ("safety","contraindications","table"),
    "precautions":                       ("safety","precautions","text"),
    "precautions_table":                 ("safety","precautions","table"),
    "general_precautions":               ("safety","general_precautions","text"),
    "general_precautions_table":         ("safety","general_precautions","table"),
    "do_not_use":                        ("safety","do_not_use","text"),
    "stop_use":                          ("safety","stop_use","text"),
    "when_using":                        ("safety","when_using","text"),
    "other_safety_information":          ("safety","other_safety_information","text"),
    "safe_handling_warning":             ("safety","safe_handling_warning","text"),
    "risks":                             ("safety","risks","text"),
    "user_safety_warnings":              ("safety","user_safety_warnings",None),
    "adverse_reactions":                 ("adverse_events","adverse_reactions","text"),
    "adverse_reactions_tables":          ("adverse_events","adverse_reactions","table"),
    "overdosage":                        ("adverse_events","overdosage","text"),
    "overdosage_table":                  ("adverse_events","overdosage","table"),
    "clinical_pharmacology":             ("clinical","clinical_pharmacology","text"),
    "clinical_pharmacology_table":       ("clinical","clinical_pharmacology","table"),
    "clinical_studies":                  ("clinical","clinical_studies","text"),
    "clinical_studies_table":            ("clinical","clinical_studies","table"),
    "mechanism_of_action":               ("clinical","mechanism_of_action","text"),
    "pharmacodynamics":                  ("clinical","pharmacodynamics","text"),
    "pharmacodynamics_table":            ("clinical","pharmacodynamics","table"),
    "pharmacokinetics":                  ("clinical","pharmacokinetics","text"),
    "pharmacokinetics_table":            ("clinical","pharmacokinetics","table"),
    "microbiology":                      ("clinical","microbiology","text"),
    "microbiology_table":                ("clinical","microbiology","table"),
    "nonclinical_toxicology":            ("clinical","nonclinical_toxicology","text"),
    "nonclinical_toxicology_table":      ("clinical","nonclinical_toxicology","table"),
    "animal_pharmacology_and_or_toxicology":      ("clinical","animal_pharmacology_and_or_toxicology","text"),
    "animal_pharmacology_and_or_toxicology_table":("clinical","animal_pharmacology_and_or_toxicology","table"),
    "pharmacogenomics":                  ("clinical","pharmacogenomics","text"),
    "references":                        ("clinical","references","text"),
    "references_table":                  ("clinical","references","table"),
    "information_for_patients":          ("patient_info","information_for_patients","text"),
    "information_for_patients_table":    ("patient_info","information_for_patients","table"),
    "patient_medication_information":    ("patient_info","patient_medication_information","text"),
    "patient_medication_information_table":("patient_info","patient_medication_information","table"),
    "medication_guide":                  ("patient_info","medication_guide","text"),
    "spl_medguide_table":                ("patient_info","medication_guide","table"),
    "instructions_for_use":              ("patient_info","instructions_for_use","text"),
    "instructions_for_use_table":        ("patient_info","instructions_for_use","table"),
    "ask_doctor":                        ("patient_info","ask_doctor","text"),
    "ask_doctor_or_pharmacist":          ("patient_info","ask_doctor_or_pharmacist","text"),
    "spl_patient_package_insert":        ("patient_info","spl_patient_package_insert","text"),
    "spl_patient_package_insert_table":  ("patient_info","spl_patient_package_insert","table"),
    "pregnancy_or_breast_feeding":       ("patient_info","pregnancy_or_breast_feeding","text"),
    "keep_out_of_reach":                 ("patient_info","keep_out_of_reach","text"),
    "keep_out_of_reach_of_children_table":("patient_info","keep_out_of_reach","table"),
    "questions":                         ("patient_info","questions","text"),
    "information_for_owners_or_caregivers":("patient_info","information_for_owners_or_caregivers","text"),
    "geriatric_use":                     ("population_specific","geriatric_use","text"),
    "geriatric_use_table":               ("population_specific","geriatric_use","table"),
    "pediatric_use":                     ("population_specific","pediatric_use","text"),
    "pediatric_use_table":               ("population_specific","pediatric_use","table"),
    "use_in_pregnancy":                  ("population_specific","use_in_pregnancy","text"),
    "nursing_mothers":                   ("population_specific","nursing_mothers","text"),
    "labor_and_delivery":                ("population_specific","labor_and_delivery","text"),
    "use_in_specific_populations":       ("population_specific","use_in_specific_populations","text"),
    "use_in_specific_populations_table": ("population_specific","use_in_specific_populations","table"),
    "carcinogenesis_and_mutagenesis_and_impairment_of_fertility":
                                         ("population_specific","carcinogenesis_and_mutagenesis_and_impairment_of_fertility","text"),
    "carcinogenesis_and_mutagenesis_and_impairment_of_fertility_table":
                                         ("population_specific","carcinogenesis_and_mutagenesis_and_impairment_of_fertility","table"),
    "teratogenic_effects":               ("population_specific","teratogenic_effects","text"),
    "nonteratogenic_effects":            ("population_specific","nonteratogenic_effects","text"),
    "drug_interactions":                 ("drug_interactions","drug_interactions","text"),
    "drug_interactions_table":           ("drug_interactions","drug_interactions","table"),
    "drug_and_or_laboratory_test_interactions":    ("drug_interactions","drug_and_or_laboratory_test_interactions","text"),
    "drug_and_or_laboratory_test_interactions_table":("drug_interactions","drug_and_or_laboratory_test_interactions","table"),
    "laboratory_tests":                  ("drug_interactions","laboratory_tests","text"),
    "how_supplied_storage":              ("supply_storage","how_supplied_storage","text"),
    "how_supplied_table":                ("supply_storage","how_supplied_storage","table"),
    "storage_and_handling":              ("supply_storage","storage_and_handling","text"),
    "storage_and_handling_table":        ("supply_storage","storage_and_handling","table"),
    "package_label":                     ("supply_storage","package_label","text"),
    "package_label_principal_display_panel_table":("supply_storage","package_label","table"),
    "components":                        ("supply_storage","components","text"),
    "abuse":                             ("abuse_dependence","abuse","text"),
    "dependence":                        ("abuse_dependence","dependence","text"),
    "controlled_substance":              ("abuse_dependence","controlled_substance","text"),
    "drug_abuse_and_dependence":         ("abuse_dependence","drug_abuse_and_dependence","text"),
    "drug_abuse_and_dependence_table":   ("abuse_dependence","drug_abuse_and_dependence","table"),
}

OPENFDA_SPECIAL = {
    "drug_info", "openfda_metadata",
    "set_id","document_id","version","effective_date","spl_product_data_elements",
}

DAILYMED_SECTION_TO_CAT = {
    sec: cat
    for cat, secs in {
        "labeling_content":   ["indications_and_usage","dosage_and_administration","unclassified","other_sections"],
        "safety":             ["warnings","precautions","contraindications","boxed_warning","warnings_and_precautions","general_precautions"],
        "adverse_events":     ["adverse_reactions","overdosage"],
        "clinical":           ["clinical_pharmacology","clinical_studies","pharmacokinetics","mechanism_of_action","carcinogenesis","references"],
        "patient_info":       ["information_for_patients"],
        "population_specific":["geriatric_use","pediatric_use","nursing_mothers","labor_and_delivery","teratogenic_effects","nonteratogenic_effects"],
        "drug_interactions":  ["drug_interactions","laboratory_tests"],
        "supply_storage":     ["how_supplied","storage_and_handling","package_label"],
    }.items()
    for sec in secs
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_nested(obj, *keys):
    for k in keys:
        if not isinstance(obj, dict) or k not in obj:
            return "__MISSING__"
        obj = obj[k]
    return obj

def vals_equal(a, b):
    return json.dumps(a, sort_keys=True, default=str) == json.dumps(b, sort_keys=True, default=str)

def short(v, n=60):
    s = json.dumps(v, default=str)
    return s[:n] + "…" if len(s) > n else s

# ── OpenFDA checker ───────────────────────────────────────────────────────────

def check_openfda(cr, sr, row_num, rid):
    issues = []
    ok_count = 0

    for src_k, (cat, tgt_k, sub) in OPENFDA_FLAT_MAP.items():
        if src_k not in cr:
            continue
        src_val = cr[src_k]
        if src_val is None:
            continue

        if sub is None:
            dst_val = get_nested(sr, cat, tgt_k)
        else:
            dst_val = get_nested(sr, cat, tgt_k, sub)

        if dst_val == "__MISSING__":
            issues.append(f"  ✗ MISSING   {src_k} → {cat}.{tgt_k}" +
                          (f".{sub}" if sub else "") +
                          f"  |  src={short(src_val)}")
        elif not vals_equal(src_val, dst_val):
            issues.append(f"  ✗ MISMATCH  {src_k} → {cat}.{tgt_k}" +
                          (f".{sub}" if sub else "") +
                          f"\n      src={short(src_val)}\n      dst={short(dst_val)}")
        else:
            ok_count += 1

    # Special fields
    # identification
    for src_k, tgt_k in [("set_id","set_id"),("document_id","document_id"),
                          ("version","version"),("effective_date","effective_date"),
                          ("spl_product_data_elements","spl_product_data_elements")]:
        if src_k not in cr:
            continue
        dst_val = get_nested(sr, "identification", tgt_k)
        if dst_val == "__MISSING__":
            issues.append(f"  ✗ MISSING   {src_k} → identification.{tgt_k}")
        elif not vals_equal(cr[src_k], dst_val):
            issues.append(f"  ✗ MISMATCH  {src_k} → identification.{tgt_k}")
        else:
            ok_count += 1

    # drug_info and openfda_metadata
    for k in ("drug_info", "openfda_metadata"):
        if k not in cr:
            continue
        dst_val = get_nested(sr, k)
        if dst_val == "__MISSING__":
            issues.append(f"  ✗ MISSING   {k} → {k}")
        elif not vals_equal(cr[k], dst_val):
            issues.append(f"  ✗ MISMATCH  {k} → {k}\n      src={short(cr[k])}\n      dst={short(dst_val)}")
        else:
            ok_count += 1

    return ok_count, issues

# ── DailyMed checker ──────────────────────────────────────────────────────────

def check_dailymed(cr, sr, row_num, rid):
    issues = []
    ok_count = 0

    # products → drug_info.products
    if "products" in cr:
        dst = get_nested(sr, "drug_info", "products")
        if dst == "__MISSING__":
            issues.append("  ✗ MISSING   products → drug_info.products")
        elif not vals_equal(cr["products"], dst):
            issues.append(f"  ✗ MISMATCH  products → drug_info.products\n      src={short(cr['products'])}\n      dst={short(dst)}")
        else:
            ok_count += 1

    # manufacturer → drug_info.manufacturer
    if "manufacturer" in cr:
        dst = get_nested(sr, "drug_info", "manufacturer")
        if dst == "__MISSING__":
            issues.append("  ✗ MISSING   manufacturer → drug_info.manufacturer")
        elif not vals_equal(cr["manufacturer"], dst):
            issues.append(f"  ✗ MISMATCH  manufacturer → drug_info.manufacturer")
        else:
            ok_count += 1

    # drug_label → identification.drug_label
    if "drug_label" in cr:
        dst = get_nested(sr, "identification", "drug_label")
        if dst == "__MISSING__":
            issues.append("  ✗ MISSING   drug_label → identification.drug_label")
        elif not vals_equal(cr["drug_label"], dst):
            issues.append(f"  ✗ MISMATCH  drug_label → identification.drug_label")
        else:
            ok_count += 1

    # label_sections.*
    for sec_k, sec_v in cr.get("label_sections", {}).items():
        cat = DAILYMED_SECTION_TO_CAT.get(sec_k)
        if cat:
            dst = get_nested(sr, cat, sec_k)
            if dst == "__MISSING__":
                issues.append(f"  ✗ MISSING   label_sections.{sec_k} → {cat}.{sec_k}")
            elif not vals_equal(sec_v, dst):
                issues.append(f"  ✗ MISMATCH  label_sections.{sec_k} → {cat}.{sec_k}")
            else:
                ok_count += 1
        else:
            # Should be in unmapped_fields
            dst = get_nested(sr, "unmapped_fields", f"label_sections.{sec_k}")
            if dst == "__MISSING__":
                issues.append(f"  ✗ UNMAPPED  label_sections.{sec_k} (not in category map, not in unmapped_fields)")
            else:
                ok_count += 1

    return ok_count, issues

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    conn = psycopg2.connect(**DB)
    cur  = conn.cursor()

    report_lines = []
    total_ok = total_issues = 0
    all_results = {}

    for source in ["openfda", "dailymed"]:
        cur.execute("""
            SELECT id, clean_record, standardized_records
            FROM "DrugSourceMaster"
            WHERE source = %s
              AND clean_record IS NOT NULL
              AND standardized_records IS NOT NULL
            ORDER BY RANDOM() LIMIT 10
        """, (source,))
        rows = cur.fetchall()

        src_ok = src_issues = 0
        src_results = []
        checker = check_openfda if source == "openfda" else check_dailymed

        hdr = f"\n{'='*70}\n{source.upper()}  —  10 random rows\n{'='*70}"
        print(hdr)
        report_lines.append(hdr)

        for i, (rid, cr, sr) in enumerate(rows):
            ok, issues = checker(cr, sr, i+1, rid)
            src_ok     += ok
            src_issues += len(issues)
            total_ok     += ok
            total_issues += len(issues)

            status = "✓ ALL MATCH" if not issues else f"✗ {len(issues)} ISSUE(S)"
            n_src_fields = sum(
                1 for k in (cr.keys() if source == "openfda"
                            else list(cr.keys()) + list(cr.get("label_sections", {}).keys()))
                if k != "label_sections"
            )
            line = (f"\n  Row {i+1:>2}  id={str(rid)[:8]}…  "
                    f"src_fields={ok + len(issues)}  checked={ok + len(issues)}  "
                    f"status={status}")
            print(line)
            report_lines.append(line)

            for iss in issues:
                print(iss)
                report_lines.append(iss)

            src_results.append({
                "id": str(rid),
                "source": source,
                "fields_ok": ok,
                "issues": issues,
            })

        summary = (f"\n  Source summary: {src_ok} fields matched, "
                   f"{src_issues} issues across 10 rows")
        print(summary)
        report_lines.append(summary)
        all_results[source] = src_results

    # Grand summary
    grand = (f"\n{'='*70}\nGRAND SUMMARY\n{'='*70}\n"
             f"  Total fields verified : {total_ok + total_issues}\n"
             f"  Matched correctly     : {total_ok}\n"
             f"  Issues found          : {total_issues}\n"
             f"  Result: {'✓ ZERO DATA LOSS CONFIRMED' if total_issues == 0 else '✗ DATA ISSUES FOUND — SEE ABOVE'}\n"
             f"{'='*70}")
    print(grand)
    report_lines.append(grand)

    conn.close()

    with open("comparison_report.txt", "w") as f:
        f.write("\n".join(report_lines))
    with open("comparison_detail.json", "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print("\n✓ Saved: comparison_report.txt")
    print("✓ Saved: comparison_detail.json")

if __name__ == "__main__":
    main()
