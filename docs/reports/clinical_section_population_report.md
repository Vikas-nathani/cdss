# drugdb.clinical_section Population Report

## Execution Summary

**Date:** 2026-05-06
**Script:** `populate_clinical_section.py`
**Total Time:** 56.8 min (3,407.8s)
**Status:** ✅ Success — 0 errors

---

## Statistics

### Source Data

| Metric | Value |
|---|---|
| DrugMasterLinkage records processed | 50,111 |
| Formulation IDs matched | 88,983 |
| Records skipped (no formulation match — data gap) | 2,492 |

### Output

| Metric | Value |
|---|---|
| Total rows inserted into `drugdb.clinical_section` | **2,887,910** |
| Rows skipped (ON CONFLICT) | 0 |
| Sections skipped (empty text + subsections) | 0 |
| Batches processed (5,000 records each) | 11 |

---

## Breakdown by Source

| Source | Rows Inserted | % |
|---|---|---|
| `openfda` | 1,840,508 | 63.7% |
| `dailymed` | 1,047,402 | 36.3% |
| **Total** | **2,887,910** | **100%** |

---

## Breakdown by Section (drugs with section, descending)

| Section | Drugs |
|---|---|
| `package_label` | 47,602 |
| `indications_and_usage` | 45,647 |
| `dosage_and_administration` | 45,600 |
| `adverse_reactions` | 45,376 |
| `contraindications` | 45,330 |
| `how_supplied_storage` | 45,267 |
| `how_supplied` | 45,083 |
| `clinical_pharmacology` | 45,004 |
| `other_sections` | 44,416 |
| `overdosage` | 41,815 |
| `drug_interactions` | 35,338 |
| `use_in_pregnancy` | 35,315 |
| `pediatric_use` | 35,305 |
| `information_for_patients` | 34,138 |
| `carcinogenesis_and_mutagenesis_and_impairment_of_fertility` | 32,105 |
| `geriatric_use` | 30,420 |
| `pharmacokinetics` | 30,095 |
| `clinical_studies` | 25,058 |
| `use_in_specific_populations` | 24,379 |
| `warnings_and_cautions` | 24,327 |
| `warnings_and_precautions` | 24,295 |
| `dosage_forms_and_strengths` | 24,051 |
| `mechanism_of_action` | 23,966 |
| `nonclinical_toxicology` | 23,718 |
| `nursing_mothers` | 21,566 |
| `precautions` | 21,179 |
| `unclassified` | 19,848 |
| `warnings` | 19,825 |
| `pharmacodynamics` | 17,897 |
| `boxed_warning` | 16,553 |
| `medication_guide` | 12,048 |
| `general_precautions` | 11,109 |
| `recent_major_changes` | 10,065 |
| `drug_abuse_and_dependence` | 9,659 |
| `labor_and_delivery` | 9,282 |
| `storage_and_handling` | 8,729 |
| `laboratory_tests` | 6,977 |
| `spl_patient_package_insert` | 6,726 |
| `references` | 6,460 |
| `animal_pharmacology_and_or_toxicology` | 5,679 |
| `teratogenic_effects` | 5,315 |
| `controlled_substance` | 4,306 |
| `abuse` | 4,161 |
| `dependence` | 3,952 |
| `drug_and_or_laboratory_test_interactions` | 3,615 |
| `microbiology` | 2,467 |

**Total unique sections discovered:** 45

---

## Per-Batch Performance

| Batch | Records | Rows Inserted | Time |
|---|---|---|---|
| 1 | 5,000 | 314,463 | ~38 min (incl. startup) |
| 2 | 5,000 | 278,420 | 310.3s |
| 3 | 5,000 | 281,366 | 283.7s |
| 4 | 5,000 | 287,008 | 261.8s |
| 5 | 5,000 | 281,100 | 265.9s |
| 6 | 5,000 | 275,168 | 266.2s |
| 7 | 5,000 | 273,940 | 260.6s |
| 8 | 5,000 | 273,153 | 335.0s |
| 9 | 5,000 | 298,231 | 342.2s |
| 10 | 5,000 | 318,663 | 322.1s |
| 11 | 111 | 6,398 | 6.4s |

---

## Schema

```sql
CREATE TABLE IF NOT EXISTS drugdb.clinical_section (
    id                 SERIAL PRIMARY KEY,
    formulation_id     UUID NOT NULL
                       REFERENCES drugdb.drug(formulation_id)
                       ON DELETE CASCADE,
    section            TEXT NOT NULL,
    text               TEXT,
    subsections        JSONB DEFAULT '[]',
    source             TEXT,
    source_document_id TEXT,
    UNIQUE(formulation_id, section, source)
);

CREATE INDEX IF NOT EXISTS idx_cs_formulation ON drugdb.clinical_section(formulation_id);
CREATE INDEX IF NOT EXISTS idx_cs_section     ON drugdb.clinical_section(section);
```

---

## Extraction Logic

### OpenFDA
- Parent keys traversed: `safety`, `labeling_content`, `clinical`, `adverse_events`, `drug_interactions`, `patient_info`, `supply_storage`, `abuse_dependence`, `population_specific`
- Text field: `child["text"]` → stored as NULL when empty
- Subsections: always `[]` (OpenFDA has no subsection structure)
- Source document ID: `openfda.identification.set_id`

### DailyMed
- Same parent keys as OpenFDA
- Text field: `child["content"]` → stored as NULL when empty
- Subsections: `child["subsections"]` → transformed to `{subsection_id, title, text}` objects
- Source document ID: `dailymed.identification.drug_label.document_id`

### Skip logic
A section is skipped only when **both** text is empty/None **and** subsections array is empty.

---

## Notes

- Sections are discovered **dynamically** by traversing parent keys — no hardcoded section names
- `formulation_id` is UUID type with FK to `drugdb.drug(formulation_id) ON DELETE CASCADE`
- Both OpenFDA and DailyMed rows stored as **separate rows** — no merging
- Script is idempotent: re-runs safely via `ON CONFLICT (formulation_id, section, source) DO NOTHING`
- 2,492 skipped records are a known data gap: `DrugMasterLinkage` entries with no matching row in `drugdb.drug`
- Two-connection architecture: read connection streams with server-side cursor; write connection handles all inserts and commits independently
- Full run log: `clinical_section_population.log`
