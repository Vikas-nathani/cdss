# drugdb.label_table Population Report

## Execution Summary

**Date:** 2026-05-06
**Script:** `populate_label_table.py`
**Total Time:** 14.6 min (873.7s)
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
| Total rows inserted into `drugdb.label_table` | **510,527** |
| Batches flushed (5,000 rows each) | 101 + 1 final |
| Avg insert time per batch | ~1.5s |

---

## Breakdown by Semantic Type

| semantic_type | Rows | % |
|---|---|---|
| `adverse_event` | 159,368 | 31.2% |
| `clinical_study` | 118,377 | 23.2% |
| `pharmacokinetics` | 91,968 | 18.0% |
| `dosing` | 77,520 | 15.2% |
| NULL (unmapped section) | 40,648 | 8.0% |
| `interaction` | 22,506 | 4.4% |
| `contraindication` | 140 | 0.0% |
| **Total** | **510,527** | **100%** |

---

## Breakdown by Section

| section | Rows |
|---|---|
| `adverse_reactions` | 132,820 |
| `clinical_studies` | 118,377 |
| `dosage_and_administration` | 77,520 |
| `clinical_pharmacology` | 58,326 |
| `pharmacokinetics` | 33,642 |
| `warnings_and_cautions` | 26,548 |
| `drug_interactions` | 22,506 |
| `recent_major_changes` | 6,303 |
| `unclassified` | 6,956 |
| `pharmacodynamics` | 5,878 |
| `warnings` | 4,211 |
| `precautions` | 3,338 |
| `use_in_specific_populations` | 2,964 |
| `dosage_forms_and_strengths` | 2,808 |
| `microbiology` | 2,214 |
| `pediatric_use` | 1,806 |
| `indications_and_usage` | 1,709 |
| `geriatric_use` | 543 |
| `nonclinical_toxicology` | 346 |
| `general_precautions` | 390 |
| `drug_and_or_laboratory_test_interactions` | 269 |
| `boxed_warning` | 258 |
| `references` | 207 |
| `carcinogenesis_and_mutagenesis_and_impairment_of_fertility` | 288 |
| `animal_pharmacology_and_or_toxicology` | 45 |
| `overdosage` | 75 |
| `active_ingredient` | 21 |
| `purpose` | 19 |

---

## Schema

```sql
CREATE TABLE IF NOT EXISTS drugdb.label_table (
    id             SERIAL PRIMARY KEY,
    formulation_id UUID NOT NULL,
    table_id       TEXT NOT NULL,
    caption        TEXT,
    semantic_type  TEXT,
    section        TEXT,
    headers        TEXT[],
    rows_data      JSONB DEFAULT '[]'
);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_label_table_fid_tid
    ON drugdb.label_table (formulation_id, table_id);
```

---

## Notes

- `formulation_id` is UUID with no FK constraint at DB level; referential integrity enforced by the script
- `table_id` format: `{formulation_id}_{section_key}_table_{N}` where N is parsed from the caption ("Table N") or the loop index
- `rows_data` stores rows as-is — supports both array-of-arrays and array-of-objects formats
- Headers promoted from first row when all values match `n/%` pattern
- Script is idempotent: re-runs safely via `ON CONFLICT (formulation_id, table_id) DO NOTHING`
- 2,492 skipped records are a known data gap: `DrugMasterLinkage` entries with no matching row in `drugdb.drug`
- Full run log: `label_table_population.log`
