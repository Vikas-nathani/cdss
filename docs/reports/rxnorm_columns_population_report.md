# Drug Table RxNorm Columns Population Report

## Execution Summary

**Date:** 2026-05-02  
**Scripts run:**
1. `update_drug_rxnorm_columns.py` — main population (~4 min)
2. `fix_rxnorm_uncleaned_rows.py` — patch for UUID-mismatch rows (~5 min)

**Total Time:** ~9 minutes  
**Status:** ✅ Success — 100.00% coverage

---

## Statistics

### Source Data
| Metric | Value |
|---|---|
| DrugMasterLinkage records processed | 50,111 |
| Records skipped (no rxnorm / null jsonb) | 2,492 |
| RxNorm entries found | 98,660 |
| RxNorm entries with rxcui | 88,983 |
| RxNorm entries without rxcui (no formulation string) | 9,677 |

### Cleaning Method Breakdown (main pass)
| Stage | Count | % |
|---|---|---|
| Stage 1 — Direct suffix match (e.g. "Oral Tablet") | 72,853 | 81.9% |
| Stage 2 — Regex mapped suffix (e.g. "TABLET, EXTENDED RELEASE") | 16,130 | 18.1% |
| No change (no suffix to strip) | 0 | 0% |

### Matching Results
| Pass | Rows Targeted | Matched | Failed |
|---|---|---|---|
| Main pass (`update_drug_rxnorm_columns.py`) | 88,983 | 71,855 | 17,128 |
| Fix pass (`fix_rxnorm_uncleaned_rows.py`) | 17,128 | 17,128 | 0 |
| **Combined total** | **88,983** | **88,983** | **0** |

### Database Updates
| Metric | Value |
|---|---|
| Total rows in `drugdb.drug` | 88,983 |
| Rows with `rxcui` populated | **88,983** |
| Rows with `rxnorm_generic_formulation` populated | **88,983** |
| Rows still NULL | **0** |
| Coverage | **100.00%** |

---

## Root Cause: The 17,128 UUID-Mismatch Rows

**Why they were missed by the main pass:**

The `formulation_id` UUID is a deterministic `uuid5` seeded from
`"{master_linkage_id}|{cleaned_generic_formulation}|{dosage_form}"`.

The original `populate_drug_table.py` had a bug: `strip_dosage_form_suffix()`
used exact string matching only, which failed for EU/uppercase dosage form codes
like `"TABLET, EXTENDED RELEASE"`. Those 17,128 rows were inserted with the
**uncleaned** formulation in the seed, so:

- **Stored** `formulation_id` = `uuid5("…|24 HR metformin 1000 MG Extended Release Oral Tablet|TABLET, EXTENDED RELEASE")`
- **Main pass** computed = `uuid5("…|24 HR metformin 1000 MG|TABLET, EXTENDED RELEASE")` ← different UUID
- Result: `WHERE formulation_id = <computed>` matched 0 rows

**Fix:** `fix_rxnorm_uncleaned_rows.py` uses `generic_formulation_original`
(the uncleaned value stored during the 2026-05-02 cleanup migration) as the
lookup key, matching `raw_formulation` directly from DrugMasterLinkage. 17,128/17,128
matched on the first pass.

---

## Data Quality

### Coverage Analysis
| Column | Populated | NULL | Coverage |
|---|---|---|---|
| `rxcui` | 88,983 | 0 | **100.00%** |
| `rxnorm_generic_formulation` | 88,983 | 0 | **100.00%** |

### Sample Updated Rows (10 rows)
| generic_formulation (cleaned) | rxnorm_generic_formulation (original) | rxcui |
|---|---|---|
| `alprazolam 0.25 MG` | `alprazolam 0.25 MG Oral Tablet` | 308047 |
| `warfarin sodium 4 MG` | `warfarin sodium 4 MG Oral Tablet` | 855324 |
| `24 HR metformin hydrochloride 750 MG` | `24 HR metformin hydrochloride 750 MG Extended Release Oral Tablet` | 860981 |
| `24 HR bupropion hydrochloride 150 MG` | `24 HR bupropion hydrochloride 150 MG Extended Release Oral Tablet` | 993541 |
| `octreotide 1 MG/ML` | `octreotide 1 MG/ML Injectable Solution` | 312071 |
| `insulin aspart, human 100 UNT/ML` | `insulin aspart, human 100 UNT/ML Injectable Solution` | 311040 |
| `oxygen 99.2 %` | `oxygen 99.2 % Gas for Inhalation` | 348831 |
| `carboplatin 10 MG/ML` | `carboplatin 10 MG/ML Injectable Solution` | 597195 |
| `paclitaxel 100 MG` | `paclitaxel 100 MG Injection` | 583214 |
| `72 HR scopolamine 0.0139 MG/HR` | `72 HR scopolamine 0.0139 MG/HR Transdermal System` | 226552 |

---

## Indexes Created
| Index | Status |
|---|---|
| `idx_drug_rxcui` on `drugdb.drug(rxcui)` | ✅ Created |
| `idx_drug_rxnorm_formulation` on `drugdb.drug(rxnorm_generic_formulation)` | ✅ Created |

---

## Scripts Used
| Script | Role | Log |
|---|---|---|
| `scripts/add_rxnorm_columns.sql` | DDL: ALTER TABLE + CREATE INDEX | — |
| `scripts/update_drug_rxnorm_columns.py` | Main population (88,983 UPDATEs) | `logs/rxnorm_update.log` |
| `scripts/fix_rxnorm_uncleaned_rows.py` | Patch for 17,128 UUID-mismatch rows | `logs/rxnorm_fix.log` |

---

## Next Steps
1. ✅ `drugdb.drug` now has `rxcui` and `rxnorm_generic_formulation` on all 88,983 rows
2. ⏭️ Ready to run `populate_drug_synonym_formulation.py` (Step 2) — can now use direct rxcui lookup
3. ⏭️ Ready to run `populate_drug_ingredient_mapping.py` (Step 3) — same benefit

---

## Verification Status
- [✅] Query 1 — 100.00% population coverage (88,983 / 88,983)
- [✅] Query 2 — Sample rows show correct cleaned vs original formulation pairs
- [✅] Query 3 — Zero NULL rows remaining
- [✅] Query 4 — Both indexes exist (`idx_drug_rxcui`, `idx_drug_rxnorm_formulation`)
- [✅] Documentation updated (`DRUG_DATABASE_SCHEMA_DOCUMENTATION.md`)
- [✅] 100% coverage achieved
