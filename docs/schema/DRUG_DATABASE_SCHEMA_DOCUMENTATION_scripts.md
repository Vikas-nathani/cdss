# Drug Database Schema Documentation

**Project:** CDSS (Clinical Decision Support System)
**Database Host:** `178.236.185.230` (PostgreSQL)
**Credentials:** _Sensitive — redacted. See secure credentials store._
**Last Updated:** 2026-05-02 (RxNorm original data columns added: `rxnorm_generic_formulation`, `rxcui` — enables direct-lookup matching in downstream scripts)

---

## Pipeline Status Dashboard

| Step | Task | Status | Rows / Coverage | Last Run |
|---|---|---|---|---|
| 0 | `drugdb.ingredients` — RxCUI fill | ✅ Done | 2,134 / 20,034 (10.7%) | 2026-05-02 |
| — | `drugdb.indian_brand_ingredient` — DrugBank ID mapping | ✅ Done | 575,839 / 580,669 (99.17%) | 2026-05-02 |
| 1 | `drugdb.drug` — formulation table | ✅ Done | 88,983 rows, 0 errors | 2026-05-02 |
| 1a | `drugdb.drug` — dosage form suffix cleanup | ✅ Done | 17,128 / 88,983 rows corrected | 2026-05-02 |
| 1b | `drugdb.drug` — rxnorm original data columns | ✅ Done | 88,983 / 88,983 (100%) | 2026-05-02 |
| 2 | `drugdb.drug_synonym_formulation` — synonyms | ✅ Done | 66,154 / 88,983 (74.34%) | 2026-05-02 |
| 3 | `drugdb.drug_ingredient_mapping` — ingredient links | ✅ Done | 92,570 rows, 84,316 / 88,983 formulations (94.76%) | 2026-05-02 |
| 1c | `drugdb.drug` — enrichment columns (9 new columns) | ✅ Done | 88,983 / 88,983 (100%) | 2026-05-05 |

> **Next step:** Run `populate_drug_synonym_formulation.py` (Step 2). `drugdb.drug` is fully populated including `rxcui` and `rxnorm_generic_formulation` on all 88,983 rows.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Table Population Sequence (CRITICAL)](#2-table-population-sequence-critical)
3. [Detailed Table Documentation](#3-detailed-table-documentation)
   - [drugdb.drug](#table-publicdrug)
   - [drugdb.drug_synonym_formulation](#table-publicdrug_synonym_formulation)
   - [drugdb.drug_ingredient_mapping](#table-publicdrug_ingredient_mapping)
   - [drugdb.indian_brand_ingredient (DrugBank ID mapping)](#table-drugdbindian_brand_ingredient)
4. [Database Diagram](#4-database-diagram)
5. [Population Scripts Reference](#5-population-scripts-reference)
6. [Running the Scripts](#6-running-the-scripts)
7. [Notes and Considerations](#7-notes-and-considerations)
8. [Future Tables](#8-future-tables)
9. [Changelog](#9-changelog)

---

## 1. Overview

This database stores normalized pharmaceutical data extracted from `DrugMasterLinkage.combined_clean_jsonb`, which aggregates data from the following upstream sources:

- **OpenFDA**
- **RxNorm**
- **DrugBank**
- **DailyMed**

The tables documented here store the **processed, queryable form** of that data — distinct from the raw JSONB source. Each table is populated by a dedicated Python script that reads from `DrugMasterLinkage`, applies transformations, and writes structured rows into the target tables.

---

## 2. Table Population Sequence (CRITICAL)

Tables must be populated in the following order due to foreign key dependencies. Running scripts out of order will result in FK constraint violations.

| Order | Table | Dependency | Script | Status |
|---|---|---|---|---|
| 0 | `drugdb.ingredients` (rxcui UPDATE) | None | `update_ingredient_rxcui.py` | ✅ Completed 2026-05-02 |
| 1 | `drugdb.drug` | None (base table) | `populate_drug_table.py` | ✅ Completed 2026-05-02 |
| 2 | `drugdb.drug_synonym_formulation` | `drug.formulation_id` | `populate_drug_synonym_formulation.py` | ✅ Completed 2026-05-02 |
| 3 | `drugdb.drug_ingredient_mapping` | `drug.formulation_id` + `drugdb.ingredients.id` | `populate_drug_ingredient_mapping.py` | ✅ Completed 2026-05-02 |
| 4+ | _(Future tables TBD)_ | TBD | TBD | — |
| — | `drugdb.indian_brand_ingredient` | `drugdb.ingredients` + `drugdb.ingredient_synonyms` | `update_indian_brand_drugbank_id.py` (UPDATE only) | ✅ Completed 2026-05-02 |

> **Warning:** Always populate `drug` before any table that references it. Both `drug_synonym_formulation` and `drug_ingredient_mapping` will fail to insert if the corresponding `formulation_id` does not yet exist in `drug`. `drug_ingredient_mapping` additionally requires the `public.ingredients` table to be populated before it is run. `update_indian_brand_drugbank_id.py` requires both `drugdb.ingredients` and `drugdb.ingredient_synonyms` to be populated first before it can resolve DrugBank IDs.

---

## 3. Detailed Table Documentation

### Table: `drugdb.ingredients` (RxCUI Update) ✅ Completed 2026-05-02

**Purpose:** Enrich the pre-existing DrugBank ingredients table with RxNorm RxCUI codes extracted from `DrugMasterLinkage.combined_clean_jsonb`, enabling cross-reference between DrugBank and RxNorm data.

#### Background

- Table pre-existed with **20,028** DrugBank ingredient rows; **18,125** had `rxcui = NULL`
- Source: 2,137 unique `(ingredient_name, ing_rxcui)` pairs extracted from `DrugMasterLinkage`
- After run: **2,134** rows have rxcui; **17,900** remain NULL (DrugBank-only drugs with no RxNorm equivalent)
- Total rows after run: **20,034** (6 new RxNorm-only ingredients inserted)

#### Schema (relevant columns)

```sql
-- Pre-existing table — only rxcui is updated/inserted by our script
CREATE TABLE drugdb.ingredients (
    id          UUID PRIMARY KEY,
    drugbank_id VARCHAR,
    unii        VARCHAR,
    name        VARCHAR,
    rxcui       VARCHAR,   -- ← SET by update_ingredient_rxcui.py
    indications TEXT,
    general_function TEXT,
    pharmacodynamics TEXT,
    classification_description TEXT,
    food_interactions JSONB,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    created_by  VARCHAR
);
```

#### Data Source

| Field | Path in JSONB |
|---|---|
| Ingredient name | `DrugMasterLinkage.combined_clean_jsonb → rxnorm[] → ingredients[] → name` |
| RxCUI | `DrugMasterLinkage.combined_clean_jsonb → rxnorm[] → ingredients[] → ing_rxcui` |

#### 4-Method Matching Strategy

| Method | Description | Match type | Count (2026-05-02 run) |
|---|---|---|---|
| 1 | `ingredients.name` = source name (case-insensitive) | Exact name | 206 matched |
| 2 | `ingredients.name` starts with source name | Prefix name | 154 matched |
| 3 | `ingredient_synonyms.synonym` = source name | Exact synonym | 17 matched |
| 4 | `ingredient_synonyms.synonym` starts with source name | Prefix synonym | 93 matched |
| — | No match → INSERT new row | New insert | 6 inserted |

#### Action Classification

Each matched ingredient falls into one of four actions:

| Action | Condition | Count |
|---|---|---|
| `Updated (NULL → rxcui)` | Ingredient found, rxcui was NULL | **226** |
| `Updated (rxcui changed)` | Ingredient found, rxcui value changed in source | **4** |
| `Skipped (already correct)` | Ingredient found, rxcui already matches source | **1,901** |
| `Inserted (new)` | No match found → new row inserted | **6** |

> The script loads **all** ingredients (not just NULL-rxcui) so re-runs are idempotent — previously processed rows are detected and skipped rather than double-counted.

#### Update Statistics (actual run — 2026-05-02)

| Metric | Value |
|---|---|
| Source ingredients extracted | 2,137 unique (name, rxcui) pairs |
| Updated NULL → rxcui | 226 |
| Updated changed rxcui | 4 (`cupric oxide`, `glutamate`, `glutamine`, `sodium phosphate, dibasic`) |
| Skipped already correct | 1,901 |
| Inserted new rows | 6 (`factor VIIa`, `goat milk allergenic extract`, `rabbit allergenic extract`, `sennosides USP`, `sodium nitrite`, `zinc-DTPA`) |
| Errors | 0 |
| **Total rows with rxcui after run** | **2,134 / 20,034 (10.7%)** |
| Still NULL (DrugBank-only) | 17,900 (89.3%) — no RxNorm equivalent |

> The 17,900 NULL rows are expected: they are DrugBank-specific entries (enzymes, targets, transporters, experimental drugs) that have no RxNorm code.

#### Script Fix Note (2026-05-02)

The original script loaded only `WHERE rxcui IS NULL` — causing re-runs to re-attempt 1,667 previously-inserted rows as new inserts (blocked by `ON CONFLICT`, but stats were wrong). Fixed to load **all** ingredients and classify each action as NULL→value / changed / already-correct / insert.

#### Population Script

`scripts/update_ingredient_rxcui.py`

```bash
# Dry run (always run first)
python3 scripts/update_ingredient_rxcui.py --password <pwd> --dry-run --verbose

# Actual run
python3 scripts/update_ingredient_rxcui.py --password <pwd> --verbose --log-file logs/update_ingredient_rxcui.log
```

---

### Table: `drugdb.drug` ✅ Completed 2026-05-02

**Status:** **88,983 rows inserted, 0 errors.** Table is fully populated and FK-verified against `DrugMasterLinkage`. Safe to re-run — deterministic UUIDs ensure idempotency.

**Post-population cleanup (2026-05-02):** 17,128 rows had dosage form suffixes incorrectly left in `generic_formulation`. These were cleaned via a one-time migration. The root cause (`strip_dosage_form_suffix()` using only exact matching) was also fixed in `populate_drug_table.py` — future runs will produce clean data.

**Purpose:** Stores one row per unique drug formulation. Each RxNorm entry in the source data corresponds to exactly one row in this table.

#### Schema

```sql
CREATE TABLE drugdb.drug (
    formulation_id               UUID  PRIMARY KEY,
    master_linkage_id            UUID,
    generic_name                 TEXT,
    generic_formulation          TEXT,
    dosage_forms                 TEXT,
    generic_formulation_original TEXT,    -- backup column added 2026-05-02 for cleanup audit
    rxnorm_generic_formulation   TEXT,    -- original uncleaned formulation from RxNorm (added 2026-05-02)
    rxcui                        VARCHAR(50)  -- RxNorm Concept Unique Identifier (added 2026-05-02)
);
CREATE INDEX IF NOT EXISTS idx_drug_master_linkage_id    ON drugdb.drug (master_linkage_id);
CREATE INDEX IF NOT EXISTS idx_drug_generic_name         ON drugdb.drug (generic_name);
CREATE INDEX IF NOT EXISTS idx_drug_formulation_dosage   ON drugdb.drug (generic_formulation, dosage_forms);
CREATE INDEX IF NOT EXISTS idx_drug_rxnorm_formulation   ON drugdb.drug (rxnorm_generic_formulation);
CREATE INDEX IF NOT EXISTS idx_drug_rxcui                ON drugdb.drug (rxcui);
ALTER TABLE drugdb.drug ADD CONSTRAINT drug_master_linkage_fkey
    FOREIGN KEY (master_linkage_id)
    REFERENCES public."DrugMasterLinkage" (master_linkage_id) ON DELETE CASCADE;
```

> **Note:** All text columns are `TEXT` (not `VARCHAR`). Multi-ingredient parenteral nutrition formulations can exceed 500 characters (max observed: 544 chars). Using `VARCHAR(500)` caused batch failures — fixed to `TEXT`.

> **Backup column:** `generic_formulation_original` stores the pre-cleanup value for every row. Retained for audit. To roll back the cleanup, run `UPDATE drugdb.drug SET generic_formulation = generic_formulation_original WHERE generic_formulation != generic_formulation_original`. To remove the column once satisfied: `ALTER TABLE drugdb.drug DROP COLUMN generic_formulation_original`.

#### Columns

| Column | Type | Nullable | Description | Source |
|---|---|---|---|---|
| `formulation_id` | `UUID` | NOT NULL | Primary key; deterministic UUID from `(master_linkage_id, generic_formulation, dosage_form)` — ensures idempotent re-runs | `uuid.uuid5(NAMESPACE_OID, seed)` |
| `master_linkage_id` | `UUID` | YES | Links this row back to the originating source record | `DrugMasterLinkage.master_linkage_id` |
| `generic_name` | `TEXT` | YES | Drug generic name | `combined_clean_jsonb → openfda → drug_info → generic_name` |
| `generic_formulation` | `TEXT` | YES | Formulation string with the dosage form suffix stripped | `rxnorm[].generic_formulation` (processed — see transformation below) |
| `dosage_forms` | `TEXT` | YES | Dosage form type (e.g. "Oral Tablet") | `rxnorm[].specific_dosage_form` |
| `generic_formulation_original` | `TEXT` | YES | Pre-cleanup backup of `generic_formulation`; added 2026-05-02 | Set by one-time cleanup migration |
| `rxnorm_generic_formulation` | `TEXT` | YES | **Original, uncleaned** formulation string from RxNorm (e.g. `"aspirin 81 MG Delayed Release Oral Tablet"`); added 2026-05-02 | `rxnorm[].generic_formulation` (raw, no suffix stripping) |
| `rxcui` | `VARCHAR(50)` | YES | RxNorm Concept Unique Identifier (e.g. `"1191"`); added 2026-05-02 | `rxnorm[].rxcui` |

#### Population Statistics (actual run — 2026-05-02)

| Metric | Value |
|---|---|
| DrugMasterLinkage records processed | 50,111 |
| Records skipped (no rxnorm / null jsonb) | 2,492 |
| RxNorm entries found | 88,983 |
| **Rows inserted into `drugdb.drug`** | **88,983** |
| Errors | 0 |
| Rows with empty `generic_name` | 5,696 (expected — some FDA entries have no generic name) |

#### Key Bug Fixes Applied During Implementation

| Bug | Symptom | Fix |
|---|---|---|
| Named cursor killed by commit | `psycopg2.ProgrammingError: named cursor isn't valid anymore` | Two-connection pattern: `read_conn` never commits; all commits on `write_conn` only |
| Duplicate rows on re-run | 100,988 rows instead of 88,983 on second run | Changed `uuid.uuid4()` → `uuid.uuid5(NAMESPACE_OID, seed)` — same content always produces same UUID |
| `VARCHAR(500)` overflow | 2 batch failures, 2,002 rows lost | Changed all text columns from `VARCHAR(500)` → `TEXT` |
| `strip_dosage_form_suffix()` exact-match-only | 17,128 rows left with RxNorm suffix in `generic_formulation` when `dosage_forms` is a EU/uppercase code | Updated function with 50-entry `_DOSAGE_FORM_SUFFIX_MAP` + pre-compiled regex patterns; one-time cleanup migration run 2026-05-02 |

#### Relationships

- **One-to-Many** → `drug_synonym_formulation`: one formulation has at most one synonym row (enforced via `UNIQUE` on `formulation_id` in child table)
- **Many-to-One** → `DrugMasterLinkage`: many formulations may originate from a single linkage record

#### Transformation Example

The `generic_formulation` value in the source data includes the dosage form as a suffix. The population script strips this suffix before inserting.

**Case 1 — Human-readable form names (exact suffix match):**

```
Input:  generic_formulation  = "quetiapine 100 MG Oral Tablet"
        specific_dosage_form = "Oral Tablet"
Output: generic_formulation  = "quetiapine 100 MG"
        dosage_forms         = "Oral Tablet"
```

**Case 2 — EU/uppercase code with embedded RxNorm suffix (mapped match):**

```
Input:  generic_formulation  = "24 HR metformin hydrochloride 1000 MG Extended Release Oral Tablet"
        specific_dosage_form = "TABLET, EXTENDED RELEASE"
Output: generic_formulation  = "24 HR metformin hydrochloride 1000 MG"
        dosage_forms         = "TABLET, EXTENDED RELEASE"
```

**Updated `strip_dosage_form_suffix()` logic (as of 2026-05-02):**

```python
# Case 1: exact (case-insensitive) suffix match — covers human-readable form names
if df and gf.lower().endswith(df.lower()):
    return gf[:-len(df)].strip()

# Case 2: mapped suffix via pre-compiled regex — covers 50 EU/uppercase codes
# e.g. "TABLET, EXTENDED RELEASE" → strip r'\s+Extended\s+Release\s+Oral\s+Tablet\s*$'
for df_key, patterns in _COMPILED_PATTERNS:
    if df.upper() == df_key.upper():
        for pat in patterns:
            cleaned = pat.sub('', gf).strip()
            if cleaned != gf:
                return cleaned
        break
```

The 50-entry `_DOSAGE_FORM_SUFFIX_MAP` dict and pre-compiled `_COMPILED_PATTERNS` list are defined at module level in `populate_drug_table.py`.

**Full EU code → RxNorm suffix mapping (50 entries):**

| EU code (`dosage_forms`) | RxNorm suffix stripped |
|---|---|
| TABLET, EXTENDED RELEASE | Extended Release Oral Tablet |
| TABLET, DELAYED RELEASE (OBS 06-25-01) | Delayed Release Oral Tablet |
| GASTRO-RESISTANT TABLET | Delayed Release Oral Tablet |
| CAPSULE, EXTENDED RELEASE | Extended Release Oral Capsule |
| TABLET,DISINTEGRATING | Disintegrating Oral Tablet |
| TABLET, SUBLINGUAL | Sublingual Tablet |
| TABLET,CHEWABLE | Chewable Tablet |
| TABLET, BUCCAL | Buccal Tablet |
| INHALATION GAS | Gas for Inhalation |
| INHALATION SOLUTION | Inhalation Solution |
| INHALATION SUSPENSION | Inhalation Suspension |
| INHALATION POWDER | Inhalation Powder |
| SOLUTION FOR INJECTION | Injectable Solution |
| SUSPENSION FOR INJECTION | Injectable Suspension |
| SUSPENSION, ORAL (FINAL DOSE FORM) | Oral Suspension |
| SOLUTION, ORAL | Oral Solution |
| GRANULES FOR ORAL SUSPENSION | Granules for Oral Suspension |
| GRANULES FOR ORAL SOLUTION | Granules for Oral Solution |
| POWDER FOR ORAL SUSPENSION | Powder for Oral Suspension |
| POWDER FOR ORAL SOLUTION | Powder for Oral Solution |
| SUSPENSION,EXTENDED RELEASE VIAL (ML) | Extended Release Suspension |
| ORAL POWDER | Oral Powder |
| ORAL GEL | Oral Gel |
| CUTANEOUS SOLUTION | Topical Solution |
| CUTANEOUS FOAM | Topical Foam |
| CUTANEOUS POWDER | Topical Powder |
| EYE OINTMENT | Ophthalmic Ointment |
| EYE GEL | Ophthalmic Gel |
| SUPPOSITORY, RECTAL | Rectal Suppository |
| RECTAL CREAM | Rectal Cream |
| RECTAL GEL | Rectal Gel |
| RECTAL FOAM | Rectal Foam |
| RECTAL OINTMENT | Rectal Ointment |
| VAGINAL CREAM | Vaginal Cream |
| VAGINAL GEL | Vaginal Gel |
| RING, VAGINAL | Vaginal System |
| MOUTHWASH | Mouthwash |
| GARGLE | Mouthwash |
| LOZENGE | Oral Lozenge |
| TROCHE | Oral Lozenge |
| SOLUTION, IRRIGATION | Irrigation Solution |
| TRANSDERMAL PATCH | Transdermal System |
| AUTO-INJECTOR (EA) | Auto-Injector |
| PELLET (EA) | Oral Pellet |
| ENEMA (EA) | Enema |
| ENEMA (ML) | Enema |
| NASAL GEL | Nasal Gel |
| NASAL POWDER | Nasal Powder |
| TAPE, MEDICATED | Medicated Tape |
| TOOTHPASTE | Toothpaste |

#### Deterministic UUID Generation

`formulation_id` is derived from content, not random — the same `(master_linkage_id, generic_formulation, dosage_form)` always produces the same UUID. This means re-runs skip existing rows cleanly via `ON CONFLICT DO NOTHING`.

```python
seed = f"{master_linkage_id}|{generic_formulation}|{dosage_form}"
formulation_id = str(uuid.uuid5(uuid.NAMESPACE_OID, seed))
```

#### One-Time Data Cleanup (2026-05-02)

Rows inserted before `strip_dosage_form_suffix()` was fixed had dosage form suffixes left in `generic_formulation`. A migration was run to correct all affected rows.

**Migration summary:**

| Metric | Value |
|---|---|
| Total rows in `drugdb.drug` | 88,983 |
| Rows corrected | **17,128** |
| Rows already clean | 71,855 |
| Patterns applied | 50 |
| Rows with residual suffix after cleanup | **0** |

**Top 10 corrected dosage forms:**

| dosage_forms | rows corrected |
|---|---|
| TABLET, EXTENDED RELEASE | 3,725 |
| SOLUTION FOR INJECTION | 3,174 |
| CAPSULE, EXTENDED RELEASE | 1,567 |
| INHALATION GAS | 1,370 |
| SOLUTION, ORAL | 1,201 |
| SUSPENSION, ORAL (FINAL DOSE FORM) | 923 |
| TABLET, DELAYED RELEASE (OBS 06-25-01) | 814 |
| GASTRO-RESISTANT TABLET | 814 |
| TABLET,DISINTEGRATING | 484 |
| TRANSDERMAL PATCH | 356 |

**Migration scripts (stored in `/cdss/`):**

| Script | Purpose |
|---|---|
| `build_dosage_mappings.py` | Generates `dosage_form_mappings.json` and `dosage_form_regex_patterns.json` from the 50-entry mapping |
| `verify_dosage_cleanup.py` | Dry-run: queries DB and previews all before/after changes; generates `verification_report.txt` |
| `execute_stage1_updates.py` | Executes all 50 UPDATE statements inside a single transaction; generates `stage1_execution_log.txt` |

**Rollback:**
```sql
BEGIN;
UPDATE drugdb.drug
SET generic_formulation = generic_formulation_original
WHERE generic_formulation != generic_formulation_original;
COMMIT;
```

#### Population Script

`populate_drug_table.py`

```bash
# Dry run first — validate without writing
python3 scripts/populate_drug_table.py --password <pwd> --dry-run --verbose

# Full run with logging
python3 scripts/populate_drug_table.py --password <pwd> --log-file logs/drug.log --verbose
```

**Results (2026-05-02):** 50,111 DrugMasterLinkage records processed; 88,983 rows inserted; 0 errors.

---

### Schema Update: RxNorm Original Data Columns (Step 1b)

**Date Added:** 2026-05-02
**Script:** `update_drug_rxnorm_columns.py`
**SQL:** `add_rxnorm_columns.sql`

#### Purpose

Adds `rxnorm_generic_formulation` and `rxcui` to `drugdb.drug` so downstream scripts (`populate_drug_synonym_formulation.py`, `populate_drug_ingredient_mapping.py`) can look up formulations by their stable RxCUI instead of re-running the two-stage suffix-cleaning logic on every run.

#### New Columns

| Column | Type | Example | Source path |
|---|---|---|---|
| `rxnorm_generic_formulation` | `TEXT` | `"aspirin 81 MG Delayed Release Oral Tablet"` | `DrugMasterLinkage → rxnorm[].generic_formulation` (raw, no suffix stripping) |
| `rxcui` | `VARCHAR(50)` | `"1191"` | `DrugMasterLinkage → rxnorm[].rxcui` |

#### Matching Strategy

`formulation_id` is a deterministic UUID5 seeded from `"{master_linkage_id}|{cleaned_formulation}|{dosage_form}"`.
The update script recomputes this UUID for every rxnorm entry (same formula as `populate_drug_table.py`) — no in-memory lookup dict is needed, matching is O(1) and exact.

```python
seed           = f"{master_linkage_id}|{cleaned_formulation}|{dosage_form}"
formulation_id = str(uuid.uuid5(uuid.NAMESPACE_OID, seed))
# UPDATE drugdb.drug SET rxcui=..., rxnorm_generic_formulation=... WHERE formulation_id=...
```

#### Indexes Created

```sql
CREATE INDEX IF NOT EXISTS idx_drug_rxnorm_formulation ON drugdb.drug (rxnorm_generic_formulation);
CREATE INDEX IF NOT EXISTS idx_drug_rxcui              ON drugdb.drug (rxcui);
```

#### Population Statistics (actual run — 2026-05-02)

| Metric | Value |
|---|---|
| Total rows in `drugdb.drug` | 88,983 |
| Rows with `rxcui` populated | **88,983 (100.00%)** |
| Rows with `rxnorm_generic_formulation` populated | **88,983 (100.00%)** |
| RxNorm entries without formulation string | 9,677 (expected — not in drug table) |
| Errors | 0 |
| Main pass (update_drug_rxnorm_columns.py) | 71,855 rows updated |
| Fix pass (fix_rxnorm_uncleaned_rows.py) | 17,128 rows patched |

> **Note on 17,128 fix pass rows:** These rows had `formulation_id` UUIDs derived from the
> original *uncleaned* formulation string (the buggy `strip_dosage_form_suffix()` pre-2026-05-02
> fix). The main pass recomputed the "correct" cleaned UUID and missed them. `fix_rxnorm_uncleaned_rows.py`
> uses `generic_formulation_original` as the lookup key to find these rows and patch them.

#### Impact on Downstream Scripts

| Script | Before | After |
|---|---|---|
| `populate_drug_synonym_formulation.py` | Two-stage cleaning to match formulation_id | Direct lookup by rxcui |
| `populate_drug_ingredient_mapping.py` | Two-stage cleaning to match formulation_id | Direct lookup by rxcui |

#### Script Usage

```bash
# Step 1: Add columns and indexes, dry-run first 100 records
python3 scripts/update_drug_rxnorm_columns.py --password <pwd> --dry-run --verbose --limit 100

# Step 2: Full population with verification
python3 scripts/update_drug_rxnorm_columns.py --password <pwd> --verify --log-file logs/rxnorm_update.log --verbose

# Step 3: Run verification queries manually
psql -h 178.236.185.230 -U postgres -d postgres -c "
SELECT COUNT(*) AS total, COUNT(rxcui) AS with_rxcui, COUNT(rxnorm_generic_formulation) AS with_rxnorm
FROM drugdb.drug;"
```

---

### Table: `drugdb.drug_synonym_formulation`

**Purpose:** Stores all RxNorm synonyms for each formulation as a PostgreSQL `TEXT` array. There is exactly one row per `formulation_id` (enforced by a `UNIQUE` constraint).

#### Schema

```sql
CREATE TABLE drugdb.drug_synonym_formulation (
    id             SERIAL  PRIMARY KEY,
    formulation_id UUID    UNIQUE REFERENCES drugdb.drug(formulation_id),
    synonyms       TEXT[]
);
```

#### Columns

| Column | Type | Nullable | Description | Source |
|---|---|---|---|---|
| `id` | `SERIAL` | NOT NULL | Auto-increment surrogate primary key | Auto-generated |
| `formulation_id` | `UUID` | NOT NULL | Foreign key to `drug.formulation_id`; unique per row | Matched from `drug` table at insert time |
| `synonyms` | `TEXT[]` | YES | All synonym strings associated with this formulation | `rxnorm[].synonyms[]` |

#### Matching Logic (simplified — 2026-05-02)

Replaced the old two-stage cleaning approach with direct RxCUI lookup:

1. At startup, load the entire `drug` table into a dict: `rxcui → List[formulation_id]` (~10 MB RAM, 0.34 s)
2. For every `rxnorm[]` entry in `DrugMasterLinkage`, look up `rxcui` → `formulation_ids` (O(1)).
3. Insert one row per `formulation_id` with the synonyms array.
4. `ON CONFLICT DO NOTHING` makes re-runs safe.

```python
# At startup
rxcui_lookup: dict[str, list[str]] = {}
# { "1191": ["uuid-aspirin-tablet", "uuid-aspirin-capsule", ...], ... }

# Per entry
formulation_ids = rxcui_lookup.get(rxcui)
for fid in formulation_ids:
    batch.append((fid, synonyms))
```

> **Why this works:** `rxcui` has 100% coverage on `drugdb.drug` (populated by `update_drug_rxnorm_columns.py` + `fix_rxnorm_uncleaned_rows.py`). No string cleaning or suffix-stripping is needed.

#### Synonyms Source Note

The `synonyms` field within `combined_clean_jsonb` may appear in two formats depending on the upstream source:

- A proper **JSON array**: `["name1", "name2"]`
- A **Python-repr string**: `"['name1', 'name2']"`

Both formats are handled by the population script — the script detects the format and parses accordingly before inserting as a native `TEXT[]` array.

#### Indexes

```sql
ALTER TABLE drugdb.drug_synonym_formulation
    ADD CONSTRAINT uq_drug_synonym_formulation_formulation_id UNIQUE (formulation_id);
CREATE INDEX IF NOT EXISTS idx_drug_synonym_formulation_formulation_id
    ON drugdb.drug_synonym_formulation (formulation_id);
CREATE INDEX IF NOT EXISTS idx_dsf_synonyms_gin
    ON drugdb.drug_synonym_formulation USING GIN (synonyms);
```

#### Population Statistics

| Metric | Value |
|---|---|
| Source records streamed | 50,111 |
| Source records skipped (no rxnorm/jsonb) | — |
| Unique rxcuis in drug table | 8,074 |
| Formulation_ids loaded into lookup | 88,983 |
| **Rows inserted into `drug_synonym_formulation`** | **66,154 (74.34% of 88,983)** |
| Formulations without synonyms | 22,829 (no synonym data in source — expected) |
| Empty synonym rows | 0 |
| Errors | 0 |
| Max synonyms per formulation | 48 (PRENATAL SUPPLEMENT WITH DHA) |

#### Population Script

`populate_drug_synonym_formulation.py`

```bash
# Dry run — test first 100 records
python3 scripts/populate_drug_synonym_formulation.py --password <pwd> --dry-run --limit 100 --verbose

# Full population with verification
python3 scripts/populate_drug_synonym_formulation.py --password <pwd> --verify --log-file logs/synonym_population.log
```

---

### Table: `drugdb.drug_ingredient_mapping`

**Purpose:** Stores the mapping between each drug formulation and its constituent ingredients, including the strength (mass + unit) of each ingredient. One row is created per (formulation, ingredient) pair — formulations with multiple ingredients produce multiple rows.

#### Schema

```sql
CREATE TABLE drugdb.drug_ingredient_mapping (
    formulation_id UUID    NOT NULL REFERENCES drugdb.drug(formulation_id),
    ingredient_id  UUID    NOT NULL REFERENCES public.ingredients(id),
    mass           NUMERIC,
    unit           VARCHAR(50),
    PRIMARY KEY (formulation_id, ingredient_id)
);
```

#### Columns

| Column | Type | Nullable | Description | Source |
|---|---|---|---|---|
| `formulation_id` | `UUID` | NOT NULL | Part of composite PK; FK to `drug.formulation_id` | Matched from `drug` table at insert time |
| `ingredient_id` | `UUID` | NOT NULL | Part of composite PK; FK to `ingredients.id` | Matched from `ingredients` table at insert time |
| `mass` | `NUMERIC` | YES | Quantity of the ingredient in the formulation | `rxnorm[].ingredients[].scdc.mass` |
| `unit` | `VARCHAR(50)` | YES | Unit of measurement for the mass (e.g. `"MG"`, `"G"`) | `rxnorm[].ingredients[].scdc.unit` |

> **Note:** This table has **no surrogate `id` column**. The primary key is the composite `(formulation_id, ingredient_id)`.

#### Indexes

- `PRIMARY KEY` on `(formulation_id, ingredient_id)` — enforces uniqueness and enables fast lookups by formulation
- `idx_dim_formulation_id` on `formulation_id` — speeds up joins from `drug`
- `idx_dim_ingredient_id` on `ingredient_id` — speeds up joins from `ingredients`

#### Relationships

- **Many-to-One** → `drug`: many ingredient rows can belong to the same formulation
- **Many-to-One** → `ingredients`: many formulations can reference the same ingredient

#### Data Path

```
combined_clean_jsonb
  └─ rxnorm[]
       └─ ingredients[]
            ├─ name            → matched against public.ingredients.name (case-insensitive)
            └─ scdc
                 ├─ mass       → stored in drug_ingredient_mapping.mass
                 └─ unit       → stored in drug_ingredient_mapping.unit
```

#### Matching Logic (simplified — 2026-05-02)

Replaced the old string-based formulation matching with direct RxCUI lookup. Two in-memory dicts are built at startup:

**Formulation match (via rxcui):**
```python
# At startup — only loads formulations NOT yet in drug_ingredient_mapping
# so resume runs are automatic
rxcui_lookup: dict[str, list[str]] = {}
# { "1191": ["uuid-aspirin-tablet", "uuid-aspirin-capsule"], ... }

# Per entry
formulation_ids = rxcui_lookup.get(rxcui)  # O(1)
```

> **Resume-safe:** The lookup query filters out formulation_ids already in `drug_ingredient_mapping`, so restarted runs only process uncovered formulations and naturally pick up from where they left off.

**Ingredient match (case-insensitive):**
```python
ingredient_lookup: dict[str, str] = {}
# { "quetiapine": "uuid-ingredient-id", ... }

ingredient_id = ingredient_lookup.get(ingredient_name.lower().strip())  # O(1)
```

#### Edge Cases

| Scenario | Behaviour |
|---|---|
| `ingredients[]` array is empty or missing | Entire rxnorm entry is skipped |
| `mass` value is non-numeric | Stored as `NULL`; debug logged |
| `rxcui` not found in drug table | Entry skipped; rxcui logged |
| `ingredient_id` not found in ingredients table | That ingredient skipped; name logged |
| Duplicate `(formulation_id, ingredient_id)` | Silently ignored via `ON CONFLICT DO NOTHING` |
| All formulations already mapped (on resume) | Lookup is empty; script exits cleanly |

#### Transformation Example

**Input JSON:**
```json
{
  "rxnorm": [
    {
      "rxcui": "1049221",
      "ingredients": [
        { "name": "quetiapine", "scdc": { "mass": 100, "unit": "MG" } }
      ]
    }
  ]
}
```

**Output row in `drug_ingredient_mapping`:**
```
formulation_id  = <UUID from drug WHERE rxcui='1049221'>
ingredient_id   = <UUID from drugdb.ingredients WHERE lower(name)='quetiapine'>
mass            = 100
unit            = "MG"
```

**Multi-ingredient example (two rows produced):**
```
rxcui = "1049503"  →  formulation_id = <UUID for acetaminophen 325 MG / oxycodone 5 MG Oral Tablet>

ingredient_id = <UUID for acetaminophen>  | mass = 325 | unit = "MG"
ingredient_id = <UUID for oxycodone>      | mass = 5   | unit = "MG"
```

#### Prerequisites

Must be run **after**:
1. `populate_drug_table.py` — `drugdb.drug` must exist and be populated
2. `update_drug_rxnorm_columns.py` + `fix_rxnorm_uncleaned_rows.py` — `drug.rxcui` must have 100% coverage
3. `drugdb_migration.sql` — `drugdb.ingredients` must be populated (19,842 rows)

#### Population Script

`populate_drug_ingredient_mapping.py`

```bash
# Dry run — test first 100 records
python3 scripts/populate_drug_ingredient_mapping.py --password <pwd> --dry-run --limit 100 --verbose

# Full population with verification
python3 scripts/populate_drug_ingredient_mapping.py --password <pwd> --verify --log-file logs/ingredient_mapping.log
```

#### Population Statistics (actual run — 2026-05-02)

| Metric | Value |
|---|---|
| **Total rows in `drug_ingredient_mapping`** | **92,570** |
| Formulations with ingredient mapping | **84,316 (94.76% of 88,983)** |
| Formulations without ingredient mapping | 4,667 (no ingredient data in source — expected) |
| Errors | 0 |

> **Note on 4,667 uncovered formulations:** These formulations have no `ingredients[]` array in their source `rxnorm[]` entry. This is expected — some RxNorm entries describe combination products or formulations where ingredient-level data is not available. The `drug_synonym_formulation` table covers these cases at 74.34%.

---

---

### Table: `drugdb.indian_brand_ingredient` (DrugBank ID Mapping)

**Purpose:** Maps Indian brand ingredient names to DrugBank IDs by matching raw/normalized ingredient names against the DrugBank ingredients reference tables. This is an **UPDATE** operation on a pre-existing table — the script fills in `NULL` `drugbank_id` values; it does not insert new rows.

#### Schema (pre-existing table, not created by our scripts)

```sql
-- Existing table structure (relevant columns only)
CREATE TABLE drugdb.indian_brand_ingredient (
    ingredient_name_raw  VARCHAR,
    ingredient_name_norm VARCHAR,
    drugbank_id          VARCHAR   -- NULL before update; filled by our script
);
```

#### Columns Updated

| Column | Type | Nullable | Description | Source |
|---|---|---|---|---|
| `ingredient_name_raw` | `VARCHAR` | YES | Raw ingredient name from Indian brand data | Pre-existing |
| `ingredient_name_norm` | `VARCHAR` | YES | Normalized (lowercased) ingredient name | Pre-existing |
| `drugbank_id` | `VARCHAR` | YES | DrugBank ID matched from `drugdb.ingredients` | **Set by `update_indian_brand_drugbank_id.py`** |

#### Source Tables Used for Matching

| Table | Role |
|---|---|
| `drugdb.ingredients` | Direct name lookup (Tier 1) and DrugBank ID source |
| `drugdb.ingredient_synonyms` | Synonym lookup (Tier 2); `id` used to resolve `drugbank_id` from `ingredients` |

#### 4-Tier Matching Strategy

All matching is **case-insensitive**. For each record, `ingredient_name_raw` is tried first, then `ingredient_name_norm`. The script stops at the first successful match across all tiers.

| Tier | Method | Match Type | Source Table | Tie-breaking |
|---|---|---|---|---|
| 1 | `ingredient_name` = `ingredients.name` | Exact | `drugdb.ingredients` | N/A |
| 2 | `ingredient_name` = `ingredient_synonyms.synonym` | Exact | `drugdb.ingredient_synonyms` → `drugdb.ingredients` | N/A |
| 3 | `ingredients.name`.startswith(`ingredient_name`) | Prefix | `drugdb.ingredients` | Shortest match wins |
| 4 | `ingredient_synonyms.synonym`.startswith(`ingredient_name`) | Prefix | `drugdb.ingredient_synonyms` → `drugdb.ingredients` | Shortest match wins |

```python
# Prefix match tie-breaking
best = min(candidates, key=lambda x: len(x['name_lower']))
```

#### Matching Examples

**Tier 1 — Exact name match:**
```
Input:   ingredient_name_raw = "Abacavir"
Match:   ingredients.name = "Abacavir"
Output:  drugbank_id = "DB01048"
```

**Tier 2 — Exact synonym match:**
```
Input:   ingredient_name_raw = "EACA"
Match:   ingredient_synonyms.synonym = "EACA"
Lookup:  ingredients WHERE id = fd548b68-dc2b-5fc1-97f5-29657f9aa552
Output:  drugbank_id = "DB00513"  (Aminocaproic acid)
```

**Tier 3 — Prefix match on ingredient name:**
```
Input:   ingredient_name_raw = "Aminocap"
Match:   ingredients.name = "Aminocaproic acid"  (starts with "aminocap")
Output:  drugbank_id = "DB00513"
```

**Tier 4 — Prefix match on synonym:**
```
Input:   ingredient_name_raw = "Aminocapron"
Match:   ingredient_synonyms.synonym = "Aminocapronasure"  (starts with "aminocapron")
Lookup:  ingredients WHERE id = fd548b68-dc2b-5fc1-97f5-29657f9aa552
Output:  drugbank_id = "DB00513"  (Aminocaproic acid)
```

#### Two-Phase Execution

The script runs in two explicit phases to ensure safety:

**Phase 1 — Dry Run (always runs first, no DB writes):**
1. Loads all records with `drugbank_id IS NULL`
2. Runs 4-tier matching entirely in memory
3. Prints statistics, tier samples (5 per tier), and all unmatched records
4. Saves results to `match_statistics.json`
5. Prompts for confirmation before proceeding

**Phase 2 — Actual Update (only after user confirms):**
1. Executes `UPDATE drugdb.indian_brand_ingredient SET drugbank_id = %s WHERE ingredient_name_raw = %s AND drugbank_id IS NULL`
2. Commits in configurable batches (default 500)
3. Prints per-tier counts and any errors

> **Performance note:** The default Phase 2 row-by-row batch approach is very slow for large tables (~23 hours for 580k rows). Use `bulk_update_drugbank_fast.py` instead — it parses match results from the Phase 1 log and applies all updates via a single `UPDATE … FROM` on a temp table, completing in under 15 minutes. Run Phase 1 first to generate the log, then stop Phase 2 and run the bulk script.

#### Update Statistics

Two passes were run. Pass 1 used exact/prefix matching; Pass 2 used fuzzy matching on the remaining unmatched ingredients.

**Pass 1 — Exact/Prefix matching (2026-05-01)**

| Metric | Rows | Distinct ingredients |
|---|---|---|
| Total records with NULL at start | 580,669 | — |
| Successfully matched & updated | **559,356** (96.33%) | **1,592** |
| Unmatched after Pass 1 | 21,313 (3.67%) | 285 |
| Tier 1 — exact name match | 467,141 | 1,372 |
| Tier 2 — exact synonym match | 79,243 | 163 |
| Tier 3 — prefix on ingredient name | 7,179 | 35 |
| Tier 4 — prefix on synonym | 5,793 | 22 |

> Scripts: `update_indian_brand_drugbank_id.py` (Phase 1 dry-run) + `bulk_update_drugbank_fast.py` (bulk apply)

**Pass 2 — Fuzzy matching (2026-05-02)**

| Metric | Rows | Distinct ingredients |
|---|---|---|
| Unmapped ingredients analysed | 21,313 | 285 |
| Tier 5.1 — parenthetical extraction | 4,910 | 25 |
| Tier 5.2 — high-confidence fuzzy (≥95%) | 1,377 | 24 |
| Tier 5.3 — token-based exact match | 7,239 | 83 |
| Tier 5.4 — medium-confidence fuzzy (85–94%) | 2,992 | 24 |
| Tier 5.5 — partial/substring | 0 (skipped — false positives) | — |
| **Total newly mapped in Pass 2** | **+16,483** | **156** |
| Still unmapped after Pass 2 | 4,830 | ~129 |

> Scripts: `fuzzy_match_indian_ingredients.py` (Phase 1 analysis) + `apply_fuzzy_matches.py` (apply)
> Results saved in: `fuzzy_match_results.json`

**Excluded matches (known bad — not applied):**

| Ingredient | Matched to | Reason excluded |
|---|---|---|
| `Calcitonin (Salmon)` | `Salmon` | "Salmon" is not the drug name |
| `Pegylated Interferon Alpha 2B` | `alpha 2a` | Alpha-2B ≠ Alpha-2A |
| `Zinc pyrithione` | `Zinc` | Element ≠ compound |
| `n-acetylcarnosine` | `n-acetyltyrosine` | Different molecules |
| Tier 5.5 (124 ingredients) | single-letter DB entries | `fuzz.partial_ratio` matched 1-char names |

**Current state (after both passes):**

| Metric | Value |
|---|---|
| Total records | 580,669 |
| With drugbank_id | **575,839** |
| Coverage | **99.17%** |
| Still NULL | 4,830 (~129 distinct ingredients) |

> Remaining unmapped ingredients (e.g. `Tricholine`, `Divalproex`, `Lactic acid bacillus`) have no reliable DrugBank match and require manual mapping or an alternative data source.

> **Rows vs. distinct ingredients:** The same ingredient name appears in many product rows. "Rows" = DB rows updated; "Distinct" = unique ingredient names matched.

#### Population Script

`update_indian_brand_drugbank_id.py`

> **Note:** Use `--skip-confirm` for non-interactive/automated runs. Always review the Phase 1 dry-run output manually before the first production run.

---

## 4. Database Diagram

```
┌──────────────────────────────────────────┐
│           DrugMasterLinkage              │
│  master_linkage_id  UUID  PK             │
│  combined_clean_jsonb  JSONB             │
│    └─ openfda.drug_info.generic_name     │
│    └─ rxnorm[].generic_formulation       │
│    └─ rxnorm[].specific_dosage_form      │
│    └─ rxnorm[].synonyms[]               │
│    └─ rxnorm[].ingredients[].name        │
│    └─ rxnorm[].ingredients[].scdc.mass   │
│    └─ rxnorm[].ingredients[].scdc.unit   │
└────────────────┬─────────────────────────┘
                 │ source (1 record → N formulations)
                 ▼
┌──────────────────────────────────────────┐
│                 drug  ✅                  │
│  formulation_id      UUID  PK            │◄─────────────────────────┐
│  master_linkage_id   UUID                │                          │ FK
│  generic_name        TEXT                │◄──────────┐              │
│  generic_formulation TEXT                │           │ FK           │
│  dosage_forms        TEXT                │           │              │
└──────────────────────────────────────────┘           │              │
                                        ┌──────────────┴──────────┐   │
                                        │ drug_synonym_formulation│   │
                                        │  id          SERIAL  PK │   │
                                        │  formulation_id UUID FK │   │
                                        │  synonyms    TEXT[]     │   │
                                        └─────────────────────────┘   │
                                                                       │
┌─────────────────────────┐             ┌──────────────────────────────┴───┐
│      ingredients        │             │     drug_ingredient_mapping      │
│  id    UUID  PK         │◄── FK ──────│  formulation_id  UUID  PK / FK   │
│  name  VARCHAR          │             │  ingredient_id   UUID  PK / FK   │
│  rxcui VARCHAR          │             │  mass            NUMERIC         │
│  ...                    │             │  unit            VARCHAR(50)     │
└─────────────────────────┘             └──────────────────────────────────┘
```

---

## 5. Population Scripts Reference

| Script | Target Table | Dependencies | Run Order | Key Arguments |
|---|---|---|---|---|
| `update_ingredient_rxcui.py` | `drugdb.ingredients` (rxcui UPDATE/INSERT) | `DrugMasterLinkage` populated | 0 | `--password`, `--dry-run`, `--verbose`, `--batch-size`, `--log-file` |
| `populate_drug_table.py` | `drugdb.drug` | None | 1 | `--password`, `--dry-run`, `--verbose`, `--batch-size`, `--log-file` |
| `update_drug_rxnorm_columns.py` | `drugdb.drug` (UPDATE: rxcui + rxnorm_generic_formulation) | `drugdb.drug` populated | 1b | `--password`, `--dry-run`, `--verbose`, `--batch-size`, `--log-file`, `--limit`, `--verify`, `--skip-ddl` |
| `populate_drug_synonym_formulation.py` | `drugdb.drug_synonym_formulation` | `drugdb.drug` | 2 | `--password`, `--dry-run`, `--verbose`, `--batch-size`, `--log-file` |
| `populate_drug_ingredient_mapping.py` | `drugdb.drug_ingredient_mapping` | `drugdb.drug` + `public.ingredients` | 3 | `--password`, `--dry-run`, `--verbose`, `--batch-size`, `--log-file` |
| `update_indian_brand_drugbank_id.py` | `drugdb.indian_brand_ingredient` (UPDATE) | `drugdb.ingredients` + `drugdb.ingredient_synonyms` | — (UPDATE only) | `--password`, `--dry-run` (Phase 1 automatic), `--skip-confirm`, `--batch-size`, `--log-file`, `--verbose` |
| `bulk_update_drugbank_fast.py` | `drugdb.indian_brand_ingredient` (bulk UPDATE) | Phase 1 log file (`logs/indian_brand_drugbank.log`) | — (run after Phase 1; replaces slow Phase 2) | _(no CLI args — edit constants at top of file)_ |
| `fuzzy_match_indian_ingredients.py` | `drugdb.indian_brand_ingredient` (analysis only) | DB connection + unmapped rows already exist | — (Phase 1 analysis; generates `fuzzy_match_results.json`) | _(no CLI args)_ |
| `apply_fuzzy_matches.py` | `drugdb.indian_brand_ingredient` (UPDATE) | `fuzzy_match_results.json` from above | — (run after fuzzy analysis; interactive confirm) | _(no CLI args — exclusion list editable at top of file)_ |

---

## 6. Running the Scripts

> **Always use `--dry-run` before the first real run** to validate data extraction and transformation without writing to the database.

### Step 0: Update `drugdb.ingredients` with RxCUI ✅ Completed 2026-05-02

> Prerequisite: `DrugMasterLinkage` must be populated. Safe to re-run — already-correct rows are detected and skipped.

```bash
# Dry run first
python3 scripts/update_ingredient_rxcui.py --password <pwd> --dry-run --verbose

# Actual run
python3 scripts/update_ingredient_rxcui.py --password <pwd> --verbose --log-file logs/update_ingredient_rxcui.log
```

**Results (2026-05-02):** 226 NULL→rxcui updates, 4 changed values, 1,901 skipped (already correct), 6 new inserts. Total: 2,134 / 20,034 ingredients now have rxcui (10.7%).

---

### Step 1: Populate the `drug` table ✅ Completed 2026-05-02

> Prerequisite: `DrugMasterLinkage.combined_clean_jsonb` must be populated. Safe to re-run — deterministic UUIDs mean re-runs skip existing rows cleanly.

```bash
# Dry run first — validate without writing
python3 scripts/populate_drug_table.py --password <pwd> --dry-run --verbose

# Full run with logging
python3 scripts/populate_drug_table.py --password <pwd> --log-file logs/drug.log --verbose
```

**Results (2026-05-02):** 50,111 DrugMasterLinkage records read; 2,492 skipped (null jsonb); **88,983 rows inserted**; 0 errors.

---

### Step 1b: Add and populate RxNorm columns in `drug`

> Only run after Step 1 has completed. Idempotent — rows already having `rxcui` set are skipped.

```bash
# Dry-run on first 100 records to validate
python3 scripts/update_drug_rxnorm_columns.py --password <pwd> --dry-run --verbose --limit 100

# Full run with verification report
python3 scripts/update_drug_rxnorm_columns.py --password <pwd> --verify --log-file logs/rxnorm_update.log --verbose

# If columns already exist (re-run), skip DDL
python3 scripts/update_drug_rxnorm_columns.py --password <pwd> --skip-ddl --verify --verbose
```

---

### Step 2: Populate the `drug_synonym_formulation` table

> Only run after Step 1b has completed successfully (100% rxcui coverage on `drugdb.drug`).

```bash
# Dry run first — validate without writing
python3 scripts/populate_drug_synonym_formulation.py --password <pwd> --dry-run --limit 100 --verbose

# Full run with verification and logging (idempotent — safe to restart)
python3 scripts/populate_drug_synonym_formulation.py --password <pwd> --verify --log-file logs/synonym_population.log
```

> **Resume safety:** Re-runs skip already-inserted `formulation_id`s via `ON CONFLICT DO NOTHING`. No data will be duplicated.

### Step 3: Populate the `drug_ingredient_mapping` table

> Prerequisites: Step 1b complete + `drugdb.ingredients` populated (19,842 rows).

```bash
# Dry run first — validate without writing
python3 scripts/populate_drug_ingredient_mapping.py --password <pwd> --dry-run --limit 100 --verbose

# Full run with verification and logging (resume-safe)
python3 scripts/populate_drug_ingredient_mapping.py --password <pwd> --verify --log-file logs/ingredient_mapping.log
```

> **Resume safety:** The rxcui lookup at startup filters out formulation_ids already in `drug_ingredient_mapping` — restarted runs automatically skip completed work.

These reports help identify data quality gaps between the source JSONB and the reference tables.

### Update: `drugdb.indian_brand_ingredient` DrugBank IDs

> Prerequisites: `drugdb.ingredients` and `drugdb.ingredient_synonyms` must be populated.
> This script updates an **existing** table — it does not create a new one.

**Recommended workflow (fast path):**

```bash
# Step 1 — Run Phase 1 only (dry run, no DB writes) to produce the match log
python3 scripts/update_indian_brand_drugbank_id.py --password <pwd> --skip-confirm --log-file logs/indian_brand_drugbank.log --verbose
# (stop the process after Phase 1 completes, before Phase 2 starts writing row-by-row)

# Step 2 — Apply all matches in a single bulk UPDATE (completes in ~5-15 minutes)
python3 bulk_update_drugbank_fast.py
```

**Legacy (slow) path — avoid for large tables:**

```bash
# Full run including Phase 2 row-by-row (very slow — ~23 hours for 580k rows)
python3 scripts/update_indian_brand_drugbank_id.py --password <pwd> --skip-confirm --log-file logs/indian_brand_drugbank.log --verbose
```

After Phase 1 completes, check `match_statistics.json` for the full unmatched list and tier breakdown.

### Fuzzy Matching: remaining unmapped ingredients

> Run after the exact/prefix pass above. Requires `fuzzywuzzy` and `python-Levenshtein` (`pip install fuzzywuzzy python-Levenshtein`).

```bash
# Phase 1 — analysis only, saves fuzzy_match_results.json (takes ~8 minutes for 285 ingredients)
echo "no" | python3 fuzzy_match_indian_ingredients.py

# Review the output, then apply with the exclusion list in apply_fuzzy_matches.py
python3 apply_fuzzy_matches.py
```

> Edit the `EXCLUDE` dict at the top of `apply_fuzzy_matches.py` before running to remove any bad matches identified during analysis. Tier 5.5 (partial/substring) should always be reviewed carefully — single-letter DB entries can produce false 100% scores.

### Performance Tuning

Both scripts default to a batch size of `1000` rows per commit. Adjust with `--batch-size` as needed:

```bash
python3 scripts/populate_drug_table.py --password <pwd> --batch-size 5000 --verbose
```

---

## 7. Notes and Considerations

- **Idempotency:** Scripts are safe to re-run. Existing rows will not be duplicated (`ON CONFLICT … DO NOTHING` is used throughout). This is important because `combined_clean_jsonb` may still be actively populating at time of script execution.
- **Synonyms format:** The `synonyms` field in the source JSONB may be a proper JSON array or a Python-repr string (e.g. `"['name1', 'name2']"`). Both formats are handled transparently by `populate_drug_synonym_formulation.py`.
- **UUID generation:** `formulation_id` values are generated using `uuid.uuid5(NAMESPACE_OID, seed)` where `seed = f"{master_linkage_id}|{generic_formulation}|{dosage_form}"`. This is deterministic — the same content always produces the same UUID, making re-runs fully idempotent via `ON CONFLICT DO NOTHING`.
- **Composite PK:** `drug_ingredient_mapping` uses a composite primary key `(formulation_id, ingredient_id)` — no surrogate `id` column.
- **In-memory lookups:** `populate_drug_ingredient_mapping.py` and `populate_drug_synonym_formulation.py` pre-load their reference tables (drug, ingredients) into memory at startup to avoid per-row DB round-trips. On very large tables, ensure the host has sufficient RAM.
- **Batch size:** Default batch size is `1000` rows per commit. Tune with `--batch-size` for performance.
- **Dry run discipline:** Use `--dry-run` before every first run against a new environment or after significant source data changes.
- **Source data availability:** If `combined_clean_jsonb` is not yet fully populated, scripts will process only what is available. Re-run after population completes to capture remaining records.
- **Missing data reports:** `populate_drug_ingredient_mapping.py` outputs frequency-sorted reports of ingredient names and formulation strings that could not be matched. Review these after each run to identify data quality gaps.
- `update_indian_brand_drugbank_id.py` performs in-memory matching — both `drugdb.ingredients` (~20k rows) and `drugdb.ingredient_synonyms` (~50k rows) are loaded at startup and held in dicts for O(1) exact lookups and O(n) prefix scans.
- Prefix matches are disambiguated by taking the shortest candidate name (most specific match). Review `match_statistics.json` tier_3 and tier_4 examples after the first dry run to validate prefix match quality.

---

## 8. Future Tables

_This section is reserved for documentation of additional tables as they are designed and implemented._

> Add future table documentation here following the same format as Section 3.

---

---

### Update: New Columns Added to drugdb.drug (Phase — Drug Enrichment)

**Date:** 2026-05-05
**SQL:** `alter_drug_table_new_columns.sql`
**Script:** `update_drug_new_columns.py`
**Status:** ✅ Completed — 88,983 / 88,983 rows updated, 0 errors, elapsed 738s

#### Why These Columns Were Added

The initial `drugdb.drug` population (Step 1 / 1b) captured only RxNorm-derived identity columns. This enrichment phase adds clinical and provenance metadata drawn directly from `DrugMasterLinkage.combined_clean_jsonb` — data needed by the CDSS API for drug detail pages, filtering by route/product type, and clinical content display (mechanism of action).

#### Join Strategy

```
drugdb.drug.master_linkage_id = public."DrugMasterLinkage".master_linkage_id
```

One `master_linkage_id` maps to **multiple** `drug` rows (one per RxNorm formulation). The same enrichment values are written to all rows sharing the same `master_linkage_id` — this is correct and expected.

The script uses a **server-side streaming JOIN** (no bulk Python dict load) to keep memory usage constant across the full 88,983-row run.

#### New Columns

| Column | Type | Source JSON path | Example value |
|---|---|---|---|
| `product_type` | `TEXT` | `dailymed → identification → drug_label ->> label_type` (strip trailing ` LABEL`) | `"HUMAN PRESCRIPTION DRUG"` |
| `routes` | `TEXT[]` | `dailymed → drug_info → products[] ->> route_of_administration` (distinct across all products) | `{ORAL}`, `{INTRADERMAL,SUBCUTANEOUS}` |
| `mechanism_of_action` | `TEXT` | `openfda → clinical → mechanism_of_action ->> text` (truncated at 5000 chars) | `"Alprazolam is a 1,4 benzodiazepine..."` |
| `record_version` | `TEXT DEFAULT '1.0'` | `dailymed → identification → drug_label ->> version` (fallback `'1.0'` if null) | `"1"`, `"20"` |
| `last_ingested_at` | `TIMESTAMPTZ DEFAULT NOW()` | Not from JSON — set to `NOW()` at population time | `2026-05-05 07:22:53+00` |
| `has_openfda` | `BOOLEAN DEFAULT FALSE` | `True` if `combined_clean_jsonb → openfda` exists and is not null | `true` |
| `has_dailymed` | `BOOLEAN DEFAULT FALSE` | `True` if `combined_clean_jsonb → dailymed` exists and is not null | `true` |
| `has_rxnorm` | `BOOLEAN DEFAULT FALSE` | `True` if `combined_clean_jsonb → rxnorm` exists and is a non-empty array | `true` |
| `has_drugbank` | `BOOLEAN DEFAULT FALSE` | `True` if `combined_clean_jsonb → drugbank` exists and is a non-empty array | `true` |

#### Columns Intentionally Skipped

| Column | Reason skipped |
|---|---|
| `brand_names` | Stored in `drug_synonym_formulation` — no duplication needed |
| `drug_class` | Highly inconsistent across sources; requires normalisation pass before adding |
| `manufacturer` | Available in DailyMed but not consistently mapped to formulation level |

#### Existing Columns Not Touched

`formulation_id`, `master_linkage_id`, `generic_name`, `generic_formulation`, `rxnorm_generic_formulation`, `rxcui`, `dosage_forms`, `generic_formulation_original` — all untouched.

#### Population Statistics (actual run — 2026-05-05)

| Metric | Value |
|---|---|
| Total drug rows processed | 88,983 |
| Successfully updated | **88,983 (100%)** |
| Skipped (no linkage match) | 0 |
| Errors | 0 |
| `product_type` populated | 88,983 (100%) |
| `routes` populated | 87,182 (97.98%) — 1,801 rows have no products array in DailyMed |
| `mechanism_of_action` populated | 47,024 (52.8%) — remainder have no OpenFDA clinical text |
| `record_version` populated | 88,983 (100%) — fallback `'1.0'` used where null |
| `last_ingested_at` populated | 88,983 (100%) |
| `has_openfda` = true | 88,983 (100%) |
| `has_dailymed` = true | 88,983 (100%) |
| `has_rxnorm` = true | 88,983 (100%) |
| `has_drugbank` = true | 88,983 (100%) |
| Elapsed time | 738s (~12 min) |

> **Note on `mechanism_of_action` array handling:** 331 DrugMasterLinkage records store `openfda.clinical.mechanism_of_action.text` as a JSON array instead of a string. The script detects this and joins array elements with a space before storing.

#### Verification Queries

```sql
-- Coverage check for all 9 new columns
SELECT
  COUNT(*)                                        AS total_rows,
  COUNT(product_type)                             AS product_type_filled,
  COUNT(routes)                                   AS routes_filled,
  COUNT(mechanism_of_action)                      AS moa_filled,
  COUNT(record_version)                           AS record_version_filled,
  COUNT(last_ingested_at)                         AS last_ingested_filled,
  COUNT(CASE WHEN has_openfda  = TRUE THEN 1 END) AS has_openfda_true,
  COUNT(CASE WHEN has_dailymed = TRUE THEN 1 END) AS has_dailymed_true,
  COUNT(CASE WHEN has_rxnorm   = TRUE THEN 1 END) AS has_rxnorm_true,
  COUNT(CASE WHEN has_drugbank = TRUE THEN 1 END) AS has_drugbank_true
FROM drugdb.drug;

-- Top routes distribution
SELECT routes, COUNT(*) AS rows
FROM drugdb.drug
GROUP BY routes
ORDER BY rows DESC
LIMIT 10;

-- Top product types
SELECT product_type, COUNT(*) AS rows
FROM drugdb.drug
GROUP BY product_type
ORDER BY rows DESC
LIMIT 5;

-- Sample mechanism of action (non-null)
SELECT generic_name, mechanism_of_action
FROM drugdb.drug
WHERE mechanism_of_action IS NOT NULL
LIMIT 3;
```

---

## 9. Changelog

| Date | Change |
|---|---|
| 2026-05-01 | Initial schema documented: `drug`, `drug_synonym_formulation` |
| 2026-05-01 | Added `drug_ingredient_mapping` table; updated diagram, sequence, scripts reference, and notes |
| 2026-05-01 | Added drugdb.indian_brand_ingredient update documentation; created match_statistics.json template |
| 2026-05-01 | DrugBank ID bulk update completed: 559,356 / 580,669 rows updated (96.33%); real match statistics filled in; `bulk_update_drugbank_fast.py` added as fast-path alternative to slow Phase 2 row-by-row updates |
| 2026-05-02 | Fuzzy matching pass completed on remaining 285 unmapped ingredients; 156 mapped (+16,483 rows); coverage raised from 96.33% → 99.17%; 4,830 rows (129 distinct ingredients) still NULL; `fuzzy_match_indian_ingredients.py` + `apply_fuzzy_matches.py` added; Tier 5.5 bug documented (single-letter false positives); 4 bad matches manually excluded |
| 2026-05-02 | `drugdb.ingredients` RxCUI update completed: 226 NULL→value, 4 changed, 6 new inserts; 2,134/20,034 rows now have rxcui (10.7%); `update_ingredient_rxcui.py` fixed to load all ingredients (not just NULL) for correct incremental-run behaviour; Section 3 and population sequence updated |
| 2026-05-02 | `drugdb.drug` table populated: 88,983 rows inserted, 0 errors; schema fixed: all text columns changed from `VARCHAR(500)` → `TEXT` (multi-ingredient formulations exceed 500 chars); `formulation_id` changed from `uuid.uuid4()` → `uuid.uuid5(NAMESPACE_OID, seed)` for deterministic idempotent re-runs; two-connection pattern applied to keep named cursor alive; Step 1 status updated to ✅ Completed |
| 2026-05-02 | **Dosage form suffix cleanup:** 17,128 rows in `drugdb.drug` had RxNorm suffix still embedded in `generic_formulation` (root cause: `strip_dosage_form_suffix()` used exact string matching, which failed for EU/uppercase `dosage_forms` codes like `TABLET, EXTENDED RELEASE`). Fix: (1) one-time migration applied 50 regex patterns inside a single transaction — 0 errors, 0 residual dirty rows; (2) `strip_dosage_form_suffix()` in `populate_drug_table.py` updated with `_DOSAGE_FORM_SUFFIX_MAP` (50 entries) + pre-compiled `_COMPILED_PATTERNS`; (3) `generic_formulation_original` backup column added for audit/rollback; Pipeline Dashboard updated with Step 1a |
| 2026-05-02 | **RxNorm original data columns populated:** `rxnorm_generic_formulation` (TEXT) and `rxcui` (VARCHAR 50) added and fully populated on all 88,983 rows (100.00% coverage, 0 errors); main pass via `update_drug_rxnorm_columns.py` (71,855 rows); fix pass via `fix_rxnorm_uncleaned_rows.py` (17,128 rows whose `formulation_id` was seeded from uncleaned pre-cleanup formulation strings); two indexes created (`idx_drug_rxcui`, `idx_drug_rxnorm_formulation`); pipeline Step 1b marked ✅ |
| 2026-05-02 | **`drug_synonym_formulation` population complete:** 66,154 rows inserted (74.34% coverage); 22,829 formulations have no synonym data in source (expected — RxNorm simply has no synonyms for them). Matching logic replaced: old two-stage cleaning → direct RxCUI lookup. Resume-safety optimization added: lookup filtered to uncovered formulation_ids only + FETCH_SQL limited to records with uncovered formulations — reduced runtime from ~3 hours to ~15 minutes. |
| 2026-05-02 | **`drug_ingredient_mapping` script and docs updated:** Matching logic replaced old string-based formulation matching with rxcui lookup. Added resume-safety: lookup query filters out formulation_ids already in `drug_ingredient_mapping`, so restarts pick up from where they left off automatically. Prerequisites section added. Script usage instructions documented. |
| 2026-05-02 | **`drug_ingredient_mapping` population complete:** 92,570 rows inserted; 84,316 / 88,983 formulations covered (94.76%); 4,667 formulations have no ingredient data in source (expected); 0 errors. Pipeline Step 3 marked ✅. |
| 2026-05-05 | **Drug enrichment columns added to `drugdb.drug`:** 9 new columns (`product_type`, `routes`, `mechanism_of_action`, `record_version`, `last_ingested_at`, `has_openfda`, `has_dailymed`, `has_rxnorm`, `has_drugbank`) added via `alter_drug_table_new_columns.sql` and populated via `update_drug_new_columns.py`. 88,983 / 88,983 rows updated, 0 errors, 738s. MOA array-type fix applied (331 records had JSON array instead of string). Memory-efficient server-side streaming JOIN used instead of bulk Python dict load. |
