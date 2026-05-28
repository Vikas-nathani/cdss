# Table Creation & Population Reference

Quick-reference commands for creating and populating `drugdb` tables.

---

## drugdb.drug

**Rows:** 88,983 | **Source:** `DrugMasterLinkage.combined_clean_jsonb` via `rxnorm[]` entries

```bash
# Dry-run (preview only, no writes)
python3 scripts/populate_drug_table.py --password Admin@123 --dry-run --verbose

# Full run
python3 scripts/populate_drug_table.py --password Admin@123 --log-file logs/drug_populate.log --verbose
```

---

## drugdb.drug_identifier

**Rows:** 578,635 | **Source:** JOIN of `DrugMasterLinkage` + `drugdb.drug`

```bash
python3 scripts/populate_drug_identifier.py
```

---

## drugdb.label_table

**Rows:** 510,527 | **Source:** `DrugMasterLinkage.combined_clean_jsonb` → all `openfda.*.*.table[]` arrays

```bash
# Test mode (2 records, no insert — shows preview)
python3 populate_label_table.py
# → answer n to exit after preview

# Test insert (5 records, verify inserts work)
python3 populate_label_table.py --test-insert 5
# → answer y to confirm

# Full run (50,111 records, 510,527 rows)
python3 populate_label_table.py
# → answer y to confirm full run
```

**Log:** `label_table_population.log`

**Schema:**
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

**Notes:**
- `formulation_id` has no FK constraint at DB level — referential integrity managed by script
- `table_id` format: `{formulation_id}_{section_key}_table_{N}`
- `rows_data` stores rows as-is (array-of-arrays or array-of-objects)
- Script is idempotent — safe to re-run; duplicate rows skipped via `ON CONFLICT DO NOTHING`
- 2,492 `DrugMasterLinkage` records have no matching formulation in `drugdb.drug` (data gap, logged as WARNING)

---

## drugdb.clinical_section

**Rows:** 2,887,910 | **Source:** `DrugMasterLinkage.combined_clean_jsonb` → openfda + dailymed narrative label sections

```bash
# Test mode (2 records, no insert — shows preview)
python3 populate_clinical_section.py
# → answer n to exit after preview

# Full run (50,111 records, 2,887,910 rows)
python3 populate_clinical_section.py
# → answer yes to proceed with full run
# → answer yes/no to truncate existing rows before starting
```

**Log:** `clinical_section_population.log`

**Schema:**
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

**Notes:**
- Sections discovered dynamically by traversing 9 parent keys — no hardcoded section names
- `source` is either `openfda` or `dailymed` — both stored as separate rows, never merged
- OpenFDA rows: text from `child["text"]`, subsections always `[]`
- DailyMed rows: text from `child["content"]`, subsections transformed to `{subsection_id, title, text}`
- `text` stored as NULL when empty string found
- Script creates the table automatically if it does not exist
- Idempotent: re-runs safely via `ON CONFLICT (formulation_id, section, source) DO NOTHING`
- 2,492 `DrugMasterLinkage` records skipped (no matching row in `drugdb.drug` — known data gap)
- openfda: 1,840,508 rows | dailymed: 1,047,402 rows | 45 unique sections

---

## drugdb.ingredients / ingredient_synonyms / ingredient_interactions

**Source:** `DrugSourceMaster.standardized_records` WHERE source='drugbank'

```bash
psql -h 178.236.185.230 -U postgres -d postgres -f schemas/drugdb_migration.sql
```
