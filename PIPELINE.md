# CDSS Pipeline Documentation

Detailed documentation for each data pipeline stage. Run stages in this order:
`00 → 01 → 09 → 08 → 02 → 03 → 04 → 05 → 06 → 07 → 10 → 11`

## Table of Contents

- [Stage 00: Setup](#stage-00-setup)
- [Stage 01: Drug Table](#stage-01-drug-table)
- [Stage 02: Ingredient Nodes](#stage-02-ingredient-nodes)
- [Stage 03: Drug-Ingredient Mapping](#stage-03-drug-ingredient-mapping)
- [Stage 04: Drug Interactions](#stage-04-drug-interactions)
- [Stage 05: Drug Class](#stage-05-drug-class)
- [Stage 06: Dosing Regimen](#stage-06-dosing-regimen)
- [Stage 07: Indications](#stage-07-indications)
- [Stage 08: Clinical Sections](#stage-08-clinical-sections)
- [Stage 09: Label Table](#stage-09-label-table)
- [Stage 10: Vector Embeddings](#stage-10-vector-embeddings)
- [Stage 11: Indian Brands](#stage-11-indian-brands)
- [RxCUI Backfill](#rxcui-backfill)
- [Sibling Enrichment Pass](#sibling-enrichment-pass)

---

# Stage 00: Setup

## Purpose
Establishes the complete database foundation for the CDSS DrugDB pipeline. This stage creates all PostgreSQL tables and indexes in the `drugdb` schema (including pgvector, pg_trgm, and uuid-ossp extensions), loads the UMLS MRREL relationship file into `umls.mrrel`, and applies Neo4j constraints and indexes. It also provides a utility script to export the `"DrugMasterLinkage"` source table for inspection, and a final graph-population script (`neo4j_populate.py`) that is run after all other stages complete.

## Prerequisites
- Access to PostgreSQL at `$DB_HOST:5432` (database: `postgres`, user: `postgres`)
- Access to Neo4j at `bolt://localhost:7687` (user: `neo4j`, password: `$NEO4J_PASSWORD`)
- UMLS MRREL.RRF file present at `/home/nathanivikas890_gmail_com/umls/MRREL.RRF`
- PostgreSQL extensions `vector` (pgvector), `pg_trgm`, and `uuid-ossp` available on the server
- Python packages: `psycopg2-binary`, `tqdm`, `asyncpg`, `neo4j`

## Database Tables

### Tables Written To
| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `umls.mrrel` | CREATE + bulk COPY | ~60–70M rows |
| `drugdb.drug` | CREATE TABLE (DDL only) | 0 at this stage |
| `drugdb.drug_identifier` | CREATE TABLE (DDL only) | 0 at this stage |
| `drugdb.drug_indication` | CREATE TABLE (DDL only) | 0 at this stage |
| `drugdb.drug_interaction` | CREATE TABLE (DDL only) | 0 at this stage |
| `drugdb.dosing_regimen` | CREATE TABLE (DDL only) | 0 at this stage |
| `drugdb.clinical_section` | CREATE TABLE (DDL only) | 0 at this stage |
| `drugdb.label_table` | CREATE TABLE (DDL only) | 0 at this stage |
| `drugdb.rag_chunk` | CREATE TABLE (DDL only) | 0 at this stage |
| `drugdb.indian_brand` | CREATE TABLE (DDL only) | 0 at this stage |
| `drugdb.indian_brand_ingredient` | CREATE TABLE (DDL only) | 0 at this stage |
| `drugdb.query_audit_log` | CREATE TABLE (DDL only) | 0 at this stage |
| (+ 9 other DDL-only tables) | CREATE TABLE (DDL only) | 0 at this stage |

### Tables Read From
| Table | Why |
|-------|-----|
| `public."DrugMasterLinkage"` | `export_durg_master_linkage.py` streams `combined_clean_jsonb` for inspection; `neo4j_populate.py` reads all child tables after pipeline completes |

### Table Schema (key columns)

**`umls.mrrel`** — UMLS concept relationship table:
| Column | Type | Notes |
|--------|------|-------|
| `cui1` | VARCHAR(8) | Source concept CUI |
| `rel` | VARCHAR(4) | Relationship type (RN, RB, SY, etc.) |
| `cui2` | VARCHAR(8) | Target concept CUI |
| `rela` | VARCHAR(100) | Specific relationship attribute |
| `sab` | VARCHAR(40) | Source vocabulary (RXNORM, SNOMEDCT_US, etc.) |
| `rui` | VARCHAR(11) | Relationship unique identifier |

**`drugdb.drug`** — Core drug formulation table (DDL created here, populated in Stage 01):
| Column | Type | Notes |
|--------|------|-------|
| `formulation_id` | TEXT | Primary key |
| `generic_name` | TEXT NOT NULL | Drug generic name from OpenFDA |
| `normalized_name` | TEXT NOT NULL | Salt-stripped INN for joining |
| `brand_names` | TEXT[] | Array of brand names |
| `drug_class` | TEXT[] | Pharmacologic/therapeutic classes |
| `has_openfda` | BOOLEAN | Source coverage flag |
| `has_rxnorm` | BOOLEAN | Source coverage flag |

**`drugdb.rag_chunk`** — Vector embedding table:
| Column | Type | Notes |
|--------|------|-------|
| `chunk_id` | TEXT | Primary key |
| `formulation_id` | TEXT | FK to drugdb.drug |
| `semantic_type` | TEXT NOT NULL | Chunk content classification |
| `text` | TEXT NOT NULL | Raw chunk text |
| `embedding` | vector(1024) | bge-large-en-v1.5 (1024 dims) |

## How Data Flows Into This Stage
This is the first stage. No upstream data is consumed. The UMLS MRREL.RRF file is loaded from the local filesystem at `/home/nathanivikas890_gmail_com/umls/MRREL.RRF`. The `"DrugMasterLinkage"` table (738,197 records) is a pre-existing source table in the `postgres` database and is not modified by this stage.

## Key Logic

**`postgres_schema.sql`**: Creates the `drugdb` schema and all tables in dependency order. Installs three extensions (`vector`, `pg_trgm`, `uuid-ossp`). Creates all indexes including GIN trigram indexes for fuzzy search. Defines the `resolve_drug(input_name TEXT)` PL/pgSQL function that attempts 6 resolution strategies in order: exact Indian brand → FDC ingredient decomposition → FDA generic exact → normalized (salt-stripped) generic → fuzzy Indian brand (trigram > 0.6) → fuzzy generic (trigram > 0.5). A commented-out IVFFlat index creation block at the bottom must be run manually after Stage 10 completes.

**`load_mrrel.py`**: Opens MRREL.RRF and streams rows into `umls.mrrel` using PostgreSQL `COPY FROM STDIN` via a `StringIO` buffer. Commits every 500,000 rows for visibility. Supports crash resume by querying `COUNT(*)` at startup and skipping that many lines from the file. After the data load, creates 6 indexes (`idx_mrrel_cui1`, `idx_mrrel_cui2`, `idx_mrrel_rel`, `idx_mrrel_rela`, `idx_mrrel_sab`, `idx_mrrel_rui`). Sets `work_mem = 256MB` and `maintenance_work_mem = 512MB` for the session.

**`neo4j_schema.cypher`**: Applies 7 uniqueness constraints on nodes (`Drug.formulation_id`, `Ingredient.name`, `Enzyme.name`, `Target.name`, `DrugClass.name`, `Indication.icd10`, `IndianBrand.(brand_name, manufacturer_india)`) and 9 property indexes using `IF NOT EXISTS` guards.

**`neo4j_populate.py`**: Run after all stages 01–11 are complete. Uses `asyncpg` (async Postgres) and `AsyncGraphDatabase` (async Neo4j) to create all graph nodes and edges in batches of 500. Steps: Drug nodes → Ingredient nodes + `CONTAINS_ACTIVE`/`CONTAINS_EXCIPIENT` edges → Enzyme nodes + `METABOLISED_BY`/`INHIBITS`/`INDUCES` edges (extracted by regex from `clinical_section` text) → Target nodes + `TARGETS` edges → DrugClass nodes + `BELONGS_TO_CLASS` edges → Indication nodes + `INDICATED_FOR` edges → bidirectional `INTERACTS_WITH` edges (partner resolved via drugbank_id → rxcui → normalized name → substring match) → `CONTRAINDICATED_WITH` edges → `ALTERNATIVE_TO` derived edges (shared class + indication) → IndianBrand nodes + `BRAND_OF`/`BRAND_OF_INGREDIENT` edges. Reads connection details from `DATABASE_URL`, `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` env vars.

**`export_durg_master_linkage.py`**: Utility-only. Streams all rows from `"DrugMasterLinkage".combined_clean_jsonb` via a server-side cursor and writes pretty-printed JSON to a timestamped text file. Supports `--host`, `--port`, `--dbname`, `--user`, `--password`, `--output-dir`, `--batch-size` arguments.

## Checkpoint / Recovery
`load_mrrel.py` is resume-safe. At startup it runs `SELECT COUNT(*) FROM umls.mrrel` and skips that many input lines before resuming the `COPY` loop. If killed mid-load, simply re-run the script with the same arguments.

## How to Run

```bash
# Step 1: Create PostgreSQL schema
psql -h $DB_HOST -U postgres -d postgres -f pipeline/00_setup/sql/postgres_schema.sql

# Step 2: Load UMLS MRREL
python pipeline/00_setup/load_mrrel.py

# Step 3: Apply Neo4j constraints and indexes
cypher-shell -u neo4j -p $NEO4J_PASSWORD -f pipeline/00_setup/sql/neo4j_schema.cypher

# Step 4 (optional): Export DrugMasterLinkage for inspection
python pipeline/00_setup/export_durg_master_linkage.py \
    --host $DB_HOST \
    --dbname postgres \
    --user postgres \
    --password $DB_PASSWORD \
    --output-dir /tmp/exports

# Step 5: Populate Neo4j graph (run AFTER stages 01-11 all complete)
export DATABASE_URL="postgresql://postgres:$DB_PASSWORD@$DB_HOST:5432/postgres"
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="$NEO4J_PASSWORD"
python pipeline/00_setup/neo4j_populate.py
```

## Expected Runtime
- `postgres_schema.sql`: under 30 seconds (DDL only)
- `load_mrrel.py`: 20–45 minutes (MRREL.RRF is ~3–4 GB, ~60M rows)
- `neo4j_schema.cypher`: under 30 seconds
- `neo4j_populate.py`: 30–90 minutes depending on data volume and Neo4j hardware

## Verification

```sql
-- Confirm UMLS loaded
SELECT COUNT(*) FROM umls.mrrel;
-- Expected: ~60 million rows

-- Confirm drugdb schema tables exist
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'drugdb'
ORDER BY table_name;

-- Confirm extensions
SELECT extname FROM pg_extension WHERE extname IN ('vector', 'pg_trgm', 'uuid-ossp');

-- Sample MRREL rows
SELECT cui1, rel, cui2, rela, sab FROM umls.mrrel LIMIT 5;

-- Confirm UMLS indexes
SELECT indexname FROM pg_indexes WHERE tablename = 'mrrel' AND schemaname = 'umls';
```

## Output / What the Next Stage Needs
- All `drugdb.*` tables exist with correct column definitions and indexes
- `umls.mrrel` is fully loaded and indexed (~60M rows)
- Neo4j constraints are in place (required before `neo4j_populate.py` can safely MERGE nodes)
- Stage 01 can begin inserting into `drugdb.drug`

## Troubleshooting
| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `FileNotFoundError` on MRREL.RRF | File not at hardcoded path `/home/nathanivikas890_gmail_com/umls/MRREL.RRF` | Download UMLS 2024AB release and place the file at that exact path |
| `extension "vector" does not exist` during schema creation | pgvector not installed on PostgreSQL 16 | Run `apt install postgresql-16-pgvector` on the DB server, then retry |
| `neo4j_populate.py` raises `ServiceUnavailable` | Neo4j not running or bolt port blocked | Start Neo4j (`neo4j start`) and verify `NEO4J_URI=bolt://localhost:7687` is reachable |
| MRREL load stalls with wrong row count on resume | MRREL.RRF was replaced between runs; existing row count exceeds new file line count | Truncate `umls.mrrel` (`TRUNCATE umls.mrrel;`) and restart load from scratch |
| `CREATE TABLE` fails with `schema "drugdb" does not exist` | Schema creation statement missing or failed silently | Run `CREATE SCHEMA IF NOT EXISTS drugdb;` manually before re-running the SQL file |


---

# Stage 01: Drug Table

## Purpose
Populates `drugdb.drug`, `drugdb.drug_identifier`, and `drugdb.drug_synonym_formulation` — the central drug registry that every downstream stage references via `formulation_id`. Each row in `drugdb.drug` represents one unique drug formulation extracted from the RxNorm entries inside `"DrugMasterLinkage".combined_clean_jsonb`. This stage also mirrors Drug nodes into Neo4j.

## Prerequisites
- Stage 00 must have completed (all `drugdb.*` tables exist)
- `public."DrugMasterLinkage"` must be populated (738,197 records)
- `drugdb.drug` table must exist (created by `sql/create_drug_table.sql` or Stage 00)
- Python packages: `psycopg2-binary`, `neo4j`

## Database Tables

### Tables Written To
| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `drugdb.drug` | INSERT (ON CONFLICT DO NOTHING) | ~88,983 rows |
| `drugdb.drug` | UPDATE (rxcui, rxnorm_generic_formulation columns) | ~88,983 rows |
| `drugdb.drug` | UPDATE (pharmacologic_class, therapeutic_class, mechanism_class columns) | ~88,983 rows |
| `drugdb.drug_synonym_formulation` | INSERT (ON CONFLICT DO NOTHING) | ~88,983 rows |
| `drugdb.drug_identifier` | INSERT (ON CONFLICT DO NOTHING) | ~400,000–600,000 rows |

### Tables Read From
| Table | Why |
|-------|-----|
| `public."DrugMasterLinkage"` | Source of all JSONB records; every script streams from here |
| `drugdb.drug` | `update_drug_rxnorm_columns.py` reads existing rows to recompute UUID match; `populate_drug_synonym_formulation.py` loads rxcui lookup |
| `drugdb.drug_identifier` | `populate_drug_identifier.py` reads back to avoid duplicates via ON CONFLICT |
| `drugdb.drug_synonym_formulation` | Checked to find uncovered formulation_ids on resume |

### Table Schema (key columns)

**`drugdb.drug`**:
| Column | Type | Notes |
|--------|------|-------|
| `formulation_id` | UUID | Primary key; deterministic UUID5 from `(master_linkage_id\|generic_formulation\|dosage_form)` |
| `master_linkage_id` | UUID | FK to `public."DrugMasterLinkage"` |
| `generic_name` | TEXT | From `combined_clean_jsonb → openfda → drug_info → generic_name` |
| `generic_formulation` | TEXT | RxNorm `generic_formulation` with dosage-form suffix stripped |
| `dosage_forms` | TEXT | From `rxnorm[].specific_dosage_form` |
| `rxcui` | VARCHAR(50) | From `rxnorm[].rxcui`; added by `update_drug_rxnorm_columns.py` |
| `rxnorm_generic_formulation` | TEXT | Raw (unstripped) `rxnorm[].generic_formulation`; added by same script |
| `pharmacologic_class` | TEXT[] | LLM-extracted classes; added by `populate_drug_nodes.py` |
| `therapeutic_class` | TEXT[] | LLM-extracted classes |
| `mechanism_class` | TEXT[] | LLM-extracted classes |
| `drug_class_source` | TEXT | Set to `'llm'` after Stage 05 populates classes |

**`drugdb.drug_synonym_formulation`**:
| Column | Type | Notes |
|--------|------|-------|
| `id` | SERIAL | Primary key |
| `formulation_id` | UUID | FK to `drugdb.drug`, UNIQUE |
| `synonyms` | TEXT[] | Array of RxNorm synonym names from `rxnorm[].synonyms` |

**`drugdb.drug_identifier`**:
| Column | Type | Notes |
|--------|------|-------|
| `id` | SERIAL | Primary key |
| `formulation_id` | TEXT | FK to `drugdb.drug` |
| `id_type` | TEXT | One of: `rxcui`, `ndc_product`, `ndc_package`, `unii`, `drugbank`, `upc`, `spl_id`, `spl_set_id`, `application_number` |
| `id_value` | TEXT | The identifier value |

## How Data Flows Into This Stage
The source of truth is `public."DrugMasterLinkage".combined_clean_jsonb`. Each record is a merged JSON blob containing `openfda`, `rxnorm`, `dailymed`, and `drugbank` sub-objects. The `run.py` script walks `combined_clean_jsonb → rxnorm[]` and produces one `drugdb.drug` row per RxNorm entry after stripping the dosage-form suffix (e.g. `"quetiapine 100 MG Oral Tablet"` becomes `"quetiapine 100 MG"`, `dosage_forms = "Oral Tablet"`).

The `formulation_id` is a deterministic `uuid.uuid5(NAMESPACE_OID, f"{master_linkage_id}|{generic_formulation}|{dosage_form}")`, ensuring idempotent re-runs.

## Key Logic

**`run.py` (populate_drug_table.py)**: Opens two connections (read_conn with named server-side cursor for streaming; write_conn for batch commits). For each `DrugMasterLinkage` record, calls `extract_rows_from_record()` which iterates `rxnorm[]`, strips the dosage-form suffix via a 50+ entry lookup map (`_DOSAGE_FORM_SUFFIX_MAP`), and builds `DrugRow` dataclass instances. Batches of 1,000 rows are committed. Supports `--dry-run`, `--batch-size`, `--log-file`, `--verbose`.

**`update_drug_rxnorm_columns.py`**: Adds `rxnorm_generic_formulation` (TEXT) and `rxcui` (VARCHAR(50)) columns to `drugdb.drug`, then re-streams `DrugMasterLinkage` and recomputes the same UUID5 seed to find the matching `formulation_id`. Updates only rows where those columns are NULL. Supports `--skip-ddl` and `--verify` flags.

**`populate_drug_synonym_formulation.py`**: Loads an in-memory dict `rxcui → [formulation_id]` for all uncovered formulations. Streams only the `DrugMasterLinkage` records whose children are not yet covered. For each RxNorm entry with synonyms, inserts one row per formulation_id. Uses `ON CONFLICT ON CONSTRAINT uq_drug_synonym_formulation_formulation_id DO NOTHING` for idempotency.

**`populate_drug_identifier.py`**: Streams a JOIN of `DrugMasterLinkage` × `drugdb.drug` grouped by `master_linkage_id`. For each group, resolves the correct `formulation_id` for NDC/application_number using active ingredient strength matching (tries `active_ingredients[0].strength` → product imprint → single-formulation fallback). Extracts 8 identifier types: rxcui, ndc_product, ndc_package, unii, upc, spl_id, spl_set_id, application_number, drugbank.

**`populate_drug_nodes.py`**: Reads `drugdb.drug` columns including `pharmacologic_class`, `therapeutic_class`, `mechanism_class` via a server-side cursor and upserts each row as a Neo4j `Drug` node via `MERGE` with batch size 500. Verifies pre-run that the `Drug.formulation_id` uniqueness constraint exists (creates it if absent). Writes a failed-IDs file if any batches fail.

## Checkpoint / Recovery
No file-based checkpoint. All inserts use `ON CONFLICT DO NOTHING`, so the full stage can be re-run safely. `populate_drug_synonym_formulation.py` skips already-covered `formulation_ids` via a subquery. `populate_drug_identifier.py` uses `ON CONFLICT (formulation_id, id_type, id_value) DO NOTHING`.

## How to Run

```bash
# Step 1: Populate drugdb.drug
python pipeline/01_drug_table/run.py --password $DB_PASSWORD

# Dry-run first (no writes):
python pipeline/01_drug_table/run.py --password $DB_PASSWORD --dry-run --verbose

# Step 2: Add rxcui and rxnorm_generic_formulation columns
python pipeline/01_drug_table/update_drug_rxnorm_columns.py --password $DB_PASSWORD --verify

# Step 3: Populate drug_synonym_formulation
python pipeline/01_drug_table/populate_drug_synonym_formulation.py --password $DB_PASSWORD --verify

# Step 4: Populate drug_identifier
python pipeline/01_drug_table/populate_drug_identifier.py --password $DB_PASSWORD

# Step 5: Mirror to Neo4j Drug nodes
python pipeline/01_drug_table/populate_drug_nodes.py
```

## Expected Runtime
- `run.py`: 5–15 minutes (738k source records → ~89k drug rows)
- `update_drug_rxnorm_columns.py`: 5–10 minutes
- `populate_drug_synonym_formulation.py`: 3–8 minutes
- `populate_drug_identifier.py`: 10–20 minutes
- `populate_drug_nodes.py`: 3–8 minutes (88,983 Neo4j nodes at 500/batch)

## Verification

```sql
-- Row count
SELECT COUNT(*) FROM drugdb.drug;
-- Expected: ~88,983

-- Sample rows
SELECT formulation_id, generic_name, generic_formulation, dosage_forms, rxcui
FROM drugdb.drug LIMIT 5;

-- RxCUI coverage
SELECT
    COUNT(*) AS total,
    COUNT(rxcui) AS has_rxcui,
    COUNT(*) - COUNT(rxcui) AS missing_rxcui
FROM drugdb.drug;

-- Identifier breakdown by type
SELECT id_type, COUNT(*) FROM drugdb.drug_identifier GROUP BY id_type ORDER BY id_type;

-- Synonym coverage
SELECT COUNT(*) FROM drugdb.drug_synonym_formulation;
```

## Output / What the Next Stage Needs
- `drugdb.drug` fully populated (~88,983 rows) with `formulation_id`, `generic_name`, `generic_formulation`, `dosage_forms`, `rxcui`, `master_linkage_id`
- `drugdb.drug_identifier` populated with all known external IDs
- `drugdb.drug_synonym_formulation` populated (one row per formulation with RxNorm synonyms)
- Stage 02 (`02_ingredient_nodes`) requires `drugdb.drug` to exist and be populated before it can insert `CONTAINS_ACTIVE` edges

## Troubleshooting
| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `run.py` exits with `ForeignKeyViolation` | `drugdb.drug` references `"DrugMasterLinkage"` via `master_linkage_id`; the FK was added but a record was deleted from linkage table | Run `ALTER TABLE drugdb.drug DROP CONSTRAINT drug_master_linkage_fkey` then retry |
| `update_drug_rxnorm_columns.py` shows 0 updates | `rxcui` column already populated from a previous run | Add `--skip-ddl` flag; check coverage with the verification query above |
| `populate_drug_identifier.py` shows low DrugBank match rate | `drugbank` entries in JSONB lack `indication` text (fallback heuristic fails) | Normal for OTC/non-clinical entries; acceptable if overall drugbank coverage > 80% |
| Neo4j `populate_drug_nodes.py` fails with `ConstraintViolation` | Duplicate `formulation_id` from a prior partial run | Re-run with `MERGE` (idempotent) — `populate_drug_nodes.py` uses MERGE, so re-runs are safe |
| Stage fails with `psycopg2.OperationalError: SSL SYSCALL error` | Network interruption to $DB_HOST | Add `connect_timeout=30` (already set) and re-run; `ON CONFLICT DO NOTHING` ensures no duplicate inserts |


---

# Stage 02: Ingredient Nodes

## Purpose
Creates the `drugdb.ingredients` table schema, populates `Ingredient` nodes in Neo4j from `drugdb.ingredients`, and creates `CONTAINS_ACTIVE` edges linking each `Drug` node to its ingredients via `drugdb.drug_ingredient_mapping`. Also back-fills `rxcui` on `drugdb.ingredients` by matching ingredient names from `"DrugMasterLinkage"` JSONB against the existing ingredient records using a 4-method cascade. This stage ensures the ingredient graph is fully connected before Stage 03 and Stage 04 consume it.

## Prerequisites
- Stage 00 must have completed (Neo4j constraints applied, `drugdb` schema exists)
- Stage 01 must have completed (`drugdb.drug` populated with ~88,983 rows, Neo4j Drug nodes exist)
- `drugdb.ingredients` table must be pre-populated (by an upstream DrugBank import step using `sql/ingredient_schema.sql`)
- `drugdb.drug_ingredient_mapping` table must be populated (by Stage 03)
- Neo4j `Drug.formulation_id` uniqueness constraint must exist
- Python packages: `psycopg2-binary`, `neo4j`

## Database Tables

### Tables Written To
| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `drugdb.ingredients` | CREATE (DDL via `sql/ingredient_schema.sql`) + UPDATE `rxcui` | ~30,000–50,000 rows updated |
| `drugdb.ingredient_synonyms` | CREATE (DDL) | 0 new rows at this stage |
| `drugdb.ingredient_interactions` | CREATE (DDL) | 0 new rows at this stage |
| Neo4j `:Ingredient` nodes | MERGE | ~30,000–50,000 nodes |
| Neo4j `CONTAINS_ACTIVE` edges | MERGE | ~92,000+ edges |

### Tables Read From
| Table | Why |
|-------|-----|
| `drugdb.ingredients` | `run.py` streams all rows to create Neo4j nodes; `update_ingredient_rxcui.py` loads into memory for matching |
| `drugdb.drug_ingredient_mapping` | `run.py` streams all rows to create Neo4j `CONTAINS_ACTIVE` edges |
| `drugdb.ingredient_synonyms` | `update_ingredient_rxcui.py` loads synonyms into memory as fallback matching keys |
| `public."DrugMasterLinkage"` | `update_ingredient_rxcui.py` extracts ingredient names + `ing_rxcui` via JSONB SQL |

### Table Schema (key columns)

**`drugdb.ingredients`**:
| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | Primary key, `gen_random_uuid()` |
| `drugbank_id` | VARCHAR(50) | DrugBank accession (e.g. DB00123) |
| `unii` | VARCHAR(50) | FDA UNII substance identifier |
| `rxcui` | VARCHAR(50) | RxNorm ingredient CUI; populated/updated by `update_ingredient_rxcui.py` |
| `name` | VARCHAR(255) | Canonical ingredient name |
| `type` | `ingredient_type` | ENUM: `active`, `inactive`, `both` |
| `indications` | TEXT | Free text indications |
| `general_function` | TEXT | DrugBank general function |
| `pharmacodynamics` | TEXT | DrugBank pharmacodynamics |

**`drugdb.ingredient_synonyms`**:
| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | FK to `drugdb.ingredients.id` (part of PK) |
| `synonym` | VARCHAR(500) | Synonym name (part of PK) |

**`drugdb.ingredient_interactions`**:
| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | FK to `drugdb.ingredients.id` (subject, part of PK) |
| `reacting_id` | UUID | FK to `drugdb.ingredients.id` (partner, part of PK) |
| `description` | TEXT | DrugBank interaction description text |

## How Data Flows Into This Stage
`update_ingredient_rxcui.py` extracts ingredient-rxcui pairs from `"DrugMasterLinkage"` using a JSONB unnest SQL query (`combined_clean_jsonb → rxnorm[] → ingredients[] → {name, ing_rxcui}`). It deduplicates by `(lower(name), ing_rxcui)` across all 738,197 source records. Each extracted ingredient is then matched to `drugdb.ingredients` using a 4-method cascade (see Key Logic).

`run.py` reads `drugdb.ingredients` and `drugdb.drug_ingredient_mapping` directly to create Neo4j graph entities.

## Key Logic

**`run.py` (populate_ingredient_nodes.py)**:
- **Step 1 — Ingredient nodes**: Streams all rows from `drugdb.ingredients` via a named server-side cursor, batching 500 rows at a time. For each batch, runs a `MERGE (i:Ingredient {ingredient_id: row.ingredient_id}) SET ...` Cypher query. Arrays `pharmacologic_class`, `therapeutic_class`, `mechanism_class` are passed as lists (empty list for NULL). Logs failed batches to a timestamped failed-IDs file.
- **Step 2 — CONTAINS_ACTIVE edges**: Streams all rows from `drugdb.drug_ingredient_mapping` (formulation_id, ingredient_id, mass, unit). Builds a `strength` string (`"{mass} {unit}"`). Runs `MERGE (d:Drug {formulation_id})-[r:CONTAINS_ACTIVE]->(i:Ingredient {ingredient_id}) SET r.strength = ..., r.mass = ..., r.unit = ...` in batches of 500. Reports `relationships_created` vs. already-existing counts from Cypher summary.
- **Pre-run verification**: Checks `drugdb.ingredients` and `drugdb.drug_ingredient_mapping` row counts; verifies or creates the `Ingredient.ingredient_id` uniqueness constraint.

**`update_ingredient_rxcui.py`**: Loads all ingredients and synonyms into in-memory dicts keyed by `lower(name)`. For each extracted `(name, ing_rxcui)` pair, tries:
- Method 1: Exact case-insensitive name match against `drugdb.ingredients.name`
- Method 2: Prefix match (DB name starts with source name) against ingredient names
- Method 3: Exact case-insensitive match against `drugdb.ingredient_synonyms.synonym`
- Method 4: Prefix match against synonyms

If a match is found and `rxcui` differs from the extracted value, it issues an UPDATE. If no match is found, it INSERTs a new ingredient row with the name and rxcui (leaving `drugbank_id` NULL as a skeleton record). After each match/update, the matched ingredient is removed from the in-memory index to avoid re-matching. Commits every `--batch-size` records (default 1000).

## Checkpoint / Recovery
No file-based checkpoint. `run.py` uses Neo4j `MERGE` which is idempotent — re-runs are safe. `update_ingredient_rxcui.py` only updates rows where `rxcui IS NULL` or the value differs, so re-runs are also safe.

## How to Run

```bash
# Step 1: Apply ingredient schema (if drugdb.ingredients doesn't exist yet)
psql -h $DB_HOST -U postgres -d postgres -f pipeline/02_ingredient_nodes/sql/ingredient_schema.sql

# Step 2: Back-fill rxcui on drugdb.ingredients
python pipeline/02_ingredient_nodes/update_ingredient_rxcui.py \
    --password $DB_PASSWORD \
    --batch-size 1000

# Dry-run first:
python pipeline/02_ingredient_nodes/update_ingredient_rxcui.py \
    --password $DB_PASSWORD --dry-run --verbose

# Step 3: Create Neo4j Ingredient nodes + CONTAINS_ACTIVE edges
# (run AFTER Stage 03 has populated drugdb.drug_ingredient_mapping)
python pipeline/02_ingredient_nodes/run.py
```

## Expected Runtime
- `ingredient_schema.sql`: under 5 seconds (DDL only)
- `update_ingredient_rxcui.py`: 5–20 minutes (depends on number of NULL rxcui rows)
- `run.py` (Neo4j population): 10–30 minutes (depends on ingredient and mapping counts)

## Verification

```sql
-- Total ingredients
SELECT COUNT(*) FROM drugdb.ingredients;

-- RxCUI coverage
SELECT
    COUNT(*) AS total,
    COUNT(rxcui) AS has_rxcui,
    COUNT(*) - COUNT(rxcui) AS missing_rxcui
FROM drugdb.ingredients;

-- Sample rows
SELECT id, name, rxcui, drugbank_id, unii FROM drugdb.ingredients LIMIT 5;

-- Ingredients still missing rxcui
SELECT COUNT(*) FROM drugdb.ingredients WHERE rxcui IS NULL;
```

```cypher
// Neo4j: Ingredient node count
MATCH (i:Ingredient) RETURN count(i);

// CONTAINS_ACTIVE edge count
MATCH ()-[r:CONTAINS_ACTIVE]->() RETURN count(r);

// Sample drug-ingredient connection
MATCH (d:Drug)-[r:CONTAINS_ACTIVE]->(i:Ingredient)
RETURN d.generic_name, i.name, r.strength LIMIT 5;
```

## Output / What the Next Stage Needs
- `drugdb.ingredients` has `rxcui` populated for all matched rows
- Neo4j `Ingredient` nodes exist with `ingredient_id`, `name`, `rxcui`, `drugbank_id`, `unii`
- Neo4j `CONTAINS_ACTIVE` edges exist (Drug → Ingredient) with `strength`, `mass`, `unit`
- Stage 03 (`03_drug_ingredient_mapping`) requires `drugdb.ingredients` to be fully populated before it can build `drug_ingredient_mapping`

## Troubleshooting
| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `run.py` exits with `RuntimeError: drug_ingredient_mapping is empty` when counting mappings | Stage 03 has not yet run | Run Stage 03 first, then return to run `run.py` for the Neo4j step |
| `update_ingredient_rxcui.py` inserts many new skeleton rows (high `inserted` count) | Many ingredient names in JSONB are not in the DrugBank-populated `drugdb.ingredients` | Expected for RxNorm-only ingredients; verify skeleton count is <20% of total |
| Neo4j `CONTAINS_ACTIVE` count much lower than `drug_ingredient_mapping` Postgres count | Some `Drug` or `Ingredient` nodes missing in Neo4j (prior failed batches) | Check the failed-IDs file at `/home/nathanivikas890_gmail_com/cdss/failed_ingredient_ids_*.txt` and replay those batches |
| `ConstraintError` on `MERGE (i:Ingredient {ingredient_id})` | `Ingredient.ingredient_id` constraint not yet created | Run `CREATE CONSTRAINT ingredient_id_unique IF NOT EXISTS FOR (i:Ingredient) REQUIRE i.ingredient_id IS UNIQUE` in Neo4j Browser |
| Very slow `update_ingredient_rxcui.py` with O(n²) prefix scan | Large number of ingredients in DB; Method 2/4 prefix scans are linear | Expected behavior; if total ingredients > 100k, consider skipping Methods 2 and 4 with a custom run that only uses exact matching |


---

# Stage 03: Drug-Ingredient Mapping

## Purpose
Populates `drugdb.drug_ingredient_mapping`, a many-to-many join table linking each drug formulation to its active ingredients with quantitative strength data (`mass`, `unit`). The stage runs in multiple passes: Pass 1 uses O(1) RxCUI-based exact matching (primary path); Pass 2 falls back to ingredient synonyms for anything missed; Pass 3 runs fuzzy deduplication to fill `drugbank_id` on skeleton ingredient rows; Pass 4 applies the fuzzy results to `drugdb.indian_brand_ingredient`. A final `fix_missing_35.py` patches any ingredient rows that were matched in log output but still have `drugbank_id IS NULL`.

Scripts in order:
1. `run.py` — RxCUI-exact first pass, creates and populates `drugdb.drug_ingredient_mapping`
2. `second_pass_ingredient_mapping.py` — synonym-based second pass for ingredients missed in Pass 1
3. `run_pass2.py` — placeholder (empty, 1 line); second-pass logic is in `second_pass_ingredient_mapping.py`
4. `fuzzy_ingredient_dedup.py` — fuzzy-matches skeleton ingredients (rxcui set, drugbank_id NULL) against DrugBank rows; Phase 1 analysis + Phase 2 apply
5. `apply_fuzzy_matches.py` — reads `fuzzy_match_results.json` and applies tier 5.1–5.4 matches to `drugdb.indian_brand_ingredient`
6. `fix_missing_35.py` — parses `logs/indian_brand_drugbank.log`, cross-references against still-NULL `drugdb.indian_brand_ingredient` rows, applies the voted drugbank_id

## Prerequisites
- Stage 00 must have completed (`drugdb` schema exists)
- Stage 01 must have completed (`drugdb.drug` populated with ~88,983 rows, all rows have `rxcui`)
- Stage 02 must have completed (`drugdb.ingredients` populated, `update_ingredient_rxcui.py` run)
- `drugdb.drug.rxcui` must be 100% populated (verified by `run.py` at startup)
- Python packages: `psycopg2-binary`, `fuzzywuzzy`, `python-Levenshtein`

## Database Tables

### Tables Written To
| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `drugdb.drug_ingredient_mapping` | CREATE (DDL) + INSERT (ON CONFLICT DO NOTHING) | ~92,000+ rows |
| `drugdb.ingredients` | UPDATE `drugbank_id` (fuzzy_ingredient_dedup.py Phase 2) | Skeleton rows that fuzzy-matched |
| `drugdb.indian_brand_ingredient` | UPDATE `drugbank_id` (apply_fuzzy_matches.py + fix_missing_35.py) | ~559,000–580,000 rows updated across runs |

### Tables Read From
| Table | Why |
|-------|-----|
| `public."DrugMasterLinkage"` | `run.py` streams `combined_clean_jsonb → rxnorm[] → ingredients[]` for mass/unit; `second_pass_ingredient_mapping.py` extracts same via SQL CTE |
| `drugdb.drug` | `run.py` loads `rxcui → [formulation_id]` lookup dict (~10 MB) |
| `drugdb.ingredients` | `run.py` loads `name.lower() → ingredient_id` lookup dict; `fuzzy_ingredient_dedup.py` loads skeleton vs DrugBank rows |
| `drugdb.ingredient_synonyms` | `second_pass_ingredient_mapping.py` and `fuzzy_ingredient_dedup.py` load synonym → ingredient_id for fallback matching |
| `drugdb.drug_ingredient_mapping` | `run.py` skips already-covered formulation_ids on resume; `second_pass_ingredient_mapping.py` deduplicates against already-mapped pairs |
| `drugdb.indian_brand_ingredient` | `apply_fuzzy_matches.py` and `fix_missing_35.py` update `drugbank_id` WHERE NULL |

### Table Schema (key columns)

**`drugdb.drug_ingredient_mapping`**:
| Column | Type | Notes |
|--------|------|-------|
| `formulation_id` | UUID | FK to `drugdb.drug(formulation_id)`, part of PK |
| `ingredient_id` | UUID | FK to `drugdb.ingredients(id)`, part of PK |
| `mass` | NUMERIC | Ingredient strength amount (from `rxnorm[].ingredients[].scdc.mass`) |
| `unit` | VARCHAR(50) | Strength unit (from `rxnorm[].ingredients[].scdc.unit`) |

Indexes: `idx_dim_formulation_id` (formulation_id), `idx_dim_ingredient_id` (ingredient_id).

## How Data Flows Into This Stage
`run.py` builds two in-memory lookups at startup: `rxcui → [formulation_id]` from `drugdb.drug` (only uncovered formulations, O(1) per lookup) and `name.lower() → ingredient_id` from `drugdb.ingredients`. It then streams `public."DrugMasterLinkage"` via a named server-side cursor (itersize=1000), extracting `combined_clean_jsonb → rxnorm[].rxcui + rxnorm[].ingredients[].{name, scdc.mass, scdc.unit}`. For each entry, `rxcui` is looked up to get formulation_ids, ingredient name is looked up to get ingredient_id, and each `(formulation_id, ingredient_id, mass, unit)` tuple is batched for insert.

`second_pass_ingredient_mapping.py` re-runs the same JSONB extraction as a SQL CTE (`rxnorm_ingredients`), joining through `drugdb.drug` on `master_linkage_id + rxcui`, then resolves ingredient names through the synonym lookup instead of the direct name lookup.

## Key Logic

**`run.py` (populate_drug_ingredient_mapping.py)**:
- **Prerequisites check**: Verifies `drugdb.drug` has at least one row with `rxcui IS NOT NULL`; verifies `drugdb.ingredients` table exists and is non-empty. Aborts if either check fails.
- **Resume support**: The `rxcui → [formulation_id]` lookup is built using `NOT EXISTS (SELECT 1 FROM drugdb.drug_ingredient_mapping WHERE formulation_id = d.formulation_id)`. Already-covered formulations are automatically excluded.
- **Batch insert**: Uses `execute_values` with `template="(%s::uuid, %s::uuid, %s, %s)"` and `page_size=10000`; commits every `--batch-size` rows (default 50,000). ON CONFLICT DO NOTHING ensures full idempotency.
- **Match rate alerts**: Every 5,000 source records, logs progress with rxcui match rate and ingredient match rate. Emits `WARNING ALERT` if rxcui rate drops below 95% or ingredient rate below 80%.
- **`--verify` flag**: After ETL, runs a summary query showing total rows, coverage percentage, ingredient count distribution per formulation, and the first 10 formulations with no ingredients.
- CLI: `--host`, `--dbname`, `--user`, `--password` (required), `--port`, `--dry-run`, `--batch-size` (default 50000), `--limit`, `--log-file`, `--verbose`, `--verify`

**`second_pass_ingredient_mapping.py`**:
- Hardcoded DB config: `host=$DB_HOST`, `dbname=postgres`.
- Loads `direct_lookup` (name.lower() → id) and `synonym_lookup` (synonym.lower() → id, excluding synonyms for rows without drugbank_id). Loads `already_mapped` set of (formulation_id, ingredient_id) pairs.
- Streams via a named server-side cursor (itersize=2000) using a SQL CTE that joins DrugMasterLinkage → drugdb.drug. For each ingredient name not in `direct_lookup` (already handled in Pass 1), tries `synonym_lookup`.
- Uses two-connection pattern: read_conn (autocommit=False) + write_conn; commits every 1,000 rows.
- Saves stats to `logs/drug_ingredient_mapping_second_pass.json` and `logs/drug_ingredient_mapping_second_pass.log`.
- CLI: `--dry-run` (default), `--execute` (to write to DB).

**`fuzzy_ingredient_dedup.py`**:
- Loads SET A (skeleton rows: `rxcui IS NOT NULL AND drugbank_id IS NULL`) and SET B (DrugBank rows: `drugbank_id IS NOT NULL`).
- Matching levels:
  - Level 1: Exact name match (case-insensitive)
  - Level 2: Synonym match (against synonyms of DrugBank rows only)
  - Level 3: `fuzz.token_sort_ratio` score ≥ 90 ("HIGH_CONFIDENCE")
  - Level 4: Score 75–89 ("MEDIUM_CONFIDENCE")
  - Level 5: Score 60–74 ("LOW_CONFIDENCE")
  - Level 0: No match (score < 60)
- Phase 1 prints the full match report; saves `logs/fuzzy_ingredient_match_results.json`.
- Phase 2 (auto-apply or with prompt): Updates `drugdb.ingredients SET drugbank_id = %s WHERE id = %s AND drugbank_id IS NULL` for Level 1+2+3 matches. Known false-positive exclusions: `"sodium nitrite"`, `"aminolevulinate"`. No rows are deleted.
- CLI: `--host`, `--port`, `--dbname`, `--user`, `--password` (required), `--phase1-only`, `--skip-confirm`

**`apply_fuzzy_matches.py`**:
- Reads `fuzzy_match_results.json` (hardcoded path, run from stage directory).
- Applies tiers 5.1–5.4 to `drugdb.indian_brand_ingredient SET drugbank_id = %s WHERE ingredient_name_raw = %s AND drugbank_id IS NULL`.
- Tier 5.5 is SKIPPED (124 false positives from single-letter DB entries producing 100% scores).
- Per-tier exclusions: Calcitonin (Salmon) in 5.1; Pegylated Interferon Alpha 2B in 5.2; Zinc pyrithione in 5.3; n-acetylcarnosine in 5.4.
- Interactive: shows the plan, prompts `Apply all updates? (yes/no)` before writing.

**`fix_missing_35.py`**:
- Reads `logs/indian_brand_drugbank.log`, extracts `Tier N match: raw='...' → drugbank_id=...` lines via regex.
- Groups by `ingredient_name_raw`, picks the most-common `drugbank_id` via `Counter.most_common(1)`.
- Cross-references against `drugdb.indian_brand_ingredient WHERE drugbank_id IS NULL`.
- Uses both exact-name and case/whitespace-normalized fuzzy matching to catch whitespace-different entries.
- Updates `drugdb.indian_brand_ingredient SET drugbank_id = %s WHERE ingredient_name_raw = %s AND drugbank_id IS NULL`.

## Checkpoint / Recovery
`run.py` uses `ON CONFLICT DO NOTHING` and resumes automatically by excluding already-covered formulation_ids from the rxcui lookup. No file-based checkpoint. `second_pass_ingredient_mapping.py` loads the `already_mapped` set at startup, so re-runs skip existing pairs. `fuzzy_ingredient_dedup.py` and `apply_fuzzy_matches.py` are idempotent because the UPDATE only fires `WHERE drugbank_id IS NULL`.

## How to Run

```bash
# Step 1: Primary RxCUI-based mapping (dry-run first)
python pipeline/03_drug_ingredient_mapping/run.py \
    --password $DB_PASSWORD --dry-run --limit 1000 --verbose

# Step 1 (full run):
python pipeline/03_drug_ingredient_mapping/run.py \
    --password $DB_PASSWORD --batch-size 50000 --verify

# Step 2: Synonym-based second pass (dry-run, then execute)
python pipeline/03_drug_ingredient_mapping/second_pass_ingredient_mapping.py --dry-run
python pipeline/03_drug_ingredient_mapping/second_pass_ingredient_mapping.py --execute

# Step 3: Fuzzy deduplication of skeleton ingredients (Phase 1 analysis only)
python pipeline/03_drug_ingredient_mapping/fuzzy_ingredient_dedup.py \
    --password $DB_PASSWORD --phase1-only

# Step 3 (Phase 2 apply, with confirmation prompt):
python pipeline/03_drug_ingredient_mapping/fuzzy_ingredient_dedup.py \
    --password $DB_PASSWORD

# Step 4: Apply fuzzy matches to indian_brand_ingredient
# (run from stage directory so fuzzy_match_results.json path resolves)
cd pipeline/03_drug_ingredient_mapping
python apply_fuzzy_matches.py

# Step 5: Fix any remaining NULL drugbank_id from log
python fix_missing_35.py
```

## Expected Runtime
- `run.py` (first pass): 5–15 minutes (738k source records, 50,000-row batch commits)
- `second_pass_ingredient_mapping.py`: 5–10 minutes (server-side cursor over full join)
- `fuzzy_ingredient_dedup.py` Phase 1: 2–10 minutes (O(n²) fuzz.token_sort_ratio over all DrugBank rows)
- `apply_fuzzy_matches.py`: under 1 minute
- `fix_missing_35.py`: under 1 minute

## Verification

```sql
-- Total mapping rows
SELECT COUNT(*) FROM drugdb.drug_ingredient_mapping;
-- Expected: ~92,000+

-- Coverage: formulations with at least one ingredient
SELECT
    COUNT(DISTINCT d.formulation_id) AS total_formulations,
    COUNT(DISTINCT dim.formulation_id) AS with_ingredients,
    ROUND(COUNT(DISTINCT dim.formulation_id) * 100.0 /
          COUNT(DISTINCT d.formulation_id), 2) AS coverage_pct
FROM drugdb.drug d
LEFT JOIN drugdb.drug_ingredient_mapping dim
       ON dim.formulation_id = d.formulation_id;

-- Ingredient count distribution
SELECT ingredient_count, COUNT(*) AS formulations
FROM (
    SELECT formulation_id, COUNT(*) AS ingredient_count
    FROM drugdb.drug_ingredient_mapping
    GROUP BY formulation_id
) t
GROUP BY ingredient_count
ORDER BY ingredient_count;

-- Sample mappings
SELECT d.generic_name, d.rxcui, i.name AS ingredient_name, dim.mass, dim.unit
FROM drugdb.drug_ingredient_mapping dim
JOIN drugdb.drug d ON d.formulation_id = dim.formulation_id
JOIN drugdb.ingredients i ON i.id = dim.ingredient_id
LIMIT 10;

-- Ingredients with filled drugbank_id after fuzzy dedup
SELECT COUNT(*) AS skeleton_rows_filled
FROM drugdb.ingredients
WHERE rxcui IS NOT NULL AND drugbank_id IS NOT NULL;

-- indian_brand_ingredient drugbank_id coverage
SELECT COUNT(*) AS total, COUNT(drugbank_id) AS with_id,
       ROUND(COUNT(drugbank_id) * 100.0 / COUNT(*), 2) AS pct
FROM drugdb.indian_brand_ingredient;
```

## Output / What the Next Stage Needs
- `drugdb.drug_ingredient_mapping` is fully populated with `(formulation_id, ingredient_id, mass, unit)` rows
- Stage 02 (`run.py` Neo4j step) requires this table to be populated before it can create `CONTAINS_ACTIVE` edges
- Stage 04 (`04_drug_interactions`) requires `drugdb.ingredients` to be fully resolved (drugbank_id set) so ingredient-level interactions can be attributed to formulations

## Troubleshooting
| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `run.py` exits with `Drug table has no rxcui values` | Stage 01 `update_drug_rxnorm_columns.py` has not run, or rxcui column is entirely NULL | Run `update_drug_rxnorm_columns.py --password $DB_PASSWORD --verify` from Stage 01 |
| `run.py` exits with `drugdb.ingredients table is empty!` | Stage 02 `ingredient_schema.sql` was not applied, or `update_ingredient_rxcui.py` has not run | Run Stage 02 fully before Stage 03 |
| Low ingredient match rate warning (below 80%) in `run.py` output | Many ingredient names in JSONB do not exactly match `drugdb.ingredients.name` (case differences, salt forms) | Run `second_pass_ingredient_mapping.py --execute` after the first pass to recover synonym matches |
| `second_pass_ingredient_mapping.py` shows high `unrecoverable` count | Ingredient names in JSONB have no canonical name or synonym in `drugdb.ingredients` | These are RxNorm-only ingredients not in DrugBank; expected for OTC drugs; run `fuzzy_ingredient_dedup.py` to resolve remaining skeleton rows |
| `fuzzy_ingredient_dedup.py` is very slow (>30 minutes) | O(n²) `fuzz.token_sort_ratio` scan across all DrugBank rows | Normal if DrugBank set >10,000 rows; run with `--phase1-only` first to estimate, then apply separately |
| `apply_fuzzy_matches.py` fails with `FileNotFoundError` on `fuzzy_match_results.json` | Script must be run from the stage directory | `cd pipeline/03_drug_ingredient_mapping && python apply_fuzzy_matches.py` |


---

# Stage 04: Drug Interactions

## Purpose
Resolves ingredient-level interaction pairs from `drugdb.ingredient_interactions` to formulation-level pairs and populates `drugdb.drug_interaction`. Also creates `INTERACTS_WITH` edges in Neo4j at the ingredient level. A second LLM enrichment pass uses the OpenRouter API (qwen/qwen-2.5-7b-instruct, 100 async workers) to classify each unique interaction description into a severity (contraindicated/major/moderate/minor/unknown) and extract a mechanism phrase, then mirrors A→B results to matching B→A rows.

Scripts in order:
1. `sql/drug_interaction_schema.sql` — DDL for `drugdb.drug_interaction`
2. `run.py` (populate_drug_interaction.py) — resolves ingredient interactions to formulation pairs; populates `drugdb.drug_interaction`
3. `populate_drug_interactions.py` — mirrors `drugdb.ingredient_interactions` to Neo4j `INTERACTS_WITH` edges (ingredient level)
4. `enrich_severity_mechanism.py` — async OpenRouter LLM enrichment of `drugdb.ingredient_interactions`; supports checkpoint/resume
5. `dump_cache_to_db.py` — recovery tool: reads `data/openrouter_content_cache.jsonl` and writes cached results to DB
6. `recover_from_openrouter_logs.py` — recovery tool: fetches prompt+completion per generation ID from OpenRouter API using a CSV export

## Prerequisites
- Stage 02 must have completed (`drugdb.ingredients` and `drugdb.ingredient_interactions` populated)
- Stage 03 must have completed (`drugdb.drug_ingredient_mapping` populated with ~92,000+ rows)
- Neo4j Ingredient nodes must exist (Stage 02 `run.py` Neo4j step)
- Python packages: `psycopg2-binary`, `openai` (for async OpenRouter client), `aiohttp` (for `recover_from_openrouter_logs.py`)
- OpenRouter API key required for `enrich_severity_mechanism.py`

## Database Tables

### Tables Written To
| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `drugdb.drug_interaction` | CREATE (DDL) + INSERT ON CONFLICT DO NOTHING | Derived from 2,910,556 ingredient pairs |
| `drugdb.ingredient_interactions` | UPDATE `severity`, `mechanism` | 2,910,556 rows (LLM enrichment) |
| Neo4j `INTERACTS_WITH` edges | MERGE | ~2,910,556 edges |

### Tables Read From
| Table | Why |
|-------|-----|
| `drugdb.ingredient_interactions` | `run.py` streams all ~2.9M rows via server-side cursor (itersize=10,000) to resolve subject/partner ingredient UUIDs |
| `drugdb.drug_ingredient_mapping` | `run.py` preloads into `ing_to_fids: Dict[str, List[str]]` (~92,570 rows, ~18 MB) for ingredient_id → formulation_id resolution |
| `drugdb.ingredients` | `dump_cache_to_db.py` and `recover_from_openrouter_logs.py` join to look up ingredient names |

### Table Schema (key columns)

**`drugdb.drug_interaction`**:
| Column | Type | Notes |
|--------|------|-------|
| `id` | SERIAL | Primary key |
| `interaction_id` | TEXT UNIQUE | `subject_formulation_id + '_' + partner_formulation_id` |
| `subject_formulation_id` | TEXT | FK to `drugdb.drug(formulation_id)` ON DELETE CASCADE |
| `partner_formulation_id` | TEXT | FK to `drugdb.drug(formulation_id)` ON DELETE CASCADE |
| `severity` | TEXT | DEFAULT `'unknown'`; LLM-enriched to: contraindicated, major, moderate, minor |
| `mechanism` | TEXT | LLM-extracted mechanism phrase (3–8 words); NULL before enrichment |
| `evidence_level` | TEXT | DEFAULT `'established'` (DrugBank interactions are curated) |
| `source_excerpt` | TEXT | Raw description from `drugdb.ingredient_interactions.description` |
| `created_at` | TIMESTAMPTZ | Auto-filled on insert |
| `updated_at` | TIMESTAMPTZ | Must be updated manually after LLM enrichment |

Indexes: `idx_dxi_subject` (subject_formulation_id), `idx_dxi_partner` (partner_formulation_id), `idx_dxi_severity` (severity), `idx_dxi_interaction_id` (interaction_id).

## How Data Flows Into This Stage
`run.py` preloads `ing_to_fids` (ingredient_id → [formulation_id]) from `drugdb.drug_ingredient_mapping` into memory at startup (~18 MB). It then streams `drugdb.ingredient_interactions` via a named server-side cursor in batches of 10,000. For each (subject_ingredient_id, partner_ingredient_id, description) row, both sides are looked up in `ing_to_fids`. For every cross-product of matched formulation pairs `(s_fid, p_fid)` where `s_fid != p_fid`, an `interaction_id = f"{s_fid}_{p_fid}"` is constructed and inserted with `ON CONFLICT (interaction_id) DO NOTHING`.

`enrich_severity_mechanism.py` enriches `drugdb.ingredient_interactions` (not `drugdb.drug_interaction`) so severity/mechanism lives on the ingredient interaction rows, which are the source of truth queried by the CDSS.

## Key Logic

**`run.py` (populate_drug_interaction.py)**:
- Two-connection pattern: `read_conn` (autocommit=False, named cursor) + `write_conn` (batch commits).
- Exits with `RuntimeError: drug_ingredient_mapping is empty` if `ing_to_fids` is empty — Stage 03 must run first.
- Uses `executemany` with `INSERT_SQL` and commits every `--batch-size` rows (default 5,000).
- Progress logged every 50,000 pairs with ETR estimate.
- `--dry-run` with `--limit` uses a targeted sample SQL that only returns pairs where both sides are already in `drug_ingredient_mapping`, ensuring useful hit-rate estimates.
- CLI: `--host`, `--port`, `--dbname`, `--user`, `--password` (required), `--dry-run`, `--batch-size` (default 5000), `--limit`, `--log-file` (default `logs/drug_interaction_population.log`)

**`populate_drug_interactions.py`**:
- Hardcoded config: PG_HOST=$DB_HOST, NEO4J_URI=bolt://localhost:7687, NEO4J_PASSWORD=$NEO4J_PASSWORD.
- BATCH_SIZE=1000, PROGRESS_EVERY=50,000, TOTAL_EXPECTED=2,910,556.
- CYPHER: `MATCH (a:Ingredient {ingredient_id: row.subject_ingredient_id}) MATCH (b:Ingredient {ingredient_id: row.partner_ingredient_id}) MERGE (a)-[r:INTERACTS_WITH]->(b) SET r.severity, r.mechanism, r.description`
- Pre-run verification: checks ingredient_interactions count, Neo4j Drug+Ingredient node counts, INTERACTS_WITH baseline, Ingredient.ingredient_id index (creates it if absent).
- Writes failed IDs to `/home/nathanivikas890_gmail_com/cdss/failed_interactions_{ts}.txt`.

**`enrich_severity_mechanism.py`**:
- Model: `qwen/qwen-2.5-7b-instruct` via `https://openrouter.ai/api/v1` (AsyncOpenAI client).
- Pricing: $0.04/1M input + $0.10/1M output + 5.5% platform fee; avg ~150 input / 40 output tokens.
- Total unique descriptions to classify: 1,455,278 (of 2,910,556 total rows, deduplicated by description).
- **Stage 1** (prefilter): SQL ILIKE regex classifies contraindicated/major/minor rows directly in Postgres without API calls. Conditions: `'%contraindicated%'` → contraindicated; `'%life-threatening%' OR '%serious adverse%' OR '%fatal%' OR '%severe%'` → major; `'%minor%' AND NOT major/severe` → minor.
- **Stage 2** (LLM): `asyncio.Queue(maxsize=2000)`. Producer streams unique descriptions (`FETCH_UNIQUE_SQL: SELECT DISTINCT ON (description)` where `severity = 'unknown'`). 100 async workers consume descriptions, call OpenRouter, parse `{"severity": "...", "mechanism": "..."}` JSON, flush to DB every 500 records per worker.
- UPDATE_SQL: `UPDATE drugdb.ingredient_interactions SET severity=%s, mechanism=%s WHERE id=%s::uuid AND reacting_id=%s::uuid`
- **Stage 3** (mirror): `MIRROR_SQL` propagates A→B results to B→A rows where target.severity='unknown'.
- **Stage 4** (verify): Reports severity distribution.
- Checkpoint: `logs/severity_checkpoint.json` (default) saves completed ID set; auto-loaded with `--resume`.
- CLI: `--openrouter-api-key` (required), `--db-password` (required), `--workers` (default 100), `--dry-run`, `--skip-prefilter`, `--skip-mirror`, `--max-retries` (default 5), `--log-file`, `--checkpoint`, `--resume`, `--limit`

**`dump_cache_to_db.py`**:
- Reads `data/openrouter_content_cache.jsonl` (default). Each line is a JSON object with `data.input.messages` and `data.output.completion`.
- Parses `"A: {subject_name} B: {partner_name}\n{description}"` format from user message.
- Builds `(subject_name.lower(), partner_name.lower()) → [(id, reacting_id)]` lookup from DB (only rows where severity='unknown').
- Commits 1,000 rows at a time. Skips rows with severity='unknown' in LLM output.
- CLI: `--db-password` (required), `--db-host`, `--db-port`, `--db-name`, `--db-user`, `--cache-file` (default `data/openrouter_content_cache.jsonl`), `--log-file`

**`recover_from_openrouter_logs.py`**:
- Reads a CSV export from `openrouter.ai/logs` (tab-delimited). Filters for CDSS app, non-cancelled, finish_reason=stop.
- Fetches prompt + completion via `GET https://openrouter.ai/api/v1/generation/content?id={gen_id}` (50 concurrent aiohttp requests). Caches results to `data/openrouter_content_cache.jsonl`.
- Matches generation results to DB rows via `(subject_name, partner_name)` lookup.
- CLI: `--openrouter-api-key` (required), `--db-password` (required), `--csv-file` (default `data/openrouter_generations.csv`), `--dry-run`, `--log-file`

## Checkpoint / Recovery
`enrich_severity_mechanism.py` saves a checkpoint to `logs/severity_checkpoint.json` every `CHECKPOINT_INTERVAL=10,000` rows. Use `--resume` to pick up from the checkpoint. If interrupted before the checkpoint was written, the mirror step (Stage 3) can fill in symmetric pairs. `dump_cache_to_db.py` provides an additional recovery path using the JSONL cache written during API calls. `recover_from_openrouter_logs.py` can fetch results directly from OpenRouter's generation history for any API calls that completed but were never written to DB.

## How to Run

```bash
# Step 1: Apply DDL
psql -h $DB_HOST -U postgres -d postgres \
    -f pipeline/04_drug_interactions/sql/drug_interaction_schema.sql

# Step 2: Populate drugdb.drug_interaction (dry-run first)
python pipeline/04_drug_interactions/run.py \
    --password $DB_PASSWORD --dry-run --limit 500

# Step 2 (full run):
python pipeline/04_drug_interactions/run.py \
    --password $DB_PASSWORD --batch-size 5000

# Step 3: Create Neo4j INTERACTS_WITH edges
python pipeline/04_drug_interactions/populate_drug_interactions.py

# Step 4: LLM enrichment (nohup for long runs)
nohup python pipeline/04_drug_interactions/enrich_severity_mechanism.py \
    --openrouter-api-key YOUR_KEY \
    --db-password $DB_PASSWORD \
    --workers 100 \
    --log-file logs/enrich_severity_mechanism.log \
    > logs/enrich_nohup.log 2>&1 &
echo "PID: $!"

# Monitor:
tail -f logs/enrich_severity_mechanism.log

# Step 4 (resume after interruption):
python pipeline/04_drug_interactions/enrich_severity_mechanism.py \
    --openrouter-api-key YOUR_KEY \
    --db-password $DB_PASSWORD \
    --resume --checkpoint logs/severity_checkpoint.json

# Step 5 (cache recovery, if needed):
python pipeline/04_drug_interactions/dump_cache_to_db.py \
    --db-password $DB_PASSWORD \
    --cache-file data/openrouter_content_cache.jsonl

# Step 6 (OpenRouter log recovery, if needed):
python pipeline/04_drug_interactions/recover_from_openrouter_logs.py \
    --openrouter-api-key YOUR_KEY \
    --db-password $DB_PASSWORD \
    --csv-file data/openrouter_generations.csv --dry-run
```

## Expected Runtime
- `drug_interaction_schema.sql`: under 5 seconds (DDL only)
- `run.py` (first pass): 20–60 minutes (2.9M ingredient pairs, 5,000-row batch commits)
- `populate_drug_interactions.py` (Neo4j): 30–90 minutes (2.9M MERGE ops)
- `enrich_severity_mechanism.py` (LLM): 4–12 hours (1,455,278 unique descriptions, 100 workers)
- `dump_cache_to_db.py`: 5–20 minutes

## Verification

```sql
-- Total drug_interaction rows
SELECT COUNT(*) FROM drugdb.drug_interaction;

-- Severity distribution
SELECT severity, COUNT(*) AS cnt
FROM drugdb.drug_interaction
GROUP BY severity
ORDER BY cnt DESC;

-- Enrichment coverage on ingredient_interactions
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE severity != 'unknown') AS classified,
    COUNT(*) FILTER (WHERE severity = 'unknown') AS remaining,
    COUNT(*) FILTER (WHERE mechanism IS NOT NULL) AS has_mechanism
FROM drugdb.ingredient_interactions;

-- Sample interactions
SELECT d1.generic_name AS subject, d2.generic_name AS partner,
       di.severity, di.mechanism, LEFT(di.source_excerpt, 100)
FROM drugdb.drug_interaction di
JOIN drugdb.drug d1 ON d1.formulation_id = di.subject_formulation_id
JOIN drugdb.drug d2 ON d2.formulation_id = di.partner_formulation_id
WHERE di.severity = 'major'
LIMIT 5;
```

```cypher
// Neo4j: INTERACTS_WITH edge count
MATCH ()-[r:INTERACTS_WITH]->() RETURN count(r);

// Sample: drug-drug interaction via 3-hop
MATCH (a:Drug)-[:CONTAINS_ACTIVE]->(i:Ingredient)
      -[r:INTERACTS_WITH]->(j:Ingredient)
      <-[:CONTAINS_ACTIVE]-(b:Drug)
RETURN a.generic_name, i.name, r.severity, j.name, b.generic_name
LIMIT 5;
```

## Output / What the Next Stage Needs
- `drugdb.drug_interaction` is fully populated with formulation-level pairs; `severity` and `mechanism` are enriched by LLM
- `drugdb.ingredient_interactions.severity` and `.mechanism` are set (used by Neo4j INTERACTS_WITH edges)
- Neo4j `INTERACTS_WITH` edges exist at the ingredient level with severity/mechanism properties
- Stage 05 and later stages do not depend on this stage directly

## Troubleshooting
| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `run.py` exits with `RuntimeError: drug_ingredient_mapping is empty` | Stage 03 has not run yet | Run `python pipeline/03_drug_ingredient_mapping/run.py` and verify mapping count before retrying |
| `enrich_severity_mechanism.py` shows 0 rows to process in Stage 2 | All rows already classified (prefilter covered everything, or prior run completed) | Run with `--dry-run` to check; if genuinely done, run `--skip-prefilter` to confirm |
| OpenRouter returns HTTP 429 (rate limit) | 100 workers exceeding API rate limit | Reduce `--workers` to 50 or 30; worker uses exponential backoff up to `--max-retries` (default 5) |
| `enrich_severity_mechanism.py` interrupted mid-run; checkpoint file not written | CHECKPOINT_INTERVAL=10,000 rows; last partial batch lost | Run `dump_cache_to_db.py` first to recover from JSONL cache, then `--resume` for remaining rows |
| `dump_cache_to_db.py` shows many `not_matched` entries | Ingredient names in prompt differ from `drugdb.ingredients.name` (case, salt form) | Expected for ~5–10% of rows; use `recover_from_openrouter_logs.py` with the CSV export for a secondary recovery pass |
| `populate_drug_interactions.py` fails with `Neo4j ServiceUnavailable` | Neo4j not running | Start Neo4j with `neo4j start` and verify `bolt://localhost:7687` is reachable |


---

# Stage 05: Drug Class

## Purpose
Extracts `pharmacologic_class`, `therapeutic_class`, and `mechanism_class` arrays for each drug in `drugdb.drug` using the RunPod A100 endpoint (Qwen2.5-7B-Instruct). Sets `drug_class_source = 'llm'` after update. Then creates Neo4j `DrugClass` nodes and `BELONGS_TO_CLASS` edges linking Drug nodes to their class names.

Scripts in order:
1. `run.py` — async producer/consumer pipeline calling RunPod A100; updates `drugdb.drug` with LLM-extracted class arrays; checkpointed
2. `populate_drugclass_nodes.py` — reads class arrays from `drugdb.drug`, creates Neo4j `DrugClass` nodes and `BELONGS_TO_CLASS` edges
3. `drug_class_test.py` — smoke test to verify a sample of drugs has class data after `run.py`

## Prerequisites
- Stage 01 must have completed (`drugdb.drug` populated with ~88,983 rows)
- `drugdb.drug` must have `pharmacologic_class`, `therapeutic_class`, `mechanism_class` (TEXT[]) and `drug_class_source` (TEXT) columns — added by `update_drug_rxnorm_columns.py` in Stage 01 or by the Stage 00 DDL
- RunPod endpoint `fahewj4m3wv52x` must be running with `Qwen/Qwen2.5-7B-Instruct` loaded
- Stage 01 `populate_drug_nodes.py` must have run so Neo4j Drug nodes exist
- Python packages: `aiohttp`, `asyncpg`, `psycopg2-binary`, `neo4j`

## Database Tables

### Tables Written To
| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `drugdb.drug` | UPDATE `pharmacologic_class`, `therapeutic_class`, `mechanism_class`, `drug_class_source='llm'` | ~47,000–88,983 rows |
| Neo4j `:DrugClass` nodes | MERGE | Distinct class names across all 3 arrays |
| Neo4j `BELONGS_TO_CLASS` edges | MERGE | All drug-class pairs |

### Tables Read From
| Table | Why |
|-------|-----|
| `drugdb.drug` | Producer streams `generic_name` + LLM text; skips rows where `drug_class_source='llm'` (resume logic) |
| `public."DrugMasterLinkage"` | Producer JOINs to get `openfda.clinical.mechanism_of_action.text` and `openfda.labeling_content.indications_and_usage.text` as LLM input |

### Table Schema (columns updated)

**`drugdb.drug`** (columns populated by this stage):
| Column | Type | Notes |
|--------|------|-------|
| `pharmacologic_class` | TEXT[] | LLM-extracted pharmacologic classes (e.g. `["HMG-CoA Reductase Inhibitor"]`) |
| `therapeutic_class` | TEXT[] | LLM-extracted therapeutic classes (e.g. `["Lipid Lowering Agent"]`) |
| `mechanism_class` | TEXT[] | LLM-extracted mechanism classes (e.g. `["Competitive Inhibitor of HMG-CoA Reductase"]`) |
| `drug_class_source` | TEXT | Set to `'llm'` after this stage completes |

## How Data Flows Into This Stage
The `run.py` producer uses a named server-side cursor to stream from `drugdb.drug JOIN public."DrugMasterLinkage"` using `SELECT DISTINCT ON (d.generic_name)`. The mechanism text and indication text come from `combined_clean_jsonb → openfda → clinical → mechanism_of_action → text` and `combined_clean_jsonb → openfda → labeling_content → indications_and_usage → text`. Each row is enqueued into a bounded `asyncio.Queue(maxsize=50)` for consumers to process. Rows where any drug with the same `generic_name` already has `drug_class_source='llm'` are skipped.

`populate_drugclass_nodes.py` reads back the populated class arrays via `UNNEST` SQL and drives two Neo4j batching steps: first creating DrugClass nodes, then creating BELONGS_TO_CLASS relationships.

## Key Logic

**`run.py`**:
- RunPod endpoint: `https://api.runpod.ai/v2/fahewj4m3wv52x/openai/v1/chat/completions`, model `Qwen/Qwen2.5-7B-Instruct`
- MAX_WORKERS=5, BATCH_SIZE=100, STREAM_CHUNK=500, MAX_RETRIES=1, RETRY_DELAY=5s
- System prompt instructs the model to return exactly `{"pharmacologic_class": [...], "therapeutic_class": [...], "mechanism_class": [...]}` with no markdown; strips ` ```json ``` ` wrappers if present.
- Producer (`drug_class_stream` named cursor) fetches in chunks of STREAM_CHUNK=500 and pushes individual drug dicts to `drug_queue`. Puts `MAX_WORKERS` sentinel `None` values at the end to signal consumers.
- Consumers (5 async workers) dequeue, call RunPod via aiohttp with 90-second timeout, and push results to `result_queue`.
- DB writer collects results, calls `batch_update_db` (synchronous, runs in thread pool executor via `loop.run_in_executor`) every `BATCH_SIZE=100` successes. UPDATE SQL: `UPDATE drugdb.drug SET pharmacologic_class=..., therapeutic_class=..., mechanism_class=..., drug_class_source='llm' FROM (VALUES %s) WHERE d.generic_name = v.generic_name`.
- Checkpoint: `~/cdss/drug_class_checkpoint.json` stores `completed` (set of `generic_name`) and `failed` sets. Saved every BATCH_SIZE batch. Log file: `~/cdss/drug_class_extraction.log`.
- No CLI arguments; config is hardcoded. To change workers or batch size, edit constants at top of file.

**`populate_drugclass_nodes.py`**:
- Hardcoded config: PG_HOST=$DB_HOST, NEO4J_URI=bolt://localhost:7687, NEO4J_PASSWORD=$NEO4J_PASSWORD.
- BATCH_SIZE=500, PROGRESS_EVERY=10,000.
- **Step 1**: Runs `SQL_DISTINCT_CLASSES` (UNION of UNNEST for all 3 class arrays) → builds list of unique class names → MERGEs `(c:DrugClass {name: row.name})` in batches of 500.
- **Step 2**: Uses named server-side cursor over `SQL_DRUG_CLASS_PAIRS` (UNION ALL of UNNEST for all 3 arrays with class_type label) → MERGEs `(d:Drug {formulation_id})-[r:BELONGS_TO_CLASS]->(c:DrugClass {name}) SET r.type = row.class_type`.
- Pre-run check: verifies `DrugClass.name` uniqueness constraint; creates it if absent.
- Post-run verification: checks Neo4j DrugClass count vs Postgres DISTINCT count, logs BELONGS_TO_CLASS breakdown by class_type (pharmacologic/therapeutic/mechanism).
- Writes failed batch formulation_ids to `/home/nathanivikas890_gmail_com/cdss/failed_drugclass_{ts}.txt`.

## Checkpoint / Recovery
`run.py` loads the checkpoint at startup and skips any `generic_name` already in `completed_set`. If interrupted, re-run `run.py` — it automatically resumes from the checkpoint. `populate_drugclass_nodes.py` uses Neo4j `MERGE`, so it is fully idempotent and safe to re-run.

## How to Run

```bash
# Step 1: Run LLM drug class extraction (RunPod must be running)
# Ensure checkpoint file does not exist for a fresh run, or leave it for resume
python pipeline/05_drug_class/run.py

# Monitor progress:
tail -f ~/cdss/drug_class_extraction.log

# Verify checkpoint status:
python3 -c "
import json
data = json.load(open('/home/nathanivikas890_gmail_com/cdss/drug_class_checkpoint.json'))
print('completed:', data['total_completed'], 'failed:', data['total_failed'])
"

# Step 2: Create Neo4j DrugClass nodes + BELONGS_TO_CLASS edges
python pipeline/05_drug_class/populate_drugclass_nodes.py
```

## Expected Runtime
- `run.py`: 2–8 hours (depends on RunPod endpoint availability and queue depth; 5 workers, 100-drug batch commits)
- `populate_drugclass_nodes.py`: 5–15 minutes (two passes: nodes then relationships)

## Verification

```sql
-- Drug class coverage
SELECT
    COUNT(*) AS total_drugs,
    COUNT(*) FILTER (WHERE drug_class_source = 'llm') AS llm_classified,
    COUNT(*) FILTER (WHERE pharmacologic_class IS NOT NULL) AS has_pharmacologic,
    COUNT(*) FILTER (WHERE therapeutic_class IS NOT NULL) AS has_therapeutic,
    COUNT(*) FILTER (WHERE mechanism_class IS NOT NULL) AS has_mechanism
FROM drugdb.drug;

-- Sample class assignments
SELECT generic_name, pharmacologic_class, therapeutic_class, mechanism_class
FROM drugdb.drug
WHERE drug_class_source = 'llm'
LIMIT 5;

-- Distinct class count per type
SELECT COUNT(DISTINCT c) AS pharmacologic_count
FROM drugdb.drug, UNNEST(pharmacologic_class) AS c;
```

```cypher
// Neo4j DrugClass node count
MATCH (c:DrugClass) RETURN count(c);

// BELONGS_TO_CLASS edge count by type
MATCH ()-[r:BELONGS_TO_CLASS]->()
RETURN r.type AS class_type, count(r) AS cnt
ORDER BY cnt DESC;

// Sample: drugs with all 3 class types
MATCH (d:Drug)-[r:BELONGS_TO_CLASS]->(c:DrugClass)
WITH d, collect({type: r.type, class: c.name}) AS classes,
     collect(DISTINCT r.type) AS types
WHERE size(types) = 3
RETURN d.generic_name, classes LIMIT 3;
```

## Output / What the Next Stage Needs
- `drugdb.drug.pharmacologic_class`, `.therapeutic_class`, `.mechanism_class` arrays are populated; `drug_class_source = 'llm'`
- Neo4j `DrugClass` nodes and `BELONGS_TO_CLASS` edges exist
- Stage 07 (`07_indications`) uses the LLM-enriched drug context for indication extraction
- Stage 00 `neo4j_populate.py` (final step) reads these columns to create `BELONGS_TO_CLASS` edges in the final graph

## Troubleshooting
| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `run.py` shows `HTTP 502` or `aiohttp.ClientConnectorError` | RunPod endpoint not running or pod was terminated | Restart the RunPod pod with `Qwen/Qwen2.5-7B-Instruct` loaded; verify endpoint ID `fahewj4m3wv52x` is active in RunPod console |
| Very high `failed` count in checkpoint (>20%) | LLM returning non-JSON or markdown-wrapped JSON | The parser strips ` ```json ``` ` wrappers, but some responses may still fail; check `drug_class_extraction.log` for parse errors; MAX_RETRIES=1 means failures are not retried |
| `populate_drugclass_nodes.py` shows 0 DrugClass nodes created | `drug_class_source='llm'` rows not yet populated — `run.py` has not run | Run `run.py` first and verify coverage with the SQL verification query above |
| BELONGS_TO_CLASS count in Neo4j is lower than expected | Some Drug nodes are missing in Neo4j (Stage 01 failed batches) | Check `/home/nathanivikas890_gmail_com/cdss/failed_drugclass_*.txt`; run Stage 01 `populate_drug_nodes.py` to fill missing Drug nodes |


---

# Stage 06: Dosing Regimen

## Purpose
Extracts structured dosing regimen rows from drug label text (OpenFDA dosage_and_administration section + DailyMed dosage_and_administration content) using the DeepSeek API (`deepseek-chat`). One row per unique combination of indication × age group × renal tier × hepatic tier × route. Uses a 4-stage async producer/consumer pipeline with 20 API workers. Supports checkpoint-based resume and dead-letter logging. Estimated cost: $0.000211 per drug.

Scripts in order:
1. `sql/dosing_regimen_schema.sql` — DDL for `drugdb.dosing_regimen`
2. `run.py` — main async extraction pipeline (4 stages: producer → api_worker → parser → db_writer)
3. `build_dosage_mappings.py` — utility: builds canonical dosage form mappings
4. `verify_dosage_cleanup.py` — post-run verification and cleanup of malformed rows

## Prerequisites
- Stage 01 must have completed (`drugdb.drug` populated with `master_linkage_id`)
- `public."DrugMasterLinkage"` must contain `combined_clean_jsonb.openfda.labeling_content.dosage_and_administration.text` and/or `combined_clean_jsonb.dailymed.labeling_content.dosage_and_administration.content`
- `DEEPSEEK_API_KEY` environment variable must be set
- Python packages: `aiohttp`, `asyncpg`, `jsonlines`

## Database Tables

### Tables Written To
| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `drugdb.dosing_regimen` | CREATE (DDL) + INSERT ON CONFLICT (regimen_id) DO NOTHING | Multiple rows per drug; total estimated ~150,000–300,000 rows |

### Tables Read From
| Table | Why |
|-------|-----|
| `public."DrugMasterLinkage"` | Producer streams `combined_clean_jsonb.openfda/dailymed.labeling_content.dosage_and_administration` text as LLM input |
| `drugdb.drug` | Producer JOINs on `master_linkage_id` to get `generic_name` and `formulation_id` |
| `drugdb.dosing_regimen` | Resume check: `already_done` CTE uses existing `formulation_id` to skip processed drugs |

### Table Schema (key columns)

**`drugdb.dosing_regimen`**:
| Column | Type | Notes |
|--------|------|-------|
| `id` | SERIAL | Primary key |
| `regimen_id` | TEXT UNIQUE | Deterministic ID: MD5(formulation_id\|indication\|age_group\|renal_function\|hepatic_function\|route\|sex\|pregnancy_status)[:16] |
| `formulation_id` | UUID | FK to `drugdb.drug(formulation_id)` ON DELETE CASCADE |
| `indication` | TEXT | Clinical indication for this regimen; NULL if general |
| `age_group` | TEXT | Controlled vocabulary: neonate\|infant\|pediatric\|adolescent\|adult\|geriatric\|any |
| `age_min_years` | NUMERIC | Minimum age (inclusive) |
| `age_max_years` | NUMERIC | Maximum age (inclusive) |
| `weight_min_kg` | NUMERIC | Minimum body weight (inclusive) |
| `weight_max_kg` | NUMERIC | Maximum body weight (inclusive) |
| `sex` | TEXT | any\|male\|female |
| `pregnancy_status` | TEXT | any\|pregnant\|not_pregnant\|lactating |
| `renal_function` | TEXT | any\|normal\|mild_impairment\|moderate_impairment\|severe_impairment\|esrd |
| `hepatic_function` | TEXT | any\|normal\|mild_impairment\|moderate_impairment\|severe_impairment |
| `route` | TEXT | Route of administration (oral, intravenous, subcutaneous, etc.) |
| `dose_amount` | TEXT | Human-readable dose string (e.g. "500 mg") or "CONTRAINDICATED" |
| `dose_value` | NUMERIC | Numeric dose value for computation |
| `dose_unit` | TEXT | Unit (mg, mcg, mg/kg, mg/m2, mL, units) |
| `dose_basis` | TEXT | fixed\|per_kg\|per_m2\|titrated |
| `frequency` | TEXT | QD\|BID\|TID\|QID\|q6h\|q8h\|q12h\|q48h\|q3w\|q4w\|weekly\|biweekly\|monthly\|once\|as_needed\|continuous |
| `duration` | TEXT | Treatment duration (e.g. "7 days", "indefinite") |
| `max_daily_dose` | TEXT | Maximum daily dose string |
| `administration_notes` | TEXT | Administration instructions from label |
| `adjustment_required_for` | TEXT[] | Conditions requiring dose adjustment |
| `created_at` | TIMESTAMPTZ | Auto-filled |

Indexes: `idx_dr_formulation` (formulation_id), `idx_dr_indication` (indication), `idx_dr_population` (formulation_id, age_group, renal_function, hepatic_function), `idx_dr_regimen_id` (regimen_id).

## How Data Flows Into This Stage
The producer uses `asyncpg` to query `FETCH_SQL` in `BATCH_SIZE=200` chunks with offset pagination. `FETCH_SQL` uses a `already_done` CTE to exclude drugs already in `drugdb.dosing_regimen` or listed in `drugdb.failed_drugs` with specific failure reasons. It extracts OpenFDA and DailyMed dosage text from `combined_clean_jsonb` directly via JSONB operators. Each row is enqueued into `fetch_q`.

API workers call DeepSeek with a long system prompt specifying controlled vocabulary and JSON schema, requesting a JSON array of regimen objects with `indication`, `population` (sub-object), `route`, `dose_amount`, `dose_value`, `dose_unit`, `dose_basis`, `frequency`, `duration`, `max_daily_dose`, `administration_notes`, `adjustment_required_for`. The parser validates and transforms each row. The DB writer inserts via `asyncpg` using `INSERT_SQL` with `ON CONFLICT (regimen_id) DO NOTHING`.

## Key Logic

**`run.py`**:
- API: `DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"`, `MODEL_NAME = "deepseek-chat"`, `DEEPSEEK_API_KEY` from env.
- Config: MAX_WORKERS=20, BATCH_SIZE=200, WRITE_FLUSH_SIZE=1000, TOTAL_DRUGS=47481, COST_PER_DRUG=$0.000211.
- File paths: `CHECKPOINT_FILE = ~/cdss/dosing_regimen_checkpoint.json`, `LOG_FILE = ~/cdss/logs/dosing_regimen_extraction.log`, `DEAD_LETTER_FILE = ~/cdss/logs/dosing_regimen_failed.log`, `RESULT_LOG_FILE = ~/cdss/logs/dosing_regimen_results.log`, `RESPONSE_CACHE_PATH = ~/cdss/logs/dosing_regimen_responses.jsonl`.
- **Checkpoint**: `load_checkpoint()` reads `{processed_ids: [...], offset: N, stats: {...}}`. `save_checkpoint()` writes atomically via `.json.tmp` → rename to avoid corruption on interrupt.
- **regimen_id**: MD5 hash (first 16 hex chars) of `formulation_id|indication|age_group|renal_function|hepatic_function|route|sex|pregnancy_status`. Guarantees deduplication across re-runs.
- **Parser**: `validate_and_parse()` handles two response shapes: JSON array (normal) or JSON object (picks first list value). Strips ` ```json ``` ` wrappers. Partial-recovery mode: if `json.JSONDecodeError`, uses `raw_decode` to salvage complete objects before the truncation point.
- **DB writer**: Runs in asyncio event loop; accumulates rows, calls `asyncpg Pool.executemany` when `WRITE_FLUSH_SIZE=1000` rows buffered. Result logger records every extracted row regardless of DB outcome.
- **Dead-letter logger**: On non-200 API response or DB insert failure, the `master_linkage_id` and error are appended to `DEAD_LETTER_FILE` for later reprocessing.
- System prompt caches in DeepSeek prefix cache (identical on every call), reducing cost and latency for requests 2+.

## Checkpoint / Recovery
`run.py` loads checkpoint at startup. Re-runs pick up at the stored offset, skipping already-processed `master_linkage_id`s. Because `FETCH_SQL` uses `already_done` CTE based on existing `dosing_regimen` rows, it is also safe to delete the checkpoint file and re-run (ON CONFLICT DO NOTHING ensures no duplicates). Check `~/cdss/logs/dosing_regimen_failed.log` for dead-letter entries and reprocess them separately.

## How to Run

```bash
# Step 1: Apply DDL (use with care — DROP TABLE CASCADE is in the SQL file)
psql -h $DB_HOST -U postgres -d postgres \
    -f pipeline/06_dosing_regimen/sql/dosing_regimen_schema.sql

# Step 2: Run extraction (requires DEEPSEEK_API_KEY)
export DEEPSEEK_API_KEY=your_key_here
nohup python pipeline/06_dosing_regimen/run.py \
    > ~/cdss/logs/dosing_regimen_nohup.log 2>&1 &
echo "PID: $!"

# Monitor progress:
tail -f ~/cdss/logs/dosing_regimen_extraction.log

# Step 3: Post-run verification
python pipeline/06_dosing_regimen/verify_dosage_cleanup.py

# Step 4 (optional): Build dosage form mappings
python pipeline/06_dosing_regimen/build_dosage_mappings.py
```

## Expected Runtime
- `dosing_regimen_schema.sql`: under 5 seconds
- `run.py` (full run, 47,481 drugs, 20 workers): 6–24 hours at DeepSeek rate limits; estimated total cost ~$10
- `verify_dosage_cleanup.py`: 5–15 minutes

## Verification

```sql
-- Total rows
SELECT COUNT(*) FROM drugdb.dosing_regimen;

-- Coverage: drugs with at least one regimen row
SELECT
    (SELECT COUNT(DISTINCT formulation_id) FROM drugdb.drug) AS total_drugs,
    COUNT(DISTINCT formulation_id) AS with_regimens
FROM drugdb.dosing_regimen;

-- Distribution by age group
SELECT age_group, COUNT(*) AS cnt
FROM drugdb.dosing_regimen
GROUP BY age_group ORDER BY cnt DESC;

-- Distribution by route
SELECT route, COUNT(*) AS cnt
FROM drugdb.dosing_regimen
GROUP BY route ORDER BY cnt DESC LIMIT 10;

-- CONTRAINDICATED rows
SELECT COUNT(*) FROM drugdb.dosing_regimen WHERE dose_amount = 'CONTRAINDICATED';

-- Renal impairment rows
SELECT renal_function, COUNT(*) AS cnt
FROM drugdb.dosing_regimen
GROUP BY renal_function ORDER BY cnt DESC;

-- Sample rows
SELECT generic_name, indication, age_group, route, dose_amount, frequency
FROM drugdb.dosing_regimen dr
JOIN drugdb.drug d ON d.formulation_id = dr.formulation_id
LIMIT 5;
```

## Output / What the Next Stage Needs
- `drugdb.dosing_regimen` is populated with structured dosing regimen rows
- Downstream CDSS query logic uses this table for patient-specific dosing recommendations filtered by `age_group`, `renal_function`, `hepatic_function`, `indication`

## Troubleshooting
| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `DEEPSEEK_API_KEY not set` or 401 errors | Environment variable not exported | `export DEEPSEEK_API_KEY=your_key` before running |
| High dead-letter count (>10% of drugs) | DeepSeek rate limit (HTTP 429) or transient network errors | MAX_RETRIES=2 with exponential backoff; check `DEAD_LETTER_FILE`; re-run (checkpoint auto-resumes) |
| `drugdb.dosing_regimen` count much lower than expected | Checkpoint offset advanced past available drugs on a partial run | Delete `~/cdss/dosing_regimen_checkpoint.json` and re-run; ON CONFLICT DO NOTHING prevents duplicates |
| `validate_and_parse` warnings in log (many JSON parse errors) | LLM returning markdown-wrapped or truncated JSON | Partial recovery via `raw_decode` salvages objects before truncation; accept minor loss rate |
| Very low row count per drug | Drugs with minimal label text (OTC, generic formulas) produce 0–1 rows | Expected; check `DEAD_LETTER_FILE` to distinguish from API failures |


---

# Stage 07: Indications

## Purpose
Extracts structured drug indication rows from OpenFDA and DailyMed label text (`indications_and_usage` sections) using an LLM. The default configuration uses the Groq API (`llama-3.3-70b-versatile`, CONCURRENCY=1); for production scale this stage is designed to run against a RunPod A100 endpoint with `Qwen/Qwen2.5-72B-Instruct-AWQ` (CONCURRENCY=32, ~38 drugs/sec, ~37 minutes, cost ~$1.10). Each extracted indication includes `term`, `icd10`, `snomed`, `mesh`, `population`, `line_of_therapy`, `combination_required`, `combination_agents`, and `source_excerpt`. Checkpoint is maintained in `drugdb.indication_extraction_log`.

Scripts:
1. `sql/indication_setup.sql` — pre-run DDL: creates `drugdb.drug_indication` and `drugdb.indication_extraction_log`
2. `run.py` — main async extraction pipeline (feed → CONCURRENCY workers → flush)
3. `indication_extraction.py` — earlier single-threaded extraction utility (lower quality; not recommended for production)

SQL:
- `sql/indication_setup.sql` — must be applied before `run.py`
- `sql/indication_verification.sql` — post-run verification queries

## Prerequisites

Run all 7 checks in `indication_setup.sql` before starting the RunPod job:

1. `pg_trgm` extension must be installed (`SELECT extname FROM pg_extension WHERE extname = 'pg_trgm'`)
2. `drugdb.drug_indication` must exist with correct columns
3. All 4 indexes on `drug_indication` must exist
4. `drugdb.indication_extraction_log` must exist
5. `drugdb.drug` must have rows
6. `public."DrugMasterLinkage"` must be accessible
7. `drugdb.drug_indication` should be empty (0 rows) before a fresh run

Environment variables: `DB_PASSWORD` (required), `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `VLLM_URL`, `VLLM_MODEL`.

For RunPod A100 run, also set `VLLM_URL=http://<pod-ip>:8000` and `VLLM_MODEL=Qwen/Qwen2.5-72B-Instruct-AWQ`. For Groq, set `VLLM_URL=https://api.groq.com/openai` and `VLLM_MODEL=llama-3.3-70b-versatile`.

Python packages: `aiohttp`, `psycopg2-binary`

## Database Tables

### Tables Written To

| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `drugdb.drug_indication` | INSERT ON CONFLICT DO NOTHING | Multiple per drug; estimated ~200,000–500,000 total |
| `drugdb.indication_extraction_log` | INSERT ON CONFLICT DO UPDATE | One row per formulation_id processed |

### Tables Read From

| Table | Why |
|-------|-----|
| `public."DrugMasterLinkage"` | `stream_drugs()` extracts `combined_clean_jsonb.openfda/dailymed.labeling_content.indications_and_usage.text/content/subsections` |
| `drugdb.drug` | JOINed on `master_linkage_id` to get `formulation_id`, `generic_name`, `generic_formulation` |
| `drugdb.indication_extraction_log` | `fetch_already_done()` loads set of `formulation_id` with `status='done'` to skip at startup |

### Table Schema (key columns)

**`drugdb.drug_indication`**:

| Column | Type | Notes |
|--------|------|-------|
| `id` | SERIAL | Primary key |
| `formulation_id` | UUID | FK to `drugdb.drug(formulation_id)` ON DELETE CASCADE |
| `term` | TEXT NOT NULL | Condition/disease name (max 500 chars) |
| `icd10` | TEXT | ICD-10-CM code or null |
| `snomed` | TEXT | SNOMED CT concept ID (numeric string) or null |
| `mesh` | TEXT | MeSH descriptor name or null |
| `population` | TEXT | Controlled: any\|adults\|pediatric\|geriatric\|neonates\|pediatric 2-13y\|pediatric 6-17y\|treatment-naive\|treatment-experienced |
| `line_of_therapy` | TEXT | first-line\|second-line\|adjunct\|salvage\|unspecified |
| `combination_required` | BOOLEAN | True if label says must be used WITH another drug |
| `combination_agents` | TEXT[] | Other drug names required |
| `source_section` | TEXT | Always `'indications_and_usage'` |
| `source_excerpt` | TEXT | Verbatim 1-2 sentence excerpt from label (max 1000 chars) |
| `source` | TEXT | openfda\|dailymed\|merged |
| `created_at` | TIMESTAMPTZ | Auto-filled |

Indexes: `idx_indication_formulation` (formulation_id), `idx_indication_icd10` (icd10), `idx_indication_snomed` (snomed), `idx_indication_term` (GIN trigram on term — requires pg_trgm).

**`drugdb.indication_extraction_log`**:

| Column | Type | Notes |
|--------|------|-------|
| `formulation_id` | UUID | Primary key |
| `status` | TEXT | `'done'` or `'error'` |
| `rows_inserted` | INTEGER | Number of indication rows inserted for this drug |
| `error_message` | TEXT | Error detail (json_parse_error, all_retries_failed, etc.) |
| `processed_at` | TIMESTAMPTZ | Processing timestamp |

## How Data Flows Into This Stage

`stream_drugs()` uses a named server-side cursor (`drug_stream_cursor`, itersize=500) to stream all drugs that have at least one non-null indication text from OpenFDA or DailyMed. `build_merged_text()` combines up to 3,000 chars of OpenFDA text + 1,500 chars of DailyMed text, filtered through `_keep_subsection()` (include keywords: treatment/indication/prophylaxis/use/approved; exclude: limitation/warning/safety/reference/study), capped at 4,000 chars total.

Each drug is placed into a bounded `asyncio.Queue(maxsize=200)`. Workers call the vLLM/Groq endpoint with CONCURRENCY concurrent tasks. Batches of 500 processed drugs trigger `bulk_insert_indications` + `bulk_insert_checkpoint` (both using `execute_values` with ON CONFLICT DO NOTHING / DO UPDATE).

## Key Logic

**`run.py`**:
- Default config: `VLLM_URL = "https://api.groq.com/openai"`, `VLLM_MODEL = "llama-3.3-70b-versatile"`, `CONCURRENCY = 1`. Override all three via environment variables for RunPod.
- For A100 production run: `VLLM_URL=http://<pod-ip>:8000`, `VLLM_MODEL=Qwen/Qwen2.5-72B-Instruct-AWQ`, `CONCURRENCY=32`.
- vLLM server start command (on RunPod): `python -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-72B-Instruct-AWQ --quantization awq_marlin --max-model-len 4096 --gpu-memory-utilization 0.90 --max-num-seqs 64 --enable-prefix-caching --host 0.0.0.0 --port 8000`
- Startup checks: verifies API health (`GET /v1/models`), checks model ID, verifies all 3 required DB tables exist, exits on any failure.
- `parse_llm_response()`: strips markdown fences, finds JSON array via regex `\[.*\]` (DOTALL), parses with `json.loads`. Returns `None` on failure.
- `rows_from_parsed()`: builds `IndicationRow` dataclasses; truncates `term` to 500 chars, `source_excerpt` to 1000 chars; coerces `combination_agents` to list.
- Checkpoint: `fetch_already_done()` loads `status='done'` formulation_ids at startup; processed drugs are added to `pending_checkpoints` and flushed every 500 drugs. `bulk_insert_checkpoint` uses ON CONFLICT DO UPDATE to overwrite error entries on retry.
- Log file: `indication_extraction.log` in the working directory.
- REQUEST_TIMEOUT=120s, MAX_RETRIES=3, RETRY_DELAY=2s. HTTP 500/503 triggers retry; HTTP 400 returns error immediately.

## Checkpoint / Recovery

At startup `run.py` queries `drugdb.indication_extraction_log WHERE status = 'done'` and skips those `formulation_id`s. To resume after interruption simply re-run the script — it will pick up from where it left off. Drugs with `status='error'` are NOT skipped and will be retried on re-run. To force a full re-run, truncate `drugdb.indication_extraction_log` (this does not affect `drugdb.drug_indication` since inserts use ON CONFLICT DO NOTHING).

## How to Run

```bash
# Step 1: Apply pre-run DDL (safe to re-run — all IF NOT EXISTS)
psql -h $DB_HOST -U postgres -d postgres \
    -f pipeline/07_indications/sql/indication_setup.sql

# Step 2a: Run with Groq (low CONCURRENCY, for testing)
export DB_PASSWORD=$DB_PASSWORD
export GROQ_API_KEY=your_groq_key
export VLLM_URL=https://api.groq.com/openai
export VLLM_MODEL=llama-3.3-70b-versatile
python pipeline/07_indications/run.py

# Step 2b: Run with RunPod A100 (production — set CONCURRENCY=32 in run.py)
# On RunPod pod, start vLLM first:
#   python -m vllm.entrypoints.openai.api_server \
#       --model Qwen/Qwen2.5-72B-Instruct-AWQ \
#       --quantization awq_marlin \
#       --max-model-len 4096 \
#       --gpu-memory-utilization 0.90 \
#       --max-num-seqs 64 \
#       --enable-prefix-caching \
#       --host 0.0.0.0 --port 8000
# Then from your machine:
export DB_PASSWORD=$DB_PASSWORD
export VLLM_URL=http://<runpod-pod-ip>:8000
export VLLM_MODEL=Qwen/Qwen2.5-72B-Instruct-AWQ
# Also edit CONCURRENCY=32 in run.py before launching
python pipeline/07_indications/run.py

# Step 3: Verify
psql -h $DB_HOST -U postgres -d postgres \
    -f pipeline/07_indications/sql/indication_verification.sql
```

## Expected Runtime

- `indication_setup.sql`: under 30 seconds
- `run.py` with Groq CONCURRENCY=1: many hours (not recommended for full run)
- `run.py` with A100 CONCURRENCY=32: ~84,973 drugs / 38 drugs/sec ≈ 37 minutes; cost ~$1.10 at $1.19/hr

## Verification

```sql
-- Total indication rows
SELECT COUNT(*) FROM drugdb.drug_indication;

-- Coverage: drugs with at least one indication
SELECT
    (SELECT COUNT(DISTINCT formulation_id) FROM drugdb.drug) AS total_drugs,
    COUNT(DISTINCT formulation_id) AS with_indications
FROM drugdb.drug_indication;

-- Checkpoint status breakdown
SELECT status, COUNT(*) AS cnt
FROM drugdb.indication_extraction_log
GROUP BY status;

-- Top 10 most common indications
SELECT term, COUNT(DISTINCT formulation_id) AS drug_count
FROM drugdb.drug_indication
GROUP BY term
ORDER BY drug_count DESC
LIMIT 10;

-- ICD-10 code coverage
SELECT
    COUNT(*) AS total,
    COUNT(icd10) AS has_icd10,
    COUNT(snomed) AS has_snomed
FROM drugdb.drug_indication;

-- Sample rows
SELECT d.generic_name, di.term, di.icd10, di.population, di.line_of_therapy
FROM drugdb.drug_indication di
JOIN drugdb.drug d ON d.formulation_id = di.formulation_id
LIMIT 5;
```

## Output / What the Next Stage Needs

- `drugdb.drug_indication` is populated with structured indication rows (term + ICD-10 + SNOMED + population)
- `drugdb.indication_extraction_log` records processing status for every formulation_id
- Stage 00 `neo4j_populate.py` (final step) reads this table to create `Indication` nodes and `INDICATED_FOR` edges

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `API not reachable` at startup | vLLM server not started or VLLM_URL wrong | For RunPod: verify the pod is running, port 8000 is open, and VLLM_URL is the correct proxy URL; for Groq: check GROQ_API_KEY and that VLLM_URL is exactly `https://api.groq.com/openai` |
| `DB_PASSWORD environment variable is not set` | DB_PASSWORD not exported | `export DB_PASSWORD=$DB_PASSWORD` before running |
| High error rate in `indication_extraction_log` (many `status='error'`) | LLM returning non-JSON or truncated responses (max_tokens=2048 hit) | Review `indication_extraction.log` for `json_parse_error` patterns; consider reducing CONCURRENCY to lower GPU memory pressure |
| Count in `drug_indication` grows very slowly | CONCURRENCY=1 (default) | Set `CONCURRENCY=32` in run.py and run against A100 endpoint for production throughput |
| `bulk_insert_checkpoint failed` error | Network disconnect or DB timeout during flush | Re-run the script — checkpoint insert uses ON CONFLICT DO UPDATE, so partial batches will be re-attempted and overwritten correctly |


---

# Stage 08: Clinical Sections

## Purpose
Extracts all narrative label sections from `public."DrugMasterLinkage".combined_clean_jsonb` and populates `drugdb.clinical_section` with one row per (formulation_id, section_key, source) triple. Both OpenFDA and DailyMed sub-trees are traversed across 9 parent keys. The DDL for the table is embedded inside `populate_clinical_section.py` and applied automatically at startup. A companion module `pass2_extractors.py` provides 9 structured extractors (regex and LLM-prompt builders) for downstream enrichment of the unified drug record.

Scripts:
1. `run.py` / `populate_clinical_section.py` — main extraction pipeline; Phase 1 (test mode, 2 records, no writes) → Phase 2 (full run, interactive confirmation)
2. `pass2_extractors.py` — second-pass extractor library: 3 deterministic (regex) extractors + 6 LLM prompt builders

## Prerequisites
- Stage 01 must have completed (`drugdb.drug` populated with `master_linkage_id`)
- `public."DrugMasterLinkage"` must be populated with `combined_clean_jsonb`
- Python packages: `psycopg2-binary`
- No external API keys required (`populate_clinical_section.py` is regex/parse only)

## Database Tables

### Tables Written To

| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `drugdb.clinical_section` | CREATE IF NOT EXISTS (DDL in script) + INSERT ON CONFLICT DO NOTHING | ~2.8M+ rows across all formulations |

### Tables Read From

| Table | Why |
|-------|-----|
| `public."DrugMasterLinkage"` | Server-side cursor streams `master_linkage_id` + `combined_clean_jsonb` for all records |
| `drugdb.drug` | `get_formulation_ids()` resolves each `master_linkage_id` → one or more `formulation_id` UUIDs |

### Table Schema (key columns)

**`drugdb.clinical_section`**:

| Column | Type | Notes |
|--------|------|-------|
| `id` | SERIAL | Primary key |
| `formulation_id` | UUID NOT NULL | FK to `drugdb.drug(formulation_id)` ON DELETE CASCADE |
| `section` | TEXT NOT NULL | Section key name (e.g. `indications_and_usage`, `warnings_and_cautions`) |
| `text` | TEXT | Narrative text; NULL for DailyMed rows where only subsections are present |
| `subsections` | JSONB | Default `'[]'`; DailyMed subsections as `[{subsection_id, title, text}]`; always `[]` for OpenFDA |
| `source` | TEXT | `openfda` or `dailymed` |
| `source_document_id` | TEXT | OpenFDA: `openfda.identification.set_id`; DailyMed: `dailymed.identification.drug_label.document_id` |

Unique constraint: `(formulation_id, section, source)` — prevents duplicate inserts on re-run.

Indexes: `idx_cs_formulation` (formulation_id), `idx_cs_section` (section).

## How Data Flows Into This Stage

`run_full_mode()` opens a server-side named cursor `linkage_stream` with `itersize=BATCH_SIZE` (5,000) over `public."DrugMasterLinkage"`. For each record, `get_formulation_ids()` queries `drugdb.drug WHERE master_linkage_id = %s` to get all matching formulation UUIDs. If none are found the record is counted in `total_warn_no_fid` and skipped.

For each DrugMasterLinkage record:
- `extract_openfda_sections()` walks `combined_clean_jsonb.openfda` across all 9 `PARENT_KEYS`, collecting each child `section_key → section_data` dict. Reads `section_data.get("text")`. Skips if text is empty (OpenFDA has no subsections). Sets `source_document_id` from `openfda.identification.set_id`.
- `extract_dailymed_sections()` does the same for `combined_clean_jsonb.dailymed`. Reads `section_data.get("content")` (not `"text"`). Also reads `section_data.get("subsections")` as a list of `{section_title, content}` objects. Skips only when BOTH content is empty AND no subsections exist. Sets `source_document_id` from `dailymed.identification.drug_label.document_id`.

The insert rows are built as `(fid, section_key, text, json.dumps(subsections), source, doc_id)` tuples, one per formulation_id × section. `batch_insert()` calls `psycopg2.extras.execute_values` with `page_size=INSERT_PAGE` (500) and `ON CONFLICT (formulation_id, section, source) DO NOTHING`. `conn_write.commit()` is called per DrugMasterLinkage record. The read connection (`conn_read`) is never committed during the stream, preserving the server-side cursor state.

## Key Logic

**`populate_clinical_section.py`** (also the content of `run.py`):

- **Config**: `BATCH_SIZE=5000` (DrugMasterLinkage records fetched per `fetchmany`), `INSERT_PAGE=500` (rows per `execute_values` page), `TEST_LIMIT=2`, `LOG_FILE="clinical_section_population.log"`.
- **`PARENT_KEYS`**: 9 keys traversed under both openfda and dailymed sub-trees: `safety`, `labeling_content`, `clinical`, `adverse_events`, `drug_interactions`, `patient_info`, `supply_storage`, `abuse_dependence`, `population_specific`.
- **`ensure_table()`**: Inline DDL creates `drugdb.clinical_section` with the full column set and both indexes using `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`. Runs at startup before any test mode.
- **`extract_openfda_sections()`**: For each `pk` in `PARENT_KEYS`, gets `openfda.get(pk)` dict. For each child `section_key → section_data`, reads `section_data.get("text")`. Skips if `text` is None. OpenFDA never produces subsections — the `subsections` field is always set to `[]`.
- **`extract_dailymed_sections()`**: Same parent key traversal on `dailymed`. Uses `section_data.get("content")` (not `"text"`). Normalizes subsections to `{subsection_id: "{section_key}_{i}", title: sub["section_title"], text: sub["content"]}`. Skips only when both `content` and `subsections` are absent.
- **`_clean_text()`**: Strips whitespace; returns `None` if result is empty.
- **Phase 1 — Test mode**: Fetches 2 records (`LIMIT 2`), calls both extractors without any DB writes. Prints drug name (from `openfda.drug_info.generic_name`), formulation count, section counts, and a per-section table showing source, section key, text length, subsection count, and doc_id. Prompts `Proceed with full run? (yes / no)`.
- **Phase 2 — Full run**: After confirmation, prompts `Truncate? (yes / no)`. If yes, runs `TRUNCATE TABLE drugdb.clinical_section RESTART IDENTITY`. Opens a second connection `conn_write` (autocommit=False) for all writes; `conn_read` (the first connection) holds the server-side cursor open. Progress is printed per batch as `Batch N | records_processed=... | rows_inserted=... | conflicts=...`. Final summary shows totals by source (`openfda`/`dailymed`) and breakdown by section key sorted by drug count.
- **Two-connection pattern**: `conn_read` is used only for the server-side streaming cursor; `conn_write` handles `get_formulation_ids()` lookups and all `batch_insert()` calls. Each record triggers `conn_write.commit()` on success or `conn_write.rollback()` on exception.
- **No CLI arguments**: All config is hardcoded. To change batch size or limit, edit constants at top of file.

**`pass2_extractors.py`** — second-pass extractor library (not called by `populate_clinical_section.py`; intended for a separate enrichment pipeline):

- **`extract_strengths(record)`** (deterministic, no LLM): Searches `rxnorm.clinical_formulations[].name` with regex `(\d+(?:\.\d+)?)\s*(MG|MCG|ML|MG/ML|MCG/ML|%)`. Writes `strength_value`, `strength_unit`, `strength_label` onto each clinical_formulation dict. Populates `record["structured_facts"]["available_strengths"]`. Also propagates strengths to `record["product"]["skus"]` when the SKU count matches the unique strength count.
- **`build_indication_extraction_prompt(record)`** (LLM): Builds prompt from `clinical.indications_and_usage.text`. Returns `{system, prompt, target_field, parse_instructions}` dict or `{prompt: None, reason: "no indications text"}` when section is absent. Target: `structured_facts.indications`.
- **`build_drug_class_extraction_prompt(record)`** (LLM): Uses `clinical.mechanism_of_action.text` + `drug.mechanism_of_action` + `clinical.indications_and_usage.text`. Returns pharmacologic/therapeutic/mechanism class arrays and ATC code. Target: `drug.drug_class + drug.atc_codes`.
- **`enrich_interactions_from_tables(record)`** (deterministic): Parses `label_tables` entries where `semantic_type` is `interaction` or `pharmacokinetics`. Extracts AUC/Cmax direction (↑/↓/↔) and percentage from table row cells using regex `([↑↓↔])\s*(\d+)%?\s*\(`. Sets `severity="contraindicated"` for partners found in `structured_facts.contraindications`. Returns `{extractor, enriched}` log.
- **`build_interaction_severity_prompt(record)`** (LLM): Batches up to 20 interactions still at `severity="unknown"`. Returns prompt with `{interaction_id, partner, management}` list. Target: `structured_facts.interactions[].severity + mechanism`.
- **`build_dosing_extraction_prompt(record)`** (LLM): Uses `clinical.dosage_and_administration.text`, subsections, and dosing tables from `label_tables`. Includes available strengths from `structured_facts`. Requests full structured regimen array with all population/dose/frequency fields.
- **`build_population_approval_prompt(record)`** (LLM): Reads `clinical.pediatric_use`, `clinical.geriatric_use`, `clinical.use_in_pregnancy`, `clinical.use_in_specific_populations` (up to 1,500 chars each). Returns status for 5 populations: pediatric, adolescent, geriatric, pregnant, lactating.
- **`extract_administration_timing(record)`** (deterministic): Scans text from `dosage_and_administration`, `drug_interactions`, `information_for_patients` sections using regex patterns for `with food/meal`, `empty/fasting stomach`, `without regard to food`, and drug separation hour requirements. Sets `food_requirement` and `drug_separations` in `structured_facts.administration_timing`. Propagates `administration_notes` to `structured_facts.dosing_regimens` entries.
- **`build_timing_llm_prompt(record)`** (LLM fallback): Skips if regex already set both `food_requirement` and `drug_separations`. Otherwise builds prompt from dosage and drug_interactions text.
- **`run_deterministic_extractors(record)`**: Orchestrator running `extract_strengths`, `enrich_interactions_from_tables`, `extract_administration_timing` in order. Returns list of logs.
- **`build_all_llm_prompts(record)`**: Calls all 6 `build_*_prompt` functions and returns only those where `prompt` is not None.

## Checkpoint / Recovery

`populate_clinical_section.py` has no file-based checkpoint. The `UNIQUE(formulation_id, section, source)` constraint and `ON CONFLICT DO NOTHING` make re-runs fully idempotent — already-inserted rows produce conflicts but no duplicates and no errors. To resume after interruption, simply re-run the script and confirm `yes` at both prompts. Answer `no` to the truncate prompt to preserve existing rows and skip already-processed (conflict) records. There is no automatic resume that skips already-processed `master_linkage_id`s at the query level; the script re-processes all records but inserts nothing where conflicts occur.

## How to Run

```bash
# Run extraction (interactive — confirms test output before full run)
python pipeline/08_clinical_sections/run.py
# or equivalently:
python pipeline/08_clinical_sections/populate_clinical_section.py

# The script will:
#   1. Create the table if it does not exist
#   2. Preview 2 records (Phase 1) and print section counts
#   3. Prompt "Proceed with full run? (yes / no)"
#   4. Prompt "Truncate? (yes / no)" — answer "no" to resume safely
#   5. Stream all DrugMasterLinkage records (Phase 2)

# Monitor progress in the log file:
tail -f clinical_section_population.log

# Run pass2 extractors on a sample unified record (standalone demo):
python pipeline/08_clinical_sections/pass2_extractors.py unified_sample.json
```

## Expected Runtime

- Phase 1 (test mode): under 5 seconds
- Phase 2 (full run, ~50,000 DrugMasterLinkage records, ~88,983 formulations): 45–90 minutes depending on row width and network latency to DB host

## Verification

```sql
-- Total rows inserted
SELECT COUNT(*) FROM drugdb.clinical_section;

-- Breakdown by source
SELECT source, COUNT(*) AS cnt
FROM drugdb.clinical_section
GROUP BY source;

-- Top sections by drug coverage
SELECT section, COUNT(DISTINCT formulation_id) AS drugs_with_section
FROM drugdb.clinical_section
GROUP BY section
ORDER BY drugs_with_section DESC
LIMIT 20;

-- Rows per source (openfda vs dailymed)
SELECT source, COUNT(DISTINCT formulation_id) AS formulations_covered
FROM drugdb.clinical_section
GROUP BY source;

-- Sections that have subsections (DailyMed only)
SELECT section, COUNT(*) AS rows_with_subsections
FROM drugdb.clinical_section
WHERE subsections != '[]'
GROUP BY section
ORDER BY rows_with_subsections DESC
LIMIT 10;

-- Records with no formulation match (warnings in log)
-- These cannot be queried directly; check the log file for:
-- "No formulation_id found for master_linkage_id=..."

-- Sample rows
SELECT d.generic_name, cs.section, cs.source,
       LEFT(cs.text, 100) AS text_preview,
       jsonb_array_length(cs.subsections) AS subsec_count
FROM drugdb.clinical_section cs
JOIN drugdb.drug d ON d.formulation_id = cs.formulation_id
LIMIT 5;
```

## Output / What the Next Stage Needs

- `drugdb.clinical_section` is populated with one row per (formulation_id, section_key, source) — all narrative label sections from both OpenFDA and DailyMed
- Stage 09 (`09_label_table`) reads `combined_clean_jsonb` independently for table extraction; it does not depend on `drugdb.clinical_section` but populates a companion table `drugdb.label_table`
- Stage 10 (`10_vector_embeddings`) reads `drugdb.clinical_section.text` to chunk and embed into `drugdb.rag_chunk`
- `pass2_extractors.py` is a library used by downstream enrichment pipelines that consume the unified drug record format; no direct dependency from the population script

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Script hangs at "Connecting to database" or exits with `Connection failed` | DB host $DB_HOST not reachable or `DB_CONFIG` credentials wrong | Verify network connectivity to $DB_HOST:5432; credentials are hardcoded in `DB_CONFIG` — update if changed |
| High `Warnings — no formulation match` count in final summary | `drugdb.drug` does not cover all `master_linkage_id`s in DrugMasterLinkage (Stage 01 partial run) | Run `python pipeline/01_drug/run.py --verify` to check coverage; re-run Stage 01 for missing formulations |
| `rows_inserted=0` for every batch after first run | All rows already exist (ON CONFLICT DO NOTHING absorbing all inserts) | Expected behavior on re-run — data is present; answer `yes` to Truncate prompt only if a fresh full re-extraction is needed |
| DailyMed sections missing (all `source='openfda'` rows only) | `combined_clean_jsonb.dailymed` is null or missing for all records | Verify Stage 00 merged DailyMed data: `SELECT COUNT(*) FROM public."DrugMasterLinkage" WHERE combined_clean_jsonb->'dailymed' IS NOT NULL` |
| `pass2_extractors.py` raises `FileNotFoundError` when run as `__main__` | No `unified_sample.json` file in working directory | Pass path explicitly: `python pass2_extractors.py /path/to/unified_record.json`; file must be a JSON object matching the unified record schema |


---

# Stage 09: Label Table

## Purpose

Extracts all structured HTML/SPL tables embedded in drug label JSONB (`combined_clean_jsonb`) from `public."DrugMasterLinkage"` and populates `drugdb.label_table`. Each DrugMasterLinkage record may resolve to multiple formulation_ids; one row is inserted per extracted table per formulation. OpenFDA tables are found under 6 known parent keys; DailyMed tables are found recursively anywhere a `table` key appears in the nested JSONB tree. Semantic typing maps section keys to `adverse_event`, `dosing`, `interaction`, `pharmacokinetics`, `clinical_study`, `contraindication`, or NULL for unmapped sections.

Scripts:

1. `run.py` / `populate_label_table.py` — main extraction pipeline with Phase 1 (test mode, 2 records, no writes) and Phase 2 (full run, interactive confirmation); CLI supports `--test-insert N`
2. `sql/` — no DDL files present; DDL is embedded in `ensure_schema_and_table()` inside the script

## Prerequisites

- Stage 01 must have completed (`drugdb.drug` populated with `master_linkage_id`)
- `public."DrugMasterLinkage"` must be populated with `combined_clean_jsonb`
- Python packages: `psycopg2-binary`
- No external API keys required

## Database Tables

### Tables Written To

| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `drugdb.label_table` | CREATE IF NOT EXISTS (DDL in script) + INSERT ON CONFLICT (formulation_id, table_id) DO NOTHING | ~510,000+ rows across all formulations |

### Tables Read From

| Table | Why |
|-------|-----|
| `public."DrugMasterLinkage"` | Server-side cursor `stream_linkage` streams `master_linkage_id` + `combined_clean_jsonb` |
| `drugdb.drug` | `get_formulation_ids()` resolves each `master_linkage_id` → list of `formulation_id` UUIDs |

### Table Schema (key columns)

**`drugdb.label_table`**:

| Column | Type | Notes |
|--------|------|-------|
| `id` | SERIAL | Primary key |
| `formulation_id` | UUID NOT NULL | FK to `drugdb.drug(formulation_id)` |
| `table_id` | TEXT NOT NULL | `"{formulation_id}_{section_key}_table_{tbl_idx}"` (tbl_idx from caption table number or sequential index) |
| `caption` | TEXT | Raw table caption string; NULL if empty |
| `semantic_type` | TEXT | Mapped from section key; one of: `adverse_event`, `dosing`, `interaction`, `pharmacokinetics`, `clinical_study`, `contraindication`, or NULL |
| `section` | TEXT | OpenFDA: bare section key; DailyMed: full dotted path (e.g. `dailymed.labeling_content.drug_interactions`) |
| `headers` | TEXT[] | Header row strings; may be empty `{}` if no headers detected |
| `rows_data` | JSONB | Default `'[]'`; list of row objects (list or dict per row) |

Unique constraint: `(formulation_id, table_id)` — prevents duplicates on re-run.

## How Data Flows Into This Stage

`run_full()` opens two connections: `read_conn` (autocommit=False, holds server-side named cursor `stream_linkage`) and `write_conn` (autocommit=False, for lookups and inserts). `stream_linkage` iterates `public."DrugMasterLinkage"` with `itersize=STREAM_FETCH_SIZE` (200 rows). For each record:

1. `get_formulation_ids()` runs `SELECT formulation_id FROM drugdb.drug WHERE master_linkage_id = %s::uuid` on `write_conn`. Records with no formulation match are counted in `total_skipped_no_formulation` and skipped.
2. `extract_tables(data, fid, logger)` is called per formulation_id. For OpenFDA: `_extract_openfda_tables()` iterates the 6 `OPENFDA_PARENT_KEYS`, then for each child section finds `section_data.get("table")` as a list and calls `_build_table_record()` on each element. For DailyMed: `_extract_dailymed_tables()` recursively descends the JSONB tree, acting only on `table` keys and skipping `content`/`text` leaves; the full dotted path is stored in `section`.
3. Rows accumulate in `pending` list. When `len(pending) >= BATCH_SIZE` (5,000), `_flush()` calls `psycopg2.extras.execute_batch(cur, _INSERT_SQL, pending, page_size=1000)` and commits `write_conn`.

`_build_table_record()` constructs `table_id = f"{formulation_id}_{section_key}_table_{tbl_idx}"` where `tbl_idx` is extracted from the caption via `_TABLE_NUM_PAT` regex (`\bTable\s+(\d+)\b`) or falls back to the sequential index. If the first row of `rows` looks like a header (all cells match `^[n%]+$`), it is promoted to `headers` and removed from `rows`.

## Key Logic

**`populate_label_table.py`** (also the content of `run.py`):

- **Config**: `BATCH_SIZE=5000` (pending flush threshold), `STREAM_FETCH_SIZE=200` (server-side cursor fetch chunk), `TEST_LIMIT=2`, `LOG_FILE="label_table_population.log"`.
- **`OPENFDA_PARENT_KEYS`**: 6 keys — `safety`, `adverse_events`, `labeling_content`, `clinical`, `drug_interactions`, `population_specific`. Each is checked under `combined_clean_jsonb.openfda`.
- **`SEMANTIC_TYPE_MAP`**: Maps section keys to typed categories:
  - `warnings_and_cautions` → `adverse_event`
  - `adverse_reactions` → `adverse_event`
  - `dosage_and_administration` → `dosing`
  - `drug_interactions` → `interaction`
  - `pharmacokinetics` → `pharmacokinetics`
  - `clinical_pharmacology` → `pharmacokinetics`
  - `clinical_studies` → `clinical_study`
  - `contraindications` → `contraindication`
  - Any other key → `NULL`
- **`ensure_schema_and_table()`**: Inline DDL creates `drugdb.label_table` and unique index `uidx_label_table_fid_tid ON (formulation_id, table_id)`. Uses `CREATE TABLE IF NOT EXISTS` and `CREATE UNIQUE INDEX IF NOT EXISTS`.
- **`_extract_openfda_tables()`**: Iterates `OPENFDA_PARENT_KEYS`, checks each child section for a `table` list. Non-dict table entries trigger a warning log and are skipped.
- **`_extract_dailymed_tables(node, fid, logger, path)`**: Recursive descent. When a `table` key is found and is a non-empty list, extracts all entries from that node using `_build_table_record()` with `section` set to the full dotted path. Otherwise iterates child keys (skipping `table`, `content`, `text`) and recurses into dicts and list items.
- **`_build_table_record()`**: Extracts `caption`, headers, and rows from a raw table dict. Parses table number from caption via regex; falls back to sequential `idx`. Promotes first row to headers if `_is_header_row()` returns True (all non-empty cells match `^[n%]+$`). `table_id = f"{formulation_id}_{section_key}_table_{tbl_idx}"`.
- **`_flush()`**: Calls `execute_batch` then `conn.commit()`. On error: `conn.rollback()`, logs error, returns 0 rows inserted.
- **Phase 1 — Test mode**: Fetches 2 records without inserts. For each: queries `drugdb.drug` for generic name, counts openfda sections checked, calls `extract_tables()` with the first formulation_id, prints table-by-table details (section, table_id, caption, semantic_type, header count, row count). Asks `Proceed with full run? [y/N]`.
- **Phase 2 — Full run**: `run_full(logger, record_limit=0)`. Final summary reports totals by `semantic_type` and `section` key sorted alphabetically.
- **CLI**: `--test-insert N` (optional, default 5) — runs Phase 2 but stops after N records, useful to verify inserts work before committing to a full run.
- **Two-connection pattern**: `read_conn` holds the streaming cursor open; `write_conn` handles all formulation lookups and batch inserts independently.

## Checkpoint / Recovery

No file-based checkpoint. The unique constraint `(formulation_id, table_id)` and `ON CONFLICT (formulation_id, table_id) DO NOTHING` make re-runs fully idempotent. To resume after interruption, simply re-run the script and confirm `y` at the Phase 1 prompt. Already-inserted rows produce conflicts but no errors or duplicates.

## How to Run

```bash
# Run extraction (interactive — confirms test output before full run)
python pipeline/09_label_table/run.py
# or equivalently:
python pipeline/09_label_table/populate_label_table.py

# Run with --test-insert to verify 5 records actually insert before full run:
python pipeline/09_label_table/populate_label_table.py --test-insert 5

# Monitor progress in the log file:
tail -f label_table_population.log
```

## Expected Runtime

- Phase 1 (test mode): under 5 seconds
- Phase 2 (full run, ~50,000 DrugMasterLinkage records, BATCH_SIZE=5000): 10–25 minutes

## Verification

```sql
-- Total rows inserted
SELECT COUNT(*) FROM drugdb.label_table;

-- Breakdown by semantic_type
SELECT semantic_type, COUNT(*) AS cnt
FROM drugdb.label_table
GROUP BY semantic_type
ORDER BY cnt DESC;

-- Breakdown by section
SELECT section, COUNT(*) AS cnt
FROM drugdb.label_table
GROUP BY section
ORDER BY cnt DESC
LIMIT 20;

-- Coverage: formulations with at least one table
SELECT
    (SELECT COUNT(DISTINCT formulation_id) FROM drugdb.drug) AS total_formulations,
    COUNT(DISTINCT formulation_id) AS with_tables
FROM drugdb.label_table;

-- Tables with non-empty headers
SELECT COUNT(*) FROM drugdb.label_table WHERE array_length(headers, 1) > 0;

-- Sample dosing tables
SELECT d.generic_name, lt.caption, lt.headers,
       jsonb_array_length(lt.rows_data) AS row_count
FROM drugdb.label_table lt
JOIN drugdb.drug d ON d.formulation_id = lt.formulation_id
WHERE lt.semantic_type = 'dosing'
LIMIT 5;
```

## Output / What the Next Stage Needs

- `drugdb.label_table` is populated with structured table rows tagged by `semantic_type`
- Stage 10 (`10_vector_embeddings`) may read table captions and rows for embedding
- `pass2_extractors.py` in Stage 08 reads `label_tables` from the unified drug record to enrich interaction magnitude and dosing regimen extraction

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Script prints `No formulation_ids for master_linkage_id=... — skipped` for many records | Stage 01 did not populate `drugdb.drug` for all DrugMasterLinkage records | Run `python pipeline/01_drug/run.py --verify`; re-run Stage 01 to fill gaps |
| `Batch N insert FAILED` errors in log | Unique constraint violation on re-run when `ON CONFLICT` is not firing (PG version < 9.5) or network timeout | Verify psycopg2 version; re-run — `ON CONFLICT DO NOTHING` prevents duplicates on successful commits |
| All `semantic_type=NULL` rows — no typed tables found | Section keys in the label JSONB do not match `SEMANTIC_TYPE_MAP` keys (e.g. label uses `drug_interaction` not `drug_interactions`) | Check actual JSONB keys: `SELECT DISTINCT jsonb_object_keys(combined_clean_jsonb->'openfda'->'drug_interactions')` and update `SEMANTIC_TYPE_MAP` if needed |
| `rows_data` is `[]` for many rows | Tables in the label JSON have no `rows` key or the rows list is empty | Expected for header-only or empty tables; check `_is_header_row()` promoted the only row to headers |
| Very slow run (>2 hours) | `STREAM_FETCH_SIZE=200` is low for large tables; `write_conn` doing per-record formulation lookups | Increase `STREAM_FETCH_SIZE` in config; performance is bounded by DB round-trips per record |


---

# Stage 10: Vector Embeddings

## Purpose

Chunks narrative drug label text from `drugdb.clinical_section` and embeds each chunk into `drugdb.rag_chunk` using a dense vector model (bge-large-en-v1.5, `vector(1024)`). This enables semantic (RAG) search across all drug label sections. The stage has two scripts: `run.py` contains `transform_to_unified.py` code (a utility that converts raw consolidated JSON into a unified record dict — it is misplaced in this directory but not the chunking entrypoint), and `embed_chunks.py` is a stub (1 line, not yet implemented).

**Current state**: `embed_chunks.py` is empty. The `rag_chunk` table is defined in Stage 00's `postgres_schema.sql`. The embedding pipeline must be implemented or run separately using the `rag_chunk` schema described below.

Scripts:

1. `run.py` — contains `transform_to_unified.py`: a utility that transforms a raw consolidated JSON record (openFDA + DailyMed + RxNorm + DrugBank) into a unified record matching `cdss_unified_schema.json`. Not the chunking/embedding entrypoint.
2. `embed_chunks.py` — stub file (not yet implemented); intended to read unembedded chunks from `drugdb.rag_chunk` and write `vector(1024)` embeddings back

## Prerequisites

- Stage 00 must have completed (`drugdb.rag_chunk` table created by `postgres_schema.sql`)
- Stage 08 must have completed (`drugdb.clinical_section` populated)
- `pgvector` extension must be installed: `CREATE EXTENSION IF NOT EXISTS vector`
- Python packages: `psycopg2-binary`; for embedding: a model server or HuggingFace API token

## Database Tables

### Tables Written To

| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `drugdb.rag_chunk` | INSERT (chunking pass) + UPDATE embedding (embedding pass) | Multiple chunks per clinical_section row |

### Tables Read From

| Table | Why |
|-------|-----|
| `drugdb.clinical_section` | Source of narrative text to chunk; each `text` field is split into overlapping windows |
| `drugdb.rag_chunk` | Embedding pass reads rows where `embedding IS NULL` and writes vectors back |

### Table Schema (key columns)

**`drugdb.rag_chunk`** (defined in Stage 00 `postgres_schema.sql`):

| Column | Type | Notes |
|--------|------|-------|
| `chunk_id` | TEXT | Primary key; deterministic ID (e.g. `{formulation_id}_{section}_{chunk_index}`) |
| `formulation_id` | TEXT | FK to `drugdb.drug(formulation_id)` |
| `semantic_type` | TEXT | Inherited from section (e.g. `indications_and_usage`, `dosage_and_administration`) |
| `text` | TEXT | Chunk text (overlapping window of label section) |
| `embedding` | vector(1024) | Dense embedding from bge-large-en-v1.5; NULL until embedding pass runs |

## How Data Flows Into This Stage

The intended flow (not yet fully implemented):

1. **Chunking pass** (`run.py` / to be implemented): Stream `drugdb.clinical_section` ordered by `formulation_id`. For each row, split `text` into overlapping windows (e.g. 512 tokens with 64-token overlap). Insert each window as a row in `drugdb.rag_chunk` with `embedding = NULL`.
2. **Embedding pass** (`embed_chunks.py` / stub): Read all rows from `drugdb.rag_chunk WHERE embedding IS NULL` in batches. Call a local vLLM embedding endpoint or HuggingFace Inference API with the bge-large-en-v1.5 model. Write the returned `vector(1024)` back via `UPDATE drugdb.rag_chunk SET embedding = %s WHERE chunk_id = %s`.

**`run.py` — `transform_to_unified.py` utility** (what is actually in this file):

`transform(raw: dict) -> dict`: Converts a raw consolidated JSON dict to a unified record. Calls sub-builders:

- `build_identifiers(raw)`: Merges NDC, RxCUI, UNII, DrugBank IDs from `openfda_metadata` and `dailymed.products`.
- `build_drug(raw)`: Extracts generic name, brand names, active/inactive ingredients (from DailyMed products, falling back to `openfda.drug_info.generic_name`). Stubs `drug_class=[]` and `atc_codes=[]` for downstream NLP passes.
- `build_product(raw)`: Collects manufacturer, routes, dosage forms, SKUs from DailyMed products.
- `build_rxnorm(raw)`: Normalizes `rxnorm[]` entries into `{rxcui, name, kind, dose_form, synonyms}` clinical formulations.
- `build_clinical(raw)`: Merges 17 section keys from `_SECTION_MAP` across openFDA and DailyMed. DailyMed wins on subsection structure (`_flatten_subsections()`); openFDA wins on narrative completeness. Source tagged as `merged`, `openfda`, or `dailymed`.
- `build_label_tables(raw)`: Deduplicates tables across 7 openFDA table keys (`dosage_tables`, `drug_interactions_table`, etc.) using `(caption, json.dumps(rows))` as dedup signature.
- `build_structured_facts(raw)`: Seeds `interactions` from DrugBank drug_interactions (all `severity="unknown"`), `contraindications` from `openfda.contraindications_table`, `dosing_regimens` from `openfda.dosage_tables` (stubs with `indication=None` for second-pass filling).
- `_formulation_id(openfda, dailymed)`: Returns `openfda.set_id` or `dailymed.drug_label.document_id`; falls back to SHA1 of drug_info + products JSON.

Run as standalone: `python run.py <input.json> <output.json>` — reads raw consolidated JSON, writes unified record.

## Checkpoint / Recovery

No checkpoint mechanism in either current script. The embedding pass is idempotent by design: `WHERE embedding IS NULL` skips already-embedded chunks. Re-running the chunking pass requires either a dedup constraint on `chunk_id` (PRIMARY KEY) or truncating `rag_chunk` before re-run.

## How to Run

```bash
# Prerequisite: verify pgvector extension
psql -h $DB_HOST -U postgres -d postgres \
    -c "CREATE EXTENSION IF NOT EXISTS vector;"

# Use transform_to_unified.py utility on a single raw JSON file:
python pipeline/10_vector_embeddings/run.py \
    /path/to/consolidated_record.json \
    unified_record.json

# Chunking pass (not yet implemented in run.py — implement or use a separate script):
# Read drugdb.clinical_section, chunk each text, insert into drugdb.rag_chunk

# Embedding pass (embed_chunks.py is a stub — implement before running):
# Read drugdb.rag_chunk WHERE embedding IS NULL
# Call bge-large-en-v1.5 via vLLM or HuggingFace API
# UPDATE drugdb.rag_chunk SET embedding = %s WHERE chunk_id = %s
```

## Expected Runtime

- `run.py` (transform utility, single record): under 1 second per record
- Chunking pass (full clinical_section table, ~2.8M rows): estimated 30–90 minutes
- Embedding pass (bge-large-en-v1.5, local GPU): estimated 2–8 hours depending on chunk count and GPU throughput
- Embedding pass (HuggingFace Inference API): estimated 12–48 hours at API rate limits

## Verification

```sql
-- Total chunks (after chunking pass)
SELECT COUNT(*) FROM drugdb.rag_chunk;

-- Embedded vs unembedded
SELECT
    COUNT(*) AS total_chunks,
    COUNT(embedding) AS embedded,
    COUNT(*) - COUNT(embedding) AS pending_embedding
FROM drugdb.rag_chunk;

-- Coverage by semantic type
SELECT semantic_type, COUNT(*) AS chunks
FROM drugdb.rag_chunk
GROUP BY semantic_type
ORDER BY chunks DESC;

-- Sample semantic search (replace vector literal with actual query embedding)
SELECT chunk_id, semantic_type, LEFT(text, 100) AS preview,
       1 - (embedding <=> '[0.01, 0.02, ...]'::vector) AS similarity
FROM drugdb.rag_chunk
WHERE embedding IS NOT NULL
ORDER BY embedding <=> '[0.01, 0.02, ...]'::vector
LIMIT 5;
```

## Output / What the Next Stage Needs

- `drugdb.rag_chunk` populated with text chunks and `vector(1024)` embeddings
- The CDSS query layer uses cosine similarity search (`<=>` operator) against `drugdb.rag_chunk.embedding` for semantic retrieval of relevant label sections

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `ERROR: type "vector" does not exist` | pgvector extension not installed | `CREATE EXTENSION IF NOT EXISTS vector;` on the target database |
| `embed_chunks.py` does nothing when run | File is a stub (1 line) | Implement the embedding loop or use a separate embedding script that targets `drugdb.rag_chunk WHERE embedding IS NULL` |
| `run.py` reads a JSON file instead of DB records | `run.py` contains `transform_to_unified.py` (misplaced utility) not a chunking pipeline | Use `run.py` only as a standalone transform utility; implement chunking separately reading from `drugdb.clinical_section` |
| Semantic search returns irrelevant results | Embedding model mismatch between query and stored vectors | Ensure the same model (bge-large-en-v1.5) is used for both indexing and query-time embedding |


---

# Stage 11: Indian Brands

## Purpose

Provides the Indian market localization layer for the CDSS. Loads Indian pharmaceutical brand data into `drugdb.indian_brand` and `drugdb.indian_brand_ingredient`, then links each brand to FDA unified formulation records via salt-stripped generic name matching. `drugbank_id` on `drugdb.indian_brand_ingredient` is populated in two passes: a 4-tier exact/prefix match against `drugdb.ingredients` and `drugdb.ingredient_synonyms` (Tiers 1–4), followed by a 5-tier fuzzy pass (Tiers 5.1–5.5) for remaining NULL rows.

`run.py` is a stub (1 line, not yet implemented). The core logic lives in `indian_brand_mapper.py` (normalization utilities and mapping SQL), `update_indian_brand_drugbank_id.py` (4-tier exact/prefix drugbank_id population), and `fuzzy_match_indian_ingredients.py` (fuzzy fallback).

Scripts:

1. `indian_brand_mapper.py` — normalization library: `normalize_generic_name()`, `normalize_dosage_form()`, 4-tier mapping SQL, FDC handling strategy, entity resolver SQL function, data loader template
2. `update_indian_brand_drugbank_id.py` — 4-tier exact/prefix matching: populates `drugdb.indian_brand_ingredient.drugbank_id` WHERE NULL; Phase 1 (dry run) + Phase 2 (apply updates); saves `match_statistics.json`
3. `fuzzy_match_indian_ingredients.py` — 5-tier fuzzy matching: Tier 5.1 (parenthetical extraction), 5.2 (fuzz.ratio ≥ 95%), 5.3 (token-based exact, tokens ≥ 6 chars), 5.4 (fuzz.ratio 85–94%, manual review), 5.5 (fuzz.partial_ratio ≥ 90%); Phase 1 (analysis, saves `fuzzy_match_results.json`) + Phase 2 (interactive apply)
4. `run.py` — stub (not yet implemented; intended entrypoint for loading Indian brand CSV data)

## Prerequisites

- Stage 01 must have completed (`drugdb.drug` populated)
- Stage 02 must have completed (`drugdb.ingredients` and `drugdb.ingredient_synonyms` populated)
- Stage 03 must have completed (`drugdb.drug_ingredient_mapping` populated with DrugBank IDs on ingredient rows)
- `drugdb.indian_brand` and `drugdb.indian_brand_ingredient` tables must exist (created by Stage 00 DDL or loaded from your Indian brand dataset)
- Python packages: `psycopg2-binary`, `fuzzywuzzy`, `python-Levenshtein`
- `pg_trgm` extension required for the fuzzy mapping SQL (`similarity()` function)

## Database Tables

### Tables Written To

| Table | Operation | Rows affected (approx) |
|-------|-----------|------------------------|
| `drugdb.indian_brand` | INSERT (data loading) + UPDATE `formulation_id`, `match_confidence` (mapping SQL) | One row per Indian brand-strength-form combination |
| `drugdb.indian_brand_ingredient` | UPDATE `drugbank_id` WHERE NULL | ~559,000–580,000 rows across both passes |

### Tables Read From

| Table | Why |
|-------|-----|
| `drugdb.drug` | Mapping SQL joins on salt-stripped `normalized_generic` to resolve `formulation_id` |
| `drugdb.ingredients` | `update_indian_brand_drugbank_id.py` loads all rows into memory for 4-tier matching |
| `drugdb.ingredient_synonyms` | Loaded into memory for Tier 2 (exact synonym) and Tier 4 (prefix synonym) matching |
| `drugdb.indian_brand_ingredient` | Both scripts target rows WHERE `drugbank_id IS NULL` |

### Table Schema (key columns)

**`drugdb.indian_brand`**:

| Column | Type | Notes |
|--------|------|-------|
| `indian_brand_id` | SERIAL | Primary key |
| `brand_name` | TEXT NOT NULL | e.g. `Nelficine`, `Viranelf` |
| `manufacturer_india` | TEXT | e.g. `Cipla`, `Sun Pharma`, `Dr. Reddy's` |
| `generic_name_raw` | TEXT NOT NULL | As written in source data, e.g. `Nelfinavir Mesylate` |
| `normalized_generic_name` | TEXT NOT NULL | Salt-stripped INN (uppercase), e.g. `NELFINAVIR` |
| `strength_label` | TEXT | e.g. `250 mg`, `625 mg` |
| `strength_value` | NUMERIC | Numeric dose amount |
| `strength_unit` | TEXT | e.g. `mg`, `mcg`, `ml` |
| `dosage_form_raw` | TEXT | As written in source, e.g. `Film Coated Tablet` |
| `form_canonical` | TEXT | Normalized form code, e.g. `TABLET`, `TABLET_ER`, `INJECTION` |
| `route` | TEXT | `ORAL`, `IV`, `TOPICAL`, etc. |
| `pack_size` | TEXT | e.g. `10s`, `30s`, `1x10` |
| `schedule` | TEXT | CDSCO schedule: `H`, `H1`, `X`, `G`, `OTC` |
| `mrp_inr` | NUMERIC | MRP in INR |
| `cdsco_approval` | BOOLEAN | CDSCO approved flag |
| `is_combination` | BOOLEAN | Fixed-dose combination (FDC) flag |
| `combination_ingredients` | JSONB | For FDCs: `[{name, strength, unit}]` |
| `formulation_id` | TEXT | FK to `drugdb.drug(formulation_id)`; populated by mapping SQL |
| `match_confidence` | TEXT | `exact`, `normalized`, `fuzzy`, or `manual` |

Indexes: `idx_ib_normalized` (normalized_generic_name), `idx_ib_brand` (brand_name), `idx_ib_formulation` (formulation_id), `idx_ib_form` (form_canonical), `idx_ib_strength` (normalized_generic_name, strength_value, form_canonical).

**`drugdb.indian_brand_ingredient`** (FDC junction table):

| Column | Type | Notes |
|--------|------|-------|
| `indian_brand_id` | INT | FK to `drugdb.indian_brand(indian_brand_id)`; part of PK |
| `ingredient_index` | INT | Position in FDC (1, 2, 3...); part of PK |
| `formulation_id` | TEXT | FK to `drugdb.drug(formulation_id)` for this ingredient |
| `ingredient_name_raw` | TEXT | Ingredient name as written in source data |
| `ingredient_name_norm` | TEXT | Salt-stripped normalized name |
| `drugbank_id` | TEXT | DrugBank identifier; populated by `update_indian_brand_drugbank_id.py` and `fuzzy_match_indian_ingredients.py` |
| `ingredient_strength` | TEXT | Strength for this ingredient in the FDC |
| `match_confidence` | TEXT | Match tier used |

## How Data Flows Into This Stage

**Loading (manual / `run.py` stub)**: Indian brand CSV data is loaded into `drugdb.indian_brand` using the template in `indian_brand_mapper.py`'s `build_loader_script()`. At load time, `normalize_generic_name()` strips salt forms and `normalize_dosage_form()` maps form strings to canonical codes. FDC products have each ingredient inserted into `drugdb.indian_brand_ingredient`.

**Brand-to-formulation mapping** (`build_mapping_sql()` in `indian_brand_mapper.py`): Run as SQL in four steps — first builds a materialized view `fda_generic_lookup` with salt-stripped FDA generic names; then attempts exact match (normalized name + strength + route), normalized match (name + route, any strength), fuzzy match (trigram similarity > 0.7 + route), and unmatched report. Updates `drugdb.indian_brand.formulation_id` and `match_confidence`.

**Tier 1–4 drugbank_id population** (`update_indian_brand_drugbank_id.py`): Loads all `drugdb.ingredients` rows into `ing_exact` dict (lower(name) → entry) and `ing_list` (sorted by name length for shortest-prefix wins). Loads all `drugdb.ingredient_synonyms` into `syn_exact` and `syn_list`. For each `drugdb.indian_brand_ingredient WHERE drugbank_id IS NULL`, tries `ingredient_name_raw` first then `ingredient_name_norm` through 4 tiers:
- Tier 1: `tier1_exact_ingredient()` — `ing_exact.get(input.lower())`
- Tier 2: `tier2_exact_synonym()` — `syn_exact.get(input.lower())`
- Tier 3: `tier3_prefix_ingredient()` — iterates `ing_list` sorted by length; first entry where `name_lower.startswith(prefix)` and name != prefix
- Tier 4: `tier4_prefix_synonym()` — same logic on `syn_list`

Updates via `UPDATE drugdb.indian_brand_ingredient SET drugbank_id = %s WHERE ingredient_name_raw = %s AND drugbank_id IS NULL` in batches of `--batch-size` (default 500).

**Tier 5 fuzzy drugbank_id population** (`fuzzy_match_indian_ingredients.py`): Queries distinct `(ingredient_name_raw, ingredient_name_norm, COUNT(*))` from rows still `WHERE drugbank_id IS NULL`. Loads `ingr_by_lower` and `syn_by_lower` dicts. Runs 5 tiers in sequence (first match wins):
- Tier 5.1 `try_parenthetical()`: extracts text before/inside parentheses, tries exact match in ingredients + synonyms
- Tier 5.2 `try_high_fuzzy()`: `fuzz.ratio` ≥ 95%; rejects trailing alphanumeric suffix variants (e.g. alpha-2a vs alpha-2b)
- Tier 5.3 `try_token()`: splits on whitespace/hyphen/slash; tries each token ≥ 6 chars as exact match
- Tier 5.4 `try_medium_fuzzy()`: `fuzz.ratio` in [85, 95); flags for manual review in Phase 2
- Tier 5.5 `try_partial()`: `fuzz.partial_ratio` ≥ 90%; skips names shorter than 6 chars

Saves results to `RESULTS_FILE = "fuzzy_match_results.json"`. Phase 2 applies each tier interactively (prompts `yes/no` per tier; Tier 5.4 prompts per individual match).

## Key Logic

**`normalize_generic_name(name)`**: Applies `COMPOUND_SALT_PATTERNS` (3 regex: disoproxil fumarate, sodium phosphate, calcium trihydrate) first, then `SALT_PATTERN` (40+ single-word salt forms including mesylate, besylate, hydrochloride, sulfate, sodium, potassium, maleate, fumarate, tartrate, axetil, pivoxil, proxetil, medoxomil, disoproxil, alafenamide, trihydrate, etc.) with optional trailing hydrate suffix. Returns uppercase INN. Example: `"Tenofovir Disoproxil Fumarate"` → `"TENOFOVIR"`.

**`normalize_dosage_form(form)`**: Maps 50+ form strings to ~20 canonical codes. Examples: `"Film Coated Tablet"` → `TABLET`, `"SR Tablet"` → `TABLET_ER`, `"Dry Syrup"` → `POWDER_ORAL`, `"Rotacap"` → `INHALER_DPI`, `"MD Tablet"` → `TABLET_ODT`. Returns `form_upper` unchanged for unmapped forms.

**`build_entity_resolver_sql()`**: Generates a PostgreSQL function `resolve_drug(input_name TEXT)` that tries 6 resolution strategies in order: exact Indian brand match, FDC ingredient decomposition (via `indian_brand_ingredient` junction), FDA generic exact match (including brand names in `brand_names` array), normalized generic match (inline REGEXP_REPLACE salt stripping), fuzzy brand match (trigram similarity > 0.6, LIMIT 3), fuzzy generic match (similarity > 0.5, LIMIT 3).

**`update_indian_brand_drugbank_id.py` CLI**: `--host`, `--dbname`, `--user`, `--password` (required), `--port`, `--batch-size` (default 500), `--log-file`, `--verbose`, `--skip-confirm`.

**FDC handling**: For fixed-dose combinations (e.g. Tenofovir + Emtricitabine + Efavirenz tablets), each ingredient is stored as a separate row in `drugdb.indian_brand_ingredient` with its own `formulation_id` and `drugbank_id`. When the CDSS resolves an FDC brand, it returns all N ingredient formulation_ids, runs interaction checks against all N, and returns a merged safety report.

## Checkpoint / Recovery

`update_indian_brand_drugbank_id.py`: No file-based checkpoint. UPDATE uses `WHERE drugbank_id IS NULL`, so re-runs are fully idempotent — rows already filled are skipped automatically. Phase 1 (dry run) can be re-run any number of times without side effects. `match_statistics.json` is overwritten on each Phase 1 run.

`fuzzy_match_indian_ingredients.py`: No checkpoint. `fuzzy_match_results.json` is overwritten on each Phase 1 run. Phase 2 updates use `WHERE ingredient_name_raw = %s AND drugbank_id IS NULL`, so re-applying a tier after interruption is safe.

## How to Run

```bash
# Step 1: Load Indian brand data (implement run.py or use loader template from indian_brand_mapper.py)
# See build_loader_script() in indian_brand_mapper.py for a CSV loading template

# Step 2: Run brand-to-formulation mapping SQL
# Extract the SQL from build_mapping_sql() in indian_brand_mapper.py and run in psql:
psql -h $DB_HOST -U postgres -d postgres -c "
    -- Build fda_generic_lookup materialized view, then run 3 UPDATE passes
    -- See indian_brand_mapper.build_mapping_sql() for full SQL
"

# Step 3: 4-tier drugbank_id population — dry run first
python pipeline/11_indian_brands/update_indian_brand_drugbank_id.py \
    --password $DB_PASSWORD

# Step 3 (apply with auto-confirm):
python pipeline/11_indian_brands/update_indian_brand_drugbank_id.py \
    --password $DB_PASSWORD --skip-confirm \
    --log-file logs/indian_brand_drugbank.log

# Step 4: Fuzzy fallback for remaining NULL rows
python pipeline/11_indian_brands/fuzzy_match_indian_ingredients.py
# Phase 1 runs automatically; answer 'yes' to proceed to Phase 2 interactive apply

# Step 5: Verify normalization functions (standalone demo)
python pipeline/11_indian_brands/indian_brand_mapper.py

# Step 6: Create entity resolver function in DB (extract from build_entity_resolver_sql()):
psql -h $DB_HOST -U postgres -d postgres -c "
    -- See indian_brand_mapper.build_entity_resolver_sql() for CREATE OR REPLACE FUNCTION
"
```

## Expected Runtime

- `update_indian_brand_drugbank_id.py` (Phase 1 dry run): 2–10 minutes (in-memory matching)
- `update_indian_brand_drugbank_id.py` (Phase 2 apply, ~580,000 rows, batch=500): 5–20 minutes
- `fuzzy_match_indian_ingredients.py` (Phase 1, fuzz.ratio over all ingredient names): 5–30 minutes depending on number of distinct unmapped ingredients
- `fuzzy_match_indian_ingredients.py` (Phase 2 interactive): 5–15 minutes for manual review of Tier 5.4 entries

## Verification

```sql
-- Total Indian brand rows
SELECT COUNT(*) FROM drugdb.indian_brand;

-- Match rate by confidence tier
SELECT match_confidence, COUNT(*) AS cnt
FROM drugdb.indian_brand
GROUP BY match_confidence
ORDER BY cnt DESC;

-- Unmatched brands (no formulation_id)
SELECT COUNT(*) AS unmatched FROM drugdb.indian_brand WHERE formulation_id IS NULL;

-- drugbank_id coverage on indian_brand_ingredient
SELECT
    COUNT(*) AS total,
    COUNT(drugbank_id) AS with_drugbank_id,
    ROUND(COUNT(drugbank_id) * 100.0 / COUNT(*), 2) AS pct
FROM drugdb.indian_brand_ingredient;

-- FDC count
SELECT COUNT(*) AS fdc_brands FROM drugdb.indian_brand WHERE is_combination = true;

-- Top manufacturers
SELECT manufacturer_india, COUNT(*) AS brand_count
FROM drugdb.indian_brand
GROUP BY manufacturer_india
ORDER BY brand_count DESC
LIMIT 10;

-- Sample brand→formulation resolution
SELECT ib.brand_name, ib.normalized_generic_name, ib.form_canonical,
       ib.match_confidence, d.generic_name
FROM drugdb.indian_brand ib
JOIN drugdb.drug d ON d.formulation_id = ib.formulation_id
LIMIT 5;
```

## Output / What the Next Stage Needs

- `drugdb.indian_brand` is populated with `formulation_id` and `match_confidence` for matched brands
- `drugdb.indian_brand_ingredient.drugbank_id` is populated for ingredient-level FDC resolution
- The CDSS entity resolver function (`resolve_drug()`) accepts Indian brand names and returns `formulation_id`(s) for all 9 CDSS query templates
- CDSS query templates Q1, Q2, Q3, Q4, Q6, Q7, Q9 gain Indian brand lookup as a final step after resolving FDA clinical data

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `update_indian_brand_drugbank_id.py` shows 0 records with NULL drugbank_id | All rows already have `drugbank_id` set, or `drugdb.indian_brand_ingredient` is empty | Verify: `SELECT COUNT(*) FROM drugdb.indian_brand_ingredient WHERE drugbank_id IS NULL`; if 0, Stage 11 drugbank matching is already done |
| High unmatched rate in `fuzzy_match_indian_ingredients.py` Tier 5 | Indian brand ingredient names use non-standard abbreviations or are not in DrugBank | Review `no_match` entries in `fuzzy_match_results.json`; add to `drugdb.ingredient_synonyms` manually for recurring names |
| `fuzzy_match_indian_ingredients.py` raises `ModuleNotFoundError: fuzzywuzzy` | `fuzzywuzzy` and `python-Levenshtein` not installed | `pip install fuzzywuzzy python-Levenshtein` |
| `similarity()` function not found when running mapping SQL | `pg_trgm` extension not installed | `CREATE EXTENSION IF NOT EXISTS pg_trgm;` |
| `run.py` does nothing | File is a stub (1 line) | Use the loader template from `indian_brand_mapper.build_loader_script()` to load Indian brand CSV data |
| `apply_fuzzy_matches.py` from Stage 03 raises FileNotFoundError | Must be run from `pipeline/03_drug_ingredient_mapping/` directory; reads `fuzzy_match_results.json` relative to cwd | `cd pipeline/03_drug_ingredient_mapping && python apply_fuzzy_matches.py` |


---

# RXCUI Backfill Pipeline

Backfills `drugdb.indian_brand_ingredient.rxcui_in` by resolving ingredient names
against RxNorm's `public.rxnconso` table in three progressive matching steps.

## Prerequisites

```bash
pip install psycopg2-binary
```

The pipeline reads DB credentials from `~/cdss/.env` automatically.

## First-time setup

Create the audit table:

```bash
psql -h $DB_HOST -U postgres -d postgres -f create_audit_table.sql
```

## Usage

```
python run_pipeline.py [OPTIONS]

Options:
  --dry-run         Run all steps without writing to DB (logs what WOULD happen)
  --limit N         Process only first N rows (for testing)
  --step {1,2,3,all}  Run a specific step or all (default: all)
  --batch-size N    Commit batch size (default: 5000)
  --password TEXT   DB password (overrides .env)
  --skip-audit      Skip audit table inserts (faster for testing)
```

## Steps

| Step | Method | Confidence label |
|------|--------|-----------------|
| 1 | Exact lowercase match of `ingredient_name_norm` → `rxnconso.str` | `exact` |
| 2 | Synonym expansion (INN↔USAN via `synonyms.json`) → `rxnconso.str` | `synonym` |
| 3 | Salt suffix stripped → retry exact + synonym | `salt_strip_exact` / `salt_strip_synonym` |

## Typical workflow

```bash
# Test dry-run with 100 rows
python run_pipeline.py --dry-run --limit 100

# Full step-by-step run
python run_pipeline.py --step 1
python run_pipeline.py --step 2
python run_pipeline.py --step 3

# Or all at once
python run_pipeline.py --step all
```

## Output

- `logs/step1_exact_match.log` — tab-separated match details per resolved row
- `logs/step2_synonym_match.log` — synonym match details
- `logs/step3_salt_strip_match.log` — salt-strip match details
- `logs/unresolved.log` — rows not resolved by any step
- `logs/pipeline_summary.log` — counts, percentages, durations

## Idempotent

The pipeline only touches rows where `rxcui_in IS NULL`. Running it again is safe.


---

# Sibling Enrichment Pass

## Purpose

`drug_master_linkage_unique` holds one row per unique `(generic_formulation, dosage_forms)` combination. The chosen row's `combined_clean_jsonb` is the richest available JSON (selected by longest-text rule from `DrugMasterLinkage`), but it frequently has `null` or empty fields.

This pass is **Pass 1** of a two-pass enrichment strategy:

| Pass | Method | Script |
|------|--------|--------|
| **Pass 1 — Sibling fill** (this script) | Deterministic structural enrichment from peer records | `enrich_sibling_fill.py` |
| Pass 2 — LLM extraction | Semantic gap-fill for fields with no sibling data | *(separate)* |

**Why siblings?** Multiple `master_linkage_id` values in `DrugMasterLinkage` can map to the same `(generic_formulation, dosage_forms)` via `drugdb.drug`. The row that did not win the length-based selection ("siblings") may still carry non-null values for specific fields that the winner left empty. This pass harvests those values with zero LLM cost.

---

## Tables Touched

| Table | Schema | Read / Write | Notes |
|-------|--------|-------------|-------|
| `drug_master_linkage_unique` | `drugdb` | **Read + Write** | Reads `combined_clean_jsonb`; writes `unified_json_enriched` (full-run only) |
| `drug_master_linkage_enrichment_audit` | `drugdb` | **Write (create + insert)** | New table; audit rows written on every run (dry and full) |
| `"DrugMasterLinkage"` | `public` | **Read only** | Source of sibling `combined_clean_jsonb` values |
| `drug` | `drugdb` | **Read only** | Used to resolve `(generic_formulation, dosage_forms)` → sibling `master_linkage_id` mappings |

**No existing tables are modified** except the single `ADD COLUMN` on `drug_master_linkage_unique`.

---

## Schema

### New column

```sql
ALTER TABLE drugdb.drug_master_linkage_unique
    ADD COLUMN IF NOT EXISTS unified_json_enriched JSONB;
```

Stores the gap-filled copy of `combined_clean_jsonb`. The original `combined_clean_jsonb` column is never modified.

### Audit table

```sql
CREATE TABLE IF NOT EXISTS drugdb.drug_master_linkage_enrichment_audit (
    audit_id                BIGSERIAL       PRIMARY KEY,
    run_id                  UUID            NOT NULL,
    run_mode                TEXT            NOT NULL
                                CHECK (run_mode IN ('dry_run', 'full_run')),
    target_mlid             TEXT            NOT NULL,
    generic_formulation     TEXT,
    dosage_form             TEXT,
    field_path              TEXT            NOT NULL,
    original_value          JSONB,
    filled_value            JSONB,
    original_value_length   INT,
    filled_value_length     INT,
    source_sibling_mlid     TEXT            NOT NULL,
    sibling_count           INT,
    status                  TEXT            NOT NULL
                                CHECK (status IN (
                                    'filled',
                                    'skipped_no_sibling_value',
                                    'error'
                                )),
    error_message           TEXT,
    created_at              TIMESTAMPTZ     DEFAULT NOW()
);
```

### Indexes

| Index name | Columns | Purpose |
|------------|---------|---------|
| `idx_dmlea_run_id` | `(run_id)` | All rows for a single run |
| `idx_dmlea_target_mlid` | `(target_mlid)` | Audit trail for one mLId |
| `idx_dmlea_field_path` | `(field_path)` | Most-filled-field analysis |
| `idx_dmlea_run_mode_created_at` | `(run_mode, created_at)` | Time-ordered operational queries |

---

## Join Logic

Sibling discovery uses a two-hop join:

```
drug_master_linkage_unique
    (generic_formulation, dosage_forms, master_linkage_id[chosen])
          │
          │  join on (generic_formulation, dosage_forms)
          ▼
    drugdb.drug
          │  one row per formulation; master_linkage_id may differ per row
          │  join on master_linkage_id
          ▼
    public."DrugMasterLinkage"
          combined_clean_jsonb  ← sibling JSON values
```

For a target row with `master_linkage_id = X` and pair `(gf, df)`, all rows in `drugdb.drug` where `generic_formulation = gf AND dosage_forms = df` yield candidate `master_linkage_id` values. Those that differ from `X` are siblings. Their `combined_clean_jsonb` is fetched from `DrugMasterLinkage`.

**Batch efficiency**: the script collects all unique `(gf, df)` pairs in a batch and resolves siblings in a single SQL query using a `JOIN (VALUES ...)` pattern — one round-trip per batch, not per record.

---

## Field-Fill Rules

### What qualifies as a gap (empty value)

All of the following are treated as fillable gaps:

| Value | Python type |
|-------|-------------|
| `null` (JSON) | `None` |
| `""` or `"  "` | `str` with whitespace only |
| `[]` | empty `list` |
| `{}` | empty `dict` |

Non-empty scalars (`"text"`, `0`, `false`), non-empty arrays, and non-empty objects are not gaps.

### Walker behaviour

The JSON walker recurses into each key at the current level:

```
is_empty(value)?
  YES → attempt fill from siblings; emit audit row
  NO, value is non-empty dict → recurse into sub-fields
  NO, value is non-empty list → SKIP (atomic unit; only empty arrays are gaps)
  NO, scalar               → skip
```

### Sibling selection — per gap

For each gap field, all sibling JSONs are checked at the same JSON path:

1. Collect all siblings that have a non-empty value at that path.
2. **Longest serialized text wins** (`len(json.dumps(value))`).
3. Lexicographically **smallest sibling `mLId` breaks ties** (deterministic).

### Array handling

Arrays are treated as **whole units**:
- An empty array `[]` is a gap → replaced with the winning sibling's entire array.
- A non-empty array is not a gap → skipped; no element-level merging.

### Object handling

- An empty object `{}` is a gap → replaced wholesale.
- A non-empty object is not a gap → the walker recurses into its sub-fields, which may individually be gaps.

### Idempotence

Every run starts from `combined_clean_jsonb` (the immutable source column), never from a previous `unified_json_enriched`. Re-running always produces the same enriched result for the same source data. Previous audit rows are kept for historical comparison via `run_id`.

---

## Run Modes

### `--mode=dry-run` *(always run first)*

- Computes the full enrichment (gap detection, sibling matching, winning values).
- **Writes audit rows** with `run_mode = 'dry_run'`.
- **Does NOT update** `unified_json_enriched` on `drug_master_linkage_unique`.
- Safe to run multiple times.
- Use this to verify the enrichment plan before committing.

### `--mode=full-run`

- Same computation as dry-run.
- **Writes audit rows** with `run_mode = 'full_run'`.
- **Updates** `unified_json_enriched` on `drug_master_linkage_unique`.
- If no prior dry-run is found in the audit table, the script prints a warning.

> **Dry-run must be reviewed before full-run.** The README and script both enforce this as a reminder. Inspect the sample queries below before executing full-run.

### Expected runtime (~10,752 rows, batch size 500)

| Mode | Typical time |
|------|-------------|
| dry-run | 1–3 minutes |
| full-run | 2–5 minutes |

Runtime is dominated by network latency to the remote PostgreSQL instance. Larger batch sizes reduce round-trips; see Performance Notes.

---

## How to Run

### Prerequisites

```bash
# 1. Install dependencies (psycopg2-binary and tqdm already in requirements.txt)
cd ~/cdss
pip install -r requirements.txt

# 2. Apply schema changes (one-time, idempotent)
psql "postgresql://postgres:$DB_PASSWORD@$DB_HOST:5432/postgres" \
     -f sibling_enrichment_pass/setup_schema.sql
```

### Step 1 — Dry run

```bash
cd ~/cdss/sibling_enrichment_pass

python enrich_sibling_fill.py --mode=dry-run --batch-size=500
```

Review the audit table and logs before proceeding.

### Step 2 — Full run

```bash
python enrich_sibling_fill.py --mode=full-run --batch-size=500
```

### Custom batch size

```bash
# Larger batches → fewer round-trips, more memory per batch
python enrich_sibling_fill.py --mode=dry-run --batch-size=1000
```

### Environment variable override

```bash
export DATABASE_URL="postgresql://postgres:$DB_PASSWORD@$DB_HOST:5432/postgres"
python enrich_sibling_fill.py --mode=dry-run
```

---

## Log Files

All logs are written to `sibling_enrichment_pass/logs/`:

| File | Content |
|------|---------|
| `run_<run_id>_summary.log` | Single JSON line: start/end time, totals, top-10 fields |
| `run_<run_id>_detailed.log` | One JSON line per processed record |
| `run_<run_id>_errors.log` | One JSON line per error with full traceback |
| `run_<run_id>_sections.log` | One JSON line per section path (first 3 levels, e.g. `dailymed.safety.precautions`): `filled_count`, `not_filled_count`, `filled_fields`, `not_filled_fields` |
| `run_<run_id>_console.log` | Raw console output including tqdm progress bar |

---

## Sample Queries

### Count of records enriched in the most recent full-run

```sql
WITH latest AS (
    SELECT run_id
    FROM drugdb.drug_master_linkage_enrichment_audit
    WHERE run_mode = 'full_run'
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT COUNT(DISTINCT target_mlid) AS enriched_records
FROM drugdb.drug_master_linkage_enrichment_audit a
JOIN latest l USING (run_id)
WHERE a.status = 'filled';
```

### Top 20 most frequently filled fields (across all full-runs)

```sql
SELECT
    field_path,
    COUNT(*)                          AS times_filled,
    COUNT(DISTINCT target_mlid)       AS distinct_records,
    AVG(filled_value_length)::int     AS avg_filled_len
FROM drugdb.drug_master_linkage_enrichment_audit
WHERE status = 'filled'
  AND run_mode = 'full_run'
GROUP BY field_path
ORDER BY times_filled DESC
LIMIT 20;
```

### Records still with null fields after enrichment (Pass 2 candidates)

```sql
-- Records where unified_json_enriched exists but at least one field was
-- skipped because no sibling had a value (candidates for LLM Pass 2)
WITH latest AS (
    SELECT run_id
    FROM drugdb.drug_master_linkage_enrichment_audit
    WHERE run_mode = 'full_run'
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT
    a.target_mlid,
    a.generic_formulation,
    a.dosage_form,
    COUNT(*) AS unfilled_fields,
    ARRAY_AGG(a.field_path ORDER BY a.field_path) AS unfilled_paths
FROM drugdb.drug_master_linkage_enrichment_audit a
JOIN latest l USING (run_id)
WHERE a.status = 'skipped_no_sibling_value'
GROUP BY a.target_mlid, a.generic_formulation, a.dosage_form
ORDER BY unfilled_fields DESC;
```

### Audit trail for a specific mLId

```sql
SELECT
    run_id,
    run_mode,
    field_path,
    status,
    source_sibling_mlid,
    sibling_count,
    filled_value_length,
    created_at
FROM drugdb.drug_master_linkage_enrichment_audit
WHERE target_mlid = '<your-master-linkage-id>'
ORDER BY created_at DESC, field_path;
```

### Diff between dry-run and full-run for the same enrichment round

```sql
-- Compare fill decisions between the most recent dry-run and full-run
-- to verify they are identical (they should be for the same source data)
WITH dry AS (
    SELECT run_id AS dry_run_id
    FROM drugdb.drug_master_linkage_enrichment_audit
    WHERE run_mode = 'dry_run'
    ORDER BY created_at DESC LIMIT 1
),
full AS (
    SELECT run_id AS full_run_id
    FROM drugdb.drug_master_linkage_enrichment_audit
    WHERE run_mode = 'full_run'
    ORDER BY created_at DESC LIMIT 1
)
SELECT
    d.target_mlid,
    d.field_path,
    d.status           AS dry_status,
    f.status           AS full_status,
    d.source_sibling_mlid AS dry_winner,
    f.source_sibling_mlid AS full_winner
FROM drugdb.drug_master_linkage_enrichment_audit d
JOIN dry ON d.run_id = dry.dry_run_id
LEFT JOIN drugdb.drug_master_linkage_enrichment_audit f
    ON  f.target_mlid = d.target_mlid
    AND f.field_path  = d.field_path
JOIN full ON f.run_id = full.full_run_id
WHERE d.status != f.status
   OR d.source_sibling_mlid != f.source_sibling_mlid
LIMIT 50;
```

### Verify enrichment for a specific mLId — field-level diff

Replace `<run-id>` and `<master-linkage-id>` with the values you want to inspect.

```sql
-- What fields were filled, what the original value was, and which sibling supplied the value
SELECT field_path,
       original_value,
       filled_value,
       source_sibling_mlid
FROM drugdb.drug_master_linkage_enrichment_audit
WHERE run_id    = '<run-id>'
  AND target_mlid = '<master-linkage-id>'
  AND status    = 'filled'
ORDER BY field_path;
```

Latest full-run id: `8584b1db-15c5-475e-bfd4-5f66aef2af07`

Example for ciprofloxacin 500 MG (mLId `693ed434-a41b-5041-a6d9-3b952e16c745`):

```sql
SELECT field_path, original_value, filled_value, source_sibling_mlid
FROM drugdb.drug_master_linkage_enrichment_audit
WHERE run_id     = '8584b1db-15c5-475e-bfd4-5f66aef2af07'
  AND target_mlid = '693ed434-a41b-5041-a6d9-3b952e16c745'
  AND status     = 'filled'
ORDER BY field_path;
```

### Compare original vs enriched JSON for a specific mLId

```sql
SELECT combined_clean_jsonb  AS original,
       unified_json_enriched AS enriched
FROM drugdb.drug_master_linkage_unique
WHERE master_linkage_id = '<master-linkage-id>';
```

For a quick size comparison across all enriched records:

```sql
SELECT master_linkage_id,
       generic_formulation,
       dosage_forms,
       LENGTH(combined_clean_jsonb::text)   AS original_len,
       LENGTH(unified_json_enriched::text)  AS enriched_len,
       LENGTH(unified_json_enriched::text)
         - LENGTH(combined_clean_jsonb::text) AS gained_chars
FROM drugdb.drug_master_linkage_unique
WHERE unified_json_enriched IS NOT NULL
ORDER BY gained_chars DESC
LIMIT 20;
```

### Verify enriched column was written (spot check)

```sql
SELECT
    master_linkage_id,
    generic_formulation,
    dosage_forms,
    CASE WHEN unified_json_enriched IS NULL THEN 'not_enriched'
         ELSE 'enriched' END AS enrichment_status,
    LENGTH(combined_clean_jsonb::text)      AS original_len,
    LENGTH(unified_json_enriched::text)     AS enriched_len
FROM drugdb.drug_master_linkage_unique
ORDER BY enriched_len DESC NULLS LAST
LIMIT 20;
```

---

## Stats

### Pass 1 full-run results — run `8584b1db-15c5-475e-bfd4-5f66aef2af07` (2026-05-21)

| Metric | Value |
|--------|-------|
| Total records processed | 10,752 |
| Records enriched (≥1 field filled) | 3,744 (34.8 %) |
| Records unchanged (no siblings / all fields already full) | 7,008 (65.2 %) |
| Total fields filled | 7,940 |
| Avg fields filled per enriched record | ~2.12 |
| Errors | 0 |
| Wall-clock time | 22 min 33 sec (batch size 500) |
| Most frequently filled field | query `idx_dmlea_field_path` — see query below |
| Records still with gaps (Pass 2 LLM queue) | query `skipped_no_sibling_value` — see query below |

### Sample enriched records (largest JSON gain)

| master_linkage_id | generic_formulation | dosage_form | original_len | enriched_len | gained |
| --- | --- | --- | --- | --- | --- |
| `693ed434-a41b-5041-a6d9-3b952e16c745` | ciprofloxacin 500 MG | Oral Tablet | 540,931 | 615,971 | +75,040 |
| `22fed543-9838-5063-8901-865b067c1eb1` | fluconazole 200 MG | Oral Tablet | 583,355 | 648,356 | +65,001 |
| `b3c5d2af-685d-5633-92ed-ec8d9a022991` | acetaminophen 325 MG / hydrocodone bitartrate 7.5 MG | Oral Tablet | 587,908 | 652,103 | +64,195 |

---

## Performance Notes

### Why bulk operations

- **Sibling fetch**: one `JOIN (VALUES ...)` query per batch fetches all sibling JSONs for the entire batch — `O(1)` round-trips per batch instead of `O(N)`.
- **Audit insert**: `psycopg2.extras.execute_values` generates a single multi-row `INSERT` per batch; far faster than cursor.execute per row.
- **Enriched JSON update**: single `UPDATE … FROM (VALUES …)` per batch; avoids per-row UPDATE overhead.

### Batch size tuning

| Batch size | Trade-off |
|------------|-----------|
| 100–250 | Lower memory, more round-trips |
| 500 *(default)* | Balanced |
| 1000–2000 | Fewer round-trips, higher memory per batch; monitor if JSONs are very large |

The bottleneck is usually network RTT to `$DB_HOST`. A batch size of 500–1000 keeps all three queries (fetch target, fetch siblings, write) well within psycopg2 limits.

### Expected throughput

Actual Pass 1 full-run (2026-05-21, batch size 500): **10,752 records in 22 min 33 sec** (~475 records/min). The bottleneck is network RTT to the remote DB plus sibling JSON fetch size per batch. The final server-side UPDATE (`apply_json_fills`) adds negligible time — all enriched JSONB is computed in-database without Python-side transfer.

---

## Idempotence and Re-run Guidance

The script is **fully idempotent**:

1. Every run reads from `combined_clean_jsonb` (never `unified_json_enriched`).
2. Full-runs overwrite `unified_json_enriched` from scratch — there is no accumulated state.
3. Audit rows from previous runs are preserved (they carry their own `run_id`). New runs append new rows.

To re-run cleanly:

```bash
# Simply re-run — previous audit rows are kept for comparison
python enrich_sibling_fill.py --mode=dry-run
python enrich_sibling_fill.py --mode=full-run
```

To compare two runs:

```sql
-- Count fills per run
SELECT run_id, run_mode, COUNT(*) AS filled_fields, MIN(created_at) AS run_time
FROM drugdb.drug_master_linkage_enrichment_audit
WHERE status = 'filled'
GROUP BY run_id, run_mode
ORDER BY run_time DESC;
```

---

## Rollback

### Revert unified_json_enriched (undo the column fill)

```sql
-- Reset all enriched values (leaves the column in place)
UPDATE drugdb.drug_master_linkage_unique
SET unified_json_enriched = NULL;
```

### Remove audit rows for a specific run

```sql
DELETE FROM drugdb.drug_master_linkage_enrichment_audit
WHERE run_id = '<run-id-to-remove>';
```

### Remove all audit rows

```sql
TRUNCATE TABLE drugdb.drug_master_linkage_enrichment_audit;
```

### Drop the enriched column entirely

```sql
ALTER TABLE drugdb.drug_master_linkage_unique
    DROP COLUMN IF EXISTS unified_json_enriched;
```

### Drop the audit table entirely

```sql
DROP TABLE IF EXISTS drugdb.drug_master_linkage_enrichment_audit;
```

---

## What This Pass Does NOT Do

| Exclusion | Reason |
|-----------|--------|
| Does not modify `DrugMasterLinkage` | Source table is read-only in this pass |
| Does not modify `combined_clean_jsonb` | Original column is always preserved as source of truth |
| Does not call any LLM | Purely deterministic structural enrichment |
| Does not merge array elements | Arrays are atomic units — replaced wholesale or left unchanged |
| Does not deduplicate semantically equivalent values | e.g. `"Bayer AG"` vs `"Bayer"` — dedup is out of scope |
| Does not validate filled values | Assumes sibling data is trustworthy source material |
| Does not update any other child tables | Enrichment is confined to `unified_json_enriched` |
| Does not fill fields where every sibling is also null | Those become Pass 2 (LLM) candidates |


---

