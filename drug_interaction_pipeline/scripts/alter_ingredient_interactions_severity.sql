-- alter_ingredient_interactions_severity.sql
--
-- Adds severity and mechanism columns to drugdb.ingredient_interactions.
-- Must be run BEFORE scripts/enrich_severity_mechanism.py.
--
-- severity: LLM-classified (contraindicated/major/moderate/minor/unknown)
--           Stage 1 pre-filter classifies obvious cases via regex SQL.
--           Stage 2-5 (LLM Batch API) classifies the remainder.
--           DEFAULT 'unknown' — all 2,910,556 rows start unclassified.
--
-- mechanism: short pharmacological mechanism phrase extracted by LLM from description.
--            Examples: "CYP3A4 inhibition", "decreased renal excretion",
--                      "protein binding displacement"
--            NULL until LLM enrichment runs.
--
-- Run order:
--   1. THIS FILE   (psql -f schemas/alter_ingredient_interactions_severity.sql)
--   2. Python      (python3 scripts/enrich_severity_mechanism.py --dry-run)
--   3. Python      (python3 scripts/enrich_severity_mechanism.py --api-key ... --db-password ...)

ALTER TABLE drugdb.ingredient_interactions
    ADD COLUMN IF NOT EXISTS severity  TEXT DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS mechanism TEXT;

-- Fast lookup of unclassified rows — used by Stage 1 pre-filter WHERE clause
-- and Stage 2 DISTINCT ON query to skip already-classified rows.
CREATE INDEX IF NOT EXISTS idx_ii_severity
    ON drugdb.ingredient_interactions (severity);

-- Fast lookup by description hash — used by mirror validation and ad-hoc
-- de-duplication queries. md5() keeps index size manageable vs. indexing
-- the full TEXT column.
CREATE INDEX IF NOT EXISTS idx_ii_description_hash
    ON drugdb.ingredient_interactions (md5(description));

COMMENT ON COLUMN drugdb.ingredient_interactions.severity IS
'Interaction severity: contraindicated | major | moderate | minor | unknown.
DEFAULT ''unknown'' for all rows until enrichment pipeline runs.
Classification path:
  Stage 1: regex SQL pre-filter (~15% of rows, no LLM cost)
  Stage 2-5: Alibaba Qwen-Flash Batch API for remaining rows
  Stage 6: SQL mirror copies A→B result to B→A duplicate row
Populated by: scripts/enrich_severity_mechanism.py';

COMMENT ON COLUMN drugdb.ingredient_interactions.mechanism IS
'Pharmacological mechanism extracted from description text by LLM.
Short phrase, 3-8 words.
Examples: CYP3A4 inhibition, decreased renal excretion, additive CNS depression.
NULL until LLM enrichment pipeline runs.
Populated by: scripts/enrich_severity_mechanism.py';

-- Verification: confirm columns were added and all rows start as unknown
SELECT
    COUNT(*)                                       AS total_rows,
    COUNT(severity)                                AS has_severity_col,
    COUNT(*) FILTER (WHERE severity = 'unknown')   AS severity_unknown,
    COUNT(mechanism)                               AS has_mechanism_value
FROM drugdb.ingredient_interactions;
