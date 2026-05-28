-- Add rxcui_source column to drugdb.ingredients
-- Tracks where the rxcui value came from

ALTER TABLE drugdb.ingredients
  ADD COLUMN IF NOT EXISTS rxcui_source VARCHAR(50);

-- Index for fast filtering by source
CREATE INDEX IF NOT EXISTS idx_ingredients_rxcui_source
  ON drugdb.ingredients (rxcui_source);

COMMENT ON COLUMN drugdb.ingredients.rxcui_source IS
  'Source of rxcui value: NULL = original, rxnconso_name_match = filled via rxnconso str lookup';
