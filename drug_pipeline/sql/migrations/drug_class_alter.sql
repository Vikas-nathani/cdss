-- drug_class_alter.sql
-- Adds 5 drug classification columns to drugdb.drug (idempotent)

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'drugdb'
    AND table_name = 'drug'
    AND column_name = 'pharmacologic_class'
  ) THEN
    ALTER TABLE drugdb.drug ADD COLUMN pharmacologic_class TEXT[];
    COMMENT ON COLUMN drugdb.drug.pharmacologic_class IS 'Pharmacologic class from LLM classification e.g. Atypical Antipsychotic';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'drugdb'
    AND table_name = 'drug'
    AND column_name = 'therapeutic_class'
  ) THEN
    ALTER TABLE drugdb.drug ADD COLUMN therapeutic_class TEXT[];
    COMMENT ON COLUMN drugdb.drug.therapeutic_class IS 'Therapeutic/clinical use class from LLM e.g. Antipsychotic, Mood Stabilizer';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'drugdb'
    AND table_name = 'drug'
    AND column_name = 'mechanism_class'
  ) THEN
    ALTER TABLE drugdb.drug ADD COLUMN mechanism_class TEXT[];
    COMMENT ON COLUMN drugdb.drug.mechanism_class IS 'Mechanism of action class from LLM e.g. Dopamine D2 and Serotonin 5HT2 Antagonist';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'drugdb'
    AND table_name = 'drug'
    AND column_name = 'atc_code'
  ) THEN
    ALTER TABLE drugdb.drug ADD COLUMN atc_code VARCHAR(10);
    COMMENT ON COLUMN drugdb.drug.atc_code IS 'WHO ATC code level 3 or 4 from LLM e.g. N05AH';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'drugdb'
    AND table_name = 'drug'
    AND column_name = 'drug_class_source'
  ) THEN
    ALTER TABLE drugdb.drug ADD COLUMN drug_class_source VARCHAR(20);
    COMMENT ON COLUMN drugdb.drug.drug_class_source IS 'Source of classification: llm, rules, or manual';
  END IF;
END $$;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'drugdb'
AND table_name = 'drug'
AND column_name IN (
  'pharmacologic_class',
  'therapeutic_class',
  'mechanism_class',
  'atc_code',
  'drug_class_source'
)
ORDER BY column_name;

SELECT 'drug_class_alter.sql completed successfully' AS status;
