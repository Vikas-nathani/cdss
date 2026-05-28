-- Create the schema
CREATE SCHEMA IF NOT EXISTS drugdb;

-- Required for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Define the type within the schema
CREATE TYPE drugdb.ingredient_type AS ENUM ('active', 'inactive');

ALTER TYPE drugdb.ingredient_type ADD VALUE 'both';
-- Function within the schema
CREATE OR REPLACE FUNCTION drugdb.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Main Ingredients Table
CREATE TABLE drugdb.ingredients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    drugbank_id VARCHAR(50),
    unii VARCHAR(50),
    rxcui VARCHAR(50),
    name VARCHAR(255) NOT NULL,
    indications TEXT,
    general_function TEXT,
    type drugdb.ingredient_type,
    pharmacodynamics TEXT,
    classification_description TEXT,
    food_interactions TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by VARCHAR(255)
);

-- Ingredient Synonyms Table
CREATE TABLE drugdb.ingredient_synonyms (
    id UUID REFERENCES drugdb.ingredients(id) ON DELETE CASCADE,
    synonym VARCHAR(500) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by VARCHAR(255),
    PRIMARY KEY (id, synonym)
);

-- Drug Interactions Table
CREATE TABLE drugdb.ingredient_interactions (
    id UUID REFERENCES drugdb.ingredients(id) ON DELETE CASCADE,
    reacting_id UUID REFERENCES drugdb.ingredients(id) ON DELETE CASCADE,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by VARCHAR(255),
    PRIMARY KEY (id, reacting_id)
);

-- Indexes
CREATE INDEX idx_ingredients_drugbank ON drugdb.ingredients(drugbank_id);
CREATE INDEX idx_ingredients_unii ON drugdb.ingredients(unii);
CREATE INDEX idx_ingredients_rxcui ON drugdb.ingredients(rxcui);
CREATE INDEX idx_ingredients_name ON drugdb.ingredients(name);
CREATE INDEX idx_ingredients_updated_at ON drugdb.ingredients(updated_at);
CREATE INDEX idx_synonyms_id ON drugdb.ingredient_synonyms(id);
CREATE INDEX idx_interactions_id ON drugdb.ingredient_interactions(id);
CREATE INDEX idx_interactions_reacting_id ON drugdb.ingredient_interactions(reacting_id);

-- Automated Triggers
CREATE TRIGGER trg_update_ingredients 
BEFORE UPDATE ON drugdb.ingredients 
FOR EACH ROW EXECUTE FUNCTION drugdb.update_timestamp();

CREATE TRIGGER trg_update_synonyms 
BEFORE UPDATE ON drugdb.ingredient_synonyms 
FOR EACH ROW EXECUTE FUNCTION drugdb.update_timestamp();

CREATE TRIGGER trg_update_interactions 
BEFORE UPDATE ON drugdb.ingredient_interactions 
FOR EACH ROW EXECUTE FUNCTION drugdb.update_timestamp();