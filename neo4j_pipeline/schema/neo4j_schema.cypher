// ============================================================================
// CDSS Neo4j Schema Initialization
// ============================================================================
// Adds CDSS-specific constraints and indexes alongside existing UMLS data.
// Run once after Neo4j installation.

// ============================================================================
// CONSTRAINTS
// ============================================================================

CREATE CONSTRAINT drug_formulation_id IF NOT EXISTS
FOR (d:Drug) REQUIRE d.formulation_id IS UNIQUE;

CREATE CONSTRAINT ingredient_name IF NOT EXISTS
FOR (i:Ingredient) REQUIRE i.name IS UNIQUE;

CREATE CONSTRAINT enzyme_name IF NOT EXISTS
FOR (e:Enzyme) REQUIRE e.name IS UNIQUE;

CREATE CONSTRAINT target_name IF NOT EXISTS
FOR (t:Target) REQUIRE t.name IS UNIQUE;

CREATE CONSTRAINT drugclass_name IF NOT EXISTS
FOR (c:DrugClass) REQUIRE c.name IS UNIQUE;

CREATE CONSTRAINT indication_icd10 IF NOT EXISTS
FOR (ind:Indication) REQUIRE ind.icd10 IS UNIQUE;

CREATE CONSTRAINT indian_brand_unique IF NOT EXISTS
FOR (ib:IndianBrand) REQUIRE (ib.brand_name, ib.manufacturer_india) IS UNIQUE;

// ============================================================================
// INDEXES
// ============================================================================

CREATE INDEX drug_generic_name IF NOT EXISTS
FOR (d:Drug) ON (d.generic_name);

CREATE INDEX drug_brand_names IF NOT EXISTS
FOR (d:Drug) ON (d.brand_names);

CREATE INDEX drug_class IF NOT EXISTS
FOR (d:Drug) ON (d.drug_class);

CREATE INDEX drug_manufacturer IF NOT EXISTS
FOR (d:Drug) ON (d.manufacturer);

CREATE INDEX ingredient_unii IF NOT EXISTS
FOR (i:Ingredient) ON (i.unii);

CREATE INDEX ingredient_drugbank_id IF NOT EXISTS
FOR (i:Ingredient) ON (i.drugbank_id);

CREATE INDEX ingredient_role IF NOT EXISTS
FOR (i:Ingredient) ON (i.role);

CREATE INDEX enzyme_category IF NOT EXISTS
FOR (e:Enzyme) ON (e.category);

CREATE INDEX indication_term IF NOT EXISTS
FOR (ind:Indication) ON (ind.term);

CREATE INDEX indication_snomed IF NOT EXISTS
FOR (ind:Indication) ON (ind.snomed_code);

CREATE INDEX indian_brand_generic IF NOT EXISTS
FOR (ib:IndianBrand) ON (ib.generic_name_normalized);

CREATE INDEX indian_brand_schedule IF NOT EXISTS
FOR (ib:IndianBrand) ON (ib.schedule);
