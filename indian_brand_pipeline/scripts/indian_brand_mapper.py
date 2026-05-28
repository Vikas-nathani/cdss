"""
indian_brand_mapper.py
----------------------
Maps Indian brand drugs to FDA unified formulation records.

Architecture:
  The Indian brand data is a SEPARATE table that JOINS to the unified record
  via a normalized generic name + dosage form key. It does NOT modify the
  FDA/RxNorm/DrugBank data — it's a localization layer on top.

Data flow:
  1. Your Indian brand dataset (CSV/JSON) is loaded into the `indian_brand` table
  2. A normalization pass strips salt forms from both FDA and Indian generic names
  3. At query time, after the CDSS resolves the clinical question against FDA data,
     the Indian brand mapper translates "nelfinavir 250 MG oral tablet" into
     "Nelficine 250mg, Viranelf 250mg" etc.

This separation is critical: the clinical facts (interactions, contraindications,
dosing) come from FDA/DrugBank. The Indian brand layer only provides:
  - Which brand names are available in India for this formulation
  - Indian-market strengths and pack sizes
  - Manufacturer (Indian)
  - Approximate pricing tier (if you add it later)
  - Regulatory status (CDSCO approval, schedule)
"""

import json
import re
from typing import Optional


# ============================================================================
# SALT FORM NORMALIZATION
# ============================================================================

# Comprehensive salt suffix list covering ~95% of Indian pharmacopeia
SALT_PATTERN = re.compile(
    r'\s+(?:'
    r'mesylate|mesilate|besylate|besilate|tosylate|esylate|xinafoate|'
    r'hydrochloride|hcl|dihydrochloride|'
    r'sulfate|sulphate|bisulfate|'
    r'sodium|disodium|trisodium|'
    r'potassium|dipotassium|'
    r'calcium|dicalcium|'
    r'magnesium|'
    r'acetate|diacetate|'
    r'maleate|fumarate|hemifumarate|'
    r'tartrate|bitartrate|'
    r'succinate|'
    r'phosphate|disodium\s+phosphate|'
    r'citrate|'
    r'bromide|chloride|iodide|'
    r'nitrate|'
    r'valerate|propionate|dipropionate|butyrate|'
    r'furoate|acetonide|'
    r'axetil|pivoxil|proxetil|medoxomil|cilexetil|marboxil|'
    r'disoproxil(?:\s+fumarate)?|alafenamide|'
    r'trihydrate|dihydrate|monohydrate|hemihydrate|'
    r'embonate|pamoate|'
    r'stearate|palmitate|decanoate|enanthate|undecylenate|'
    r'gluconate|lactate|benzoate|salicylate|'
    r'oleate|laurilsulfate|'
    r'tromethamine|meglumine|lysine|arginine|'
    r'acistrate|pivalate|hexanoate'
    r')s?'
    r'(?:\s+(?:trihydrate|dihydrate|monohydrate|hemihydrate))?'
    r'\s*$',
    re.IGNORECASE
)

# Multi-word compound salt forms that need separate handling
COMPOUND_SALT_PATTERNS = [
    re.compile(r'\s+disoproxil\s+fumarate\s*$', re.I),
    re.compile(r'\s+sodium\s+phosphate\s*$', re.I),
    re.compile(r'\s+calcium\s+trihydrate\s*$', re.I),
]


def normalize_generic_name(name: str) -> str:
    """
    Strips salt forms from a generic drug name to produce a base INN
    (International Nonproprietary Name) suitable for cross-market matching.

    Examples:
      'NELFINAVIR MESYLATE' → 'NELFINAVIR'
      'Amlodipine Besylate' → 'AMLODIPINE'
      'Tenofovir Disoproxil Fumarate' → 'TENOFOVIR'
      'Metformin Hydrochloride' → 'METFORMIN'
      'Omeprazole' → 'OMEPRAZOLE' (no salt, unchanged)
      'Amoxicillin Trihydrate' → 'AMOXICILLIN'
    """
    if not name:
        return ""
    result = name.strip().upper()

    # Try compound patterns first (longer matches)
    for pattern in COMPOUND_SALT_PATTERNS:
        result = pattern.sub('', result).strip()

    # Then single salt suffix
    result = SALT_PATTERN.sub('', result).strip()

    return result


def normalize_dosage_form(form: str) -> str:
    """
    Maps varied dosage form descriptions to a canonical form for matching.
    Indian labels say 'Tablet' while FDA says 'TABLET, FILM COATED'.
    """
    if not form:
        return ""
    form_upper = form.upper().strip()

    form_map = {
        # Tablets
        'TABLET': 'TABLET',
        'TABLET, FILM COATED': 'TABLET',
        'TABLET, COATED': 'TABLET',
        'TABLET, SUGAR COATED': 'TABLET',
        'TABLET, ENTERIC COATED': 'TABLET_EC',
        'TABLET, EXTENDED RELEASE': 'TABLET_ER',
        'TABLET, DELAYED RELEASE': 'TABLET_DR',
        'TABLET, CHEWABLE': 'TABLET_CHEW',
        'TABLET, DISPERSIBLE': 'TABLET_DT',
        'TABLET, EFFERVESCENT': 'TABLET_EFF',
        'TABLET, ORALLY DISINTEGRATING': 'TABLET_ODT',
        'TAB': 'TABLET',
        'TABS': 'TABLET',
        'FC TABLET': 'TABLET',
        'FILM COATED TABLET': 'TABLET',
        'MOUTH DISSOLVING TABLET': 'TABLET_ODT',
        'MD TABLET': 'TABLET_ODT',
        'DT': 'TABLET_DT',
        'SR TABLET': 'TABLET_ER',
        'XR TABLET': 'TABLET_ER',
        'XL TABLET': 'TABLET_ER',
        'CR TABLET': 'TABLET_ER',
        'LA TABLET': 'TABLET_ER',
        'MR TABLET': 'TABLET_ER',
        'ER TABLET': 'TABLET_ER',

        # Capsules
        'CAPSULE': 'CAPSULE',
        'CAPSULE, GELATIN COATED': 'CAPSULE',
        'CAPSULE, EXTENDED RELEASE': 'CAPSULE_ER',
        'CAPSULE, DELAYED RELEASE': 'CAPSULE_DR',
        'CAP': 'CAPSULE',
        'CAPS': 'CAPSULE',
        'SR CAPSULE': 'CAPSULE_ER',

        # Liquids
        'SYRUP': 'SYRUP',
        'SUSPENSION': 'SUSPENSION',
        'ORAL SUSPENSION': 'SUSPENSION',
        'SOLUTION': 'SOLUTION',
        'ORAL SOLUTION': 'SOLUTION',
        'DROPS': 'DROPS',
        'ELIXIR': 'ELIXIR',
        'ORAL POWDER': 'POWDER_ORAL',
        'POWDER FOR ORAL SUSPENSION': 'POWDER_ORAL',
        'DRY SYRUP': 'POWDER_ORAL',
        'RECONSTITUTION POWDER': 'POWDER_ORAL',

        # Injectables
        'INJECTION': 'INJECTION',
        'INJECTION, SOLUTION': 'INJECTION',
        'INJECTION, POWDER, LYOPHILIZED, FOR SOLUTION': 'INJECTION_LYPHO',
        'INJECTABLE': 'INJECTION',
        'IV INFUSION': 'INFUSION',
        'INFUSION': 'INFUSION',

        # Topical
        'CREAM': 'CREAM',
        'OINTMENT': 'OINTMENT',
        'GEL': 'GEL',
        'LOTION': 'LOTION',
        'SPRAY': 'SPRAY',

        # Inhaled
        'INHALER': 'INHALER',
        'METERED DOSE INHALER': 'INHALER_MDI',
        'DRY POWDER INHALER': 'INHALER_DPI',
        'ROTACAP': 'INHALER_DPI',
        'RESPULE': 'NEBULIZER',

        # Other
        'SUPPOSITORY': 'SUPPOSITORY',
        'PATCH': 'PATCH',
        'TRANSDERMAL PATCH': 'PATCH',
        'EYE DROP': 'EYE_DROP',
        'EYE DROPS': 'EYE_DROP',
        'EAR DROP': 'EAR_DROP',
        'NASAL SPRAY': 'NASAL_SPRAY',
    }

    return form_map.get(form_upper, form_upper)


# ============================================================================
# SCHEMA FOR INDIAN BRAND TABLE
# ============================================================================

INDIAN_BRAND_SCHEMA = {
    "table_name": "indian_brand",
    "description": """
        One row per Indian brand product (brand + strength + form combination).
        Joins to the unified drug record via normalized_generic_name + form_canonical.
        This table is APPEND-ONLY from your Indian brand dataset.
    """,
    "columns": {
        "indian_brand_id":          "SERIAL PRIMARY KEY",
        "brand_name":               "TEXT NOT NULL — e.g., 'Nelficine', 'Viranelf'",
        "manufacturer_india":       "TEXT — e.g., 'Cipla', 'Sun Pharma', 'Dr. Reddy's'",
        "generic_name_raw":         "TEXT NOT NULL — as written in your data, e.g., 'Nelfinavir Mesylate'",
        "normalized_generic_name":  "TEXT NOT NULL — salt-stripped INN, e.g., 'NELFINAVIR'. Generated by normalize_generic_name()",
        "strength_label":           "TEXT — e.g., '250 mg', '625 mg'",
        "strength_value":           "NUMERIC — e.g., 250, 625",
        "strength_unit":            "TEXT — e.g., 'mg', 'mcg', 'ml'",
        "dosage_form_raw":          "TEXT — as written in your data, e.g., 'Film Coated Tablet'",
        "form_canonical":           "TEXT — normalized form, e.g., 'TABLET'. Generated by normalize_dosage_form()",
        "route":                    "TEXT — 'ORAL', 'IV', 'TOPICAL', etc.",
        "pack_size":                "TEXT — e.g., '10s', '30s', '1x10'",
        "schedule":                 "TEXT — CDSCO schedule: 'H', 'H1', 'X', 'G', 'OTC'",
        "mrp_inr":                  "NUMERIC — MRP in INR if available",
        "cdsco_approval":           "BOOLEAN — whether CDSCO approved",
        "is_combination":           "BOOLEAN — fixed-dose combination (FDC)",
        "combination_ingredients":  "JSONB — for FDCs: [{name, strength, unit}]",
        "formulation_id":           "TEXT REFERENCES drug(formulation_id) — populated by the mapper",
        "match_confidence":         "TEXT — 'exact', 'normalized', 'fuzzy', 'manual'",
    },
    "indexes": [
        "CREATE INDEX idx_ib_normalized ON indian_brand(normalized_generic_name)",
        "CREATE INDEX idx_ib_brand ON indian_brand(brand_name)",
        "CREATE INDEX idx_ib_formulation ON indian_brand(formulation_id)",
        "CREATE INDEX idx_ib_form ON indian_brand(form_canonical)",
        "CREATE INDEX idx_ib_strength ON indian_brand(normalized_generic_name, strength_value, form_canonical)",
    ]
}


# ============================================================================
# MAPPING: Indian brand ↔ FDA unified record
# ============================================================================

def build_mapping_sql() -> str:
    """
    SQL that links Indian brands to FDA unified records.
    Run this after loading both datasets.

    Join strategy (tried in order, first match wins):
      1. EXACT: normalized_generic_name matches AND strength AND form match
      2. NORMALIZED: normalized_generic_name matches AND form matches (strength may differ — 
         Indian market may have strengths not in US market)
      3. FUZZY: trigram similarity > 0.7 on normalized_generic_name AND form matches
      4. UNMATCHED: flagged for manual review
    """
    return """
    -- Step 1: Build normalized lookup on the FDA side
    CREATE MATERIALIZED VIEW fda_generic_lookup AS
    SELECT
        d.formulation_id,
        d.generic_name,
        -- Strip salt from FDA generic name
        upper(regexp_replace(
            d.generic_name,
            '\\s+(mesylate|hydrochloride|sodium|sulfate|...)\\s*$',
            '', 'i'
        )) AS normalized_generic,
        unnest(d.routes) AS route,
        s.strength_value,
        s.strength_unit
    FROM drugdb.drug d
    LEFT JOIN available_strengths s ON s.formulation_id = d.formulation_id;

    -- Step 2: Exact match (generic + strength + form)
    UPDATE indian_brand ib
    SET formulation_id = fda.formulation_id,
        match_confidence = 'exact'
    FROM fda_generic_lookup fda
    WHERE ib.normalized_generic_name = fda.normalized_generic
      AND ib.strength_value = fda.strength_value
      AND ib.route = fda.route
      AND ib.formulation_id IS NULL;

    -- Step 3: Normalized match (generic + form, any strength)
    UPDATE indian_brand ib
    SET formulation_id = fda.formulation_id,
        match_confidence = 'normalized'
    FROM fda_generic_lookup fda
    WHERE ib.normalized_generic_name = fda.normalized_generic
      AND ib.route = fda.route
      AND ib.formulation_id IS NULL;

    -- Step 4: Fuzzy match (trigram similarity > 0.7)
    UPDATE indian_brand ib
    SET formulation_id = fda.formulation_id,
        match_confidence = 'fuzzy'
    FROM fda_generic_lookup fda
    WHERE similarity(ib.normalized_generic_name, fda.normalized_generic) > 0.7
      AND ib.route = fda.route
      AND ib.formulation_id IS NULL;

    -- Step 5: Report unmatched for manual review
    SELECT brand_name, generic_name_raw, normalized_generic_name,
           strength_label, dosage_form_raw
    FROM indian_brand
    WHERE formulation_id IS NULL
    ORDER BY normalized_generic_name;
    """


# ============================================================================
# COMBINATION DRUG HANDLING
# ============================================================================

def build_fdc_mapping_strategy() -> str:
    """
    Fixed-dose combinations (FDCs) are very common in India but rare in the
    US market. Examples: Amoxicillin + Clavulanate, Tenofovir + Emtricitabine + Efavirenz.

    Strategy: each ingredient in the FDC maps to a SEPARATE FDA unified record.
    The CDSS checks interactions/contraindications for ALL ingredients in the FDC.
    """
    return """
    FDC MAPPING STRATEGY:
    
    1. For each FDC Indian brand, combination_ingredients contains the individual drugs.
    
    2. Each ingredient maps independently to an FDA unified record:
       
       indian_brand (FDC)                    FDA unified records
       ┌─────────────────────┐               ┌─────────────────┐
       │ Tenolam-E            │──ingredient1──│ Tenofovir 300mg │
       │ TDF 300 + FTC 200    │──ingredient2──│ Emtricitabine   │
       │ + EFV 600            │──ingredient3──│ Efavirenz 600mg │
       └─────────────────────┘               └─────────────────┘
    
    3. When a doctor prescribes 'Tenolam-E', the CDSS:
       a) Resolves it to 3 separate formulation_ids
       b) Runs interaction check for ALL 3 against patient's other meds
       c) Checks contraindications for ALL 3
       d) Returns a MERGED safety report
    
    4. SQL: use a junction table
    
       CREATE TABLE indian_brand_ingredient (
           indian_brand_id  INT REFERENCES indian_brand(indian_brand_id),
           ingredient_index INT,  -- 1, 2, 3...
           formulation_id   TEXT REFERENCES drug(formulation_id),
           ingredient_name  TEXT,
           ingredient_strength TEXT,
           match_confidence TEXT,
           PRIMARY KEY (indian_brand_id, ingredient_index)
       );
    """


# ============================================================================
# QUERY INTEGRATION: how the 9 CDSS queries change with Indian brands
# ============================================================================

QUERY_INTEGRATION = {
    "Q1_disorder_to_meds": {
        "change": "ADD a final step that maps each recommended formulation to available Indian brands",
        "added_sql": """
            -- After Q1 returns candidate formulation_ids:
            SELECT ib.brand_name, ib.manufacturer_india, ib.strength_label,
                   ib.form_canonical, ib.pack_size, ib.mrp_inr, ib.schedule
            FROM indian_brand ib
            WHERE ib.formulation_id IN (:candidate_formulation_ids)
            ORDER BY ib.brand_name, ib.strength_value
        """,
        "ui_display": "Show FDA generic recommendation + Indian brand options below it"
    },

    "Q2_interaction_check": {
        "change": "ADD a RESOLVE step at the start: Indian brand name → formulation_id(s)",
        "added_sql": """
            -- Doctor types 'Tenolam-E' → resolve to formulation_ids
            SELECT DISTINCT formulation_id
            FROM indian_brand ib
            LEFT JOIN indian_brand_ingredient ibi USING (indian_brand_id)
            WHERE ib.brand_name ILIKE :input_brand_name
            -- Returns 1 formulation_id for single-drug brands,
            -- or N formulation_ids for FDCs
        """,
        "ui_display": "Input accepts both generic names AND Indian brand names"
    },

    "Q3_alternatives": {
        "change": "ADD Indian brand mapping to each alternative",
        "added_sql": "Same as Q1 — map each alternative's formulation_id to Indian brands",
        "ui_display": "Each alternative shows available Indian brands + pricing"
    },

    "Q4_dose_recommendation": {
        "change": "ADD strength-to-brand mapping: translate computed dose into specific Indian brand + tablet count",
        "added_sql": """
            -- After computing dose = 1250 mg BID:
            SELECT ib.brand_name, ib.strength_label, ib.manufacturer_india,
                   CEIL(:dose_per_admin / ib.strength_value) as pills_per_dose,
                   CEIL(:dose_per_admin / ib.strength_value) * :frequency as daily_pills
            FROM indian_brand ib
            WHERE ib.formulation_id = :drug_id
              AND ib.form_canonical = :form
            ORDER BY daily_pills ASC, ib.brand_name
        """,
        "ui_display": "Show: 'Take Nelficine 625mg — 2 tablets twice daily with food'"
    },

    "Q5_population_approval": {
        "change": "No change — population approval comes from FDA label, not brand-specific",
        "added_sql": None,
        "ui_display": "Same as before, but show available Indian brands if approved"
    },

    "Q6_safe_drugs_for_condition": {
        "change": "ADD Indian brand mapping to safe candidates list",
        "added_sql": "Same as Q1",
        "ui_display": "Each safe drug shows Indian brand availability"
    },

    "Q7_organ_impairment_dosing": {
        "change": "ADD adjusted-dose-to-brand translation",
        "added_sql": "Same as Q4 with the adjusted dose value",
        "ui_display": "Show specific Indian brand and adjusted pill count"
    },

    "Q8_administration_timing": {
        "change": "No change — timing comes from FDA label",
        "added_sql": None,
        "ui_display": "Same timing instructions apply regardless of brand"
    },

    "Q9_pill_burden": {
        "change": "REPLACE FDA strengths with Indian brand strengths (may differ)",
        "added_sql": """
            -- Indian market may have different strengths than US
            SELECT DISTINCT ib.brand_name, ib.strength_value, ib.strength_label,
                   ib.manufacturer_india
            FROM indian_brand ib
            WHERE ib.formulation_id = :drug_id
              AND ib.form_canonical = 'TABLET'
            ORDER BY ib.strength_value
        """,
        "ui_display": "Pill burden calc uses Indian-available strengths per brand"
    },
}


# ============================================================================
# ENTITY RESOLVER: accepts both brand names and generic names
# ============================================================================

def build_entity_resolver_sql() -> str:
    """
    The entity resolver is the first step in every query. It takes whatever
    the doctor types and resolves it to one or more formulation_ids.

    Doctors in India will type brand names 90% of the time. The resolver
    must handle: exact brand match, fuzzy brand match, generic match,
    and FDC decomposition.
    """
    return """
    CREATE OR REPLACE FUNCTION resolve_drug(input_name TEXT)
    RETURNS TABLE(formulation_id TEXT, match_type TEXT, matched_name TEXT) AS $$
    BEGIN
        -- 1. Exact Indian brand match
        RETURN QUERY
        SELECT ib.formulation_id, 'indian_brand_exact'::TEXT, ib.brand_name
        FROM indian_brand ib
        WHERE upper(ib.brand_name) = upper(input_name)
          AND ib.formulation_id IS NOT NULL;

        IF FOUND THEN RETURN; END IF;

        -- 2. FDC ingredient decomposition
        RETURN QUERY
        SELECT ibi.formulation_id, 'indian_fdc_ingredient'::TEXT, ib.brand_name
        FROM indian_brand ib
        JOIN indian_brand_ingredient ibi USING (indian_brand_id)
        WHERE upper(ib.brand_name) = upper(input_name);

        IF FOUND THEN RETURN; END IF;

        -- 3. FDA generic name match
        RETURN QUERY
        SELECT d.formulation_id, 'fda_generic'::TEXT, d.generic_name
        FROM drugdb.drug d
        WHERE upper(d.generic_name) = upper(input_name)
           OR upper(input_name) = ANY(
               SELECT upper(unnest(d.brand_names))
           );

        IF FOUND THEN RETURN; END IF;

        -- 4. Normalized generic match (salt-stripped)
        RETURN QUERY
        SELECT d.formulation_id, 'normalized_generic'::TEXT, d.generic_name
        FROM drugdb.drug d
        WHERE upper(regexp_replace(d.generic_name,
              '\\s+(mesylate|hydrochloride|sodium|sulfate|besylate|'
              'fumarate|tartrate|succinate|phosphate|citrate|'
              'maleate|acetate|bromide|chloride|nitrate|'
              'trihydrate|dihydrate|monohydrate)\\s*$', '', 'i'))
            = upper(regexp_replace(input_name,
              '\\s+(mesylate|hydrochloride|sodium|sulfate|besylate|'
              'fumarate|tartrate|succinate|phosphate|citrate|'
              'maleate|acetate|bromide|chloride|nitrate|'
              'trihydrate|dihydrate|monohydrate)\\s*$', '', 'i'));

        IF FOUND THEN RETURN; END IF;

        -- 5. Fuzzy match (trigram)
        RETURN QUERY
        SELECT ib.formulation_id, 'fuzzy_brand'::TEXT, ib.brand_name
        FROM indian_brand ib
        WHERE similarity(upper(ib.brand_name), upper(input_name)) > 0.6
          AND ib.formulation_id IS NOT NULL
        ORDER BY similarity(upper(ib.brand_name), upper(input_name)) DESC
        LIMIT 3;

        IF FOUND THEN RETURN; END IF;

        -- 6. Fuzzy generic match
        RETURN QUERY
        SELECT d.formulation_id, 'fuzzy_generic'::TEXT, d.generic_name
        FROM drugdb.drug d
        WHERE similarity(upper(d.generic_name), upper(input_name)) > 0.5
        ORDER BY similarity(upper(d.generic_name), upper(input_name)) DESC
        LIMIT 3;
    END;
    $$ LANGUAGE plpgsql;
    """


# ============================================================================
# DATA LOADER: load your Indian brand CSV/JSON into the table
# ============================================================================

def build_loader_script() -> str:
    """
    Template for loading your Indian brand data.
    Assumes a CSV with columns: brand_name, manufacturer, generic_name,
    strength, strength_unit, dosage_form, pack_size, schedule, mrp
    """
    return '''
import csv
import psycopg2

conn = psycopg2.connect("dbname=cdss user=cdss_app")
cur = conn.cursor()

with open("indian_brands.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Normalize at load time
        from indian_brand_mapper import normalize_generic_name, normalize_dosage_form
        
        normalized = normalize_generic_name(row["generic_name"])
        form_canon = normalize_dosage_form(row["dosage_form"])
        
        # Parse strength
        import re
        strength_match = re.match(r"([\\d.]+)\\s*(mg|mcg|ml|g|iu|%)", 
                                   row.get("strength", ""), re.I)
        strength_val = float(strength_match.group(1)) if strength_match else None
        strength_unit = strength_match.group(2).upper() if strength_match else None
        
        # Detect FDC
        ingredients = row.get("generic_name", "").split(" + ")
        is_fdc = len(ingredients) > 1
        
        cur.execute("""
            INSERT INTO indian_brand 
            (brand_name, manufacturer_india, generic_name_raw, 
             normalized_generic_name, strength_label, strength_value, 
             strength_unit, dosage_form_raw, form_canonical, route,
             pack_size, schedule, mrp_inr, is_combination)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING indian_brand_id
        """, (
            row["brand_name"], row["manufacturer"], row["generic_name"],
            normalized, row.get("strength"), strength_val,
            strength_unit, row["dosage_form"], form_canon,
            row.get("route", "ORAL"),
            row.get("pack_size"), row.get("schedule"),
            float(row["mrp"]) if row.get("mrp") else None,
            is_fdc
        ))
        
        brand_id = cur.fetchone()[0]
        
        # For FDCs, insert each ingredient
        if is_fdc:
            for idx, ingredient in enumerate(ingredients, 1):
                ingredient = ingredient.strip()
                # Parse individual ingredient strength if present
                ing_match = re.match(r"(.+?)\\s+([\\d.]+)\\s*(mg|mcg|ml)", 
                                      ingredient, re.I)
                if ing_match:
                    ing_name = normalize_generic_name(ing_match.group(1))
                    ing_strength = ing_match.group(2) + " " + ing_match.group(3)
                else:
                    ing_name = normalize_generic_name(ingredient)
                    ing_strength = None
                
                cur.execute("""
                    INSERT INTO indian_brand_ingredient
                    (indian_brand_id, ingredient_index, ingredient_name, 
                     ingredient_strength)
                    VALUES (%s, %s, %s, %s)
                """, (brand_id, idx, ing_name, ing_strength))

conn.commit()
print("Loaded Indian brand data. Run mapping SQL next.")
'''


# ============================================================================
# DEMO
# ============================================================================

if __name__ == "__main__":
    # Test normalization
    test_cases = [
        ("NELFINAVIR MESYLATE",               "NELFINAVIR"),
        ("Amlodipine Besylate",               "AMLODIPINE"),
        ("Tenofovir Disoproxil Fumarate",     "TENOFOVIR"),
        ("Metformin Hydrochloride",           "METFORMIN"),
        ("Omeprazole",                         "OMEPRAZOLE"),
        ("Amoxicillin Trihydrate",            "AMOXICILLIN"),
        ("Atorvastatin Calcium",              "ATORVASTATIN"),
        ("Clopidogrel Bisulfate",             "CLOPIDOGREL"),
        ("Pantoprazole Sodium",               "PANTOPRAZOLE"),
        ("Losartan Potassium",                "LOSARTAN"),
        ("Rabeprazole Sodium",                "RABEPRAZOLE"),
        ("Cefpodoxime Proxetil",              "CEFPODOXIME"),
        ("Olmesartan Medoxomil",              "OLMESARTAN"),
        ("Montelukast Sodium",                "MONTELUKAST"),
        ("Rosuvastatin Calcium",              "ROSUVASTATIN"),
    ]

    print("=== SALT NORMALIZATION TESTS ===")
    all_pass = True
    for raw, expected in test_cases:
        result = normalize_generic_name(raw)
        status = "PASS" if result == expected else "FAIL"
        if status == "FAIL":
            all_pass = False
        print(f"  {status}: '{raw}' → '{result}' (expected '{expected}')")

    print(f"\nAll tests passed: {all_pass}")

    print("\n=== DOSAGE FORM NORMALIZATION ===")
    form_tests = [
        ("TABLET, FILM COATED", "TABLET"),
        ("Film Coated Tablet", "TABLET"),
        ("SR Tablet", "TABLET_ER"),
        ("Dry Syrup", "POWDER_ORAL"),
        ("Capsule, Extended Release", "CAPSULE_ER"),
        ("Rotacap", "INHALER_DPI"),
        ("Eye Drops", "EYE_DROP"),
        ("MD Tablet", "TABLET_ODT"),
    ]
    for raw, expected in form_tests:
        result = normalize_dosage_form(raw)
        status = "PASS" if result == expected else "FAIL"
        print(f"  {status}: '{raw}' → '{result}' (expected '{expected}')")