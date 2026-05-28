# ============================================================================
# CDSS COMPREHENSIVE TEST CASES
# ============================================================================
# Organized by query template. Each test case specifies:
#   - ID, description, priority (P0 = critical, P1 = important, P2 = nice-to-have)
#   - Input
#   - Expected behavior / output
#   - What is being validated
# ============================================================================

# ============================================================================
# SHARED: ENTITY RESOLVER TESTS
# ============================================================================

"""
TEST_ER_01 | P0 | Exact Indian brand name resolution
  INPUT:  resolve_drug("Nelficine")
  EXPECT: formulation_id for NELFINAVIR MESYLATE, match_type = "indian_brand_exact"
  VALIDATES: Indian brand exact matching works

TEST_ER_02 | P0 | FDC decomposition
  INPUT:  resolve_drug("Tenolam-E")
  EXPECT: 3 formulation_ids (Tenofovir, Emtricitabine, Efavirenz), match_type = "indian_fdc_ingredient"
  VALIDATES: Fixed-dose combination decomposition

TEST_ER_03 | P0 | FDA generic name resolution
  INPUT:  resolve_drug("Nelfinavir Mesylate")
  EXPECT: formulation_id for NELFINAVIR MESYLATE, match_type = "fda_generic"
  VALIDATES: Direct generic name match

TEST_ER_04 | P0 | Salt-stripped normalization
  INPUT:  resolve_drug("Nelfinavir")  (without "Mesylate")
  EXPECT: formulation_id for NELFINAVIR MESYLATE, match_type = "normalized_generic"
  VALIDATES: Salt form stripping joins correctly

TEST_ER_05 | P1 | FDA brand name resolution
  INPUT:  resolve_drug("Viracept")
  EXPECT: formulation_id for NELFINAVIR MESYLATE, match_type = "fda_generic"
  VALIDATES: FDA brand name lookup in drug.brand_names[]

TEST_ER_06 | P1 | Fuzzy Indian brand match
  INPUT:  resolve_drug("Nelficin")  (misspelled)
  EXPECT: formulation_id for NELFINAVIR MESYLATE, match_type = "fuzzy_brand"
  VALIDATES: Trigram similarity handles typos

TEST_ER_07 | P1 | Unknown drug name
  INPUT:  resolve_drug("XyzNonExistent123")
  EXPECT: Empty result or DrugNotFoundError
  VALIDATES: Graceful handling of unknown drugs

TEST_ER_08 | P1 | Case insensitivity
  INPUT:  resolve_drug("nelficine"), resolve_drug("NELFICINE"), resolve_drug("NeLfIcInE")
  EXPECT: All return same formulation_id
  VALIDATES: Case-insensitive matching throughout
"""

# ============================================================================
# Q1: DISORDER → MEDICATIONS
# ============================================================================

"""
TEST_Q1_01 | P0 | Basic disorder lookup
  INPUT:  {"disorder": "HIV infection"}
  EXPECT: Response contains NELFINAVIR MESYLATE as a candidate
          Indication term includes "HIV" 
          Indian brands listed for each candidate
  VALIDATES: End-to-end Q1 pipeline

TEST_Q1_02 | P0 | ICD-10 code input
  INPUT:  {"disorder": "B20"}
  EXPECT: Same candidates as free-text "HIV infection"
  VALIDATES: ICD-10 lookup path works

TEST_Q1_03 | P1 | Population filter
  INPUT:  {"disorder": "HIV infection", "population": "pediatric"}
  EXPECT: Only drugs with pediatric indication returned
          Nelfinavir should appear (approved for ≥2 years)
  VALIDATES: Population filter on drug_indication table

TEST_Q1_04 | P1 | Line of therapy filter
  INPUT:  {"disorder": "HIV infection", "line_of_therapy": "first-line"}
  EXPECT: Only first-line drugs returned
  VALIDATES: line_of_therapy filter

TEST_Q1_05 | P1 | No matches found
  INPUT:  {"disorder": "Fictional Disease XYZ"}
  EXPECT: Empty candidate list with message "No drugs found for this disorder"
  VALIDATES: Graceful empty result

TEST_Q1_06 | P2 | Combination requirement noted
  INPUT:  {"disorder": "HIV infection"}
  EXPECT: Nelfinavir's response notes "must be used in combination with antiretroviral agents"
  VALIDATES: combination_required and combination_agents are surfaced

TEST_Q1_07 | P0 | Evidence citations present
  INPUT:  {"disorder": "HIV infection"}
  EXPECT: Response contains at least one citation in [section_name] format
  VALIDATES: LLM composition includes citations

TEST_Q1_08 | P1 | Indian brands present
  INPUT:  {"disorder": "HIV infection"}
  EXPECT: Each candidate drug has an "indian_brands" array (may be empty if no mapping)
  VALIDATES: Indian brand translation layer runs
"""

# ============================================================================
# Q2: INTERACTION CHECK
# ============================================================================

"""
TEST_Q2_01 | P0 | Known contraindicated pair
  INPUT:  {"drugs": ["Nelfinavir", "Simvastatin"]}
  EXPECT: Interaction detected with severity = "contraindicated"
          Response starts with WARNING block
          hard_blocks array is non-empty
  VALIDATES: Contraindicated interactions are detected and hard-blocked

TEST_Q2_02 | P0 | No interaction between safe pair
  INPUT:  {"drugs": ["Nelfinavir", "Metformin"]}
  EXPECT: No interactions detected (or only minor)
          No hard_blocks
  VALIDATES: No false positives for unrelated drugs

TEST_Q2_03 | P0 | FDC interaction check
  INPUT:  {"drugs": ["Tenolam-E", "Simvastatin"]}
  EXPECT: Entity resolver decomposes Tenolam-E into 3 ingredients
          Interaction check runs for ALL 3 against Simvastatin
          If any component interacts with Simvastatin, it is reported
  VALIDATES: FDC decomposition + per-ingredient interaction checking

TEST_Q2_04 | P0 | Indian brand input
  INPUT:  {"drugs": ["Nelficine", "Simvastatin"]}
  EXPECT: Same result as TEST_Q2_01 (Nelficine = Nelfinavir)
  VALIDATES: Indian brand resolution in interaction check

TEST_Q2_05 | P1 | Three-drug interaction matrix
  INPUT:  {"drugs": ["Nelfinavir", "Simvastatin", "Warfarin"]}
  EXPECT: Pairwise check for all 3 pairs (3 pairs)
          At least Nelfinavir-Simvastatin flagged
  VALIDATES: Multi-drug pairwise checking

TEST_Q2_06 | P1 | Adding a new drug to existing regimen
  INPUT:  {"drugs": ["Nelfinavir", "Metformin"], "adding_drug": "Rifampin"}
  EXPECT: Rifampin checked against both Nelfinavir and Metformin
  VALIDATES: adding_drug parameter works

TEST_Q2_07 | P1 | Shared metabolic pathway risk
  INPUT:  {"drugs": ["DrugA_CYP3A4_substrate", "DrugB_CYP3A4_inhibitor"]}
  EXPECT: pathway_risks includes shared CYP3A4 pathway
  VALIDATES: Neo4j graph traversal for indirect risks

TEST_Q2_08 | P0 | Severity floor enforcement (post-check)
  INPUT:  Any pair with severity = "contraindicated"
  EXPECT: Post-check passes if LLM mentions "contraindicated"
          Post-check FAILS if LLM says "may proceed" or omits the warning
  VALIDATES: Post-check guardrail on severity downgrade

TEST_Q2_09 | P1 | Excipient interaction flagged separately
  INPUT:  {"drugs": ["Nelfinavir", "Atazanavir"]}
  EXPECT: If interaction is from excipient (e.g., Calcium silicate), 
          response notes "interaction involves excipient, not active ingredient"
  VALIDATES: subject_substance_role distinction in output

TEST_Q2_10 | P2 | Single drug input
  INPUT:  {"drugs": ["Nelfinavir"]}
  EXPECT: No interactions (nothing to pair with)
          Response: "Only one drug provided — no interactions to check"
  VALIDATES: Edge case handling
"""

# ============================================================================
# Q3: ALTERNATIVES
# ============================================================================

"""
TEST_Q3_01 | P0 | Basic alternative search
  INPUT:  {"drug": "Nelfinavir", "patient_meds": ["Rifampin"]}
  EXPECT: Returns drugs in same therapeutic class (HIV protease inhibitors)
          All returned alternatives have no contraindicated/major interactions with Rifampin
          Indian brands listed for each alternative
  VALIDATES: Class-based alternative finding with interaction filtering

TEST_Q3_02 | P0 | No alternatives found
  INPUT:  {"drug": "UniqueClassDrugX", "patient_meds": ["InteractsWithEverything"]}
  EXPECT: Empty alternatives list with message
  VALIDATES: Graceful empty result

TEST_Q3_03 | P1 | Alternatives ranked by interaction count
  INPUT:  {"drug": "Nelfinavir", "patient_meds": ["Rifampin", "Metformin", "Warfarin"]}
  EXPECT: Alternatives sorted by total_interactions ascending
  VALIDATES: Ranking logic

TEST_Q3_04 | P1 | Indian brand input
  INPUT:  {"drug": "Nelficine", "patient_meds": ["Rifampin"]}
  EXPECT: Same alternatives as TEST_Q3_01
  VALIDATES: Indian brand resolution for target drug

TEST_Q3_05 | P2 | Reason for switch noted
  INPUT:  {"drug": "Nelfinavir", "patient_meds": ["Rifampin"], "reason": "interaction with Rifampin"}
  EXPECT: Response mentions the reason in context
  VALIDATES: Reason parameter influences LLM output
"""

# ============================================================================
# Q4: DOSE RECOMMENDATION
# ============================================================================

"""
TEST_Q4_01 | P0 | Adult standard dose
  INPUT:  {"drug": "Nelfinavir", "age": 35, "weight_kg": 70, "sex": "male",
           "renal": "normal", "hepatic": "normal", "pregnancy": "not_pregnant"}
  EXPECT: computed_dose = 1250 mg, frequency = BID (or 750 mg TID)
          Regimen matched on adult population
  VALIDATES: Adult dosing lookup and computation

TEST_Q4_02 | P0 | Pediatric weight-based dose
  INPUT:  {"drug": "Nelfinavir", "age": 9, "weight_kg": 25, "sex": "female",
           "renal": "normal", "hepatic": "normal", "pregnancy": "not_pregnant"}
  EXPECT: Weight-based dose computed (45-55 mg/kg/day)
          Dose rounded to nearest available strength (250 mg)
          Regimen matched on pediatric population
  VALIDATES: per_kg dose computation with strength rounding

TEST_Q4_03 | P0 | Contraindicated population
  INPUT:  {"drug": "Nelfinavir", "age": 50, "weight_kg": 70, "sex": "female",
           "renal": "normal", "hepatic": "severe_impairment", "pregnancy": "not_pregnant"}
  EXPECT: Hard block: "Drug should not be used in severe hepatic impairment"
  VALIDATES: CONTRAINDICATED regimen returns hard block

TEST_Q4_04 | P0 | Dose adjustment from current med
  INPUT:  {"drug": "Nelfinavir", "age": 35, "weight_kg": 70, "sex": "male",
           "renal": "normal", "hepatic": "normal", "current_meds": ["Rifabutin"]}
  EXPECT: Adjustment note about rifabutin coadministration
          May mention: "reduce rifabutin dose to 150 mg QD"
  VALIDATES: adjustment_required_for field triggers adjustment note

TEST_Q4_05 | P0 | Dose sanity post-check
  INPUT:  Same as TEST_Q4_01
  EXPECT: Post-check passes: LLM-stated dose matches computed_dose
  VALIDATES: Post-check catches dose discrepancy

TEST_Q4_06 | P1 | Indian brand with correct strength
  INPUT:  {"drug": "Nelficine", "age": 35, "weight_kg": 70}
  EXPECT: Indian brands filtered to match computed dose strength (625 mg preferred)
  VALIDATES: Indian brand strength filtering

TEST_Q4_07 | P1 | Administration timing included
  INPUT:  Any standard Q4 request
  EXPECT: Response includes "take with food" (from administration_timing)
  VALIDATES: Timing data is included in dose response

TEST_Q4_08 | P1 | No regimen found
  INPUT:  {"drug": "DrugWithNoDosingData", "age": 35, ...}
  EXPECT: Error: "No dosing regimen found for this patient profile"
  VALIDATES: Missing data handled gracefully

TEST_Q4_09 | P0 | Indian brand input
  INPUT:  {"drug": "Nelficine", "age": 35, ...}
  EXPECT: Same dose as generic "Nelfinavir"
  VALIDATES: Brand → generic → dose pipeline
"""

# ============================================================================
# Q5: POPULATION APPROVAL
# ============================================================================

"""
TEST_Q5_01 | P0 | Approved population
  INPUT:  {"drug": "Viracept", "population": "pediatric"}
  EXPECT: status = "approved", approved_age_range = "2-13 years" (or similar)
          Evidence from pediatric_use section cited
  VALIDATES: Positive approval lookup

TEST_Q5_02 | P0 | Not studied population
  INPUT:  {"drug": "Viracept", "population": "geriatric"}
  EXPECT: status = "not_studied" or "studied_not_approved"
          Notes mention insufficient subjects aged 65+
  VALIDATES: Negative/unknown approval status

TEST_Q5_03 | P1 | Pregnancy benefit-risk
  INPUT:  {"drug": "Viracept", "population": "pregnant"}
  EXPECT: status = "benefit_risk"
          Notes mention pregnancy registry
  VALIDATES: Pregnancy-specific fields (category, registry)

TEST_Q5_04 | P1 | Lactating
  INPUT:  {"drug": "Viracept", "population": "lactating"}
  EXPECT: status = "not_recommended" or "contraindicated"
          Notes mention HIV-infected mothers should not breastfeed
  VALIDATES: Lactation status

TEST_Q5_05 | P1 | Indian brand input
  INPUT:  {"drug": "Nelficine", "population": "pediatric"}
  EXPECT: Same result as TEST_Q5_01
  VALIDATES: Brand resolution for population check

TEST_Q5_06 | P2 | Indian brands only shown if approved
  INPUT:  {"drug": "Viracept", "population": "geriatric"}
  EXPECT: indian_brands array is empty or not shown (drug not approved for geriatric)
  VALIDATES: Brand display gated on approval status
"""

# ============================================================================
# Q6: SAFE DRUGS FOR CONDITION
# ============================================================================

"""
TEST_Q6_01 | P0 | Basic safe drug search
  INPUT:  {"condition": "HIV infection", "patient_meds": ["Rifampin"]}
  EXPECT: Returns drugs indicated for HIV that do NOT have severe interactions with Rifampin
          Nelfinavir may be EXCLUDED (if contraindicated with Rifampin)
          Each safe candidate has indian_brands
  VALIDATES: Q1 + Q2 composition works

TEST_Q6_02 | P0 | Excluded drugs reported
  INPUT:  Same as TEST_Q6_01
  EXPECT: Response lists which drugs were EXCLUDED and why
  VALIDATES: Exclusion reporting

TEST_Q6_03 | P1 | Multiple patient meds
  INPUT:  {"condition": "HIV infection", "patient_meds": ["Rifampin", "Warfarin", "Simvastatin"]}
  EXPECT: Candidates checked against ALL patient meds
          Drug excluded if severe interaction with ANY patient med
  VALIDATES: Multi-med safety filter

TEST_Q6_04 | P1 | No safe candidates
  INPUT:  {"condition": "Very rare condition", "patient_meds": [...]}
  EXPECT: Empty list with clear message
  VALIDATES: Graceful empty result

TEST_Q6_05 | P0 | FDC in patient meds
  INPUT:  {"condition": "HIV infection", "patient_meds": ["Tenolam-E"]}
  EXPECT: Tenolam-E decomposed to 3 ingredients
          Candidates checked against all 3 components
  VALIDATES: FDC handling in patient med list
"""

# ============================================================================
# Q7: ORGAN IMPAIRMENT DOSING
# ============================================================================

"""
TEST_Q7_01 | P0 | Mild hepatic — no adjustment
  INPUT:  {"drug": "Viracept", "impairment_type": "hepatic", "severity": "mild_impairment"}
  EXPECT: "No dose adjustment required for mild hepatic impairment" [cite section 2.4]
  VALIDATES: Label says mild is OK without adjustment

TEST_Q7_02 | P0 | Moderate/Severe hepatic — contraindicated
  INPUT:  {"drug": "Viracept", "impairment_type": "hepatic", "severity": "moderate_impairment"}
  EXPECT: Hard block or WARNING: "Should not be used in moderate or severe hepatic impairment"
  VALIDATES: Contraindicated impairment level blocked

TEST_Q7_03 | P1 | Renal — no data
  INPUT:  {"drug": "Viracept", "impairment_type": "renal", "severity": "severe_impairment"}
  EXPECT: "Safety and efficacy have not been established in renal impairment" [cite section 8.7]
  VALIDATES: Missing data communicated clearly

TEST_Q7_04 | P1 | Indian brand input
  INPUT:  {"drug": "Nelficine", "impairment_type": "hepatic", "severity": "mild_impairment"}
  EXPECT: Same as TEST_Q7_01
  VALIDATES: Brand resolution for impairment query

TEST_Q7_05 | P2 | Citations present
  INPUT:  Any Q7 request
  EXPECT: Response cites either section 2.4 (dosage) or section 8.x (specific populations)
  VALIDATES: Evidence trail
"""

# ============================================================================
# Q8: ADMINISTRATION TIMING
# ============================================================================

"""
TEST_Q8_01 | P0 | Food requirement
  INPUT:  {"drug": "Viracept"}
  EXPECT: food_requirement = "Take with food or a meal"
  VALIDATES: Deterministic food extraction works

TEST_Q8_02 | P0 | Drug separation with didanosine
  INPUT:  {"drug": "Viracept", "current_meds": ["Didanosine"]}
  EXPECT: drug_separations includes: "Take VIRACEPT 1 hour after or 2 hours before Didanosine"
  VALIDATES: Time-separation detection for specific co-prescribed drug

TEST_Q8_03 | P1 | Current med with no separation needed
  INPUT:  {"drug": "Viracept", "current_meds": ["Metformin"]}
  EXPECT: No drug separations reported
          Only food requirement shown
  VALIDATES: No false positives for unrelated meds

TEST_Q8_04 | P1 | No current meds
  INPUT:  {"drug": "Viracept"}
  EXPECT: Food requirement only, no separation section
  VALIDATES: Works without current_meds parameter

TEST_Q8_05 | P1 | Indian brand input
  INPUT:  {"drug": "Nelficine", "current_meds": ["Didanosine"]}
  EXPECT: Same as TEST_Q8_02
  VALIDATES: Brand resolution

TEST_Q8_06 | P2 | Drug with "empty stomach" requirement
  INPUT:  {"drug": "SomeDrugRequiringEmptyStomach"}
  EXPECT: food_requirement = "Take on an empty stomach"
  VALIDATES: Empty stomach detection
"""

# ============================================================================
# Q9: PILL BURDEN
# ============================================================================

"""
TEST_Q9_01 | P0 | Basic pill burden — 625mg wins
  INPUT:  {"drug": "Viracept", "daily_dose_mg": 2500, "frequency": "BID"}
  EXPECT: 
    recommendation: strength=625 MG, pills_per_dose=2, total_daily_pills=4
    comparison includes:
      250 MG: 5 pills/dose × 2 = 10/day
      625 MG: 2 pills/dose × 2 = 4/day
  VALIDATES: Correct arithmetic, correct strength recommendation

TEST_Q9_02 | P0 | TID frequency
  INPUT:  {"drug": "Viracept", "daily_dose_mg": 2250, "frequency": "TID"}
  EXPECT:
    dose_per_admin = 750 mg
    250 MG: ceil(750/250) = 3 pills × 3 = 9/day
    625 MG: ceil(750/625) = 2 pills × 3 = 6/day (but 2×625=1250 vs needed 750 → wastage 500mg)
    Recommendation should account for wastage
  VALIDATES: Wastage calculation influences recommendation

TEST_Q9_03 | P0 | No LLM call made
  INPUT:  Any Q9 request
  EXPECT: Response is pure structured data, no "evidence" or "citations" field
  VALIDATES: Q9 bypasses LLM entirely

TEST_Q9_04 | P1 | Indian brand strengths included
  INPUT:  {"drug": "Nelficine", "daily_dose_mg": 2500, "frequency": "BID"}
  EXPECT: indian_brands grouped by strength, each with brand name, manufacturer, MRP
  VALIDATES: Indian brand integration in pill burden

TEST_Q9_05 | P1 | Single strength available
  INPUT:  {"drug": "DrugWithOnlyOneStrength", "daily_dose_mg": 500, "frequency": "QD"}
  EXPECT: Only one option in comparison, that option is the recommendation
  VALIDATES: Edge case handling

TEST_Q9_06 | P1 | No strength data
  INPUT:  {"drug": "DrugWithNoStrengthInfo", "daily_dose_mg": 500, "frequency": "QD"}
  EXPECT: Error: "No strength information available for this drug"
  VALIDATES: Missing data error
"""

# ============================================================================
# CROSS-CUTTING TESTS
# ============================================================================

"""
TEST_CC_01 | P0 | Audit log written for every query
  INPUT:  Any request to any endpoint
  EXPECT: query_audit_log table has a new row with:
          query_template, request_payload, resolved_drugs, response_payload, response_time_ms
  VALIDATES: Audit compliance

TEST_CC_02 | P0 | Invalid request body rejected
  INPUT:  POST /api/v1/query/dose-recommendation with missing "drug" field
  EXPECT: 422 Unprocessable Entity with Pydantic validation error
  VALIDATES: Input validation

TEST_CC_03 | P1 | Response time < 5 seconds (excluding LLM)
  INPUT:  Any Q8 or Q9 request (no LLM)
  EXPECT: response_time_ms < 1000
  VALIDATES: SQL/computation performance

TEST_CC_04 | P1 | Response time < 15 seconds (with LLM)
  INPUT:  Any Q1-Q7 request
  EXPECT: response_time_ms < 15000
  VALIDATES: End-to-end latency acceptable for clinical use

TEST_CC_05 | P0 | Health check endpoint
  INPUT:  GET /health
  EXPECT: {"status": "ok", "postgres": "connected", "neo4j": "connected", 
           "vllm": "ready", "embedding": "ready"}
  VALIDATES: Dependency health monitoring

TEST_CC_06 | P1 | Concurrent requests
  INPUT:  10 simultaneous Q2 requests
  EXPECT: All return correct results, no deadlocks, no connection pool exhaustion
  VALIDATES: Concurrency handling
"""

# ============================================================================
# SALT NORMALIZER UNIT TESTS (run independently)
# ============================================================================

"""
TEST_SN_01 | NELFINAVIR MESYLATE          → NELFINAVIR
TEST_SN_02 | Amlodipine Besylate          → AMLODIPINE
TEST_SN_03 | Tenofovir Disoproxil Fumarate → TENOFOVIR
TEST_SN_04 | Metformin Hydrochloride      → METFORMIN
TEST_SN_05 | Omeprazole                    → OMEPRAZOLE  (no salt)
TEST_SN_06 | Amoxicillin Trihydrate       → AMOXICILLIN
TEST_SN_07 | Atorvastatin Calcium         → ATORVASTATIN
TEST_SN_08 | Clopidogrel Bisulfate        → CLOPIDOGREL
TEST_SN_09 | Pantoprazole Sodium          → PANTOPRAZOLE
TEST_SN_10 | Losartan Potassium           → LOSARTAN
TEST_SN_11 | Cefpodoxime Proxetil         → CEFPODOXIME
TEST_SN_12 | Olmesartan Medoxomil         → OLMESARTAN
TEST_SN_13 | Montelukast Sodium           → MONTELUKAST
TEST_SN_14 | Rosuvastatin Calcium         → ROSUVASTATIN
TEST_SN_15 | Escitalopram Oxalate         → ESCITALOPRAM  (test with unlisted salt)
"""

# ============================================================================
# DOSAGE FORM NORMALIZER UNIT TESTS
# ============================================================================

"""
TEST_DF_01 | TABLET, FILM COATED           → TABLET
TEST_DF_02 | SR Tablet                     → TABLET_ER
TEST_DF_03 | Dry Syrup                     → POWDER_ORAL
TEST_DF_04 | Capsule, Extended Release     → CAPSULE_ER
TEST_DF_05 | Rotacap                       → INHALER_DPI
TEST_DF_06 | Eye Drops                     → EYE_DROP
TEST_DF_07 | MD Tablet                     → TABLET_ODT
TEST_DF_08 | Film Coated Tablet            → TABLET
TEST_DF_09 | Injection, Powder, Lyophilized → INJECTION_LYPHO
TEST_DF_10 | Transdermal Patch             → PATCH
"""