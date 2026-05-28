"""
neo4j_populate.py
-----------------
Phase 5: Reads from Postgres and populates Neo4j graph.
Run AFTER Phases 2-4 (all Postgres tables populated) and AFTER Phase 6 (Indian brands loaded).

Handles all 4 issues identified in the schema audit:
  1. INHIBITS/INDUCES edges — extracted from drug_interaction mechanism field
  2. Complete population script with actual Cypher
  3. Partner resolution via normalized_name + drugbank_id fallback
  4. Bidirectional INTERACTS_WITH edges

Usage:
  python neo4j_populate.py
  
Requires:
  - Postgres connection (DATABASE_URL env var)
  - Neo4j connection (NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD env vars)
"""

import os
import re
import asyncio
import asyncpg
from neo4j import AsyncGraphDatabase


# ============================================================================
# CONFIG
# ============================================================================

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://cdss_app:cdss_secure_password@localhost:5432/cdss")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "cdss_neo4j_password")

BATCH_SIZE = 500


# ============================================================================
# STEP 5.1: DRUG NODES
# ============================================================================

async def populate_drug_nodes(pg, neo):
    """Create a :Drug node for every formulation in the drug table."""
    rows = await pg.fetch("""
        SELECT formulation_id, generic_name, normalized_name, brand_names,
               drug_class, product_type, manufacturer, routes
        FROM drugdb.drug
    """)
    
    for i in range(0, len(rows), BATCH_SIZE):
        batch = [dict(r) for r in rows[i:i+BATCH_SIZE]]
        async with neo.session() as session:
            await session.run("""
                UNWIND $batch AS row
                MERGE (d:Drug {formulation_id: row.formulation_id})
                SET d.generic_name = row.generic_name,
                    d.normalized_name = row.normalized_name,
                    d.brand_names = row.brand_names,
                    d.drug_class = row.drug_class,
                    d.product_type = row.product_type,
                    d.manufacturer = row.manufacturer,
                    d.routes = row.routes
            """, batch=batch)
    
    print(f"  Created {len(rows)} Drug nodes")


# ============================================================================
# STEP 5.2: INGREDIENT NODES + CONTAINS_ACTIVE/EXCIPIENT EDGES
# ============================================================================

async def populate_ingredient_nodes(pg, neo):
    """Create :Ingredient nodes and CONTAINS_ACTIVE/CONTAINS_EXCIPIENT edges."""
    
    # Active ingredients
    actives = await pg.fetch("""
        SELECT formulation_id, substance_name, normalized_name, 
               unii, drugbank_id, strength_label
        FROM active_ingredient
    """)
    
    for i in range(0, len(actives), BATCH_SIZE):
        batch = [dict(r) for r in actives[i:i+BATCH_SIZE]]
        async with neo.session() as session:
            await session.run("""
                UNWIND $batch AS row
                MERGE (i:Ingredient {normalized_name: row.normalized_name})
                SET i.display_name = row.substance_name,
                    i.unii = row.unii,
                    i.drugbank_id = row.drugbank_id,
                    i.role = 'active'
                WITH i, row
                MATCH (d:Drug {formulation_id: row.formulation_id})
                MERGE (d)-[:CONTAINS_ACTIVE {strength_label: row.strength_label}]->(i)
            """, batch=batch)
    
    # Inactive ingredients
    inactives = await pg.fetch("""
        SELECT formulation_id, name, normalized_name, unii, drugbank_id, role
        FROM inactive_ingredient
    """)
    
    for i in range(0, len(inactives), BATCH_SIZE):
        batch = [dict(r) for r in inactives[i:i+BATCH_SIZE]]
        async with neo.session() as session:
            await session.run("""
                UNWIND $batch AS row
                MERGE (i:Ingredient {normalized_name: row.normalized_name})
                SET i.display_name = row.name,
                    i.unii = row.unii,
                    i.drugbank_id = row.drugbank_id,
                    i.role = coalesce(row.role, 'excipient')
                WITH i, row
                MATCH (d:Drug {formulation_id: row.formulation_id})
                MERGE (d)-[:CONTAINS_EXCIPIENT]->(i)
            """, batch=batch)
    
    print(f"  Created {len(actives)} active + {len(inactives)} inactive ingredient nodes/edges")


# ============================================================================
# STEP 5.3: ENZYME NODES + METABOLISED_BY/INHIBITS/INDUCES EDGES
# ============================================================================

# Known CYP enzymes and transporters for regex extraction
ENZYME_PATTERN = re.compile(
    r'\b(CYP[123][A-Z]\d{1,2}|CYP\d[A-Z]\d|'
    r'P-gp|P-glycoprotein|BCRP|OATP\w+|OCT\d|OAT\d|MATE\d|'
    r'UGT\w+|SULT\w+|NAT\d|'
    r'MAO-[AB]|COMT|'
    r'aldehyde\s+oxidase)\b',
    re.IGNORECASE
)

INHIBIT_PATTERN = re.compile(
    r'(inhibit(?:s|ed|or|ion)?|block(?:s|ed)?)\s+(?:of\s+)?(CYP[123][A-Z]\d{1,2}|P-gp|P-glycoprotein|BCRP|OATP\w+)',
    re.IGNORECASE
)

INDUCE_PATTERN = re.compile(
    r'(induc(?:e[sd]?|tion|er))\s+(?:of\s+)?(CYP[123][A-Z]\d{1,2}|P-gp|P-glycoprotein|BCRP|OATP\w+)',
    re.IGNORECASE
)


async def populate_enzyme_nodes(pg, neo):
    """
    Create :Enzyme nodes and METABOLISED_BY/INHIBITS/INDUCES edges.
    
    Data sources:
      1. clinical_section WHERE section IN ('clinical_pharmacology', 'pharmacokinetics',
         'mechanism_of_action', 'drug_interactions') — regex extraction
      2. drug_interaction.mechanism field — regex extraction
    
    This fixes GAP #1: INHIBITS/INDUCES edges were not being extracted.
    """
    
    # Collect all enzyme relationships from clinical narratives
    enzyme_rels = []  # [{formulation_id, enzyme, rel_type}]
    
    sections = await pg.fetch("""
        SELECT formulation_id, section, text
        FROM clinical_section
        WHERE section IN ('clinical_pharmacology', 'pharmacokinetics', 
                          'mechanism_of_action', 'drug_interactions')
          AND text IS NOT NULL
    """)
    
    for row in sections:
        fid = row["formulation_id"]
        text = row["text"]
        
        # Extract METABOLISED_BY
        # Pattern: "metabolized by CYP3A4" or "CYP3A4 is the primary enzyme"
        metab_pattern = re.compile(
            r'(?:metaboli[sz]ed\s+(?:primarily\s+)?by|'
            r'primary\s+(?:route|enzyme|pathway)\s+(?:of\s+metabolism\s+)?(?:is\s+)?|'
            r'substrate\s+(?:of|for))\s+'
            r'((?:CYP[123][A-Z]\d{1,2}(?:\s*(?:,|and)\s*)?)+)',
            re.IGNORECASE
        )
        for m in metab_pattern.finditer(text):
            enzymes = ENZYME_PATTERN.findall(m.group())
            for enz in enzymes:
                enzyme_rels.append({"formulation_id": fid, "enzyme": enz.upper(), "rel_type": "METABOLISED_BY"})
        
        # Extract INHIBITS
        for m in INHIBIT_PATTERN.finditer(text):
            enzyme_rels.append({"formulation_id": fid, "enzyme": m.group(2).upper(), "rel_type": "INHIBITS"})
        
        # Extract INDUCES
        for m in INDUCE_PATTERN.finditer(text):
            enzyme_rels.append({"formulation_id": fid, "enzyme": m.group(2).upper(), "rel_type": "INDUCES"})
    
    # Also extract from drug_interaction.mechanism field
    interactions = await pg.fetch("""
        SELECT DISTINCT subject_formulation_id as formulation_id, mechanism
        FROM drug_interaction
        WHERE mechanism IS NOT NULL AND mechanism != ''
    """)
    
    for row in interactions:
        mech = row["mechanism"]
        fid = row["formulation_id"]
        
        for m in INHIBIT_PATTERN.finditer(mech):
            enzyme_rels.append({"formulation_id": fid, "enzyme": m.group(2).upper(), "rel_type": "INHIBITS"})
        for m in INDUCE_PATTERN.finditer(mech):
            enzyme_rels.append({"formulation_id": fid, "enzyme": m.group(2).upper(), "rel_type": "INDUCES"})
    
    # Deduplicate
    seen = set()
    unique_rels = []
    for r in enzyme_rels:
        key = (r["formulation_id"], r["enzyme"], r["rel_type"])
        if key not in seen:
            seen.add(key)
            unique_rels.append(r)
    
    # Separate by relationship type
    for rel_type in ["METABOLISED_BY", "INHIBITS", "INDUCES"]:
        batch = [r for r in unique_rels if r["rel_type"] == rel_type]
        if not batch:
            continue
        
        for i in range(0, len(batch), BATCH_SIZE):
            chunk = batch[i:i+BATCH_SIZE]
            async with neo.session() as session:
                await session.run(f"""
                    UNWIND $batch AS row
                    MERGE (e:Enzyme {{name: row.enzyme}})
                    WITH e, row
                    MATCH (d:Drug {{formulation_id: row.formulation_id}})
                    MERGE (d)-[:{rel_type}]->(e)
                """, batch=chunk)
        
        print(f"  Created {len(batch)} {rel_type} edges")
    
    enzyme_count = len(set(r["enzyme"] for r in unique_rels))
    print(f"  Total unique enzymes: {enzyme_count}")


# ============================================================================
# STEP 5.3b: TARGET NODES
# ============================================================================

async def populate_target_nodes(pg, neo):
    """Create :Target nodes from mechanism_of_action text."""
    # Targets come from mechanism_of_action narrative
    # For now, use drug.mechanism_of_action or clinical_section
    # This is a simplified extraction — Pass 2 should populate a targets table
    
    sections = await pg.fetch("""
        SELECT formulation_id, text FROM clinical_section
        WHERE section = 'mechanism_of_action' AND text IS NOT NULL
    """)
    
    target_pattern = re.compile(
        r'(?:inhibit(?:s|or)?|block(?:s)?|antagoni[sz]e[sd]?|agoni[sz]e[sd]?|bind[sd]?\s+to|target[sd]?)\s+'
        r'(?:the\s+)?(?:human\s+)?'
        r'([A-Z][A-Za-z0-9\-]+(?:\s+[A-Za-z0-9\-]+){0,3}?\s*(?:receptor|protease|enzyme|kinase|channel|transporter|reductase|synthase|polymerase|integrase|transferase))',
        re.IGNORECASE
    )
    
    target_rels = []
    for row in sections:
        for m in target_pattern.finditer(row["text"]):
            target_name = m.group(1).strip().upper()
            if len(target_name) > 5:  # filter noise
                target_rels.append({"formulation_id": row["formulation_id"], "target": target_name})
    
    # Deduplicate
    seen = set()
    unique = []
    for r in target_rels:
        key = (r["formulation_id"], r["target"])
        if key not in seen:
            seen.add(key)
            unique.append(r)
    
    if unique:
        for i in range(0, len(unique), BATCH_SIZE):
            batch = unique[i:i+BATCH_SIZE]
            async with neo.session() as session:
                await session.run("""
                    UNWIND $batch AS row
                    MERGE (t:Target {name: row.target})
                    WITH t, row
                    MATCH (d:Drug {formulation_id: row.formulation_id})
                    MERGE (d)-[:TARGETS]->(t)
                """, batch=batch)
    
    print(f"  Created {len(unique)} TARGETS edges, {len(set(r['target'] for r in unique))} unique targets")


# ============================================================================
# STEP 5.4: DRUG CLASS NODES
# ============================================================================

async def populate_drug_class_nodes(pg, neo):
    """Create :DrugClass nodes and BELONGS_TO_CLASS edges."""
    rows = await pg.fetch("""
        SELECT formulation_id, unnest(drug_class) as class_name
        FROM drugdb.drug
        WHERE array_length(drug_class, 1) > 0
    """)
    
    for i in range(0, len(rows), BATCH_SIZE):
        batch = [dict(r) for r in rows[i:i+BATCH_SIZE]]
        async with neo.session() as session:
            await session.run("""
                UNWIND $batch AS row
                MERGE (c:DrugClass {name: row.class_name})
                WITH c, row
                MATCH (d:Drug {formulation_id: row.formulation_id})
                MERGE (d)-[:BELONGS_TO_CLASS]->(c)
            """, batch=batch)
    
    print(f"  Created {len(rows)} BELONGS_TO_CLASS edges")


# ============================================================================
# STEP 5.5: INDICATION NODES
# ============================================================================

async def populate_indication_nodes(pg, neo):
    """Create :Indication nodes and INDICATED_FOR edges."""
    rows = await pg.fetch("""
        SELECT formulation_id, term, icd10, snomed, population, line_of_therapy
        FROM drug_indication
    """)
    
    for i in range(0, len(rows), BATCH_SIZE):
        batch = []
        for r in rows[i:i+BATCH_SIZE]:
            d = dict(r)
            # Create a stable key for the indication
            d["key"] = d["icd10"] or d["snomed"] or str(hash(d["term"]) & 0xFFFFFFFF)
            batch.append(d)
        
        async with neo.session() as session:
            await session.run("""
                UNWIND $batch AS row
                MERGE (ind:Indication {key: row.key})
                SET ind.term = row.term,
                    ind.icd10 = row.icd10,
                    ind.snomed = row.snomed
                WITH ind, row
                MATCH (d:Drug {formulation_id: row.formulation_id})
                MERGE (d)-[:INDICATED_FOR {
                    population: coalesce(row.population, 'any'),
                    line_of_therapy: coalesce(row.line_of_therapy, 'unspecified')
                }]->(ind)
            """, batch=batch)
    
    print(f"  Created {len(rows)} INDICATED_FOR edges")


# ============================================================================
# STEP 5.6: INTERACTION EDGES (BIDIRECTIONAL)
# ============================================================================

async def populate_interaction_edges(pg, neo):
    """
    Create INTERACTS_WITH edges from drug_interaction table.
    
    Fixes:
      - Issue #3: Uses normalized_name + drugbank_id fallback for partner resolution
      - Issue #4: Creates edges in BOTH directions (A->B and B->A)
    """
    rows = await pg.fetch("""
        SELECT di.interaction_id, di.subject_formulation_id, 
               di.partner_name, di.partner_rxcui, di.partner_drugbank_id,
               di.severity, di.effect_direction, di.magnitude, 
               di.mechanism, di.clinical_management,
               di.subject_substance_role, di.source
        FROM drug_interaction di
    """)
    
    # Pre-build lookup: normalized_name → formulation_id
    drug_lookup = await pg.fetch("SELECT formulation_id, normalized_name FROM drugdb.drug")
    name_to_fid = {}
    for r in drug_lookup:
        name_to_fid[r["normalized_name"]] = r["formulation_id"]
    
    # DrugBank ID lookup
    db_lookup = await pg.fetch("""
        SELECT DISTINCT formulation_id, id_value 
        FROM drugdb.drug_identifier WHERE id_type = 'drugbank'
    """)
    dbid_to_fid = {r["id_value"]: r["formulation_id"] for r in db_lookup}
    
    # RxCUI lookup
    rxcui_lookup = await pg.fetch("""
        SELECT DISTINCT formulation_id, id_value 
        FROM drugdb.drug_identifier WHERE id_type = 'rxcui'
    """)
    rxcui_to_fid = {r["id_value"]: r["formulation_id"] for r in rxcui_lookup}
    
    edges = []
    unresolved = 0
    
    for r in rows:
        partner_fid = None
        
        # Resolution order: drugbank_id → rxcui → normalized name match
        if r["partner_drugbank_id"] and r["partner_drugbank_id"] in dbid_to_fid:
            partner_fid = dbid_to_fid[r["partner_drugbank_id"]]
        elif r["partner_rxcui"] and r["partner_rxcui"] in rxcui_to_fid:
            partner_fid = rxcui_to_fid[r["partner_rxcui"]]
        else:
            # Normalize partner name and try matching
            from indian_brand_mapper import normalize_generic_name
            normalized_partner = normalize_generic_name(r["partner_name"])
            if normalized_partner in name_to_fid:
                partner_fid = name_to_fid[normalized_partner]
            else:
                # Fuzzy: try substring match
                for db_name, fid in name_to_fid.items():
                    if normalized_partner in db_name or db_name in normalized_partner:
                        partner_fid = fid
                        break
        
        if not partner_fid:
            unresolved += 1
            continue
        
        edge_props = {
            "subject_fid": r["subject_formulation_id"],
            "partner_fid": partner_fid,
            "severity": r["severity"] or "unknown",
            "effect_direction": r["effect_direction"],
            "magnitude": r["magnitude"],
            "mechanism": r["mechanism"],
            "clinical_management": r["clinical_management"],
            "subject_substance_role": r["subject_substance_role"],
            "source": r["source"],
        }
        edges.append(edge_props)
    
    # Create edges in BOTH directions (fixes Issue #4)
    for i in range(0, len(edges), BATCH_SIZE):
        batch = edges[i:i+BATCH_SIZE]
        async with neo.session() as session:
            # Forward edge: A -> B
            await session.run("""
                UNWIND $batch AS row
                MATCH (a:Drug {formulation_id: row.subject_fid})
                MATCH (b:Drug {formulation_id: row.partner_fid})
                MERGE (a)-[r:INTERACTS_WITH {source: row.source}]->(b)
                SET r.severity = row.severity,
                    r.effect_direction = row.effect_direction,
                    r.magnitude = row.magnitude,
                    r.mechanism = row.mechanism,
                    r.clinical_management = row.clinical_management,
                    r.subject_substance_role = row.subject_substance_role
            """, batch=batch)
            
            # Reverse edge: B -> A (with swapped direction)
            reverse_batch = [{
                **e,
                "subject_fid": e["partner_fid"],
                "partner_fid": e["subject_fid"],
            } for e in batch]
            
            await session.run("""
                UNWIND $batch AS row
                MATCH (a:Drug {formulation_id: row.subject_fid})
                MATCH (b:Drug {formulation_id: row.partner_fid})
                MERGE (a)-[r:INTERACTS_WITH {source: row.source}]->(b)
                SET r.severity = row.severity,
                    r.effect_direction = row.effect_direction,
                    r.magnitude = row.magnitude,
                    r.mechanism = row.mechanism,
                    r.clinical_management = row.clinical_management,
                    r.subject_substance_role = row.subject_substance_role
            """, batch=reverse_batch)
    
    print(f"  Created {len(edges) * 2} INTERACTS_WITH edges (bidirectional)")
    print(f"  Unresolved partners: {unresolved} (partner drug not in database)")
    
    # Also create CONTRAINDICATED_WITH edges from contraindication table
    contras = await pg.fetch("""
        SELECT formulation_id, term, drug_class, reason
        FROM contraindication
        WHERE kind = 'coadministered_drug'
    """)
    
    contra_edges = []
    for r in contras:
        # Try to resolve the contraindicated drug
        from indian_brand_mapper import normalize_generic_name
        for drug_name in re.split(r'[,;]', r["term"]):
            drug_name = drug_name.strip()
            if not drug_name:
                continue
            normalized = normalize_generic_name(drug_name)
            if normalized in name_to_fid:
                contra_edges.append({
                    "subject_fid": r["formulation_id"],
                    "partner_fid": name_to_fid[normalized],
                    "reason": r["reason"],
                    "drug_class": r["drug_class"],
                })
    
    if contra_edges:
        for i in range(0, len(contra_edges), BATCH_SIZE):
            batch = contra_edges[i:i+BATCH_SIZE]
            async with neo.session() as session:
                await session.run("""
                    UNWIND $batch AS row
                    MATCH (a:Drug {formulation_id: row.subject_fid})
                    MATCH (b:Drug {formulation_id: row.partner_fid})
                    MERGE (a)-[r:CONTRAINDICATED_WITH]->(b)
                    SET r.reason = row.reason,
                        r.drug_class = row.drug_class
                """, batch=batch)
    
    print(f"  Created {len(contra_edges)} CONTRAINDICATED_WITH edges")


# ============================================================================
# STEP 5.7: DERIVED ALTERNATIVE_TO EDGES
# ============================================================================

async def populate_alternative_edges(neo):
    """
    Create ALTERNATIVE_TO edges: two drugs are alternatives if they share 
    a therapeutic class AND at least one indication.
    This is a graph-only computation — no Postgres needed.
    """
    async with neo.session() as session:
        result = await session.run("""
            MATCH (a:Drug)-[:BELONGS_TO_CLASS]->(c:DrugClass)<-[:BELONGS_TO_CLASS]-(b:Drug)
            WHERE a.formulation_id < b.formulation_id
            AND EXISTS {
                MATCH (a)-[:INDICATED_FOR]->(ind:Indication)<-[:INDICATED_FOR]-(b)
            }
            MERGE (a)-[:ALTERNATIVE_TO {shared_class: c.name}]->(b)
            MERGE (b)-[:ALTERNATIVE_TO {shared_class: c.name}]->(a)
            RETURN count(*) as edge_count
        """)
        record = await result.single()
        count = record["edge_count"] if record else 0
    
    print(f"  Created {count} ALTERNATIVE_TO edge pairs")


# ============================================================================
# STEP 6.6: INDIAN BRAND NODES
# ============================================================================

async def populate_indian_brand_nodes(pg, neo):
    """Create :IndianBrand nodes and BRAND_OF edges."""
    rows = await pg.fetch("""
        SELECT indian_brand_id, brand_name, manufacturer_india,
               strength_label, form_canonical, mrp_inr, schedule,
               formulation_id
        FROM indian_brand
        WHERE formulation_id IS NOT NULL
    """)
    
    for i in range(0, len(rows), BATCH_SIZE):
        batch = [dict(r) for r in rows[i:i+BATCH_SIZE]]
        async with neo.session() as session:
            await session.run("""
                UNWIND $batch AS row
                MERGE (ib:IndianBrand {indian_brand_id: row.indian_brand_id})
                SET ib.brand_name = row.brand_name,
                    ib.manufacturer = row.manufacturer_india,
                    ib.strength_label = row.strength_label,
                    ib.form_canonical = row.form_canonical,
                    ib.mrp_inr = row.mrp_inr,
                    ib.schedule = row.schedule
                WITH ib, row
                MATCH (d:Drug {formulation_id: row.formulation_id})
                MERGE (ib)-[:BRAND_OF]->(d)
            """, batch=batch)
    
    # FDC ingredient links
    fdc_rows = await pg.fetch("""
        SELECT ibi.indian_brand_id, ibi.formulation_id
        FROM indian_brand_ingredient ibi
        WHERE ibi.formulation_id IS NOT NULL
    """)
    
    if fdc_rows:
        for i in range(0, len(fdc_rows), BATCH_SIZE):
            batch = [dict(r) for r in fdc_rows[i:i+BATCH_SIZE]]
            async with neo.session() as session:
                await session.run("""
                    UNWIND $batch AS row
                    MATCH (ib:IndianBrand {indian_brand_id: row.indian_brand_id})
                    MATCH (d:Drug {formulation_id: row.formulation_id})
                    MERGE (ib)-[:BRAND_OF_INGREDIENT]->(d)
                """, batch=batch)
    
    print(f"  Created {len(rows)} IndianBrand nodes, {len(fdc_rows)} FDC ingredient links")


# ============================================================================
# VALIDATION
# ============================================================================

async def validate_graph(neo):
    """Post-population validation checks."""
    async with neo.session() as session:
        checks = [
            ("Drug nodes",          "MATCH (d:Drug) RETURN count(d) as cnt"),
            ("Ingredient nodes",    "MATCH (i:Ingredient) RETURN count(i) as cnt"),
            ("Enzyme nodes",        "MATCH (e:Enzyme) RETURN count(e) as cnt"),
            ("Target nodes",        "MATCH (t:Target) RETURN count(t) as cnt"),
            ("DrugClass nodes",     "MATCH (c:DrugClass) RETURN count(c) as cnt"),
            ("Indication nodes",    "MATCH (ind:Indication) RETURN count(ind) as cnt"),
            ("IndianBrand nodes",   "MATCH (ib:IndianBrand) RETURN count(ib) as cnt"),
            ("CONTAINS_ACTIVE",     "MATCH ()-[r:CONTAINS_ACTIVE]->() RETURN count(r) as cnt"),
            ("INTERACTS_WITH",      "MATCH ()-[r:INTERACTS_WITH]->() RETURN count(r) as cnt"),
            ("METABOLISED_BY",      "MATCH ()-[r:METABOLISED_BY]->() RETURN count(r) as cnt"),
            ("INHIBITS",            "MATCH ()-[r:INHIBITS]->() RETURN count(r) as cnt"),
            ("INDUCES",             "MATCH ()-[r:INDUCES]->() RETURN count(r) as cnt"),
            ("BELONGS_TO_CLASS",    "MATCH ()-[r:BELONGS_TO_CLASS]->() RETURN count(r) as cnt"),
            ("INDICATED_FOR",       "MATCH ()-[r:INDICATED_FOR]->() RETURN count(r) as cnt"),
            ("ALTERNATIVE_TO",      "MATCH ()-[r:ALTERNATIVE_TO]->() RETURN count(r) as cnt"),
            ("BRAND_OF",            "MATCH ()-[r:BRAND_OF]->() RETURN count(r) as cnt"),
        ]
        
        print("\n  --- VALIDATION ---")
        for label, cypher in checks:
            result = await session.run(cypher)
            record = await result.single()
            cnt = record["cnt"] if record else 0
            status = "OK" if cnt > 0 else "EMPTY"
            print(f"  [{status}] {label}: {cnt}")


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

async def main():
    pg = await asyncpg.create_pool(DATABASE_URL)
    neo = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    try:
        print("Phase 5: Populating Neo4j graph from Postgres...")
        
        print("\n5.1 Drug nodes...")
        await populate_drug_nodes(pg, neo)
        
        print("\n5.2 Ingredient nodes + edges...")
        await populate_ingredient_nodes(pg, neo)
        
        print("\n5.3 Enzyme nodes + METABOLISED_BY/INHIBITS/INDUCES edges...")
        await populate_enzyme_nodes(pg, neo)
        
        print("\n5.3b Target nodes...")
        await populate_target_nodes(pg, neo)
        
        print("\n5.4 DrugClass nodes...")
        await populate_drug_class_nodes(pg, neo)
        
        print("\n5.5 Indication nodes...")
        await populate_indication_nodes(pg, neo)
        
        print("\n5.6 Interaction edges (bidirectional)...")
        await populate_interaction_edges(pg, neo)
        
        print("\n5.7 Alternative edges (derived)...")
        await populate_alternative_edges(neo)
        
        print("\n6.6 Indian Brand nodes...")
        await populate_indian_brand_nodes(pg, neo)
        
        print("\nValidating...")
        await validate_graph(neo)
        
        print("\nDone.")
    
    finally:
        await pg.close()
        await neo.close()


if __name__ == "__main__":
    asyncio.run(main())