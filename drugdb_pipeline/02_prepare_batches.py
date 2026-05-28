"""
02_prepare_batches.py — Stream Parquet records to JSONL batch files for Gemini.

Usage:
    python 02_prepare_batches.py --dry-run    # 1 batch of 50 → data/batches/dry_run/
    python 02_prepare_batches.py --full-run   # 25 batches   → data/batches/
"""

import argparse
import json
import logging
import math
import sys
import time
from datetime import timezone, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

import state_manager as sm

BASE_DIR = Path(__file__).parent
BATCHES_DIR = BASE_DIR / "data" / "batches"
DRY_RUN_BATCHES_DIR = BATCHES_DIR / "dry_run"
LOG_DIR = BASE_DIR / "data" / "logs"

for d in (BATCHES_DIR, DRY_RUN_BATCHES_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

def _fmt_secs(s: float) -> str:
    s = int(s)
    if s < 60: return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60: return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


IST = timezone(timedelta(hours=5, minutes=30))


class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        import datetime
        ct = datetime.datetime.fromtimestamp(record.created, tz=IST)
        if datefmt:
            return ct.strftime(datefmt)
        return ct.strftime('%Y-%m-%d %H:%M:%S IST')


def _configure_logging() -> None:
    if logging.getLogger().handlers:
        return
    fmt = "%(asctime)s [%(name)s] [%(levelname)s] %(message)s"
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(ISTFormatter(fmt))
    root.addHandler(sh)
    fh = logging.FileHandler(LOG_DIR / "pipeline.log")
    fh.setLevel(logging.INFO)
    fh.setFormatter(ISTFormatter(fmt))
    root.addHandler(fh)
    dfh = logging.FileHandler(LOG_DIR / "pipeline_detailed.log")
    dfh.setLevel(logging.DEBUG)
    dfh.setFormatter(ISTFormatter(fmt))
    root.addHandler(dfh)


_configure_logging()
logger = logging.getLogger(__name__)

STAGE = "prepare"
NUM_BATCHES = 10

SYSTEM_PROMPT = """You are a clinical pharmacology expert extracting \
medical disorders from FDA drug label sections. Your task is to \
identify every distinct named disorder associated with this drug \
and classify each one precisely.

Return ONLY a valid JSON array. No markdown, no explanation, \
no backticks, no preamble. Response must be parseable by json.loads().

════════════════════════════════════════════
THE ONE UNIVERSAL RULE — APPLIES TO EVERY SECTION
════════════════════════════════════════════

No matter which section of the label you are reading, the rule is
ALWAYS the same:

  Extract a disorder ONLY if this drug is given TO A PATIENT
  WHO HAS that condition in order to TREAT or PREVENT it.

Ask yourself: "Is this drug prescribed BECAUSE the patient has
this condition?" If YES → extract. If NO → skip.

This rule applies equally to INDICATIONS AND USAGE, WARNINGS,
CONTRAINDICATIONS, USE IN PREGNANCY, GERIATRIC USE, PEDIATRIC USE,
USE IN SPECIFIC POPULATIONS, CLINICAL PHARMACOLOGY, CLINICAL STUDIES,
PHARMACOKINETICS, MECHANISM OF ACTION, and DESCRIPTION.

WHAT TO SKIP — regardless of which section it appears in:
- Adverse effects / side effects the drug CAUSES
  (e.g., "may cause hepatotoxicity" → skip Hepatotoxicity)
- Contraindicated conditions — where the drug must NOT be used
  (e.g., "contraindicated in severe hepatic impairment" → skip)
- Drug interaction risks — conditions worsened by combining drugs
  (e.g., "concomitant use with statins increases myopathy risk" → skip)
- Patient safety populations — dosing adjustments, monitoring notes
  (e.g., "use with caution in renal impairment" → skip Renal Impairment)
- Pregnancy/embryofetal toxicity data
  (e.g., "caused fetal malformations in rats" → skip)
- Immune reconstitution syndrome examples
  (e.g., "IRIS may unmask CMV, MAC, TB" → skip CMV, MAC, TB)
- Conditions the drug is NOT indicated for
  (e.g., "not for use in status asthmaticus" → skip)
- Patient preconditions / eligibility criteria
  (e.g., "patients must be opioid-tolerant" → skip Opioid Tolerance)
- Risk outcomes / complications the drug causes or worsens
  (e.g., "risk of kernicterus in neonates" → skip Kernicterus)
- Pathophysiology / mechanism text
  (e.g., "works by inhibiting COX-2 enzyme" → skip)
- Animal study findings
  (e.g., "pancreatic tumors in rats at high doses" → skip)

EXAMPLES:
  Gabapentin label warnings say "abrupt withdrawal may cause seizures"
  → Seizures here is a WITHDRAWAL RISK, not a treated condition.
  BUT Gabapentin indications say "treatment of partial onset seizures"
  → Partial Onset Seizures IS treated → EXTRACT.

  Ketorolac warnings say "may cause peptic ulceration"
  → Peptic Ulcer is an ADVERSE EFFECT → skip.
  Ketorolac indications say "short-term management of moderately severe
  acute pain" → Acute Pain IS treated → EXTRACT.

  Nelfinavir warnings mention "hemophilia patients may bleed more"
  → Hemophilia here is a RISK POPULATION → skip.
  Nelfinavir indications say "treatment of HIV-1 infection"
  → HIV-1 Infection IS treated → EXTRACT.

════════════════════════════════════════════
WHAT TO EXTRACT — INCLUDE ONLY REAL CLINICAL DIAGNOSES
════════════════════════════════════════════

INCLUDE these:
- Named diseases and syndromes: "Major Depressive 783 DisorderDisorder",
  "Hypertension", "Type 2 Diabetes Mellitus"
- Named clinical conditions: "Renal Impairment", "Hepatic Failure",
  "Methemoglobinemia"
- Specific serious adverse conditions:
  "Stevens-Johnson Syndrome", "Neuroleptic Malignant Syndrome",
  "Serotonin Syndrome"
- Pregnancy-related CLINICAL CONDITIONS only:
  "Persistent Pulmonary Hypertension of the Newborn" YES
  "Neonatal Withdrawal Syndrome" YES
  "Neonatal Opioid Withdrawal Syndrome" YES
  "Preeclampsia" YES

DO NOT INCLUDE — skip entirely:
- Pregnancy itself, Lactation, Breastfeeding — NOT disorders
- Age groups: "Infants", "Elderly patients" — NOT disorders
- Vague risks: "Fetal Harm", "Birth Defects", "Embryotoxicity"
- Generic symptoms: "Pain", "Fatigue", "Bleeding" (unless part of
  a named syndrome)
- Drug mechanisms: "CYP3A4 Inhibition" — NOT a disorder
- Procedures: "Surgery", "Anesthesia" — NOT disorders
- Generic "Hypersensitivity" — only include if it is a named
  syndrome: "Stevens-Johnson Syndrome", "DRESS"
- Conditions preceded by "not indicated for", "not studied in",
  "not recommended in" — the drug does NOT treat these

════════════════════════════════════════════
FIELDS TO EXTRACT PER DISORDER
════════════════════════════════════════════

term:
  Standardized medical name. Use proper clinical terminology.
  "Type 2 Diabetes Mellitus" not "diabetes"
  "Major Depressive Disorder" not "depression"
  "Chronic Kidney Disease" not "kidney problems"

disorder_type:
  Pick EXACTLY ONE — the most specific that fits.
  NEVER use "other" if any category even partially fits.
  Options: psychiatric, cardiovascular, metabolic, neurological,
  renal, hepatic, respiratory, endocrine, hematologic,
  gastrointestinal, musculoskeletal, immunologic, infectious,
  dermatologic, oncologic, ophthalmologic, urologic, dental,
  obstetric, neonatal, other

organ_system:
  Pick EXACTLY ONE — the most specific that fits.
  NEVER use "other" if any option even partially fits.
  Options: nervous_system, cardiovascular, kidneys, liver, lungs,
  endocrine, blood, gastrointestinal, musculoskeletal, immune_system,
  skin, reproductive, systemic, eye, ear, urinary, oral, lymphatic,
  bone, neonatal, other

icd10:
  The most specific single ICD-10-CM code for this disorder.
  STRICT RULES:
  - Return ONE specific code: F32.9, I10, E11.9, J45.909
  - NEVER a range: "P00-P96" WRONG, "Q00-Q99" WRONG
  - NEVER a chapter letter alone: "F" or "F30-F39" WRONG
  - NEVER a date format: "2023-03-01" WRONG
  - null is better than a wrong or hallucinated code

  COMMON MISTAKES TO AVOID:
  - Hemophilia A = D66, Hemophilia B = D67 (NOT F-codes)
  - Acute pain = R52, Chronic pain = G89.29 (NOT G80.x — G80 is cerebral palsy)
  - Postherpetic neuralgia = B02.29 (NOT G62.9 — G62.9 is polyneuropathy)
  - TTR amyloid polyneuropathy = E85.1 (NOT E11.x — E11 is type 2 diabetes)
  - GIST (gastrointestinal stromal tumor) = C49.A0 (NOT C18.x — C18 is colon cancer)
  - Mucopolysaccharidosis VI = E76.22 (NOT E76.3)
  - Renal tubular acidosis = N25.89 (NOT N20.0 — N20 is kidney stones)
  - Occipital Horn Syndrome = E83.09 (NOT Q74.2)
  - CNS depression = G93.89 (NOT G95.9 — G95 is spinal cord)

icd11:
  The most specific single ICD-11 code for this disorder.
  STRICT RULES:
  - ICD-11 codes use format: letters and numbers with a dot,
    e.g. 6A80.0, 1A00, BA80.Z, 5A11.0
  - Return ONE specific code — the most precise available
  - NEVER a range or chapter heading
  - null is better than a wrong or hallucinated code

  COMMON ICD-11 CODES:
  - HIV-1 infection = 1C62.0
  - Type 2 Diabetes Mellitus = 5A11
  - Major Depressive Disorder = 6A70
  - Hypertension = BA00
  - Postherpetic neuralgia = 8B82.0
  - Multiple Sclerosis = 8A40
  - Epilepsy = 8A60
  - Breast cancer = 2C60
  - NSCLC = 2C25
  - Hemophilia A = 3B10.0
  - Hemophilia B = 3B10.1

snomed:
  SNOMED CT concept ID, numeric only.
  RULES:
  - Numeric digits only: 370143000, 38341003
  - Must be 7 to 18 digits
  - 6-digit or shorter = almost always wrong, use null
  - NEVER reuse the same SNOMED code for different disorders —
    each disorder has a unique SNOMED concept ID
  - null is better than a hallucinated or reused ID

source_section:
  EXACTLY ONE of these values — no others allowed:
  indications_and_usage | contraindications | warnings |
  use_in_pregnancy | geriatric_use | pediatric_use |
  use_in_specific_populations | drug_description |
  clinical_studies | mechanism_of_action |
  clinical_pharmacology | pharmacokinetics

  Priority when disorder appears in multiple sections:
  indications_and_usage > contraindications > warnings >
  use_in_pregnancy > geriatric_use > pediatric_use >
  use_in_specific_populations > drug_description >
  clinical_studies > mechanism_of_action >
  clinical_pharmacology > pharmacokinetics

line_of_therapy:
  JSON array. Pick from: first-line, second-line, third-line,
  salvage, adjunct, unspecified

  Step 1 — Extract from label text if explicitly stated ("first-line",
  "second-line", "when other treatments have failed", "initial therapy",
  "salvage", "adjunct", etc.).
  Step 2 — If NOT stated in the label, use your clinical knowledge to
  infer the most accurate value for this disorder/drug pair. Only use
  "unspecified" if you genuinely cannot determine it even from clinical
  knowledge.
  NEVER return null or empty array.

treatment_intent:
  JSON array. Pick from: curative, symptomatic, palliative,
  prophylactic, suppressive, other, unspecified

  Step 1 — Extract from label text if explicitly stated ("cure",
  "eradication", "prevention", "prophylaxis", "relief of symptoms", etc.).
  Step 2 — If NOT stated in the label, use your clinical knowledge to
  infer the most accurate value. Only use "unspecified" if genuinely
  uncertain even from clinical knowledge.
  NEVER return null or empty array.

treatment_phase:
  JSON array. Pick from: induction, consolidation, maintenance,
  acute_rescue, other, unspecified

  Step 1 — Extract from label text if explicitly stated.
  Step 2 — If NOT stated in the label, use your clinical knowledge to
  infer the most accurate value. Only use "unspecified" if genuinely
  uncertain even from clinical knowledge.
  NEVER return null or empty array.

regimen_role:
  JSON array. Pick from: monotherapy, combination, adjunctive,
  bridge, background, other, unspecified

  Step 1 — Extract from label text if explicitly stated ("adjunct",
  "add-on", "in combination with", "alone", "monotherapy", etc.).
  Step 2 — If NOT stated in the label, use your clinical knowledge to
  infer the most accurate value. Only use "unspecified" if genuinely
  uncertain even from clinical knowledge.
  NEVER return null or empty array.

════════════════════════════════════════════
COMPLETE OUTPUT EXAMPLE
════════════════════════════════════════════
[
  {
    "term": "Major Depressive Disorder",
    "disorder_type": "psychiatric",
    "organ_system": "nervous_system",
    "icd10": "F32.9",
    "icd11": "6A70",
    "snomed": "370143000",
    "source_section": "indications_and_usage",
    "line_of_therapy": ["first-line"],
    "treatment_intent": ["symptomatic"],
    "treatment_phase": ["maintenance"],
    "regimen_role": ["monotherapy", "adjunctive"]
  },
  {
    "term": "Type 2 Diabetes Mellitus",
    "disorder_type": "metabolic",
    "organ_system": "endocrine",
    "icd10": "E11.9",
    "icd11": "5A11",
    "snomed": "44054006",
    "source_section": "indications_and_usage",
    "line_of_therapy": ["first-line"],
    "treatment_intent": ["suppressive"],
    "treatment_phase": ["maintenance"],
    "regimen_role": ["adjunctive"]
  },
  {
    "term": "Epilepsy",
    "disorder_type": "neurological",
    "organ_system": "nervous_system",
    "icd10": "G40.909",
    "icd11": "8A60",
    "snomed": "84757009",
    "source_section": "indications_and_usage",
    "line_of_therapy": ["first-line"],
    "treatment_intent": ["suppressive"],
    "treatment_phase": ["maintenance"],
    "regimen_role": ["adjunctive"]
  }
]"""


def _build_payload(row: dict) -> dict:
    sections: dict = {}
    mapping = {
        "indications_and_usage":       row.get("indications"),
        "contraindications":           row.get("contraindications"),
        "warnings":                    row.get("warnings"),
        "use_in_pregnancy":            row.get("use_in_pregnancy"),
        "drug_description":            row.get("drug_description"),
        "geriatric_use":               row.get("geriatric_use"),
        "pediatric_use":               row.get("pediatric_use"),
        "use_in_specific_populations": row.get("use_in_specific_populations"),
        "clinical_studies":            row.get("clinical_studies"),
        "pharmacokinetics":            row.get("pharmacokinetics"),
        "mechanism_of_action":         row.get("mechanism_of_action"),
        "clinical_pharmacology":       row.get("clinical_pharmacology"),
    }
    for key, val in mapping.items():
        if val is not None and str(val).strip():
            sections[key] = str(val)

    return {
        "master_linkage_id": row["master_linkage_id"],
        "drug_name": row.get("drug_name", "unknown"),
        "source": row.get("source", "unknown"),
        "sections": sections,
    }


def _build_jsonl_line(row: dict) -> str:
    payload = _build_payload(row)
    sections_block = ""
    section_labels = {
        "indications_and_usage":       "INDICATIONS AND USAGE",
        "contraindications":           "CONTRAINDICATIONS",
        "warnings":                    "WARNINGS",
        "use_in_pregnancy":            "USE IN PREGNANCY",
        "drug_description":            "DESCRIPTION",
        "geriatric_use":               "GERIATRIC USE",
        "pediatric_use":               "PEDIATRIC USE",
        "use_in_specific_populations": "USE IN SPECIFIC POPULATIONS",
        "clinical_studies":            "CLINICAL STUDIES",
        "pharmacokinetics":            "PHARMACOKINETICS",
        "mechanism_of_action":         "MECHANISM OF ACTION",
        "clinical_pharmacology":       "CLINICAL PHARMACOLOGY",
    }
    for key, label in section_labels.items():
        text = payload["sections"].get(key)
        if text:
            sections_block += f"{label}:\n{text}\n\n"
    user_content = (
        f"Drug: {payload['drug_name']}\n\n{sections_block}"
        "Extract all clinical disorders and diagnoses. Return a JSON array.\n"
        "Remember: exclude pregnancy itself, age groups, vague outcomes, and "
        "non-disorder terms. Only real named clinical conditions."
    )
    return json.dumps(
        {
            "custom_id": row["master_linkage_id"],
            "model": "gemini-2.5-flash-lite",
            "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
            "contents": [{"role": "user", "parts": [{"text": user_content}]}],
            "generation_config": {
                "response_mime_type": "application/json",
                "temperature": 0.1,
            },
        },
        ensure_ascii=False,
    )


def write_batch(batch_num: int, rows: list[dict], out_path: Path, record_id: str) -> None:
    if sm.is_done(STAGE, record_id):
        logger.info("Batch %s already done — skipping.", record_id)
        return

    sm.mark_started(STAGE, record_id)
    try:
        t0 = time.time()
        with out_path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(_build_jsonl_line(row) + "\n")
        elapsed = time.time() - t0
        file_size_mb = out_path.stat().st_size / (1024 * 1024)
        logger.info(
            "[02_prepare] Batch %02d written | records: %d | size: %.1f MB | time: %.1fs",
            batch_num, len(rows), file_size_mb, elapsed,
        )
    except Exception as exc:
        sm.mark_failed(STAGE, record_id, str(exc))
        logger.error("Failed writing batch %s: %s", record_id, exc, exc_info=True)
        return

    sm.mark_done(STAGE, record_id, metadata={"record_count": len(rows), "path": str(out_path)})


def run_dry_run() -> None:
    parquet_path = BASE_DIR / "data" / "records" / "records_dry_run.parquet"
    if not parquet_path.exists():
        logger.error("Dry run parquet not found: %s — run 01_read_db.py --dry-run first.", parquet_path)
        sys.exit(1)

    df = pd.read_parquet(parquet_path)
    rows = df.head(50).to_dict(orient="records")
    record_id = "dry_run_batch_01"
    out_path = DRY_RUN_BATCHES_DIR / "batch_01_dry_run.jsonl"
    write_batch(1, rows, out_path, record_id)
    logger.info("Dry run batch prepared: %s (%d records)", out_path, len(rows))


def run_full() -> None:
    parquet_path = BASE_DIR / "data" / "records" / "records.parquet"
    if not parquet_path.exists():
        logger.error("Parquet not found: %s — run 01_read_db.py --full-run first.", parquet_path)
        sys.exit(1)

    t_run_start = time.time()
    df = pd.read_parquet(parquet_path)
    total = len(df)
    per_batch = math.ceil(total / NUM_BATCHES)
    logger.info("Total records: %d, batches: %d, per batch: ~%d", total, NUM_BATCHES, per_batch)

    for batch_num in range(1, NUM_BATCHES + 1):
        record_id = f"batch_{batch_num:02d}"
        out_path = BATCHES_DIR / f"batch_{batch_num:02d}.jsonl"
        start = (batch_num - 1) * per_batch
        end = min(start + per_batch, total)
        rows = df.iloc[start:end].to_dict(orient="records")
        if not rows:
            logger.info("Batch %02d: no records, skipping.", batch_num)
            continue
        write_batch(batch_num, rows, out_path, record_id)

    actual_batches = [
        i for i in range(1, NUM_BATCHES + 1)
        if (BATCHES_DIR / f"batch_{i:02d}.jsonl").exists()
    ]
    total_size_mb = sum(
        (BATCHES_DIR / f"batch_{i:02d}.jsonl").stat().st_size
        for i in actual_batches
    ) / (1024 * 1024)
    est_cost = total_size_mb * 0.075
    logger.info(
        "[02_prepare] All %d batches ready | total: %.1f MB | avg: %.1f MB/batch | "
        "est. cost: ~$%.2f | time: %s",
        len(actual_batches), total_size_mb,
        total_size_mb / len(actual_batches) if actual_batches else 0,
        est_cost, _fmt_secs(time.time() - t_run_start),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="02_prepare_batches — build JSONL batch files")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--full-run", action="store_true")
    args = parser.parse_args()

    if args.dry_run:
        run_dry_run()
    else:
        run_full()


if __name__ == "__main__":
    main()
