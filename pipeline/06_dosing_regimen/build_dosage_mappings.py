#!/usr/bin/env python3
"""
Phase 3: Build dosage form mappings and regex patterns from Phase 2 CSV analysis.
"""

import csv
import json
import re
from collections import defaultdict

# ─── Mappings: EU/uppercase dosage_forms key → suffix that appears in generic_formulation ───
DOSAGE_FORM_MAPPINGS = {
    # Extended Release Tablets
    "TABLET, EXTENDED RELEASE":                   ["Extended Release Oral Tablet"],
    # Delayed Release Tablets
    "TABLET, DELAYED RELEASE (OBS 06-25-01)":     ["Delayed Release Oral Tablet"],
    "GASTRO-RESISTANT TABLET":                    ["Delayed Release Oral Tablet"],
    # Extended Release Capsules
    "CAPSULE, EXTENDED RELEASE":                  ["Extended Release Oral Capsule"],
    # Disintegrating Tablets
    "TABLET,DISINTEGRATING":                      ["Disintegrating Oral Tablet"],
    # Sublingual Tablets
    "TABLET, SUBLINGUAL":                         ["Sublingual Tablet"],
    # Chewable Tablets
    "TABLET,CHEWABLE":                            ["Chewable Tablet"],
    # Buccal Tablets
    "TABLET, BUCCAL":                             ["Buccal Tablet"],
    # Inhalation
    "INHALATION GAS":                             ["Gas for Inhalation"],
    "INHALATION SOLUTION":                        ["Inhalation Solution"],
    "INHALATION SUSPENSION":                      ["Inhalation Suspension"],
    "INHALATION POWDER":                          ["Inhalation Powder"],
    # Injections / Solutions for Injection
    "SOLUTION FOR INJECTION":                     ["Injectable Solution"],
    "SUSPENSION FOR INJECTION":                   ["Injectable Suspension"],
    # Oral liquids
    "SUSPENSION, ORAL (FINAL DOSE FORM)":         ["Oral Suspension"],
    "SOLUTION, ORAL":                             ["Oral Solution"],
    "GRANULES FOR ORAL SUSPENSION":               ["Granules for Oral Suspension"],
    "GRANULES FOR ORAL SOLUTION":                 ["Granules for Oral Solution"],
    "POWDER FOR ORAL SUSPENSION":                 ["Powder for Oral Suspension"],
    "POWDER FOR ORAL SOLUTION":                   ["Powder for Oral Solution"],
    "SUSPENSION,EXTENDED RELEASE VIAL (ML)":      ["Extended Release Suspension"],
    "ORAL POWDER":                                ["Oral Powder"],
    "ORAL GEL":                                   ["Oral Gel"],
    # Topical
    "CUTANEOUS SOLUTION":                         ["Topical Solution"],
    "CUTANEOUS FOAM":                             ["Topical Foam"],
    "CUTANEOUS POWDER":                           ["Topical Powder"],
    # Ophthalmic
    "EYE OINTMENT":                               ["Ophthalmic Ointment"],
    "EYE GEL":                                    ["Ophthalmic Gel"],
    # Rectal
    "SUPPOSITORY, RECTAL":                        ["Rectal Suppository"],
    "RECTAL CREAM":                               ["Rectal Cream"],
    "RECTAL GEL":                                 ["Rectal Gel"],
    "RECTAL FOAM":                                ["Rectal Foam"],
    "RECTAL OINTMENT":                            ["Rectal Ointment"],
    # Vaginal
    "VAGINAL CREAM":                              ["Vaginal Cream"],
    "VAGINAL GEL":                                ["Vaginal Gel"],
    "RING, VAGINAL":                              ["Vaginal System"],
    # Mouth / Throat
    "MOUTHWASH":                                  ["Mouthwash"],
    "GARGLE":                                     ["Mouthwash"],
    # Lozenges
    "LOZENGE":                                    ["Oral Lozenge"],
    "TROCHE":                                     ["Oral Lozenge"],
    # Irrigation
    "SOLUTION, IRRIGATION":                       ["Irrigation Solution"],
    # Transdermal
    "TRANSDERMAL PATCH":                          ["Transdermal System"],
    # Injectors
    "AUTO-INJECTOR (EA)":                         ["Auto-Injector"],
    # Pellets
    "PELLET (EA)":                                ["Oral Pellet"],
    # Enema
    "ENEMA (EA)":                                 ["Enema"],
    "ENEMA (ML)":                                 ["Enema"],
    # Nasal
    "NASAL GEL":                                  ["Nasal Gel"],
    "NASAL POWDER":                               ["Nasal Powder"],
    # Tape
    "TAPE, MEDICATED":                            ["Medicated Tape"],
    # Misc
    "TOOTHPASTE":                                 ["Toothpaste"],
}


def suffix_to_pattern(suffix: str) -> str:
    """Convert a plain suffix string to a regex pattern matching it at end-of-string."""
    escaped = re.escape(suffix)
    # allow variable whitespace between words
    escaped = re.sub(r'\\ ', r'\\s+', escaped)
    return r'\s+' + escaped + r'\s*$'


def build_regex_patterns(mappings: dict) -> list[dict]:
    """
    Build list of {dosage_form, suffix, pattern} dicts for UPDATE queries.
    One entry per (dosage_form, suffix) pair.
    """
    patterns = []
    seen_patterns: dict[str, list[str]] = defaultdict(list)

    for dosage_form, suffixes in mappings.items():
        for suffix in suffixes:
            pattern = suffix_to_pattern(suffix)
            patterns.append({
                "dosage_form":  dosage_form,
                "suffix":       suffix,
                "regex_pattern": pattern,
            })
            seen_patterns[suffix].append(dosage_form)

    return patterns


def main():
    patterns = build_regex_patterns(DOSAGE_FORM_MAPPINGS)

    # Save mappings JSON
    with open("dosage_form_mappings.json", "w") as f:
        json.dump(DOSAGE_FORM_MAPPINGS, f, indent=2)
    print(f"Saved dosage_form_mappings.json  ({len(DOSAGE_FORM_MAPPINGS)} entries)")

    # Save regex patterns JSON
    with open("dosage_form_regex_patterns.json", "w") as f:
        json.dump(patterns, f, indent=2)
    print(f"Saved dosage_form_regex_patterns.json  ({len(patterns)} patterns)")

    # Quick summary to stdout
    print("\n=== Dosage Form → Suffix Mapping ===")
    for df, suffixes in DOSAGE_FORM_MAPPINGS.items():
        for s in suffixes:
            pat = suffix_to_pattern(s)
            print(f"  [{df}]  →  strip '{s}'  regex: {pat}")


if __name__ == "__main__":
    main()
