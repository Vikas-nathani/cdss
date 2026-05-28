"""
salt_patterns.py — shared salt-suffix regex patterns used by both
enrich_ingredients.py (during ingestion) and step01_match_drug_rxcui.py
(during matching).

Two pattern sets:
  SALT_PATTERN          : compiled regex matching a single trailing salt
                          term (e.g., "metoprolol tartrate" → strip "tartrate")
  COMPOUND_SALT_PATTERNS : list of compiled regexes for multi-word salt
                          phrases (e.g., "disoproxil fumarate", "calcium
                          carbonate"). Tried first because they're more
                          specific than SALT_PATTERN.

Keep this file as the single source of truth — when adding/removing a salt,
edit only here.
"""

import re

# ─────────────────────────────────────────────────────────────────────────────
# Comprehensive single-term salt pattern (~95% of Indian pharmacopeia)
# ─────────────────────────────────────────────────────────────────────────────
SALT_PATTERN = re.compile(
    r'\s+(?:'

    r'mesylate|mesilate|besylate|besilate|tosylate|esylate|'
    r'xinafoate|camsylate|napadisylate|edisylate|napsylate|'
    r'isethionate|benzenesulfonate|ethanesulfonate|'

    r'hydrochloride|hcl|dihydrochloride|trihydrochloride|'
    r'hydrobromide|hydriodide|'
    r'bromide|chloride|iodide|fluoride|'

    r'sulfate|sulphate|bisulfate|sesquisulfate|'
    r'hydrogensulfate|sulfamate|dodecylsulfate|stearylsulfate|'

    r'sodium|disodium|trisodium|'
    r'potassium|dipotassium|'
    r'calcium|dicalcium|'
    r'magnesium|'
    r'zinc|lithium|copper|manganese|'
    r'ferrous|ferric|'
    r'aluminum|aluminium|bismuth|silver|'
    r'ammonium|diammonium|triammonium|'
    r'barium|strontium|'

    r'acetate|diacetate|'

    r'maleate|fumarate|hemifumarate|'
    r'tartrate|bitartrate|'
    r'succinate|ethylsuccinate|'
    r'malate|'
    r'citrate|'
    r'oxalate|dioxalate|'
    r'adipate|malonate|suberate|sebacate|glutarate|'
    r'orotate|aspartate|glutamate|'
    r'gluconate|lactate|benzoate|salicylate|'
    r'glycyrrhizinate|picrate|'
    r'caproate|caprylate|caprate|'
    r'laurate|myristate|'
    r'glycolate|glucuronate|galactarate|mucate|'
    r'mandelate|hippurate|saccharinate|'
    r'pidolate|'

    r'phosphate|monophosphate|diphosphate|triphosphate|'
    r'pyrophosphate|glycerophosphate|acid\s+phosphate|'

    r'nitrate|nitrite|'
    r'carbonate|bicarbonate|sesquicarbonate|'

    r'valerate|isovalerate|'
    r'propionate|dipropionate|'
    r'butyrate|isobutyrate|'
    r'furoate|acetonide|acetonide\s+phosphate|'
    r'hexacetonide|'
    r'stearate|palmitate|'
    r'decanoate|enanthate|undecanoate|'
    r'undecylenate|cypionate|phenylpropionate|'
    r'isocaproate|hexanoate|pivalate|'
    r'buciclate|lauroxil|'
    r'estolate|'
    r'hyclate|'

    r'axetil|pivoxil|dipivoxil|proxetil|'
    r'medoxomil|cilexetil|marboxil|moxetil|'
    r'disoproxil(?:\s+fumarate)?|alafenamide|'
    r'etexilate(?:\s+mesylate)?|'
    r'etabonate|fosamil|'
    r'propanediol|'
    r'pyroglutamate|'

    r'oleate|laurilsulfate|'

    r'tromethamine|meglumine|'
    r'benzathine|procaine|'
    r'diethylamine|triethylamine|ethylenediamine|'
    r'piperazine|'
    r'erbumine|'
    r'tert-butylamine|'
    r'betaine|choline|'

    r'lysine|arginine|histidinate|'
    r'glycinate|ascorbate|taurate|carnosine|'

    r'acistrate|embonate|pamoate|'

    r'hydroxide|oxide|'
    r'iodate|thiocyanate|phthalate|'

    r'pantothenate|polistirex|'

    r'anhydrous|solvate|'
    r'hemihydrate|monohydrate|dihydrate|trihydrate|'
    r'tetrahydrate|pentahydrate|hexahydrate|'
    r'heptahydrate|octahydrate|sesquihydrate|'
    r'hydrate|'
    r'ethanolate|isopropanolate'

    r')s?'

    r'(?:\s+(?:'
    r'anhydrous|solvate|'
    r'hemihydrate|monohydrate|dihydrate|trihydrate|'
    r'tetrahydrate|pentahydrate|hexahydrate|'
    r'heptahydrate|octahydrate|sesquihydrate|'
    r'hydrate'
    r'))?'

    r'\s*$',
    re.IGNORECASE
)

# ─────────────────────────────────────────────────────────────────────────────
# Multi-word compound salt patterns (tried first — most specific)
# ─────────────────────────────────────────────────────────────────────────────
COMPOUND_SALT_PATTERNS = [
    re.compile(r'\s+disoproxil\s+fumarate\s*$', re.I),
    re.compile(r'\s+tenofovir\s+disoproxil\s+fumarate\s*$', re.I),
    re.compile(r'\s+tenofovir\s+alafenamide\s*$', re.I),
    re.compile(r'\s+abacavir\s+sulfate\s*$', re.I),
    re.compile(r'\s+atazanavir\s+sulfate\s*$', re.I),
    re.compile(r'\s+darunavir\s+ethanolate\s*$', re.I),
    re.compile(r'\s+oseltamivir\s+phosphate\s*$', re.I),
    re.compile(r'\s+acyclovir\s+sodium\s*$', re.I),
    re.compile(r'\s+ganciclovir\s+sodium\s*$', re.I),
    re.compile(r'\s+potassium\s+clavulanate\s*$', re.I),
    re.compile(r'\s+clavulanate\s+potassium\s*$', re.I),
    re.compile(r'\s+sulbactam\s+sodium\s*$', re.I),
    re.compile(r'\s+sodium\s+sulbactam\s*$', re.I),
    re.compile(r'\s+tazobactam\s+sodium\s*$', re.I),
    re.compile(r'\s+sodium\s+tazobactam\s*$', re.I),
    re.compile(r'\s+azithromycin\s+dihydrate\s*$', re.I),
    re.compile(r'\s+erythromycin\s+ethylsuccinate\s*$', re.I),
    re.compile(r'\s+erythromycin\s+stearate\s*$', re.I),
    re.compile(r'\s+erythromycin\s+estolate\s*$', re.I),
    re.compile(r'\s+doxycycline\s+hyclate\s*$', re.I),
    re.compile(r'\s+doxycycline\s+monohydrate\s*$', re.I),
    re.compile(r'\s+gentamicin\s+sulfate\s*$', re.I),
    re.compile(r'\s+tobramycin\s+sulfate\s*$', re.I),
    re.compile(r'\s+amikacin\s+sulfate\s*$', re.I),
    re.compile(r'\s+streptomycin\s+sulfate\s*$', re.I),
    re.compile(r'\s+neomycin\s+sulfate\s*$', re.I),
    re.compile(r'\s+colistimethate\s+sodium\s*$', re.I),
    re.compile(r'\s+sodium\s+phosphate\s*$', re.I),
    re.compile(r'\s+disodium\s+phosphate\s*$', re.I),
    re.compile(r'\s+trisodium\s+phosphate\s*$', re.I),
    re.compile(r'\s+potassium\s+phosphate\s*$', re.I),
    re.compile(r'\s+dipotassium\s+phosphate\s*$', re.I),
    re.compile(r'\s+calcium\s+phosphate\s*$', re.I),
    re.compile(r'\s+monobasic\s+sodium\s+phosphate\s*$', re.I),
    re.compile(r'\s+dibasic\s+sodium\s+phosphate\s*$', re.I),
    re.compile(r'\s+calcium\s+carbonate\s*$', re.I),
    re.compile(r'\s+sodium\s+bicarbonate\s*$', re.I),
    re.compile(r'\s+potassium\s+bicarbonate\s*$', re.I),
    re.compile(r'\s+magnesium\s+carbonate\s*$', re.I),
    re.compile(r'\s+lithium\s+carbonate\s*$', re.I),
    re.compile(r'\s+lithium\s+citrate\s*$', re.I),
    re.compile(r'\s+calcium\s+trihydrate\s*$', re.I),
    re.compile(r'\s+calcium\s+dihydrate\s*$', re.I),
    re.compile(r'\s+calcium\s+monohydrate\s*$', re.I),
    re.compile(r'\s+zinc\s+monohydrate\s*$', re.I),
    re.compile(r'\s+ferrous\s+sulfate\s+monohydrate\s*$', re.I),
    re.compile(r'\s+zinc\s+sulfate\s+monohydrate\s*$', re.I),
    re.compile(r'\s+lisinopril\s+dihydrate\s*$', re.I),
    re.compile(r'\s+trihydrate\s*$', re.I),
    re.compile(r'\s+dihydrate\s*$', re.I),
    re.compile(r'\s+monohydrate\s*$', re.I),
    re.compile(r'\s+tetrahydrate\s*$', re.I),
    re.compile(r'\s+pentahydrate\s*$', re.I),
    re.compile(r'\s+hexahydrate\s*$', re.I),
    re.compile(r'\s+heptahydrate\s*$', re.I),
    re.compile(r'\s+sesquihydrate\s*$', re.I),
    re.compile(r'\s+hemihydrate\s*$', re.I),
    re.compile(r'\s+anhydrous\s*$', re.I),
    re.compile(r'\s+hydrochloride\s+monohydrate\s*$', re.I),
    re.compile(r'\s+hydrochloride\s+dihydrate\s*$', re.I),
    re.compile(r'\s+zinc\s+sulfate\s*$', re.I),
    re.compile(r'\s+ferrous\s+sulfate\s*$', re.I),
    re.compile(r'\s+magnesium\s+sulfate\s*$', re.I),
    re.compile(r'\s+calcium\s+citrate\s*$', re.I),
    re.compile(r'\s+potassium\s+citrate\s*$', re.I),
    re.compile(r'\s+sodium\s+citrate\s*$', re.I),
    re.compile(r'\s+ferrous\s+fumarate\s*$', re.I),
    re.compile(r'\s+ferric\s+ammonium\s+citrate\s*$', re.I),
    re.compile(r'\s+morphine\s+sulfate\s*$', re.I),
    re.compile(r'\s+codeine\s+phosphate\s*$', re.I),
    re.compile(r'\s+fentanyl\s+citrate\s*$', re.I),
    re.compile(r'\s+calcium\s+lactate\s*$', re.I),
    re.compile(r'\s+zinc\s+gluconate\s*$', re.I),
    re.compile(r'\s+calcium\s+gluconate\s*$', re.I),
    re.compile(r'\s+ferrous\s+gluconate\s*$', re.I),
    re.compile(r'\s+sodium\s+gluconate\s*$', re.I),
    re.compile(r'\s+amlodipine\s+besylate\s*$', re.I),
    re.compile(r'\s+amlodipine\s+maleate\s*$', re.I),
    re.compile(r'\s+doxazosin\s+mesylate\s*$', re.I),
    re.compile(r'\s+imatinib\s+mesylate\s*$', re.I),
    re.compile(r'\s+dabigatran\s+etexilate\s*$', re.I),
    re.compile(r'\s+dabigatran\s+etexilate\s+mesylate\s*$', re.I),
    re.compile(r'\s+metoprolol\s+succinate\s*$', re.I),
    re.compile(r'\s+metoprolol\s+tartrate\s*$', re.I),
    re.compile(r'\s+methylprednisolone\s+sodium\s+succinate\s*$', re.I),
    re.compile(r'\s+hydrocortisone\s+sodium\s+succinate\s*$', re.I),
    re.compile(r'\s+trelagliptin\s+succinate\s*$', re.I),
    re.compile(r'\s+quetiapine\s+fumarate\s*$', re.I),
    re.compile(r'\s+bisoprolol\s+fumarate\s*$', re.I),
    re.compile(r'\s+chlorpheniramine\s+maleate\s*$', re.I),
    re.compile(r'\s+enalapril\s+maleate\s*$', re.I),
    re.compile(r'\s+losartan\s+potassium\s*$', re.I),
    re.compile(r'\s+candesartan\s+cilexetil\s*$', re.I),
    re.compile(r'\s+olmesartan\s+medoxomil\s*$', re.I),
    re.compile(r'\s+azilsartan\s+medoxomil\s*$', re.I),
    re.compile(r'\s+eprosartan\s+mesylate\s*$', re.I),
    re.compile(r'\s+perindopril\s+erbumine\s*$', re.I),
    re.compile(r'\s+perindopril\s+arginine\s*$', re.I),
    re.compile(r'\s+perindopril\s+tert-butylamine\s*$', re.I),
    re.compile(r'\s+fosinopril\s+sodium\s*$', re.I),
    re.compile(r'\s+atorvastatin\s+calcium\s*$', re.I),
    re.compile(r'\s+rosuvastatin\s+calcium\s*$', re.I),
    re.compile(r'\s+pitavastatin\s+calcium\s*$', re.I),
    re.compile(r'\s+fluvastatin\s+sodium\s*$', re.I),
    re.compile(r'\s+pravastatin\s+sodium\s*$', re.I),
    re.compile(r'\s+warfarin\s+sodium\s*$', re.I),
    re.compile(r'\s+heparin\s+sodium\s*$', re.I),
    re.compile(r'\s+enoxaparin\s+sodium\s*$', re.I),
    re.compile(r'\s+fondaparinux\s+sodium\s*$', re.I),
    re.compile(r'\s+acenocoumarol\s+sodium\s*$', re.I),
    re.compile(r'\s+diclofenac\s+sodium\s*$', re.I),
    re.compile(r'\s+diclofenac\s+potassium\s*$', re.I),
    re.compile(r'\s+diclofenac\s+diethylamine\s*$', re.I),
    re.compile(r'\s+naproxen\s+sodium\s*$', re.I),
    re.compile(r'\s+ibuprofen\s+sodium\s*$', re.I),
    re.compile(r'\s+ibuprofen\s+lysine\s*$', re.I),
    re.compile(r'\s+omeprazole\s+magnesium\s*$', re.I),
    re.compile(r'\s+esomeprazole\s+magnesium\s*$', re.I),
    re.compile(r'\s+esomeprazole\s+sodium\s*$', re.I),
    re.compile(r'\s+pantoprazole\s+sodium\s*$', re.I),
    re.compile(r'\s+rabeprazole\s+sodium\s*$', re.I),
    re.compile(r'\s+prednisolone\s+sodium\s+phosphate\s*$', re.I),
    re.compile(r'\s+hydrocortisone\s+sodium\s+phosphate\s*$', re.I),
    re.compile(r'\s+betamethasone\s+sodium\s+phosphate\s*$', re.I),
    re.compile(r'\s+dexamethasone\s+sodium\s+phosphate\s*$', re.I),
    re.compile(r'\s+triamcinolone\s+acetonide\s*$', re.I),
    re.compile(r'\s+triamcinolone\s+hexacetonide\s*$', re.I),
    re.compile(r'\s+fluticasone\s+propionate\s*$', re.I),
    re.compile(r'\s+fluticasone\s+furoate\s*$', re.I),
    re.compile(r'\s+mometasone\s+furoate\s*$', re.I),
    re.compile(r'\s+beclomethasone\s+dipropionate\s*$', re.I),
    re.compile(r'\s+sitagliptin\s+phosphate\s*$', re.I),
    re.compile(r'\s+alogliptin\s+benzoate\s*$', re.I),
    re.compile(r'\s+dapagliflozin\s+propanediol\s*$', re.I),
    re.compile(r'\s+ertugliflozin\s+pyroglutamate\s*$', re.I),
    re.compile(r'\s+alendronate\s+sodium\s*$', re.I),
    re.compile(r'\s+risedronate\s+sodium\s*$', re.I),
    re.compile(r'\s+ibandronate\s+sodium\s*$', re.I),
    re.compile(r'\s+pamidronate\s+disodium\s*$', re.I),
    re.compile(r'\s+etidronate\s+disodium\s*$', re.I),
    re.compile(r'\s+clodronate\s+disodium\s*$', re.I),
    re.compile(r'\s+miconazole\s+nitrate\s*$', re.I),
    re.compile(r'\s+magnesium\s+hydroxide\s*$', re.I),
    re.compile(r'\s+aluminum\s+hydroxide\s*$', re.I),
    re.compile(r'\s+aluminium\s+hydroxide\s*$', re.I),
    re.compile(r'\s+zinc\s+oxide\s*$', re.I),
    re.compile(r'\s+magnesium\s+oxide\s*$', re.I),
    re.compile(r'\s+calcium\s+pantothenate\s*$', re.I),
    re.compile(r'\s+testosterone\s+enanthate\s*$', re.I),
    re.compile(r'\s+testosterone\s+cypionate\s*$', re.I),
    re.compile(r'\s+testosterone\s+propionate\s*$', re.I),
    re.compile(r'\s+testosterone\s+undecanoate\s*$', re.I),
    re.compile(r'\s+nandrolone\s+decanoate\s*$', re.I),
    re.compile(r'\s+nandrolone\s+phenylpropionate\s*$', re.I),
    re.compile(r'\s+estradiol\s+valerate\s*$', re.I),
    re.compile(r'\s+estradiol\s+cypionate\s*$', re.I),
    re.compile(r'\s+estradiol\s+benzoate\s*$', re.I),
    re.compile(r'\s+medroxyprogesterone\s+acetate\s*$', re.I),
    re.compile(r'\s+norethindrone\s+acetate\s*$', re.I),
    re.compile(r'\s+fluphenazine\s+decanoate\s*$', re.I),
    re.compile(r'\s+haloperidol\s+decanoate\s*$', re.I),
    re.compile(r'\s+paliperidone\s+palmitate\s*$', re.I),
    re.compile(r'\s+aripiprazole\s+lauroxil\s*$', re.I),
    re.compile(r'\s+tromethamine\s*$', re.I),
    re.compile(r'\s+meglumine\s*$', re.I),
    re.compile(r'\s+gadopentetate\s+dimeglumine\s*$', re.I),
]
