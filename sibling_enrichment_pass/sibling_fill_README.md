# sibling_fill — Null Field Backfill via Sibling Formulation Borrowing

## Overview

`fill_null_siblings.py` fills NULL values in `drugdb.drug` for formulations
that are present in `drugdb.drug_formulation_linkage_map_unique`.

Rather than using an LLM or external lookup, it borrows values from **sibling
formulations** — other rows in `drugdb.drug` that share the same
`generic_formulation` AND `dosage_forms` as the row being filled.

### Why sibling borrowing?

Many drug databases have the same generic compound available under multiple
product names or pack sizes. These variants almost always share the same
pharmacological profile (mechanism of action, routes, therapeutic class, etc.).
When one variant has a NULL in those fields and another variant of the same
compound-form has a value, it is safe to copy it over.

This is a **zero-hallucination, zero-cost** way to close a significant portion
of the NULL gap before any LLM enrichment is needed.

---

## Pre-run Stats (NULL Audit)

Scope: formulations present in `drugdb.drug_formulation_linkage_map_unique`

| Field                | Total  | NULL  | % Missing |
|----------------------|--------|-------|-----------|
| routes               | 10 752 |   362 |   3.4 %   |
| mechanism_of_action  | 10 752 | 4 215 |  39.2 %   |
| pharmacologic_class  | 10 752 | 2 575 |  24.0 %   |
| therapeutic_class    | 10 752 |   582 |   5.4 %   |
| mechanism_class      | 10 752 | 2 450 |  22.8 %   |

---

## How It Works

### Step-by-step

1. **Fetch in-scope formulations** — all rows in `drugdb.drug` whose
   `formulation_id` appears in `drug_formulation_linkage_map_unique`.

2. **Group by sibling key** — rows are grouped by
   `(generic_formulation, dosage_forms)`. Every row in a group is a sibling of
   every other row in the same group.

3. **Identify nulls** — for each formulation in a group, check each of the five
   target fields. If a field is NULL (or empty array / empty string), it is a
   candidate for filling.

4. **Collect sibling values** — look at every other formulation in the same
   group. Gather all non-null values for that field.

5. **Resolve conflicts** — if more than one sibling has a non-null value, apply
   the conflict resolution strategy (see below).

6. **Write (or log)** — in `--full-run` mode the resolved value is written back
   to `drugdb.drug`. In `--dry-run` mode it is only logged.

### Conflict resolution

| Field type          | Fields                                                        | Strategy                                      |
|---------------------|---------------------------------------------------------------|-----------------------------------------------|
| `text`              | `mechanism_of_action`                                         | Pick the **longest** non-null string           |
| `array`             | `routes`, `pharmacologic_class`, `therapeutic_class`, `mechanism_class` | **Union** all non-null arrays + deduplicate (case-insensitive) |

The rationale:
- Longer text → more complete description.
- Array union → captures every known route/class without losing information.

---

## How to Run

### Prerequisites

```bash
# Install dependencies (if not already installed)
pip install psycopg2-binary python-dotenv
```

Make sure a `.env` file is reachable (project root or current directory) with:

```
DB_HOST=<host>
DB_PORT=5432
DB_NAME=<dbname>
DB_USER=<user>
DB_PASSWORD=<password>
```

### Always dry-run first

```bash
cd ~/cdss/scripts/sibling_fill

# Step 1 — inspect what WOULD be filled (no DB writes)
python fill_null_siblings.py --dry-run

# Step 2 — review the log file in logs/
ls -lh logs/

# Step 3 — if the dry run looks correct, execute
python fill_null_siblings.py --full-run
```

---

## Log File

**Location:** `logs/sibling_fill_YYYYMMDD_HHMMSS.log`

Each run creates a timestamped log file alongside the INFO-level console output.

### What is logged

| Level   | Content                                                                 |
|---------|-------------------------------------------------------------------------|
| `INFO`  | Run mode, DB connection status, formulation count, fill action count, summary table |
| `DEBUG` | Per-action detail: `formulation_id`, `generic_formulation`, `dosage_forms`, `field`, `sibling_ids`, resolution strategy, final value |

### Reading the logs

- Search for `MULTIPLE SIBLINGS FOUND` to review every case where conflict
  resolution was applied.
- Search for `[DRY-RUN]` or `[FULL-RUN]` to filter per-action entries.
- The **SUMMARY REPORT** block at the end shows before/after null counts for
  every field.

---

## Before vs After Comparison

| Field                | Total  | Null Before | % Before | Filled | Null After | % After | Reduction |
|----------------------|--------|-------------|----------|--------|------------|---------|-----------|
| routes               | 10 752 | 362         | 3.4 %    | 134    | 228        | 2.1 %   | -37.0 %   |
| mechanism_of_action  | 10 752 | 4 215       | 39.2 %   | 521    | 3 694      | 34.4 %  | -12.4 %   |
| pharmacologic_class  | 10 752 | 2 575       | 24.0 %   | 280    | 2 295      | 21.3 %  | -10.9 %   |
| therapeutic_class    | 10 752 | 582         | 5.4 %    | 231    | 351        | 3.3 %   | -39.7 %   |
| mechanism_class      | 10 752 | 2 450       | 22.8 %   | 280    | 2 170      | 20.2 %  | -11.4 %   |

---

## Post-run Stats

Run executed: 2026-05-22 — full-run committed to `drugdb.drug`.

| Field                | Null Before | Filled by Sibling | Null After | % Missing After |
|----------------------|-------------|-------------------|------------|-----------------|
| routes               | 362         | 134               | 228        | 2.1 %           |
| mechanism_of_action  | 4 215       | 521               | 3 694      | 34.4 %          |
| pharmacologic_class  | 2 575       | 280               | 2 295      | 21.3 %          |
| therapeutic_class    | 582         | 231               | 351        | 3.3 %           |
| mechanism_class      | 2 450       | 280               | 2 170      | 20.2 %          |
| **Total**            | **10 584**  | **1 446**         | **8 738**  |                 |

### Key observations

- `routes` and `therapeutic_class` are nearly resolved (2–3% remaining).
- `mechanism_of_action`, `pharmacologic_class`, and `mechanism_class` still
  have significant gaps (20–34%) — these are the primary LLM enrichment targets.
- Formulations that could not be filled (e.g. `oxygen 99 %`) belong to sibling
  groups where every member shares the same nulls — no donor existed within the
  exact `(generic_formulation, dosage_forms)` group.

The "Null After" column is the remaining gap for LLM-based enrichment in the
next pipeline stage.
