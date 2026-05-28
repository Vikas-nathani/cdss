"""
config.py — DB connection parameters, file paths, and constants.
"""

import os
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────

PIPELINE_DIR = Path(__file__).parent
LOGS_DIR = PIPELINE_DIR / "logs"
SYNONYMS_JSON = Path(os.getenv("SYNONYMS_JSON_PATH", str(PIPELINE_DIR.parent.parent / "synonyms.json")))

# ── Database ──────────────────────────────────────────────────────────────────

def get_db_config(password_override: str = None) -> dict:
    """Return psycopg2 connection kwargs. Password comes from .env or CLI arg."""
    pw = password_override or os.getenv("DATABASE_PASSWORD") or os.getenv("PG_PASSWORD")
    if not pw:
        raise RuntimeError("No DB password found. Set DATABASE_PASSWORD in .env or pass --password.")
    # Strip surrounding quotes that bash env-file parsers sometimes leave
    pw = pw.strip('"').strip("'")
    return {
        "host": os.getenv("DATABASE_HOST", "localhost"),
        "port": int(os.getenv("DATABASE_PORT", "5432")),
        "dbname": os.getenv("DATABASE_NAME", "postgres"),
        "user": os.getenv("DATABASE_USER", "postgres"),
        "password": pw,
        "connect_timeout": 10,
        "application_name": "rxcui_backfill_pipeline",
    }

# ── Constants ─────────────────────────────────────────────────────────────────

DEFAULT_BATCH_SIZE = 5000
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 5
MIN_STRIPPED_LENGTH = 3

RXNCONSO_QUERY = """
    SELECT LOWER(str) AS str_lower, rxcui, tty
    FROM public.rxnconso
    WHERE sab = 'RXNORM' AND suppress = 'N' AND tty IN ('IN', 'PIN')
"""

TARGET_ROWS_QUERY = """
    SELECT id, indian_brand_id, ingredient_name_norm
    FROM drugdb.indian_brand_ingredient
    WHERE rxcui_in IS NULL
    ORDER BY id
"""
