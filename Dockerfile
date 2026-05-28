# ============================================================================
# CDSS FastAPI Dockerfile
# ============================================================================

FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY app/ app/
COPY scripts/ scripts/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]

# ============================================================================
# requirements.txt contents:
# ============================================================================
# fastapi==0.111.0
# uvicorn[standard]==0.30.1
# asyncpg==0.29.0
# neo4j==5.20.0
# httpx==0.27.0
# pydantic==2.7.0
# numpy==1.26.4
# pgvector==0.3.0
# python-dotenv==1.0.1
# ============================================================================