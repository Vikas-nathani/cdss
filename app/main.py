import os
import time
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from neo4j import AsyncGraphDatabase

PG_HOST = os.getenv("DATABASE_HOST", "localhost")
PG_PORT = int(os.getenv("DATABASE_PORT", "5432"))
PG_DB = os.getenv("DATABASE_NAME", "cdss")
PG_USER = os.getenv("DATABASE_USER", "cdss_app")
PG_PASS = os.getenv("DATABASE_PASSWORD", "")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD", "")

pg_pool = None
neo4j_driver = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool, neo4j_driver
    pg_pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT, database=PG_DB,
        user=PG_USER, password=PG_PASS,
        min_size=2, max_size=10
    )
    neo4j_driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    yield
    await pg_pool.close()
    await neo4j_driver.close()


app = FastAPI(
    title="CDSS API",
    description="Clinical Decision Support System",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def timing_middleware(request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    response.headers["X-Response-Time-Ms"] = str(int((time.monotonic() - start) * 1000))
    return response


@app.get("/")
async def root():
    return {"message": "CDSS API is running", "version": "1.0.0"}


@app.get("/health")
async def health():
    pg_status = "unknown"
    neo4j_status = "unknown"

    try:
        async with pg_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        pg_status = "healthy"
    except Exception as e:
        pg_status = f"error: {e}"

    try:
        async with neo4j_driver.session() as session:
            result = await session.run("RETURN 1 AS n")
            await result.single()
        neo4j_status = "healthy"
    except Exception as e:
        neo4j_status = f"error: {e}"

    overall = "ok" if pg_status == "healthy" and neo4j_status == "healthy" else "degraded"
    return {"status": overall, "postgresql": pg_status, "neo4j": neo4j_status}


@app.get("/api/v1/stats")
async def stats():
    results = {}
    async with pg_pool.acquire() as conn:
        results["tables"] = await conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'"
        )
        results["drug_count"] = await conn.fetchval("SELECT COUNT(*) FROM drugdb.drug")
        results["rag_chunk_count"] = await conn.fetchval("SELECT COUNT(*) FROM rag_chunk")
    async with neo4j_driver.session() as session:
        r = await session.run("MATCH (n) RETURN labels(n) AS label, count(n) AS cnt")
        records = await r.data()
        results["neo4j_nodes"] = {str(rec["label"]): rec["cnt"] for rec in records}
    return results


if __name__ == "__main__":
    import uvicorn
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
