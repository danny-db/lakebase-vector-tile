"""
Dynamic Vector Tiles PoC — FastAPI backend.

Serves vector tiles dynamically from Lakebase using PostGIS ST_AsMVT,
eliminating the need for pre-generating tiles with tippecanoe.

Uses a native Postgres role with static credentials.

Environment variables (set in Databricks App config or a .env file):
    LAKEBASE_HOST              Postgres hostname (or PGHOST)
    LAKEBASE_PORT              Postgres port  (default 5432)
    LAKEBASE_DATABASE          Database name  (default databricks_postgres)
    LAKEBASE_SCHEMA            Schema to use  (default "geo")
    LAKEBASE_USER              Postgres role username
    LAKEBASE_PASSWORD          Postgres role password
"""

import os
import ssl
import logging
import traceback
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from tiles import router as tiles_router

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dtp-tiles")

# Path to the React production build
_app_dir = Path(__file__).resolve().parent
STATIC_DIR = _app_dir / "static" if (_app_dir / "static").is_dir() else _app_dir / "frontend" / "dist"


def _get_lakebase_credentials() -> dict:
    """Get Lakebase connection credentials."""
    pg_host = os.getenv("PGHOST") or os.getenv("LAKEBASE_HOST", "localhost")
    pg_port = os.getenv("PGPORT") or os.getenv("LAKEBASE_PORT", "5432")
    pg_database = os.getenv("PGDATABASE") or os.getenv("LAKEBASE_DATABASE", "databricks_postgres")
    pg_user = os.getenv("PGUSER") or os.getenv("LAKEBASE_USER", "postgres")
    pg_password = os.getenv("PGPASSWORD") or os.getenv("LAKEBASE_PASSWORD", "")

    logger.info(f"Connecting as {pg_user} to {pg_host}:{pg_port}/{pg_database}")

    return {
        "host": pg_host,
        "port": int(pg_port),
        "database": pg_database,
        "user": pg_user,
        "password": pg_password,
    }


async def create_pool(app: FastAPI) -> asyncpg.Pool:
    """Create a new asyncpg connection pool."""
    schema = app.state._schema
    ssl_ctx = app.state._ssl_ctx

    creds = _get_lakebase_credentials()

    pool = await asyncpg.create_pool(
        host=creds["host"],
        port=creds["port"],
        database=creds["database"],
        user=creds["user"],
        password=creds["password"],
        min_size=2,
        max_size=10,
        server_settings={"search_path": f"{schema}, public"},
        ssl=ssl_ctx,
    )
    logger.info("Database pool created successfully")
    return pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create the asyncpg pool on startup and close it on shutdown."""
    app.state._schema = os.getenv("LAKEBASE_SCHEMA", "geo")
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    app.state._ssl_ctx = ssl_ctx

    try:
        app.state.pool = await create_pool(app)
        app.state.db_error = None
    except Exception as e:
        logger.error(f"Failed to create database pool: {e}")
        logger.error(traceback.format_exc())
        app.state.pool = None
        app.state.db_error = str(e)

    yield

    if app.state.pool:
        await app.state.pool.close()


app = FastAPI(
    title="Dynamic Vector Tiles PoC",
    description="Serve vector tiles dynamically from Lakebase using PostGIS ST_AsMVT",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register tile router
app.include_router(tiles_router)


@app.get("/api/health", tags=["health"])
async def health_check(request: Request):
    """Health check that verifies database connectivity."""
    if not request.app.state.pool:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "database": "not connected",
                "error": getattr(request.app.state, "db_error", "unknown"),
            },
        )
    try:
        async with request.app.state.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
            postgis = await conn.fetchval("SELECT PostGIS_Version()")
        return {
            "status": "healthy",
            "database": "connected",
            "postgis_version": postgis,
        }
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "database": str(exc)},
        )


# Serve React SPA (static files + catch-all fallback)
if STATIC_DIR.is_dir():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str):
        file_path = STATIC_DIR / full_path
        if file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(STATIC_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
