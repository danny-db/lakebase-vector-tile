"""
Database connection helpers for asyncpg.

Provides thin wrappers around the asyncpg connection pool so that route
handlers can execute queries and get results back as plain Python dicts
instead of asyncpg Record objects.

Handles automatic pool refresh: if a query fails with
InvalidPasswordError (expired Lakebase token), the pool is recreated
with fresh credentials and the query is retried once.
"""

import asyncio
import logging

from fastapi import FastAPI
import asyncpg

logger = logging.getLogger("dtp-tiles")

# Lock to prevent multiple concurrent pool refreshes
_refresh_lock = asyncio.Lock()


def get_pool(app: FastAPI) -> asyncpg.Pool:
    """Retrieve the asyncpg connection pool stored on the app instance."""
    return app.state.pool


async def _refresh_pool(app: FastAPI) -> None:
    """Close the stale pool and create a new one with fresh credentials."""
    async with _refresh_lock:
        from app import create_pool

        old_pool = app.state.pool
        if old_pool:
            try:
                await old_pool.close()
            except Exception:
                pass

        app.state.pool = await create_pool(app)
        logger.info("Connection pool refreshed with new credentials")


async def fetch_all(app: FastAPI, query: str, *args) -> list[dict]:
    """Execute a query and return every row as a list of dicts."""
    try:
        async with app.state.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]
    except asyncpg.exceptions.InvalidPasswordError:
        logger.warning("Token expired during fetch_all, refreshing pool...")
        await _refresh_pool(app)
        async with app.state.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]


async def fetch_one(app: FastAPI, query: str, *args) -> dict | None:
    """Execute a query and return the first row as a dict, or None."""
    try:
        async with app.state.pool.acquire() as conn:
            row = await conn.fetchrow(query, *args)
            return dict(row) if row else None
    except asyncpg.exceptions.InvalidPasswordError:
        logger.warning("Token expired during fetch_one, refreshing pool...")
        await _refresh_pool(app)
        async with app.state.pool.acquire() as conn:
            row = await conn.fetchrow(query, *args)
            return dict(row) if row else None


async def fetch_val(app: FastAPI, query: str, *args):
    """Execute a query and return a single scalar value."""
    try:
        async with app.state.pool.acquire() as conn:
            return await conn.fetchval(query, *args)
    except asyncpg.exceptions.InvalidPasswordError:
        logger.warning("Token expired during fetch_val, refreshing pool...")
        await _refresh_pool(app)
        async with app.state.pool.acquire() as conn:
            return await conn.fetchval(query, *args)


async def execute(app: FastAPI, query: str, *args) -> str:
    """Execute a query (INSERT / UPDATE / DELETE) and return the status."""
    try:
        async with app.state.pool.acquire() as conn:
            return await conn.execute(query, *args)
    except asyncpg.exceptions.InvalidPasswordError:
        logger.warning("Token expired during execute, refreshing pool...")
        await _refresh_pool(app)
        async with app.state.pool.acquire() as conn:
            return await conn.execute(query, *args)
