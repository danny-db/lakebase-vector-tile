"""
Vector tile endpoint — the key new code for this PoC.

Serves Mapbox Vector Tiles (MVT/protobuf) dynamically from Lakebase
using PostGIS ST_AsMVT. This eliminates the need for pre-generating
tiles with tippecanoe.

Routes:
    GET /api/tiles/{layer}/{z}/{x}/{y}.pbf  — binary MVT tile
    GET /api/tiles/metadata                 — available layers + bounds
"""

import logging
import math
import time

from fastapi import APIRouter, Request, Response, HTTPException

from db import fetch_val, fetch_all

logger = logging.getLogger("dtp-tiles")

router = APIRouter(prefix="/api/tiles", tags=["tiles"])

# Simple in-memory tile cache: (layer, z, x, y) -> (bytes, timestamp)
_tile_cache: dict[tuple, tuple[bytes, float]] = {}
_CACHE_TTL = 300  # 5 minutes
_CACHE_MAX_SIZE = 1000


def _evict_stale_cache() -> None:
    """Remove expired entries from the tile cache."""
    now = time.time()
    stale = [k for k, (_, ts) in _tile_cache.items() if now - ts > _CACHE_TTL]
    for k in stale:
        del _tile_cache[k]
    # If still too large, drop oldest half
    if len(_tile_cache) > _CACHE_MAX_SIZE:
        sorted_keys = sorted(_tile_cache, key=lambda k: _tile_cache[k][1])
        for k in sorted_keys[: len(sorted_keys) // 2]:
            del _tile_cache[k]


def tile_to_bbox(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """
    Convert tile coordinates to Web Mercator (EPSG:3857) bounding box.

    Returns (xmin, ymin, xmax, ymax) in meters.
    Used as fallback when ST_TileEnvelope is not available.
    """
    EARTH_CIRCUMFERENCE = 20037508.342789244
    n = 2**z
    tile_size = 2 * EARTH_CIRCUMFERENCE / n

    xmin = -EARTH_CIRCUMFERENCE + x * tile_size
    xmax = xmin + tile_size
    ymax = EARTH_CIRCUMFERENCE - y * tile_size
    ymin = ymax - tile_size

    return (xmin, ymin, xmax, ymax)


# Layer configurations: name -> (table, columns, geometry_column)
LAYERS = {
    "regions": {
        "table": "geo.regions",
        "columns": ["name", "iso_a3", "iso_3166_2", "type", "pop_est", "continent"],
        "geom_col": "geom",
        "description": "Australian state/territory boundaries",
        "min_zoom": 0,
        "max_zoom": 14,
    },
    "suburbs": {
        "table": "geo.suburbs",
        "columns": ["name", "sal_code", "state_code", "state_name", "area_sqkm"],
        "geom_col": "geom",
        "description": "Victorian suburb boundaries (ABS SAL 2021)",
        "min_zoom": 0,
        "max_zoom": 16,
    },
}


@router.get("/metadata")
async def tile_metadata():
    """Return available tile layers and their configuration."""
    return {
        "layers": {
            name: {
                "description": cfg["description"],
                "min_zoom": cfg["min_zoom"],
                "max_zoom": cfg["max_zoom"],
            }
            for name, cfg in LAYERS.items()
        },
        "tile_url": "/api/tiles/{layer}/{z}/{x}/{y}.pbf",
    }


@router.get("/{layer}/{z}/{x}/{y}.pbf")
async def get_tile(request: Request, layer: str, z: int, x: int, y: int):
    """
    Serve a single vector tile as Mapbox Vector Tile (protobuf).

    Uses PostGIS ST_AsMVT + ST_AsMVTGeom to dynamically generate tiles
    from Lakebase geometry data. Falls back to manual bbox calculation
    if ST_TileEnvelope is not available.
    """
    if layer not in LAYERS:
        raise HTTPException(status_code=404, detail=f"Unknown layer: {layer}")

    cfg = LAYERS[layer]

    # Validate tile coordinates
    max_tile = 2**z
    if x < 0 or x >= max_tile or y < 0 or y >= max_tile:
        raise HTTPException(status_code=400, detail="Tile coordinates out of range")

    # Check cache
    cache_key = (layer, z, x, y)
    if cache_key in _tile_cache:
        tile_data, ts = _tile_cache[cache_key]
        if time.time() - ts < _CACHE_TTL:
            return Response(
                content=tile_data,
                media_type="application/x-protobuf",
                headers={
                    "Cache-Control": "public, max-age=300",
                    "Access-Control-Allow-Origin": "*",
                },
            )

    # Build column list for the query
    col_list = ", ".join(cfg["columns"])
    geom_col = cfg["geom_col"]
    table = cfg["table"]

    # Use ST_TileEnvelope (PostGIS 3.1+) for envelope calculation.
    # The query transforms geometry to Web Mercator (3857), clips to tile
    # bounds, and encodes as MVT protobuf.
    query = f"""
        SELECT ST_AsMVT(q, $1, 4096, 'geom') FROM (
            SELECT
                ST_AsMVTGeom(
                    ST_Transform({geom_col}, 3857),
                    ST_TileEnvelope($2::integer, $3::integer, $4::integer),
                    4096, 256, true
                ) AS geom,
                {col_list}
            FROM {table}
            WHERE ST_Intersects(
                ST_Transform({geom_col}, 3857),
                ST_TileEnvelope($2::integer, $3::integer, $4::integer)
            )
        ) q
    """

    try:
        tile_data = await fetch_val(request.app, query, layer, z, x, y)
    except Exception as e:
        error_msg = str(e)
        # Fallback: if ST_TileEnvelope not available, use manual bbox
        if "ST_TileEnvelope" in error_msg:
            logger.warning("ST_TileEnvelope not available, using manual bbox")
            xmin, ymin, xmax, ymax = tile_to_bbox(z, x, y)
            query_fallback = f"""
                SELECT ST_AsMVT(q, $1, 4096, 'geom') FROM (
                    SELECT
                        ST_AsMVTGeom(
                            ST_Transform({geom_col}, 3857),
                            ST_MakeEnvelope($2, $3, $4, $5, 3857),
                            4096, 256, true
                        ) AS geom,
                        {col_list}
                    FROM {table}
                    WHERE ST_Intersects(
                        ST_Transform({geom_col}, 3857),
                        ST_MakeEnvelope($2, $3, $4, $5, 3857)
                    )
                ) q
            """
            tile_data = await fetch_val(
                request.app, query_fallback, layer, xmin, ymin, xmax, ymax
            )
        else:
            logger.error(f"Tile query failed: {e}")
            raise HTTPException(status_code=500, detail="Tile generation failed")

    if tile_data is None:
        tile_data = b""

    # Ensure tile_data is bytes
    if isinstance(tile_data, memoryview):
        tile_data = bytes(tile_data)

    # Cache the result
    _evict_stale_cache()
    _tile_cache[cache_key] = (tile_data, time.time())

    return Response(
        content=tile_data,
        media_type="application/x-protobuf",
        headers={
            "Cache-Control": "public, max-age=300",
            "Access-Control-Allow-Origin": "*",
        },
    )
