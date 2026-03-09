-- ============================================================
-- Dynamic Vector Tiles PoC - Lakebase Setup
-- ============================================================
-- Run this against the Lakebase Postgres endpoint to enable
-- PostGIS and create the regions table for polygon data.
-- ============================================================

-- Enable PostGIS for spatial queries
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create schema for geographic data
CREATE SCHEMA IF NOT EXISTS geo;

-- ------------------------------------------------------------
-- Regions table: polygon data (Australian state/territory boundaries)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS geo.regions (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    iso_a3      VARCHAR(10),
    iso_3166_2  VARCHAR(10),
    type        VARCHAR(100),
    pop_est     BIGINT,
    continent   VARCHAR(100),
    geom        geometry(MultiPolygon, 4326)
);

-- Spatial index for fast intersection queries (critical for tile serving)
CREATE INDEX IF NOT EXISTS idx_regions_geom
    ON geo.regions USING GIST (geom);

-- Index on name for metadata lookups
CREATE INDEX IF NOT EXISTS idx_regions_name
    ON geo.regions (name);
