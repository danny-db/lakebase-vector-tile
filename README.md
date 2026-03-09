# Lakebase Vector Tiles

**Serve vector tiles dynamically from Databricks Lakebase using PostGIS `ST_AsMVT` — no tippecanoe, no pre-generation.**

This PoC proves that Databricks Lakebase (PostgreSQL + PostGIS) can serve Mapbox Vector Tiles (MVT) directly to a MapLibre GL JS frontend, eliminating the entire tile pre-generation pipeline.

**Live demo:** https://dtp-tiles-7474652113843941.aws.databricksapps.com

## What It Does

- **2 tile layers** served dynamically from Lakebase:
  - **Regions** — 9 Australian state/territory boundaries (Natural Earth 50m)
  - **Suburbs** — 2,944 Victorian suburb boundaries (ABS SAL 2021)
- **Click-to-inspect** popups showing metadata for each feature
- **Zoom-dependent rendering** — states visible at all zoom levels, suburbs appear at zoom 6+, labels at zoom 10+
- **Sub-second tile generation** via PostGIS `ST_AsMVT` + `ST_AsMVTGeom`

## Architecture

```
Browser (MapLibre GL JS)
  ↓ GET /api/tiles/{layer}/{z}/{x}/{y}.pbf
FastAPI (Databricks App)
  ↓ ST_AsMVT query via asyncpg
Lakebase (PostgreSQL 17 + PostGIS 3.5)
  ↓ geo.regions (9 rows) + geo.suburbs (2,944 rows)
```

### Why This Matters

| Traditional (tippecanoe) | This PoC (ST_AsMVT) |
|--------------------------|----------------------|
| Pre-generate all tiles offline | Generate tiles on-demand |
| Minutes/hours of processing | Sub-second per tile |
| Static — must regenerate on data change | Always fresh from the database |
| Requires external tooling outside Databricks | Runs entirely on Lakebase |
| Storage costs for tile pyramids | No tile storage needed |
| Separate tile hosting infrastructure | Databricks App serves everything |

## Project Structure

```
lakebase-vector-tile/
├── databricks.yml                  # DAB bundle config
├── app.yaml                        # Databricks App config (template)
├── requirements.txt                # Python deps (fastapi, asyncpg, uvicorn)
├── app.py                          # FastAPI app (lifespan, health, SPA serving)
├── db.py                           # asyncpg helpers (pool management)
├── tiles.py                        # Vector tile routes + ST_AsMVT queries
├── setup.sql                       # PostGIS schema DDL (reference)
├── load_data.py                    # Load Natural Earth state boundaries
├── .env.example                    # Env var template
├── frontend/
│   ├── package.json                # React 18 + MapLibre GL + Vite + Tailwind
│   ├── vite.config.ts              # Proxy /api → localhost:8000
│   └── src/
│       ├── App.tsx                  # Header + full-screen map
│       └── components/
│           └── VectorMap.tsx        # MapLibre GL JS with 2 vector tile layers
└── src/
    └── notebooks/
        ├── 01_setup_lakebase.py    # Provision Lakebase + PostGIS + load regions
        └── 02_load_vic_suburbs.py  # Download ABS SAL data + load suburbs
```

## Deployment Guide

### Prerequisites

- Databricks CLI configured with a workspace profile
- Python 3.11+
- Node.js 18+
- `gh` CLI (for GitHub operations)

### Step 1: Deploy the DAB (notebooks + jobs)

```bash
# Validate the bundle
databricks bundle validate -t dev -p <PROFILE>

# Deploy notebooks and job definitions
databricks bundle deploy -t dev -p <PROFILE>
```

### Step 2: Provision Lakebase (run the setup notebook)

Run `01_setup_lakebase.py` as a job or interactively in a notebook. It will:

1. Create a Lakebase Autoscaling project `dtp-tiles` (PG17, native login)
2. Create a branch + endpoint
3. Enable PostGIS extension
4. Create `geo` schema + `geo.regions` table + GIST spatial index
5. Create a native Postgres role `dtp_tiles_app`
6. Download Natural Earth 50m Admin-1 GeoJSON, filter to Australian states
7. Insert 9 state/territory boundaries
8. Register a Unity Catalog catalog

```bash
# Option A: Run as a DAB job
databricks bundle run setup_lakebase -t dev -p <PROFILE>

# Option B: Run interactively
# Open the notebook in the Databricks workspace and run all cells
```

**Note:** The notebook prints the Lakebase endpoint host and app role credentials at the end. You'll need these for Step 4.

### Step 3: Load Victorian suburbs

Run `02_load_vic_suburbs.py` after the Lakebase project is provisioned:

```bash
# Option A: Run as a DAB job
databricks bundle run load_vic_suburbs -t dev -p <PROFILE>

# Option B: Run interactively in the workspace
```

This downloads ABS SAL 2021 boundary data (~99 MB shapefile), filters to Victoria (STE_CODE21 = '2'), and loads 2,944 suburb boundaries into `geo.suburbs`.

### Step 4: Build the frontend

```bash
cd frontend
npm install
npm run build
cd ..
```

### Step 5: Prepare the deploy directory

```bash
# Create deploy directory
mkdir -p deploy/static

# Copy backend files
cp app.py db.py tiles.py requirements.txt deploy/

# Copy built frontend
cp -r frontend/dist/* deploy/static/

# Create app.yaml with your Lakebase credentials
cat > deploy/app.yaml << 'EOF'
command:
  - uvicorn
  - app:app
  - --host
  - "0.0.0.0"
  - --port
  - "8000"

env:
  - name: LAKEBASE_HOST
    description: Lakebase Autoscaling endpoint host
    value: <YOUR_ENDPOINT_HOST>
  - name: LAKEBASE_PORT
    description: Lakebase Postgres port
    value: "5432"
  - name: LAKEBASE_DATABASE
    description: Lakebase database name
    value: databricks_postgres
  - name: LAKEBASE_SCHEMA
    description: Postgres schema name
    value: geo
  - name: LAKEBASE_USER
    description: Native Postgres role username
    value: <YOUR_APP_ROLE>
  - name: LAKEBASE_PASSWORD
    description: Native Postgres role password
    value: "<YOUR_APP_ROLE_PASSWORD>"
EOF
```

Replace `<YOUR_ENDPOINT_HOST>`, `<YOUR_APP_ROLE>`, and `<YOUR_APP_ROLE_PASSWORD>` with values from Step 2 output.

### Step 6: Deploy the Databricks App

```bash
# Create the app (first time only)
databricks apps create dtp-tiles -p <PROFILE>

# Upload files to workspace
databricks workspace import-dir deploy/ \
  "/Workspace/Users/<YOUR_EMAIL>/apps/dtp-tiles" \
  --overwrite -p <PROFILE>

# Deploy
databricks apps deploy dtp-tiles \
  --source-code-path "/Workspace/Users/<YOUR_EMAIL>/apps/dtp-tiles" \
  -p <PROFILE>
```

### Step 7: Verify

```bash
# Check app status
databricks apps get dtp-tiles -p <PROFILE>

# Test health endpoint
curl https://<APP_URL>/api/health

# Test tile endpoint
curl -o test.pbf https://<APP_URL>/api/tiles/regions/4/14/9.pbf
```

## Local Development

### Backend

```bash
cp .env.example .env
# Edit .env with your Lakebase credentials

pip install -r requirements.txt
python app.py
# FastAPI on http://localhost:8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev
# Vite dev server on http://localhost:5173 (proxies /api to :8000)
```

## Key Technical Details

### The Core Query

```sql
SELECT ST_AsMVT(q, 'suburbs', 4096, 'geom') FROM (
    SELECT
        ST_AsMVTGeom(
            ST_Transform(geom, 3857),
            ST_TileEnvelope(z, x, y),
            4096, 256, true
        ) AS geom,
        name, sal_code, state_code, state_name, area_sqkm
    FROM geo.suburbs
    WHERE ST_Intersects(
        ST_Transform(geom, 3857),
        ST_TileEnvelope(z, x, y)
    )
) q
```

- `ST_TileEnvelope(z, x, y)` — Web Mercator bounding box for a tile
- `ST_AsMVTGeom` — clips and transforms geometry to tile-local coordinates (4096x4096)
- `ST_AsMVT` — encodes the result set as a Mapbox Vector Tile protobuf
- Falls back to `ST_MakeEnvelope` if `ST_TileEnvelope` is unavailable

### Tile Layers

Defined in `tiles.py:LAYERS`. Adding a new layer:

```python
LAYERS["my_layer"] = {
    "table": "schema.table_name",
    "columns": ["col1", "col2"],
    "geom_col": "geom",
    "description": "My new layer",
    "min_zoom": 0,
    "max_zoom": 14,
}
```

### Caching

In-memory LRU cache (1000 tiles, 5-min TTL) + HTTP `Cache-Control: public, max-age=300`.

## Data Sources

| Layer | Source | Features | License |
|-------|--------|----------|---------|
| Regions | [Natural Earth 50m Admin-1](https://www.naturalearthdata.com/downloads/50m-cultural-vectors/) | 9 Australian states/territories | Public domain |
| Suburbs | [ABS SAL 2021 (ASGS Ed. 3)](https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files) | 2,944 Victorian suburbs | CC BY 4.0 |

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Tile server | FastAPI + asyncpg |
| Tile encoding | PostGIS ST_AsMVT |
| Database | Lakebase Autoscaling (PG17 + PostGIS 3.5) |
| Map rendering | MapLibre GL JS |
| Frontend | React 18 + TypeScript + Vite + Tailwind CSS |
| Deployment | Databricks Apps + DAB |
| Data provisioning | Databricks Notebooks (Python) |

## Redeploying to a Different Workspace

To deploy this to a new workspace:

1. **Update `databricks.yml`** — change the `workspace.host` URL
2. **Run the setup notebook** — provisions a new Lakebase project with PostGIS
3. **Run the suburbs notebook** — loads Victorian suburb data
4. **Update credentials** — use the new endpoint host and role password in `deploy/app.yaml`
5. **Deploy the app** — follow Steps 4-6 above

The notebooks are fully idempotent and safe to re-run.

## Background

This PoC was built to demonstrate that Databricks Lakebase with PostGIS can replace the tippecanoe tile pre-generation pipeline entirely. Instead of pre-generating static tile pyramids offline, tiles are generated on-demand from PostGIS using `ST_AsMVT`, keeping everything within the Databricks platform.
