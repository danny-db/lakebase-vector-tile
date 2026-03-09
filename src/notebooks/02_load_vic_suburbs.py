# Databricks notebook source
# MAGIC %md
# MAGIC # Load Victorian Suburb Boundaries into Lakebase
# MAGIC
# MAGIC Downloads ABS Suburbs and Localities (SAL) 2021 boundary data,
# MAGIC filters to Victoria (~2,944 suburbs), and loads into `geo.suburbs`.
# MAGIC
# MAGIC **Data source**: [ABS Digital Boundary Files — ASGS Edition 3](https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files)
# MAGIC
# MAGIC **License**: Creative Commons Attribution 4.0 International
# MAGIC
# MAGIC Every step is idempotent — safe to re-run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies

# COMMAND ----------

# MAGIC %pip install psycopg2-binary fiona shapely "databricks-sdk>=0.90.0" --upgrade --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import os
import time
import traceback

# Lakebase connection
LAKEBASE_PROJECT = "dtp-tiles"
LAKEBASE_BRANCH = "production"
LAKEBASE_DATABASE = "databricks_postgres"
APP_ROLE = "dtp_tiles_app"

# ABS SAL shapefile URL (GDA2020 format, ~99MB)
SAL_URL = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SAL_2021_AUST_GDA2020_SHP.zip"

# Filter to Victoria (STE_CODE21 = '2')
TARGET_STATE_CODE = "2"
TARGET_STATE_NAME = "Victoria"

print("Configuration loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Download ABS SAL shapefile

# COMMAND ----------

import urllib.request
import zipfile
import ssl
import tempfile

download_dir = tempfile.mkdtemp(prefix="abs_sal_")
zip_path = os.path.join(download_dir, "SAL_2021_AUST_GDA2020_SHP.zip")

print(f"Downloading SAL shapefile from ABS...")
print(f"URL: {SAL_URL}")

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

req = urllib.request.Request(SAL_URL)
with urllib.request.urlopen(req, context=ctx) as resp:
    data = resp.read()
    with open(zip_path, "wb") as f:
        f.write(data)

print(f"Downloaded {len(data) / 1024 / 1024:.1f} MB to {zip_path}")

# Extract
with zipfile.ZipFile(zip_path, "r") as z:
    z.extractall(download_dir)
    print(f"Extracted: {z.namelist()}")

shp_path = os.path.join(download_dir, "SAL_2021_AUST_GDA2020.shp")
print(f"Shapefile: {shp_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Explore the data

# COMMAND ----------

import fiona

with fiona.open(shp_path) as src:
    total = len(src)
    print(f"Total features (all of Australia): {total}")
    print(f"CRS: {src.crs}")
    print(f"Schema: {src.schema}")

    # Count by state
    state_counts = {}
    for feat in src:
        ste = feat["properties"]["STE_CODE21"]
        ste_name = feat["properties"]["STE_NAME21"]
        key = f"{ste_name} ({ste})"
        state_counts[key] = state_counts.get(key, 0) + 1

    print(f"\nSuburbs by state:")
    for k in sorted(state_counts.keys()):
        marker = " <-- TARGET" if TARGET_STATE_CODE in k else ""
        print(f"  {k}: {state_counts[k]}{marker}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Connect to Lakebase

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

endpoint_path = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/primary"
endpoint = w.postgres.get_endpoint(name=endpoint_path)
pg_host = endpoint.status.hosts.host
print(f"Host: {pg_host}")
print(f"State: {endpoint.status.current_state}")

cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
pg_token = cred.token

me = w.current_user.me()
pg_user = me.user_name
print(f"Authenticated as: {pg_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create suburbs table (if not exists)

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(
    host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
    user=pg_user, password=pg_token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS geo.suburbs (
        id          SERIAL PRIMARY KEY,
        sal_code    VARCHAR(10) NOT NULL,
        name        VARCHAR(200) NOT NULL,
        state_code  VARCHAR(5),
        state_name  VARCHAR(50),
        area_sqkm   DOUBLE PRECISION,
        geom        geometry(MultiPolygon, 4326)
    );
""")
print("Table 'geo.suburbs' ready")

cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_suburbs_geom
        ON geo.suburbs USING GIST (geom);
""")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_suburbs_name
        ON geo.suburbs (name);
""")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_suburbs_state
        ON geo.suburbs (state_code);
""")
print("Indexes created")

# Grant permissions to app role
cur.execute(f"GRANT SELECT ON geo.suburbs TO {APP_ROLE};")
print(f"Granted SELECT to '{APP_ROLE}'")

cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Load Victorian suburbs

# COMMAND ----------

import fiona
import psycopg2
from shapely.geometry import shape, MultiPolygon

conn = psycopg2.connect(
    host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
    user=pg_user, password=pg_token, sslmode="require",
)
cur = conn.cursor()

# Clear existing VIC suburbs (idempotent reload)
cur.execute(f"DELETE FROM geo.suburbs WHERE state_code = '{TARGET_STATE_CODE}'")
conn.commit()
print(f"Cleared existing {TARGET_STATE_NAME} suburbs")

inserted = 0
skipped = 0
start = time.time()

with fiona.open(shp_path) as src:
    for feat in src:
        props = feat["properties"]
        if props["STE_CODE21"] != TARGET_STATE_CODE:
            continue

        geom = feat.get("geometry")
        if geom is None or geom.get("type") is None:
            skipped += 1
            print(f"  Skipped {props['SAL_NAME21']} (null geometry)")
            continue

        shp_geom = shape(geom)
        if shp_geom.is_empty:
            skipped += 1
            continue

        # Ensure MultiPolygon (table column type)
        if shp_geom.geom_type == "Polygon":
            shp_geom = MultiPolygon([shp_geom])

        # GDA2020 (EPSG:7844) is effectively identical to WGS84 (EPSG:4326)
        # for web mapping purposes (sub-meter difference)
        wkt = shp_geom.wkt

        cur.execute(
            """
            INSERT INTO geo.suburbs (sal_code, name, state_code, state_name, area_sqkm, geom)
            VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
            """,
            (
                props["SAL_CODE21"],
                props["SAL_NAME21"],
                props["STE_CODE21"],
                props["STE_NAME21"],
                props["AREASQKM21"],
                wkt,
            ),
        )
        inserted += 1
        if inserted % 500 == 0:
            conn.commit()
            elapsed = time.time() - start
            print(f"  Inserted {inserted} suburbs ({elapsed:.1f}s)")

conn.commit()
elapsed = time.time() - start
print(f"\nInserted {inserted} {TARGET_STATE_NAME} suburbs, skipped {skipped} in {elapsed:.1f}s")

cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify data

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(
    host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
    user=pg_user, password=pg_token, sslmode="require",
)
cur = conn.cursor()

cur.execute(f"SELECT COUNT(*) FROM geo.suburbs WHERE state_code = '{TARGET_STATE_CODE}'")
count = cur.fetchone()[0]
print(f"{TARGET_STATE_NAME} suburbs: {count}")

cur.execute(f"""
    SELECT name, area_sqkm FROM geo.suburbs
    WHERE state_code = '{TARGET_STATE_CODE}'
    ORDER BY area_sqkm DESC LIMIT 10
""")
print(f"\nLargest 10 suburbs:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:.1f} sq km")

cur.execute(f"""
    SELECT name, area_sqkm FROM geo.suburbs
    WHERE state_code = '{TARGET_STATE_CODE}'
    ORDER BY area_sqkm ASC LIMIT 10
""")
print(f"\nSmallest 10 suburbs:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:.4f} sq km")

# Test spatial query (suburbs near Melbourne CBD)
cur.execute("""
    SELECT name, area_sqkm,
           ST_Distance(geom::geography, ST_MakePoint(144.9631, -37.8136)::geography) / 1000 AS dist_km
    FROM geo.suburbs
    WHERE state_code = '2'
      AND ST_DWithin(geom::geography, ST_MakePoint(144.9631, -37.8136)::geography, 5000)
    ORDER BY dist_km
    LIMIT 10
""")
print(f"\nSuburbs within 5km of Melbourne CBD:")
for row in cur.fetchall():
    print(f"  {row[0]} ({row[1]:.2f} sq km) — {row[2]:.1f} km")

cur.execute("SELECT PostGIS_Version();")
print(f"\nPostGIS version: {cur.fetchone()[0]}")

cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC | Metric | Value |
# MAGIC |--------|-------|
# MAGIC | Data source | ABS Suburbs and Localities (SAL) 2021 |
# MAGIC | State | Victoria (STE_CODE21 = 2) |
# MAGIC | Features loaded | ~2,944 suburb boundaries |
# MAGIC | Table | `geo.suburbs` |
# MAGIC | Spatial index | GIST on `geom` column |
# MAGIC | Tile layer | `/api/tiles/suburbs/{z}/{x}/{y}.pbf` |
# MAGIC
# MAGIC The suburbs appear on the map at zoom level 7+ and show labels at zoom 11+.

# COMMAND ----------

print("=" * 60)
print(f"{TARGET_STATE_NAME} suburb data loaded successfully!")
print(f"Suburbs: {count}")
print(f"Table: geo.suburbs")
print(f"Tile endpoint: /api/tiles/suburbs/{{z}}/{{x}}/{{y}}.pbf")
print("=" * 60)

dbutils.notebook.exit("SUCCESS")
