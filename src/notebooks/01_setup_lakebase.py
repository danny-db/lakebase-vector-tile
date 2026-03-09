# Databricks notebook source
# MAGIC %md
# MAGIC # DTP Tiles - Provision Lakebase & Load Data
# MAGIC
# MAGIC **Fully repeatable** — creates all Lakebase infrastructure from scratch:
# MAGIC 1. Autoscaling project `dtp-tiles` (PG17, native login)
# MAGIC 2. Production branch + primary endpoint
# MAGIC 3. UC catalog registration (`dtp_tiles`)
# MAGIC 4. Native Postgres role `dtp_tiles_app` with password
# MAGIC 5. PostGIS extension
# MAGIC 6. `geo` schema + `geo.regions` table + GIST spatial index
# MAGIC 7. Download Natural Earth Admin-1 GeoJSON, filter to Australia
# MAGIC 8. Insert Australian state/territory boundaries into `geo.regions`
# MAGIC 9. Grant permissions to the app role
# MAGIC 10. Verify connectivity with the native role
# MAGIC 11. Print endpoint + credentials for app.yaml
# MAGIC
# MAGIC Every step is idempotent — safe to re-run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies

# COMMAND ----------

# MAGIC %pip install psycopg2-binary "databricks-sdk>=0.90.0" --upgrade --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Lakebase Autoscaling target
LAKEBASE_PROJECT = "dtp-tiles"
LAKEBASE_BRANCH = "production"
LAKEBASE_DATABASE = "databricks_postgres"   # autoscaling default
LAKEBASE_CATALOG = "dtp_tiles"

# Native Postgres role for the application
APP_ROLE = "dtp_tiles_app"
APP_PASSWORD = dbutils.widgets.get("app_password") if "dbutils" in dir() else os.environ.get("APP_PASSWORD", "CHANGE_ME")

# Natural Earth Admin-1 States/Provinces (110m resolution)
DATA_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
    "master/geojson/ne_50m_admin_1_states_provinces.geojson"
)

print("Configuration loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Lakebase Autoscaling project (if not exists)

# COMMAND ----------

import time
import traceback

print("Importing SDK...")
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Project, ProjectSpec, ProjectDefaultEndpointSettings,
    Branch, Endpoint, EndpointSpec, EndpointType,
)
print("SDK imported successfully")

w = WorkspaceClient()

project_path = f"projects/{LAKEBASE_PROJECT}"
branch_path = f"{project_path}/branches/{LAKEBASE_BRANCH}"
endpoint_path = f"{branch_path}/endpoints/primary"

# --- Create project ---
try:
    project = w.postgres.get_project(name=project_path)
    print(f"Project '{LAKEBASE_PROJECT}' already exists (PG{project.status.pg_version})")
except Exception as e:
    if "NOT_FOUND" in str(e) or "not found" in str(e).lower():
        print(f"Creating project '{LAKEBASE_PROJECT}'...")
        w.postgres.create_project(
            project=Project(
                spec=ProjectSpec(
                    display_name="DTP Tiles - Dynamic Vector Tiles",
                    pg_version=17,
                    enable_pg_native_login=True,
                    default_endpoint_settings=ProjectDefaultEndpointSettings(
                        autoscaling_limit_min_cu=0.5,
                        autoscaling_limit_max_cu=2,
                    ),
                )
            ),
            project_id=LAKEBASE_PROJECT,
        )
        # Wait for project to be ready
        for attempt in range(12):
            try:
                p = w.postgres.get_project(name=project_path)
                print(f"  Project state: created (PG{p.status.pg_version})")
                break
            except Exception:
                pass
            time.sleep(5)
        print(f"Project '{LAKEBASE_PROJECT}' created!")
    else:
        raise

# --- Verify branch exists (auto-created with project) ---
try:
    branch = w.postgres.get_branch(name=branch_path)
    print(f"Branch '{LAKEBASE_BRANCH}': {branch.status.current_state}")
except Exception as e:
    if "NOT_FOUND" in str(e) or "not found" in str(e).lower():
        print(f"Creating branch '{LAKEBASE_BRANCH}'...")
        w.postgres.create_branch(
            parent=project_path,
            branch=Branch(),
            branch_id=LAKEBASE_BRANCH,
        )
        time.sleep(5)
        print(f"Branch '{LAKEBASE_BRANCH}' created!")
    else:
        raise

# --- Verify endpoint exists (auto-created with project) ---
try:
    ep = w.postgres.get_endpoint(name=endpoint_path)
    print(f"Endpoint: {ep.status.hosts.host} ({ep.status.current_state})")
except Exception as e:
    if "NOT_FOUND" in str(e) or "not found" in str(e).lower():
        print("Creating primary endpoint...")
        w.postgres.create_endpoint(
            parent=branch_path,
            endpoint=Endpoint(
                spec=EndpointSpec(
                    endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                    autoscaling_limit_min_cu=0.5,
                    autoscaling_limit_max_cu=2,
                )
            ),
            endpoint_id="primary",
        )
        time.sleep(10)
        ep = w.postgres.get_endpoint(name=endpoint_path)
        print(f"Endpoint created: {ep.status.hosts.host}")
    else:
        raise

print("Step 1 DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Register UC catalog

# COMMAND ----------

print("Step 2: Register UC catalog...")

# Look up the database resource path on the branch
databases = list(w.postgres.list_databases(parent=branch_path))
pg_db = next(
    (d for d in databases if d.status and d.status.postgres_database == LAKEBASE_DATABASE),
    None,
)
if pg_db:
    db_resource_path = pg_db.name
    print(f"Database resource: {db_resource_path} ({pg_db.status.postgres_database})")
else:
    db_resource_path = f"{branch_path}/databases/{LAKEBASE_DATABASE}"
    print(f"Database not found, using path: {db_resource_path}")

try:
    cat = w.catalogs.get(name=LAKEBASE_CATALOG)
    print(f"UC Catalog '{LAKEBASE_CATALOG}' already exists")
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
        print(f"Registering UC Catalog '{LAKEBASE_CATALOG}'...")
        result = w.api_client.do(
            "POST",
            "/api/2.0/postgres/catalogs",
            body={
                "name": LAKEBASE_CATALOG,
                "parent": branch_path,
                "catalog_id": LAKEBASE_CATALOG,
                "database": db_resource_path,
                "create_database_if_not_exists": True,
            },
        )
        print(f"UC Catalog registered: {result}")
    else:
        raise
print("Step 2 DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Connect to Lakebase & get endpoint host

# COMMAND ----------

print("Step 3: Getting endpoint and credentials...")
# Get the autoscaling endpoint host
endpoint = w.postgres.get_endpoint(name=endpoint_path)
pg_host = endpoint.status.hosts.host
print(f"Project: {LAKEBASE_PROJECT}")
print(f"Branch: {LAKEBASE_BRANCH}")
print(f"Host: {pg_host}")
print(f"State: {endpoint.status.current_state}")

# Generate credential for the autoscaling endpoint
cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
pg_token = cred.token
print(f"Token obtained (length: {len(pg_token)})")

# Get current user
me = w.current_user.me()
pg_user = me.user_name
print(f"Authenticated as: {pg_user}")
print("Step 3 DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create native Postgres role for the application

# COMMAND ----------

print("Step 4: Creating native Postgres role...")
import psycopg2

try:
    admin_conn = psycopg2.connect(
        host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
        user=pg_user, password=pg_token, sslmode="require",
    )
    admin_conn.autocommit = True
    admin_cur = admin_conn.cursor()

    # Create the native Postgres role (idempotent)
    admin_cur.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{APP_ROLE}') THEN
                CREATE ROLE {APP_ROLE} WITH LOGIN PASSWORD '{APP_PASSWORD}';
                RAISE NOTICE 'Created role %', '{APP_ROLE}';
            ELSE
                ALTER ROLE {APP_ROLE} WITH PASSWORD '{APP_PASSWORD}';
                RAISE NOTICE 'Role % already exists, password updated', '{APP_ROLE}';
            END IF;
        END
        $$;
    """)

    print(f"Native Postgres role '{APP_ROLE}' ready")
    admin_cur.close()
    admin_conn.close()
except Exception as e:
    print(f"ERROR in Step 4: {e}")
    traceback.print_exc()
    raise
print("Step 4 DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Enable PostGIS & create geo schema + regions table

# COMMAND ----------

print("Step 5: Creating PostGIS + schema + table...")
import psycopg2

try:
    conn = psycopg2.connect(
        host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
        user=pg_user, password=pg_token, sslmode="require",
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Enable PostGIS
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    print("PostGIS extension enabled")

    # Create geo schema
    cur.execute("CREATE SCHEMA IF NOT EXISTS geo;")
    print("Schema 'geo' created")

    # Create regions table
    cur.execute("""
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
    """)
    print("Table 'geo.regions' created")

    # Spatial index
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_regions_geom
            ON geo.regions USING GIST (geom);
    """)
    print("GIST spatial index created")

    # Name index
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_regions_name
            ON geo.regions (name);
    """)
    print("Name index created")

    cur.close()
    conn.close()
except Exception as e:
    print(f"ERROR in Step 5: {e}")
    traceback.print_exc()
    raise
print("Step 5 DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Download Natural Earth data & insert Australian boundaries

# COMMAND ----------

print("Step 6: Downloading and inserting data...")
import json
import ssl
import urllib.request
import psycopg2

try:
    # Download GeoJSON
    print(f"Downloading from {DATA_URL} ...")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(DATA_URL)
    with urllib.request.urlopen(req, context=ctx) as resp:
        geojson = json.loads(resp.read().decode())

    print(f"Downloaded {len(geojson.get('features', []))} total features")

    # Filter to Australia
    au_features = []
    for feat in geojson.get("features", []):
        props = feat.get("properties", {})
        if props.get("iso_a2") == "AU" or props.get("admin") == "Australia":
            au_features.append(feat)

    print(f"Found {len(au_features)} Australian state/territory features")

    if not au_features:
        raise RuntimeError("No Australian features found in Natural Earth data!")


    def geometry_to_wkt(geom):
        """Convert a GeoJSON geometry to WKT MultiPolygon."""
        geom_type = geom["type"]
        coords = geom["coordinates"]

        if geom_type == "Polygon":
            coords = [coords]
        elif geom_type != "MultiPolygon":
            raise ValueError(f"Unsupported geometry type: {geom_type}")

        polys = []
        for polygon in coords:
            rings = []
            for ring in polygon:
                pts = ", ".join(f"{lon} {lat}" for lon, lat in ring)
                rings.append(f"({pts})")
            polys.append(f"({', '.join(rings)})")

        return f"MULTIPOLYGON({', '.join(polys)})"


    # Connect and insert
    conn = psycopg2.connect(
        host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
        user=pg_user, password=pg_token, sslmode="require",
    )
    cur = conn.cursor()

    # Clear existing data (idempotent reload)
    cur.execute("DELETE FROM geo.regions")
    print("Cleared existing rows from geo.regions")

    inserted = 0
    for feat in au_features:
        props = feat.get("properties", {})
        geom = feat.get("geometry")
        if not geom:
            continue

        wkt = geometry_to_wkt(geom)
        name = props.get("name", "Unknown")
        iso_a3 = props.get("iso_a3", props.get("adm0_a3", ""))
        iso_3166_2 = props.get("iso_3166_2", "")
        region_type = props.get("type_en", props.get("type", ""))
        pop_est = props.get("pop_est")
        continent = props.get("continent", "Oceania")

        if pop_est is not None:
            try:
                pop_est = int(float(pop_est))
            except (ValueError, TypeError):
                pop_est = None

        cur.execute(
            """
            INSERT INTO geo.regions (name, iso_a3, iso_3166_2, type, pop_est, continent, geom)
            VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
            """,
            (name, iso_a3, iso_3166_2, region_type, pop_est, continent, wkt),
        )
        inserted += 1
        print(f"  Inserted: {name} ({iso_3166_2})")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nInserted {inserted} regions into geo.regions")
except Exception as e:
    print(f"ERROR in Step 6: {e}")
    traceback.print_exc()
    raise
print("Step 6 DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Grant permissions to app role

# COMMAND ----------

print("Step 7: Granting permissions...")
import psycopg2

try:
    conn = psycopg2.connect(
        host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
        user=pg_user, password=pg_token, sslmode="require",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"GRANT USAGE ON SCHEMA geo TO {APP_ROLE};")
    cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA geo TO {APP_ROLE};")
    cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA geo GRANT SELECT ON TABLES TO {APP_ROLE};")
    cur.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA geo TO {APP_ROLE};")
    cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA geo GRANT USAGE, SELECT ON SEQUENCES TO {APP_ROLE};")

    print(f"Permissions granted to '{APP_ROLE}' on schema 'geo'")
    cur.close()
    conn.close()
except Exception as e:
    print(f"ERROR in Step 7: {e}")
    traceback.print_exc()
    raise
print("Step 7 DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify with native Postgres role

# COMMAND ----------

print("Step 8: Verifying with native role...")
import psycopg2

try:
    verify_conn = psycopg2.connect(
        host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
        user=APP_ROLE, password=APP_PASSWORD, sslmode="require",
    )
    verify_cur = verify_conn.cursor()

    verify_cur.execute("SELECT COUNT(*) FROM geo.regions;")
    print(f"Regions: {verify_cur.fetchone()[0]}")

    verify_cur.execute("SELECT name, type, pop_est FROM geo.regions ORDER BY name;")
    print("\nAustralian states/territories:")
    for row in verify_cur.fetchall():
        pop = f"{row[2]:,}" if row[2] else "N/A"
        print(f"  {row[0]} ({row[1]}) - pop: {pop}")

    verify_cur.execute("SELECT PostGIS_Version();")
    print(f"\nPostGIS version: {verify_cur.fetchone()[0]}")

    verify_cur.close()
    verify_conn.close()
    print(f"\nNative Postgres role '{APP_ROLE}' verified!")
except Exception as e:
    print(f"ERROR in Step 8: {e}")
    traceback.print_exc()
    raise
print("Step 8 DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!

# COMMAND ----------

print("=" * 60)
print("APP.YAML CONFIGURATION")
print("=" * 60)
print(f"LAKEBASE_HOST:     {pg_host}")
print(f"LAKEBASE_PORT:     5432")
print(f"LAKEBASE_DATABASE: {LAKEBASE_DATABASE}")
print(f"LAKEBASE_SCHEMA:   geo")
print(f"LAKEBASE_USER:     {APP_ROLE}")
print(f"LAKEBASE_PASSWORD: {APP_PASSWORD}")
print("=" * 60)

dbutils.notebook.exit("SUCCESS")
