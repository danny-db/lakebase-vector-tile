"""
Load Australian state/territory boundaries into Lakebase.

Downloads Natural Earth Admin-1 (states/provinces) GeoJSON, filters to
Australia, and inserts geometries into the geo.regions table.

Usage:
    pip install psycopg2-binary requests
    python load_data.py
"""

import json
import os
import ssl
import urllib.request

import psycopg2

# Natural Earth Admin-1 States/Provinces (110m resolution)
DATA_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
    "master/geojson/ne_50m_admin_1_states_provinces.geojson"
)
LOCAL_PATH = os.path.join(os.path.dirname(__file__), "data", "au_states.geojson")


def download_data() -> dict:
    """Download or load cached GeoJSON."""
    if os.path.exists(LOCAL_PATH):
        print(f"Using cached file: {LOCAL_PATH}")
        with open(LOCAL_PATH) as f:
            return json.load(f)

    print(f"Downloading from {DATA_URL} ...")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(DATA_URL)
    with urllib.request.urlopen(req, context=ctx) as resp:
        data = json.loads(resp.read().decode())

    # Cache locally
    os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
    with open(LOCAL_PATH, "w") as f:
        json.dump(data, f)
    print(f"Cached to {LOCAL_PATH}")

    return data


def filter_australia(geojson: dict) -> list[dict]:
    """Filter features to only Australian states/territories."""
    features = []
    for feat in geojson.get("features", []):
        props = feat.get("properties", {})
        # Natural Earth uses iso_a2 or admin for country identification
        if props.get("iso_a2") == "AU" or props.get("admin") == "Australia":
            features.append(feat)
    return features


def geometry_to_wkt(geom: dict) -> str:
    """Convert a GeoJSON geometry to WKT MultiPolygon."""
    geom_type = geom["type"]
    coords = geom["coordinates"]

    if geom_type == "Polygon":
        # Wrap single polygon as multi
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


def load_into_lakebase(features: list[dict]) -> None:
    """Insert features into geo.regions table."""
    host = os.getenv("PGHOST") or os.getenv("LAKEBASE_HOST", "localhost")
    port = os.getenv("PGPORT") or os.getenv("LAKEBASE_PORT", "5432")
    database = os.getenv("PGDATABASE") or os.getenv("LAKEBASE_DATABASE", "databricks_postgres")
    user = os.getenv("PGUSER") or os.getenv("LAKEBASE_USER", "postgres")
    password = os.getenv("PGPASSWORD") or os.getenv("LAKEBASE_PASSWORD", "")

    print(f"Connecting to {host}:{port}/{database} as {user} ...")

    conn = psycopg2.connect(
        host=host,
        port=int(port),
        database=database,
        user=user,
        password=password,
        sslmode="require",
    )
    cur = conn.cursor()

    # Clear existing data
    cur.execute("DELETE FROM geo.regions")
    print(f"Cleared existing rows from geo.regions")

    inserted = 0
    for feat in features:
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
    print(f"\nDone! Inserted {inserted} regions into geo.regions")


def main():
    geojson = download_data()
    features = filter_australia(geojson)
    print(f"Found {len(features)} Australian state/territory features")

    if not features:
        print("No Australian features found. Loading all countries as fallback...")
        features = geojson.get("features", [])[:20]

    load_into_lakebase(features)


if __name__ == "__main__":
    main()
