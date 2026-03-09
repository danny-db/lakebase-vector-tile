import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

const REGIONS_TILE_URL = "/api/tiles/regions/{z}/{x}/{y}.pbf";
const SUBURBS_TILE_URL = "/api/tiles/suburbs/{z}/{x}/{y}.pbf";

// Center on Victoria
const INITIAL_CENTER: [number, number] = [145.0, -37.0];
const INITIAL_ZOOM = 6;

// Color palette for states
const STATE_FILL_COLORS: [string, string][] = [
  ["New South Wales", "#4e79a7"],
  ["Victoria", "#f28e2b"],
  ["Queensland", "#e15759"],
  ["South Australia", "#76b7b2"],
  ["Western Australia", "#59a14f"],
  ["Tasmania", "#edc948"],
  ["Northern Territory", "#b07aa1"],
  ["Australian Capital Territory", "#ff9da7"],
];

function buildStateFillColorExpr(): maplibregl.ExpressionSpecification {
  const expr: unknown[] = ["match", ["get", "name"]];
  for (const [name, color] of STATE_FILL_COLORS) {
    expr.push(name, color);
  }
  expr.push("#aab");
  return expr as maplibregl.ExpressionSpecification;
}

// Generate a deterministic color from suburb name
function buildSuburbFillColorExpr(): maplibregl.ExpressionSpecification {
  // Use sal_code modulo to assign colors from a palette
  return [
    "interpolate",
    ["linear"],
    ["%", ["to-number", ["get", "sal_code"], 0], 12],
    0, "#4e79a7",
    1, "#f28e2b",
    2, "#e15759",
    3, "#76b7b2",
    4, "#59a14f",
    5, "#edc948",
    6, "#b07aa1",
    7, "#ff9da7",
    8, "#9c755f",
    9, "#bab0ac",
    10, "#af7aa1",
    11, "#86bcb6",
  ] as maplibregl.ExpressionSpecification;
}

export default function VectorMap() {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: {
        version: 8,
        sources: {
          "osm-raster": {
            type: "raster",
            tiles: [
              "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png",
              "https://b.tile.openstreetmap.org/{z}/{x}/{y}.png",
              "https://c.tile.openstreetmap.org/{z}/{x}/{y}.png",
            ],
            tileSize: 256,
            attribution:
              '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
          },
        },
        layers: [
          {
            id: "osm-base",
            type: "raster",
            source: "osm-raster",
            minzoom: 0,
            maxzoom: 19,
          },
        ],
      },
      center: INITIAL_CENTER,
      zoom: INITIAL_ZOOM,
    });

    map.addControl(new maplibregl.NavigationControl(), "top-right");

    map.on("load", () => {
      // === State/territory boundaries (regions) ===
      map.addSource("regions", {
        type: "vector",
        tiles: [window.location.origin + REGIONS_TILE_URL],
        minzoom: 0,
        maxzoom: 14,
      });

      map.addLayer({
        id: "regions-fill",
        type: "fill",
        source: "regions",
        "source-layer": "regions",
        paint: {
          "fill-color": buildStateFillColorExpr(),
          "fill-opacity": [
            "interpolate",
            ["linear"],
            ["zoom"],
            2, 0.4,
            6, 0.2,
            8, 0.1,
            10, 0.05,
          ],
        },
      });

      map.addLayer({
        id: "regions-outline",
        type: "line",
        source: "regions",
        "source-layer": "regions",
        paint: {
          "line-color": "#1e293b",
          "line-width": [
            "interpolate",
            ["linear"],
            ["zoom"],
            2, 1,
            6, 2,
            10, 2.5,
          ],
          "line-opacity": 0.7,
        },
      });

      map.addLayer({
        id: "regions-label",
        type: "symbol",
        source: "regions",
        "source-layer": "regions",
        layout: {
          "text-field": ["get", "name"],
          "text-size": [
            "interpolate",
            ["linear"],
            ["zoom"],
            3, 10,
            6, 14,
            9, 12,
          ],
          "text-anchor": "center",
          "text-allow-overlap": false,
        },
        paint: {
          "text-color": "#1e293b",
          "text-halo-color": "#ffffff",
          "text-halo-width": 2,
        },
        maxzoom: 10,
      });

      // === Suburb boundaries (VIC) ===
      map.addSource("suburbs", {
        type: "vector",
        tiles: [window.location.origin + SUBURBS_TILE_URL],
        minzoom: 0,
        maxzoom: 16,
      });

      map.addLayer({
        id: "suburbs-fill",
        type: "fill",
        source: "suburbs",
        "source-layer": "suburbs",
        minzoom: 6,
        paint: {
          "fill-color": buildSuburbFillColorExpr(),
          "fill-opacity": [
            "interpolate",
            ["linear"],
            ["zoom"],
            6, 0.15,
            8, 0.25,
            11, 0.35,
          ],
        },
      });

      map.addLayer({
        id: "suburbs-outline",
        type: "line",
        source: "suburbs",
        "source-layer": "suburbs",
        minzoom: 6,
        paint: {
          "line-color": "#334155",
          "line-width": [
            "interpolate",
            ["linear"],
            ["zoom"],
            6, 0.5,
            9, 1,
            12, 1.5,
            14, 2,
          ],
          "line-opacity": [
            "interpolate",
            ["linear"],
            ["zoom"],
            6, 0.4,
            9, 0.7,
            14, 0.9,
          ],
        },
      });

      map.addLayer({
        id: "suburbs-label",
        type: "symbol",
        source: "suburbs",
        "source-layer": "suburbs",
        minzoom: 10,
        layout: {
          "text-field": ["get", "name"],
          "text-size": [
            "interpolate",
            ["linear"],
            ["zoom"],
            11, 9,
            14, 13,
          ],
          "text-anchor": "center",
          "text-allow-overlap": false,
          "text-padding": 4,
        },
        paint: {
          "text-color": "#334155",
          "text-halo-color": "#ffffff",
          "text-halo-width": 1.5,
        },
      });

      // === Click handler — suburbs take priority over regions ===
      map.on("click", (e) => {
        // Check suburbs first (higher priority)
        const suburbFeatures = map.queryRenderedFeatures(e.point, {
          layers: ["suburbs-fill"],
        });

        if (suburbFeatures.length > 0) {
          const props = suburbFeatures[0].properties;
          const areaStr = props.area_sqkm
            ? `${Number(props.area_sqkm).toFixed(2)} km\u00B2`
            : "N/A";

          new maplibregl.Popup({ maxWidth: "300px" })
            .setLngLat(e.lngLat)
            .setHTML(`
              <div style="font-family: system-ui, sans-serif; min-width: 180px;">
                <h3 style="margin: 0 0 8px; font-size: 16px; font-weight: 600;">${props.name || "Unknown"}</h3>
                <table style="font-size: 13px; border-collapse: collapse; width: 100%;">
                  <tr><td style="color: #64748b; padding: 2px 8px 2px 0;">SAL Code</td><td>${props.sal_code || ""}</td></tr>
                  <tr><td style="color: #64748b; padding: 2px 8px 2px 0;">State</td><td>${props.state_name || ""}</td></tr>
                  <tr><td style="color: #64748b; padding: 2px 8px 2px 0;">Area</td><td>${areaStr}</td></tr>
                </table>
                <div style="margin-top: 8px; font-size: 11px; color: #94a3b8;">
                  1 of ${suburbFeatures.length.toLocaleString()} suburb(s) at this point &middot; Vector tile from Lakebase via ST_AsMVT
                </div>
              </div>
            `)
            .addTo(map);
          return;
        }

        // Fall back to regions
        const regionFeatures = map.queryRenderedFeatures(e.point, {
          layers: ["regions-fill"],
        });

        if (regionFeatures.length > 0) {
          const props = regionFeatures[0].properties;

          new maplibregl.Popup({ maxWidth: "300px" })
            .setLngLat(e.lngLat)
            .setHTML(`
              <div style="font-family: system-ui, sans-serif; min-width: 180px;">
                <h3 style="margin: 0 0 8px; font-size: 16px; font-weight: 600;">${props.name || "Unknown"}</h3>
                <table style="font-size: 13px; border-collapse: collapse; width: 100%;">
                  ${props.type ? `<tr><td style="color: #64748b; padding: 2px 8px 2px 0;">Type</td><td>${props.type}</td></tr>` : ""}
                  ${props.iso_3166_2 ? `<tr><td style="color: #64748b; padding: 2px 8px 2px 0;">Code</td><td>${props.iso_3166_2}</td></tr>` : ""}
                  ${props.continent ? `<tr><td style="color: #64748b; padding: 2px 8px 2px 0;">Continent</td><td>${props.continent}</td></tr>` : ""}
                </table>
                <div style="margin-top: 8px; font-size: 11px; color: #94a3b8;">
                  Vector tile from Lakebase via ST_AsMVT
                </div>
              </div>
            `)
            .addTo(map);
        }
      });

      // Cursor pointer on hover
      map.on("mouseenter", "suburbs-fill", () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseleave", "suburbs-fill", () => {
        map.getCanvas().style.cursor = "";
      });
      map.on("mouseenter", "regions-fill", () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseleave", "regions-fill", () => {
        map.getCanvas().style.cursor = "";
      });
    });

    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  return <div ref={containerRef} className="absolute inset-0" />;
}
