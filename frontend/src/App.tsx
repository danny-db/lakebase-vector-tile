import VectorMap from "./components/VectorMap";

function App() {
  return (
    <>
      {/* Header */}
      <div className="bg-slate-800 text-white px-4 py-3 flex items-center justify-between gap-4 z-10">
        <div className="flex items-center gap-4">
          <h1 className="text-lg font-bold tracking-tight whitespace-nowrap">
            Dynamic Vector Tiles
          </h1>
          <span className="text-sm text-slate-400">
            Lakebase + PostGIS + MapLibre GL
          </span>
        </div>
        <div className="text-xs text-slate-500">
          ST_AsMVT served directly from Lakebase
        </div>
      </div>

      {/* Map */}
      <div className="flex-1 relative">
        <VectorMap />
      </div>
    </>
  );
}

export default App;
