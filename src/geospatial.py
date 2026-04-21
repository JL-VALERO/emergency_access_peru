"""
Spatial joins, CRS operations, and GeoDataFrame construction.

Inputs:  data/processed/  (outputs of cleaning.py)
Outputs: data/processed/  (enriched GeoPackages)

Key operations
--------------
1. join_facilities_to_districts  – point-in-polygon: which district each IPRESS falls in
2. join_renipress_to_districts   – same for SUSALUD RENIPRESS facilities
3. nearest_facility              – sjoin_nearest: closest IPRESS for every populated center
4. build_district_layer          – aggregate facility counts + emergency volume to district polygons
"""

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

# ---------------------------------------------------------------------------
# CRS constants
# ---------------------------------------------------------------------------
WGS84 = "EPSG:4326"
# UTM 18S covers most of Peru – used only for metric distance calculations
UTM_PERU = "EPSG:32718"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------
def load_processed() -> dict:
    """
    Load all cleaned datasets from data/processed/.
    Returns a dict keyed by dataset name.
    """
    files = {
        "centros_poblados": PROCESSED / "centros_poblados.gpkg",
        "distritos":        PROCESSED / "distritos.gpkg",
        "ipress_minsa":     PROCESSED / "ipress_minsa.gpkg",
        "renipress_susalud": PROCESSED / "renipress_susalud.gpkg",
        "emergencias_susalud": PROCESSED / "emergencias_susalud.parquet",
    }

    datasets = {}
    for name, path in files.items():
        if not path.exists():
            print(f"  [warn] {path.name} not found – skipping.")
            datasets[name] = None
            continue
        if path.suffix == ".parquet":
            datasets[name] = pd.read_parquet(path)
        else:
            datasets[name] = gpd.read_file(path)
        print(f"  Loaded {name}: {len(datasets[name]):,} rows")

    return datasets


# ---------------------------------------------------------------------------
# CRS helpers
# ---------------------------------------------------------------------------
def ensure_crs(gdf: gpd.GeoDataFrame, crs: str = WGS84) -> gpd.GeoDataFrame:
    """Reproject *gdf* to *crs* if it isn't already there."""
    if gdf is None or gdf.empty:
        return gdf
    if gdf.crs is None:
        return gdf.set_crs(crs)
    if gdf.crs.to_epsg() != int(crs.split(":")[1]):
        return gdf.to_crs(crs)
    return gdf


def to_utm(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Project to UTM 18S for metric distance operations."""
    return ensure_crs(gdf, UTM_PERU)


def to_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return ensure_crs(gdf, WGS84)


# ---------------------------------------------------------------------------
# Spatial join 1: facilities → districts (point-in-polygon)
# ---------------------------------------------------------------------------
def join_facilities_to_districts(
    facilities: gpd.GeoDataFrame,
    distritos: gpd.GeoDataFrame,
    facility_label: str = "ipress",
) -> gpd.GeoDataFrame:
    """
    Spatial join: attach district attributes to each facility point.

    Returns the facility GDF with added columns from distritos
    (prefixed to avoid collisions).  Rows that fall outside any district
    geometry are kept with NaN district fields.
    """
    if facilities is None or facilities.empty:
        print(f"  [skip] {facility_label}: empty input.")
        return gpd.GeoDataFrame()
    if distritos is None or distritos.empty:
        print("  [skip] distritos: empty input.")
        return facilities

    # Align CRS
    fac = ensure_crs(facilities.copy(), WGS84)
    dist = ensure_crs(distritos.copy(), WGS84)

    # Rename any overlapping columns in facilities before joining so that
    # the authoritative district columns (ubigeo, nombre_distrito, etc.) win.
    dist_cols = {c for c in dist.columns if c != "geometry"}
    fac_cols  = {c for c in fac.columns if c != "geometry"}
    overlap   = fac_cols & dist_cols
    if overlap:
        rename_map = {c: f"{c}_fac" for c in overlap}
        fac = fac.rename(columns=rename_map)

    joined = gpd.sjoin(fac, dist, how="left", predicate="within")

    # sjoin adds an 'index_right' column – remove it
    joined = joined.drop(columns=["index_right"], errors="ignore")

    ubigeo_col = "ubigeo" if "ubigeo" in joined.columns else None
    matched = joined[ubigeo_col].notna().sum() if ubigeo_col else "N/A"
    print(f"  {facility_label} → districts: "
          f"{matched} / {len(joined):,} matched")

    return joined


# ---------------------------------------------------------------------------
# Spatial join 2: populated centers → nearest health facility
# ---------------------------------------------------------------------------
def nearest_facility(
    centros: gpd.GeoDataFrame,
    facilities: gpd.GeoDataFrame,
    distance_col: str = "dist_nearest_m",
    facility_name_col: str = "nombre_ipress",
) -> gpd.GeoDataFrame:
    """
    For each populated center find the nearest IPRESS using sjoin_nearest.

    Distance is computed in UTM 18S (metres) and stored in *distance_col*.
    The result is in WGS84.
    """
    if centros is None or centros.empty:
        print("  [skip] centros_poblados: empty input.")
        return gpd.GeoDataFrame()
    if facilities is None or facilities.empty:
        print("  [skip] facilities: empty input.")
        return centros

    # Project both layers to metric CRS for distance accuracy
    cc_utm  = to_utm(centros.copy())
    fac_utm = to_utm(facilities.copy())

    # Keep only the columns we want to carry over from facilities
    carry_cols = ["geometry"]
    if facility_name_col in fac_utm.columns:
        carry_cols.append(facility_name_col)
    if "ubigeo" in fac_utm.columns:
        carry_cols.append("ubigeo")

    fac_slim = fac_utm[list(dict.fromkeys(carry_cols))]

    # Rename to avoid collision with centros columns
    rename_map = {}
    for col in carry_cols:
        if col != "geometry" and col in cc_utm.columns:
            rename_map[col] = f"nearest_{col}"
    fac_slim = fac_slim.rename(columns=rename_map)

    joined = gpd.sjoin_nearest(
        cc_utm,
        fac_slim,
        how="left",
        distance_col=distance_col,
    )

    # Drop sjoin artefact
    joined = joined.drop(columns=["index_right"], errors="ignore")

    # Back to WGS84
    joined = to_wgs84(joined)

    valid = joined[distance_col].notna()
    print(f"  Nearest facility: {valid.sum():,} / {len(joined):,} centres matched")
    if valid.any():
        print(f"    Distance stats (m): "
              f"min={joined[distance_col].min():.0f}  "
              f"median={joined[distance_col].median():.0f}  "
              f"max={joined[distance_col].max():.0f}")

    return joined


# ---------------------------------------------------------------------------
# Spatial join 3: district-level aggregation layer
# ---------------------------------------------------------------------------
def build_district_layer(
    distritos: gpd.GeoDataFrame,
    ipress_in_districts: gpd.GeoDataFrame,
    renipress_in_districts: gpd.GeoDataFrame,
    emergencias: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """
    Aggregate facility counts and emergency volumes to district polygons.

    Columns added to distritos:
      - n_ipress_minsa      : count of MINSA IPRESS per district
      - n_renipress_susalud : count of SUSALUD IPRESS per district
      - total_emergencias   : sum of emergency attendances (latest year available)
    """
    if distritos is None or distritos.empty:
        print("  [skip] distritos: empty.")
        return gpd.GeoDataFrame()

    dist = ensure_crs(distritos.copy(), WGS84)

    # --- MINSA IPRESS count per district ---
    if ipress_in_districts is not None and not ipress_in_districts.empty and "ubigeo" in ipress_in_districts.columns:
        n_ipress = (
            ipress_in_districts.groupby("ubigeo").size().rename("n_ipress_minsa")
        )
        dist = dist.merge(n_ipress, on="ubigeo", how="left")
    else:
        dist["n_ipress_minsa"] = np.nan

    # --- RENIPRESS SUSALUD count per district ---
    if renipress_in_districts is not None and not renipress_in_districts.empty and "ubigeo" in renipress_in_districts.columns:
        n_reni = (
            renipress_in_districts.groupby("ubigeo").size().rename("n_renipress_susalud")
        )
        dist = dist.merge(n_reni, on="ubigeo", how="left")
    else:
        dist["n_renipress_susalud"] = np.nan

    # --- Emergency volume per district (aggregate across all periods) ---
    if emergencias is not None and not emergencias.empty and "ubigeo" in emergencias.columns:
        emerg_agg_cols = [c for c in ["total_emergencias", "total_atenciones"] if c in emergencias.columns]
        if emerg_agg_cols:
            emerg_grp = (
                emergencias.groupby("ubigeo")[emerg_agg_cols]
                .sum()
                .reset_index()
            )
            dist = dist.merge(emerg_grp, on="ubigeo", how="left")

    # Fill zero for districts with no matched facilities
    fill_cols = ["n_ipress_minsa", "n_renipress_susalud"]
    for col in fill_cols:
        if col in dist.columns:
            dist[col] = dist[col].fillna(0).astype(int)

    print(f"  District layer: {len(dist):,} districts")
    for col in ["n_ipress_minsa", "n_renipress_susalud", "total_emergencias"]:
        if col in dist.columns:
            print(f"    {col}: sum={dist[col].sum():,.0f}  districts with >0: "
                  f"{(dist[col] > 0).sum():,}")

    return dist


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_geospatial_pipeline(datasets: dict | None = None) -> dict:
    """
    Run the full geospatial pipeline.

    Parameters
    ----------
    datasets : dict, optional
        If provided, uses pre-loaded datasets (output of load_processed or
        cleaning.run_cleaning_pipeline).  If None, loads from data/processed/.
    """
    print("=== Geospatial Pipeline ===\n")

    if datasets is None:
        print("[0] Loading processed datasets …")
        datasets = load_processed()

    distritos        = datasets.get("distritos")
    ipress_minsa     = datasets.get("ipress_minsa")
    renipress_susalud = datasets.get("renipress_susalud")
    centros_poblados = datasets.get("centros_poblados")
    emergencias      = datasets.get("emergencias_susalud")

    results = {}

    # 1. IPRESS MINSA → districts
    print("\n[1/4] Joining IPRESS MINSA → districts …")
    ipress_dist = join_facilities_to_districts(
        ipress_minsa, distritos, facility_label="ipress_minsa"
    )
    if not ipress_dist.empty:
        out = PROCESSED / "ipress_minsa_districts.gpkg"
        ipress_dist.reset_index(drop=True).to_file(out, driver="GPKG", engine="pyogrio")
        print(f"  Saved → {out}")
    results["ipress_minsa_districts"] = ipress_dist

    # 2. RENIPRESS SUSALUD → districts
    print("\n[2/4] Joining RENIPRESS SUSALUD → districts …")
    reni_dist = join_facilities_to_districts(
        renipress_susalud, distritos, facility_label="renipress_susalud"
    )
    if not reni_dist.empty:
        out = PROCESSED / "renipress_susalud_districts.gpkg"
        reni_dist.reset_index(drop=True).to_file(out, driver="GPKG", engine="pyogrio")
        print(f"  Saved → {out}")
    results["renipress_susalud_districts"] = reni_dist

    # 3. Populated centers → nearest facility
    print("\n[3/4] Finding nearest facility per populated center …")
    # Prefer RENIPRESS (broader coverage); fall back to IPRESS MINSA
    facility_layer = renipress_susalud if (renipress_susalud is not None and not renipress_susalud.empty) else ipress_minsa
    cc_nearest = nearest_facility(centros_poblados, facility_layer)
    if cc_nearest is not None and not cc_nearest.empty:
        out = PROCESSED / "centros_nearest_facility.gpkg"
        cc_nearest.reset_index(drop=True).to_file(out, driver="GPKG", engine="pyogrio")
        print(f"  Saved → {out}")
    results["centros_nearest_facility"] = cc_nearest

    # 4. District-level summary layer
    print("\n[4/4] Building district-level summary layer …")
    district_layer = build_district_layer(
        distritos, ipress_dist, reni_dist, emergencias
    )
    if district_layer is not None and not district_layer.empty:
        out = PROCESSED / "districts_summary.gpkg"
        district_layer.reset_index(drop=True).to_file(out, driver="GPKG", engine="pyogrio")
        print(f"  Saved → {out}")
    results["districts_summary"] = district_layer

    print("\n=== Geospatial pipeline complete ===")
    return results


if __name__ == "__main__":
    run_geospatial_pipeline()
