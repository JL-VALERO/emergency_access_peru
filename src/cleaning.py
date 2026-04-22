"""
Standardize, clean, and save all raw datasets to data/processed/.

For each dataset:
  - Rename columns to snake_case
  - Re-project to EPSG:4326 (WGS84) where applicable
  - Drop rows missing critical fields
  - Fill or flag non-critical nulls
  - Export to Parquet (tabular) or GeoPackage (spatial)
"""

import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

TARGET_CRS = "EPSG:4326"

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _to_snake(name: str) -> str:
    """Convert any column name to snake_case."""
    name = str(name).strip()
    name = re.sub(r"[\s\-]+", "_", name)
    name = re.sub(r"[^\w]", "", name)
    name = re.sub(r"([a-z])([A-Z])", r"\1_\2", name)
    return name.lower()


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [_to_snake(c) for c in df.columns]
    return df


def _report_nulls(df: pd.DataFrame, name: str) -> None:
    null_pct = df.isnull().mean().mul(100).round(1)
    cols_with_nulls = null_pct[null_pct > 0]
    if cols_with_nulls.empty:
        print(f"  {name}: no nulls found.")
    else:
        print(f"  {name} null rates (%):\n{cols_with_nulls.to_string()}")


def _reproject(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        print("  [warn] CRS is None – assuming EPSG:4326.")
        gdf = gdf.set_crs(TARGET_CRS)
    elif gdf.crs.to_epsg() != 4326:
        print(f"  Reprojecting from {gdf.crs} → {TARGET_CRS}")
        gdf = gdf.to_crs(TARGET_CRS)
    return gdf


def _coords_to_geodataframe(
    df: pd.DataFrame, lat_col: str, lon_col: str
) -> gpd.GeoDataFrame:
    """Convert a DataFrame with lat/lon columns to a GeoDataFrame."""
    df = df.copy()
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
    valid = df[lat_col].notna() & df[lon_col].notna()
    print(f"  Rows with valid coordinates: {valid.sum():,} / {len(df):,}")
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs=TARGET_CRS,
    )
    return gdf


# ---------------------------------------------------------------------------
# Dataset 1 – Centros Poblados
# ---------------------------------------------------------------------------
_CCPP_KEEP = [
    # IGN field names (may vary by release year)
    "nombcp", "codccpp", "codgeo", "region", "provincia", "distrito",
    "ubigeo", "latitud", "longitud", "altitud", "poblacion", "geometry",
]

_CCPP_RENAME = {
    "nombcp": "nombre_centro_poblado",
    "codccpp": "codigo_centro_poblado",
    "codgeo": "codigo_geo",
    "region": "departamento",
    "latitud": "lat",
    "longitud": "lon",
    "altitud": "altitud_m",
    "poblacion": "poblacion",
}


def clean_centros_poblados(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    print("\n--- Cleaning: Centros Poblados ---")
    gdf = _snake_columns(gdf).copy()

    # Keep only columns that exist
    keep = [c for c in _CCPP_KEEP if c in gdf.columns] + ["geometry"]
    gdf = gdf[list(dict.fromkeys(keep))]  # deduplicate while preserving order

    gdf = gdf.rename(columns={k: v for k, v in _CCPP_RENAME.items() if k in gdf.columns})

    # CRS
    gdf = _reproject(gdf)

    # Drop rows with no geometry
    before = len(gdf)
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]
    print(f"  Dropped {before - len(gdf):,} rows with null/empty geometry.")

    # Numeric coercion
    for col in ["lat", "lon", "altitud_m", "poblacion"]:
        if col in gdf.columns:
            gdf[col] = pd.to_numeric(gdf[col], errors="coerce")

    _report_nulls(gdf.drop(columns="geometry"), "centros_poblados")

    out = PROCESSED / "centros_poblados.gpkg"
    gdf.reset_index(drop=True).to_file(out, driver="GPKG", engine="pyogrio")
    print(f"  Saved → {out}  ({len(gdf):,} rows)")
    return gdf


# ---------------------------------------------------------------------------
# Dataset 2 – DISTRITOS
# ---------------------------------------------------------------------------
_DIST_RENAME = {
    "nombdist": "nombre_distrito",
    "nombprov": "nombre_provincia",
    "nombdep": "nombre_departamento",
    "ubigeo": "ubigeo",
    "iddist": "ubigeo",        # alternative field name
    "ccdd": "cod_departamento",
    "ccpp": "cod_provincia",
    "ccdi": "cod_distrito",
    "shape_area": "area_shape",
    "shape_leng": "perimetro_shape",
}


def clean_distritos(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    print("\n--- Cleaning: DISTRITOS ---")
    gdf = _snake_columns(gdf).copy()

    gdf = gdf.rename(columns={k: v for k, v in _DIST_RENAME.items() if k in gdf.columns})

    # CRS
    gdf = _reproject(gdf)

    # Drop rows with no geometry
    before = len(gdf)
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]
    print(f"  Dropped {before - len(gdf):,} rows with null/empty geometry.")

    # Ensure ubigeo is a 6-char zero-padded string
    if "ubigeo" in gdf.columns:
        gdf["ubigeo"] = gdf["ubigeo"].astype(str).str.zfill(6)

    _report_nulls(gdf.drop(columns="geometry"), "distritos")

    out = PROCESSED / "distritos.gpkg"
    gdf.reset_index(drop=True).to_file(out, driver="GPKG", engine="pyogrio")
    print(f"  Saved → {out}  ({len(gdf):,} rows)")
    return gdf


# ---------------------------------------------------------------------------
# Dataset 3 – IPRESS MINSA
# ---------------------------------------------------------------------------
_IPRESS_LAT_CANDIDATES = ["latitud", "lat", "latitude", "y"]
_IPRESS_LON_CANDIDATES = ["longitud", "lon", "longitude", "x"]

_IPRESS_RENAME = {
    "nombre": "nombre_ipress",
    "categoria": "categoria",
    "tipo": "tipo_establecimiento",
    "departamento": "departamento",
    "provincia": "provincia",
    "distrito": "distrito",
    "ubigeo": "ubigeo",
    "direccion": "direccion",
    "telefono": "telefono",
    "estado": "estado",
}


def clean_ipress_minsa(df: pd.DataFrame) -> gpd.GeoDataFrame:
    print("\n--- Cleaning: IPRESS MINSA ---")
    if df.empty:
        print("  [skip] Empty dataframe.")
        return gpd.GeoDataFrame()

    df = _snake_columns(df).copy()
    df = df.rename(columns={k: v for k, v in _IPRESS_RENAME.items() if k in df.columns})

    # Detect lat/lon columns
    lat_col = next((c for c in _IPRESS_LAT_CANDIDATES if c in df.columns), None)
    lon_col = next((c for c in _IPRESS_LON_CANDIDATES if c in df.columns), None)

    if lat_col and lon_col:
        gdf = _coords_to_geodataframe(df, lat_col, lon_col)
    else:
        print("  [warn] No lat/lon columns found – returning plain GeoDataFrame.")
        gdf = gpd.GeoDataFrame(df, crs=TARGET_CRS)

    # Ubigeo zero-padding
    if "ubigeo" in gdf.columns:
        gdf["ubigeo"] = gdf["ubigeo"].astype(str).str.zfill(6)

    # Drop rows without a name
    if "nombre_ipress" in gdf.columns:
        before = len(gdf)
        gdf = gdf[gdf["nombre_ipress"].notna()]
        print(f"  Dropped {before - len(gdf):,} rows with null nombre_ipress.")

    _report_nulls(gdf.drop(columns="geometry", errors="ignore"), "ipress_minsa")

    out = PROCESSED / "ipress_minsa.gpkg"
    gdf.reset_index(drop=True).to_file(out, driver="GPKG", engine="pyogrio")
    print(f"  Saved → {out}  ({len(gdf):,} rows)")
    return gdf


# ---------------------------------------------------------------------------
# Dataset 4 – RENIPRESS SUSALUD
# ---------------------------------------------------------------------------
_RENI_RENAME = {
    "nombre_ipress": "nombre_ipress",
    "codigo_renaes": "codigo_renaes",
    "categoria": "categoria",
    "tipo_ipress": "tipo_ipress",
    "departamento": "departamento",
    "provincia": "provincia",
    "distrito": "distrito",
    "ubigeo": "ubigeo",
    "latitud": "lat",
    "longitud": "lon",
    "estado": "estado",
}


def clean_renipress_susalud(df: pd.DataFrame) -> gpd.GeoDataFrame:
    print("\n--- Cleaning: RENIPRESS SUSALUD ---")
    if df.empty:
        print("  [skip] Empty dataframe.")
        return gpd.GeoDataFrame()

    df = _snake_columns(df).copy()
    df = df.rename(columns={k: v for k, v in _RENI_RENAME.items() if k in df.columns})

    lat_col = next((c for c in ["lat", "latitud", "latitude"] if c in df.columns), None)
    lon_col = next((c for c in ["lon", "longitud", "longitude"] if c in df.columns), None)

    if lat_col and lon_col:
        gdf = _coords_to_geodataframe(df, lat_col, lon_col)
    else:
        gdf = gpd.GeoDataFrame(df, crs=TARGET_CRS)

    if "ubigeo" in gdf.columns:
        gdf["ubigeo"] = gdf["ubigeo"].astype(str).str.zfill(6)

    _report_nulls(gdf.drop(columns="geometry", errors="ignore"), "renipress_susalud")

    out = PROCESSED / "renipress_susalud.gpkg"
    gdf.reset_index(drop=True).to_file(out, driver="GPKG", engine="pyogrio")
    print(f"  Saved → {out}  ({len(gdf):,} rows)")
    return gdf


# ---------------------------------------------------------------------------
# Dataset 5 – Emergencias SUSALUD
# ---------------------------------------------------------------------------
_EMERG_RENAME = {
    "codigo_ipress": "codigo_ipress",
    "nombre_ipress": "nombre_ipress",
    "departamento": "departamento",
    "provincia": "provincia",
    "distrito": "distrito",
    "ubigeo": "ubigeo",
    "periodo": "periodo",
    "año": "anio",
    "mes": "mes",
    "total_atenciones": "total_atenciones",
    "total_emergencias": "total_emergencias",
}

_EMERG_NUMERIC = ["total_atenciones", "total_emergencias", "anio", "mes"]


def clean_emergencias_susalud(df: pd.DataFrame) -> pd.DataFrame:
    print("\n--- Cleaning: Emergencias SUSALUD ---")
    if df.empty:
        print("  [skip] Empty dataframe.")
        return pd.DataFrame()

    df = _snake_columns(df).copy()
    df = df.rename(columns={k: v for k, v in _EMERG_RENAME.items() if k in df.columns})

    # Coerce numeric columns
    for col in _EMERG_NUMERIC:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows missing both facility ID and name
    id_col = next((c for c in ["codigo_ipress", "nombre_ipress"] if c in df.columns), None)
    if id_col:
        before = len(df)
        df = df[df[id_col].notna()]
        print(f"  Dropped {before - len(df):,} rows with null {id_col}.")

    if "ubigeo" in df.columns:
        df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)

    _report_nulls(df, "emergencias_susalud")

    out = PROCESSED / "emergencias_susalud.parquet"
    df.to_parquet(out, index=False)
    print(f"  Saved → {out}  ({len(df):,} rows)")
    return df


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_cleaning_pipeline(datasets: dict) -> dict:
    """
    Run all cleaning functions on the raw datasets dict returned by load_all().
    Returns a dict of cleaned datasets.
    """
    print("\n=== Cleaning Pipeline ===")
    cleaned = {}

    cleaned["centros_poblados"] = clean_centros_poblados(datasets["centros_poblados"])
    cleaned["distritos"] = clean_distritos(datasets["distritos"])
    cleaned["ipress_minsa"] = clean_ipress_minsa(datasets["ipress_minsa"])
    cleaned["renipress_susalud"] = clean_renipress_susalud(datasets["renipress_susalud"])
    cleaned["emergencias_susalud"] = clean_emergencias_susalud(datasets["emergencias_susalud"])

    print("\n=== Cleaning complete ===")
    return cleaned


if __name__ == "__main__":
    from data_loader import load_all
    raw = load_all()
    run_cleaning_pipeline(raw)
