"""
Download and load all raw datasets for the emergency healthcare access project.

Datasets:
  1. Centros Poblados          – IGN shapefile (datosabiertos.gob.pe)
  2. DISTRITOS                 – District boundaries (course GitHub repo)
  3. IPRESS MINSA              – MINSA health facilities CSV (datosabiertos.gob.pe)
  4. RENIPRESS SUSALUD         – National IPRESS registry CSV (datosabiertos.gob.pe)
  5. Emergencias SUSALUD       – Emergency production by IPRESS (datos.susalud.gob.pe)
"""

import io
import os
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Source URLs
# ---------------------------------------------------------------------------
URLS = {
    "centros_poblados_zip": (
        "https://www.datosabiertos.gob.pe/sites/default/files/CCPP_0.zip"
    ),
    "distritos_base": (
        "https://raw.githubusercontent.com/d2cml-ai/Data-Science-Python"
        "/main/_data/Folium/DISTRITOS"
    ),
    "ipress_minsa": (
        "https://www.datosabiertos.gob.pe/sites/default/files/recursos/2017/09/IPRESS.csv"
    ),
    # SUSALUD national IPRESS registry (datosabiertos.gob.pe mirror – more reliable)
    "renipress_susalud": (
        "https://www.datosabiertos.gob.pe/datastore/dump"
        "/8bb014bd-bb39-40d8-bfd7-0c8bcb4eb37d?bom=True"
    ),
    # Emergency production dataset – hosted on datos.susalud.gob.pe
    # If this URL fails, download manually from:
    #   http://datos.susalud.gob.pe/dataset/consulta-c1-produccion-asistencial-en-emergencia-por-ipress
    # and place the CSV at data/raw/emergencias_susalud.csv
    "emergencias_susalud": (
        "http://datos.susalud.gob.pe/datastore/dump"
        "/c1-produccion-asistencial-emergencia-ipress?bom=True"
    ),
}

DISTRITOS_EXTS = [".shp", ".dbf", ".prj", ".shx", ".cpg", ".shp.xml"]

TIMEOUT = 60  # seconds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _download(url: str, dest: Path, *, timeout: int = TIMEOUT) -> Path:
    """Stream-download *url* to *dest*; skip if already present."""
    if dest.exists():
        print(f"  [skip] {dest.name} already downloaded.")
        return dest
    print(f"  Downloading {dest.name} …")
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        dest.write_bytes(r.content)
    print(f"  Saved → {dest}")
    return dest


# ---------------------------------------------------------------------------
# Dataset 1 – Centros Poblados
# ---------------------------------------------------------------------------
def load_centros_poblados() -> gpd.GeoDataFrame:
    """Download and load IGN Centros Poblados shapefile."""
    zip_path = RAW / "CCPP_0.zip"
    extract_dir = RAW / "centros_poblados"

    _download(URLS["centros_poblados_zip"], zip_path)

    if not extract_dir.exists():
        print("  Extracting CCPP_0.zip …")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

    # Locate the .shp inside the extracted folder (may be nested)
    shp_files = list(extract_dir.rglob("*.shp"))
    if not shp_files:
        raise FileNotFoundError(f"No .shp found inside {extract_dir}")

    gdf = gpd.read_file(shp_files[0])
    print(f"  Centros Poblados loaded: {len(gdf):,} rows, CRS={gdf.crs}")
    return gdf


# ---------------------------------------------------------------------------
# Dataset 2 – DISTRITOS
# ---------------------------------------------------------------------------
def load_distritos() -> gpd.GeoDataFrame:
    """Download and load Peruvian district boundaries shapefile."""
    distritos_dir = RAW / "distritos"
    distritos_dir.mkdir(exist_ok=True)

    base = URLS["distritos_base"]
    for ext in DISTRITOS_EXTS:
        dest = distritos_dir / f"DISTRITOS{ext}"
        try:
            _download(f"{base}{ext}", dest)
        except requests.HTTPError as e:
            # .shp.xml is optional metadata – skip if missing
            if ext == ".shp.xml":
                print(f"  [warn] {dest.name} not found (optional), skipping.")
            else:
                raise e

    gdf = gpd.read_file(distritos_dir / "DISTRITOS.shp")
    print(f"  DISTRITOS loaded: {len(gdf):,} rows, CRS={gdf.crs}")
    return gdf


# ---------------------------------------------------------------------------
# Dataset 3 – IPRESS MINSA
# ---------------------------------------------------------------------------
def load_ipress_minsa() -> pd.DataFrame:
    """Download and load MINSA IPRESS health facilities CSV."""
    dest = RAW / "ipress_minsa.csv"
    _download(URLS["ipress_minsa"], dest)

    df = pd.read_csv(dest, encoding="latin-1", low_memory=False)
    print(f"  IPRESS MINSA loaded: {len(df):,} rows, {df.shape[1]} cols")
    return df


# ---------------------------------------------------------------------------
# Dataset 4 – RENIPRESS SUSALUD (national IPRESS registry)
# ---------------------------------------------------------------------------
def load_renipress_susalud() -> pd.DataFrame:
    """Download and load SUSALUD national IPRESS registry CSV."""
    dest = RAW / "renipress_susalud.csv"
    try:
        _download(URLS["renipress_susalud"], dest)
        df = pd.read_csv(dest, low_memory=False)
        print(f"  RENIPRESS SUSALUD loaded: {len(df):,} rows, {df.shape[1]} cols")
        return df
    except Exception as e:
        print(f"  [warn] RENIPRESS SUSALUD download failed: {e}")
        print("  Manual download: https://www.datosabiertos.gob.pe/dataset/"
              "registro-nacional-de-ipress-renipress-superintendencia-nacional-de-salud-susalud")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Dataset 5 – Emergencias SUSALUD (C1 emergency production)
# ---------------------------------------------------------------------------
def load_emergencias_susalud() -> pd.DataFrame:
    """
    Download emergency care production by IPRESS from SUSALUD.

    If datos.susalud.gob.pe is unreachable, place the CSV manually at:
        data/raw/emergencias_susalud.csv
    and re-run.
    """
    dest = RAW / "emergencias_susalud.csv"

    if dest.exists():
        print(f"  [skip] {dest.name} already present.")
    else:
        try:
            _download(URLS["emergencias_susalud"], dest, timeout=30)
        except Exception as e:
            print(f"  [warn] Emergencias SUSALUD download failed: {e}")
            print(
                "  Manual download:\n"
                "    http://datos.susalud.gob.pe/dataset/"
                "consulta-c1-produccion-asistencial-en-emergencia-por-ipress\n"
                "  Save the file to: data/raw/emergencias_susalud.csv"
            )
            return pd.DataFrame()

    df = pd.read_csv(dest, low_memory=False, encoding="utf-8-sig")
    print(f"  Emergencias SUSALUD loaded: {len(df):,} rows, {df.shape[1]} cols")
    return df


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def load_all() -> dict:
    """Run the full ingestion pipeline and return all raw datasets."""
    print("=== Data Ingestion Pipeline ===\n")
    datasets = {}

    print("[1/5] Centros Poblados …")
    datasets["centros_poblados"] = load_centros_poblados()

    print("\n[2/5] DISTRITOS …")
    datasets["distritos"] = load_distritos()

    print("\n[3/5] IPRESS MINSA …")
    datasets["ipress_minsa"] = load_ipress_minsa()

    print("\n[4/5] RENIPRESS SUSALUD …")
    datasets["renipress_susalud"] = load_renipress_susalud()

    print("\n[5/5] Emergencias SUSALUD …")
    datasets["emergencias_susalud"] = load_emergencias_susalud()

    print("\n=== Ingestion complete ===")
    return datasets


if __name__ == "__main__":
    load_all()
