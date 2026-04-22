"""
District-level access metrics: facility density, emergency activity,
spatial access, and composite underservice index.

Inputs
------
  data/processed/districts_summary.gpkg       (from geospatial.build_district_layer)
  data/processed/centros_nearest_facility.gpkg (from geospatial.nearest_facility)

Outputs
-------
  data/output/tables/district_metrics.parquet
  data/output/tables/district_metrics.csv

Metric families
---------------
  A. Facility density      – facilities per 100 km² (baseline)
                             facilities per 10 000 pop (alternative, if population available)
  B. Emergency activity    – total emergencies per district; emergencies per facility
  C. Spatial access        – mean / p75 distance from populated centres to nearest facility;
                             share of centres beyond a hard threshold
  D. Composite index       – min-max normalised, inverted so high = underserved
                             Baseline:    equal weights across A–C
                             Alternative: population-weighted normalisation
"""

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.stats import rankdata

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT      = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
TABLES    = ROOT / "output" / "tables"
TABLES.mkdir(parents=True, exist_ok=True)

UTM_PERU = "EPSG:32718"   # metric CRS for area computation

# Hard-distance threshold used for "% far centres" metric (metres)
FAR_THRESHOLD_M = 10_000


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------
def _load_gpkg(name: str) -> gpd.GeoDataFrame | None:
    path = PROCESSED / name
    if not path.exists():
        print(f"  [warn] {name} not found – some metrics will be NaN.")
        return None
    gdf = gpd.read_file(path)
    print(f"  Loaded {name}: {len(gdf):,} rows")
    return gdf


def load_inputs() -> tuple[gpd.GeoDataFrame | None, gpd.GeoDataFrame | None]:
    """Return (districts_summary, centros_nearest_facility)."""
    print("[load] Reading processed layers …")
    districts = _load_gpkg("districts_summary.gpkg")
    centros   = _load_gpkg("centros_nearest_facility.gpkg")
    return districts, centros


# ---------------------------------------------------------------------------
# A. Facility density
# ---------------------------------------------------------------------------
def _area_km2(gdf: gpd.GeoDataFrame) -> pd.Series:
    """Compute polygon area in km² using UTM 18S."""
    utm = gdf.to_crs(UTM_PERU)
    return utm.geometry.area / 1e6


def compute_facility_density(districts: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Baseline  : facilities per 100 km²
    Alternative: facilities per 10 000 population (requires pop_total column)
    """
    df = districts.drop(columns="geometry").copy()

    # Area-based density (baseline)
    area_km2 = _area_km2(districts).rename("area_km2")
    df = df.join(area_km2)
    df["area_km2"] = df["area_km2"].replace(0, np.nan)

    for col, out in [
        ("n_ipress_minsa",      "density_ipress_per100km2"),
        ("n_renipress_susalud", "density_renipress_per100km2"),
    ]:
        if col in df.columns:
            df[out] = df[col] / df["area_km2"] * 100
        else:
            df[out] = np.nan

    # Population-based density (alternative)
    if "pop_total" in df.columns:
        df["pop_total"] = pd.to_numeric(df["pop_total"], errors="coerce").replace(0, np.nan)
        for col, out in [
            ("n_ipress_minsa",      "density_ipress_per10kpop"),
            ("n_renipress_susalud", "density_renipress_per10kpop"),
        ]:
            if col in df.columns:
                df[out] = df[col] / df["pop_total"] * 10_000

    return df[[c for c in df.columns if c != "geometry"]]


# ---------------------------------------------------------------------------
# B. Emergency activity
# ---------------------------------------------------------------------------
def compute_emergency_activity(districts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Baseline  : total_emergencias per district
    Alternative: emergencias per active facility (proxy for facility load)
    """
    df = districts_df.copy()

    total_col = next(
        (c for c in ["total_emergencias", "total_atenciones"] if c in df.columns), None
    )

    if total_col is None:
        print("  [warn] No emergency count column found – activity metrics set to NaN.")
        df["total_emergencias"]       = np.nan
        df["emergencias_per_facility"] = np.nan
        return df

    df["total_emergencias"] = pd.to_numeric(df[total_col], errors="coerce").fillna(0)

    # Emergencies per facility (alternative spec)
    n_fac = df.get("n_renipress_susalud", df.get("n_ipress_minsa"))
    if n_fac is not None:
        n_fac = pd.to_numeric(n_fac, errors="coerce").replace(0, np.nan)
        df["emergencias_per_facility"] = df["total_emergencias"] / n_fac
    else:
        df["emergencias_per_facility"] = np.nan

    return df


# ---------------------------------------------------------------------------
# C. Spatial access (from centros_nearest_facility)
# ---------------------------------------------------------------------------
def compute_spatial_access(
    centros: gpd.GeoDataFrame | None,
    ubigeo_col: str = "ubigeo",
    dist_col: str = "dist_nearest_m",
    pop_col: str = "poblacion",
) -> pd.DataFrame:
    """
    Per district, aggregate distance from populated centres to nearest facility.

    Baseline  : mean distance
    Alternative: population-weighted mean distance; p75 distance; % far centres
    """
    if centros is None or centros.empty or dist_col not in centros.columns:
        print("  [warn] centros_nearest_facility not available – spatial access NaN.")
        return pd.DataFrame(columns=[
            "ubigeo", "mean_dist_nearest_m", "p75_dist_nearest_m",
            "pct_centres_far", "wmean_dist_nearest_m",
        ])

    cc = centros.copy()
    cc[dist_col] = pd.to_numeric(cc[dist_col], errors="coerce")

    # District ubigeo on centros comes from the spatial join in geospatial.py
    if ubigeo_col not in cc.columns:
        print(f"  [warn] '{ubigeo_col}' not in centros layer – cannot aggregate by district.")
        return pd.DataFrame()

    grp = cc.groupby(ubigeo_col)

    agg = pd.DataFrame({
        "mean_dist_nearest_m": grp[dist_col].mean(),
        "p75_dist_nearest_m":  grp[dist_col].quantile(0.75),
        "pct_centres_far":     grp[dist_col].apply(
            lambda x: (x > FAR_THRESHOLD_M).mean() * 100
        ),
    })

    # Population-weighted mean distance (alternative)
    if pop_col in cc.columns:
        cc[pop_col] = pd.to_numeric(cc[pop_col], errors="coerce").fillna(1)
        def _wmean(sub):
            w = sub[pop_col]
            d = sub[dist_col]
            valid = d.notna() & w.notna()
            if valid.sum() == 0:
                return np.nan
            return np.average(d[valid], weights=w[valid])
        agg["wmean_dist_nearest_m"] = grp.apply(_wmean, include_groups=False)

    return agg.reset_index()


# ---------------------------------------------------------------------------
# D. Composite underservice index
# ---------------------------------------------------------------------------
_HIGHER_IS_WORSE = [
    "mean_dist_nearest_m",
    "p75_dist_nearest_m",
    "wmean_dist_nearest_m",
    "pct_centres_far",
    "emergencias_per_facility",   # high load → worse access
]

_LOWER_IS_WORSE = [
    "density_ipress_per100km2",
    "density_renipress_per100km2",
    "density_ipress_per10kpop",
    "density_renipress_per10kpop",
    "total_emergencias",          # zero emergencies may signal no facility
]


def _minmax(s: pd.Series) -> pd.Series:
    lo, hi = s.min(), s.max()
    if hi == lo:
        return pd.Series(0.0, index=s.index)
    return (s - lo) / (hi - lo)


def compute_composite_index(
    df: pd.DataFrame,
    weight_col: str | None = None,
) -> pd.DataFrame:
    """
    Build two composite underservice scores:

    baseline_index    : equal-weight average of normalised component scores
    alternative_index : same components but normalised using population weights
                        (districts with larger populations penalise worse access more)

    Both scores are in [0, 1] where 1 = most underserved.
    A percentile rank (0–100) is also added for interpretability.
    """
    result = df.copy()
    component_scores = []

    for col in _HIGHER_IS_WORSE:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        if s.notna().sum() < 2:
            continue
        normed = _minmax(s.fillna(s.median()))   # fill NaN with median before scaling
        component_scores.append(normed.rename(f"_score_{col}"))

    for col in _LOWER_IS_WORSE:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        if s.notna().sum() < 2:
            continue
        # Invert: low density → high underservice score
        normed = 1 - _minmax(s.fillna(s.median()))
        component_scores.append(normed.rename(f"_score_{col}"))

    if not component_scores:
        print("  [warn] No valid components for composite index.")
        result["baseline_index"]    = np.nan
        result["alternative_index"] = np.nan
        return result

    score_df = pd.concat(component_scores, axis=1)

    # Baseline: equal weights
    result["baseline_index"] = score_df.mean(axis=1)

    # Alternative: population-weighted component normalisation
    if weight_col and weight_col in df.columns:
        pop = pd.to_numeric(df[weight_col], errors="coerce").fillna(1)
        pop_norm = pop / pop.sum()
        # Re-scale each component by district population share before averaging
        alt_scores = score_df.multiply(pop_norm, axis=0)
        # Normalise back to [0,1]
        alt_sum = alt_scores.sum(axis=1)
        lo, hi = alt_sum.min(), alt_sum.max()
        result["alternative_index"] = (alt_sum - lo) / (hi - lo) if hi > lo else 0.0
    else:
        # Without population: alternative uses rank-based normalisation
        result["alternative_index"] = score_df.apply(
            lambda col: pd.Series(rankdata(col, method="average") / len(col), index=col.index)
        ).mean(axis=1)

    # Percentile ranks (0–100, higher = more underserved)
    for idx_col in ["baseline_index", "alternative_index"]:
        result[f"{idx_col}_pct"] = (
            result[idx_col].rank(pct=True, na_option="bottom") * 100
        ).round(1)

    return result


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_metrics_pipeline(
    districts: gpd.GeoDataFrame | None = None,
    centros: gpd.GeoDataFrame | None = None,
) -> pd.DataFrame:
    """
    Compute all district-level metrics and return a flat DataFrame.

    Parameters
    ----------
    districts : GeoDataFrame, optional
        Output of geospatial.build_district_layer(). Loaded from disk if None.
    centros : GeoDataFrame, optional
        Output of geospatial.nearest_facility(). Loaded from disk if None.
    """
    print("=== Metrics Pipeline ===\n")

    if districts is None or centros is None:
        _d, _c = load_inputs()
        if districts is None:
            districts = _d
        if centros is None:
            centros = _c

    if districts is None or districts.empty:
        print("[error] No district layer available. Run geospatial pipeline first.")
        return pd.DataFrame()

    # A. Facility density
    print("[A] Facility density …")
    df = compute_facility_density(districts)

    # B. Emergency activity
    print("[B] Emergency activity …")
    df = compute_emergency_activity(df)

    # C. Spatial access
    print("[C] Spatial access …")
    spatial = compute_spatial_access(centros)
    if not spatial.empty and "ubigeo" in df.columns:
        df = df.merge(spatial, on="ubigeo", how="left")

    # D. Composite index
    print("[D] Composite underservice index …")
    weight_col = "pop_total" if "pop_total" in df.columns else None
    df = compute_composite_index(df, weight_col=weight_col)

    # Summary
    print(f"\n  Final metrics table: {len(df):,} districts × {df.shape[1]} columns")
    for col in ["baseline_index", "alternative_index"]:
        if col in df.columns:
            top5 = df.nlargest(5, col)[["ubigeo", col]].to_string(index=False)
            print(f"\n  Top-5 underserved by {col}:\n{top5}")

    # Save
    out_pq  = TABLES / "district_metrics.parquet"
    out_csv = TABLES / "district_metrics.csv"
    df.to_parquet(out_pq, index=False)
    df.to_csv(out_csv, index=False)
    print(f"\n  Saved → {out_pq}")
    print(f"  Saved → {out_csv}")
    print("\n=== Metrics pipeline complete ===")
    return df


if __name__ == "__main__":
    run_metrics_pipeline()
