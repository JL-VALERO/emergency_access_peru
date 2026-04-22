"""
Static charts and geospatial maps answering the four analytical questions.

Q1 — Which districts have lower/higher facility and emergency care availability?
Q2 — Which districts show weaker populated-centre access to emergency services?
Q3 — Which districts appear most/least underserved when combining all factors?
Q4 — How sensitive are results to alternative methodological definitions?

Geospatial outputs
------------------
  Static choropleths (matplotlib/geopandas) — 4 maps on one figure
  Interactive map (Folium)                  — HTML with choropleth + tooltips

Inputs : output/tables/district_metrics.csv  (or a pre-loaded DataFrame)
         data/processed/districts_summary.gpkg
         data/processed/centros_nearest_facility.gpkg  (optional)
Outputs: output/figures/  (PNG 150 dpi + HTML)
"""

from pathlib import Path

import folium
import geopandas as gpd
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
from folium.plugins import FloatImage
from scipy.stats import spearmanr

# ---------------------------------------------------------------------------
# Paths & style
# ---------------------------------------------------------------------------
ROOT      = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
TABLES    = ROOT / "output" / "tables"
FIGURES   = ROOT / "output" / "figures"
FIGURES.mkdir(parents=True, exist_ok=True)

METRICS_CSV = TABLES / "district_metrics.csv"

DPI    = 150
ACCENT = "#C0392B"   # red – highlights worst-off districts
BLUE   = "#2980B9"
GREY   = "#7F8C8D"
LIGHT  = "#ECF0F1"

sns.set_theme(style="whitegrid", palette="muted", font_scale=0.95)
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor":   "white",
    "axes.edgecolor":   "#BDC3C7",
    "grid.color":       "#ECF0F1",
    "grid.linewidth":   0.8,
    "axes.spines.top":  False,
    "axes.spines.right": False,
})


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------
def load_metrics(df: pd.DataFrame | None = None) -> pd.DataFrame:
    if df is not None and not df.empty:
        return df.copy()
    if not METRICS_CSV.exists():
        raise FileNotFoundError(
            f"{METRICS_CSV} not found. Run metrics.run_metrics_pipeline() first."
        )
    return pd.read_csv(METRICS_CSV)


def _dept_label(df: pd.DataFrame) -> str:
    """Return the department column name (varies by shapefile vintage)."""
    for c in ["nombre_departamento", "departamen", "departamento", "DEPARTAMEN"]:
        if c in df.columns:
            return c
    return df.columns[0]


def _save(fig: plt.Figure, name: str) -> Path:
    path = FIGURES / f"{name}.png"
    fig.savefig(path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved → {path.name}")
    return path


# ---------------------------------------------------------------------------
# Q1 – Facility and emergency availability
# ---------------------------------------------------------------------------
def plot_q1_facility_availability(df: pd.DataFrame) -> list[Path]:
    """Three charts addressing Q1."""
    dept_col = _dept_label(df)
    saved = []

    # --- 1a. Facility density distribution (histogram + rug) ---
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    fig.suptitle(
        "Q1 — Facility availability across Peruvian districts",
        fontweight="bold", y=1.01,
    )

    for ax, col, label in [
        (axes[0], "density_ipress_per100km2",     "MINSA IPRESS per 100 km²"),
        (axes[1], "density_renipress_per100km2",  "RENIPRESS per 100 km²"),
    ]:
        s = df[col].dropna()
        s_trim = s[s <= s.quantile(0.98)]   # trim extreme outliers for readability
        ax.hist(s_trim, bins=50, color=BLUE, edgecolor="white", linewidth=0.4)
        ax.axvline(s.median(), color=ACCENT, lw=1.6, ls="--",
                   label=f"Median = {s.median():.2f}")
        ax.set_xlabel(label)
        ax.set_ylabel("Number of districts")
        ax.legend(fontsize=8)

    fig.tight_layout()
    saved.append(_save(fig, "q1a_density_distribution"))

    # --- 1b. Top / bottom 15 departments by mean IPRESS density ---
    dept_density = (
        df.groupby(dept_col)["density_ipress_per100km2"]
        .median()
        .dropna()
        .sort_values()
    )

    n_show = min(15, len(dept_density))
    plot_data = pd.concat([dept_density.head(n_show), dept_density.tail(n_show)])
    colors = [ACCENT if v <= dept_density.quantile(0.25) else BLUE
              for v in plot_data.values]

    fig, ax = plt.subplots(figsize=(9, max(5, n_show * 0.45)))
    ax.barh(range(len(plot_data)), plot_data.values, color=colors)
    ax.set_yticks(range(len(plot_data)))
    ax.set_yticklabels(
        [str(d)[:22] for d in plot_data.index], fontsize=8
    )
    ax.axvline(dept_density.median(), color=GREY, lw=1.2, ls=":",
               label="National median")
    ax.set_xlabel("Median IPRESS per 100 km² (district-level)")
    ax.set_title("Q1 — Department ranking: IPRESS facility density\n"
                 "(bottom 15 in red, top 15 in blue)",
                 fontweight="bold")
    ax.legend(fontsize=8)
    fig.tight_layout()
    saved.append(_save(fig, "q1b_dept_facility_ranking"))

    # --- 1c. Emergency volume by department (box plot) ---
    top_depts = (
        df.groupby(dept_col)["total_emergencias"]
        .sum()
        .nlargest(12)
        .index
    )
    sub = df[df[dept_col].isin(top_depts)].copy()
    order = (
        sub.groupby(dept_col)["total_emergencias"]
        .median()
        .sort_values(ascending=False)
        .index
    )

    fig, ax = plt.subplots(figsize=(11, 5))
    sns.boxplot(
        data=sub, x=dept_col, y="total_emergencias",
        order=order, palette="Blues_r",
        flierprops={"marker": ".", "alpha": 0.4, "markersize": 3},
        ax=ax,
    )
    ax.set_xticklabels(
        [str(l.get_text())[:14] for l in ax.get_xticklabels()],
        rotation=35, ha="right", fontsize=8,
    )
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else str(int(x))
    ))
    ax.set_xlabel("")
    ax.set_ylabel("Total emergencies (district)")
    ax.set_title("Q1 — Emergency volume distribution by department (top 12)",
                 fontweight="bold")
    fig.tight_layout()
    saved.append(_save(fig, "q1c_emergency_volume_dept"))

    return saved


# ---------------------------------------------------------------------------
# Q2 – Populated-centre access to emergency services
# ---------------------------------------------------------------------------
def plot_q2_spatial_access(df: pd.DataFrame) -> list[Path]:
    """Charts addressing Q2 (distance-based access metrics)."""
    dept_col = _dept_label(df)
    saved = []

    dist_col = "mean_dist_nearest_m"
    available = df[dist_col].notna().sum()
    if available == 0:
        print(f"  [skip Q2] No data in '{dist_col}' — spatial access charts skipped.")
        return saved

    # --- 2a. Distance distribution ---
    fig, ax = plt.subplots(figsize=(9, 4))
    s = df[dist_col].dropna() / 1000   # convert to km
    ax.hist(s, bins=40, color=BLUE, edgecolor="white", linewidth=0.4)
    ax.axvline(s.median(), color=ACCENT, lw=1.6, ls="--",
               label=f"Median = {s.median():.1f} km")
    ax.axvline(10, color=GREY, lw=1.2, ls=":",
               label="10 km threshold")
    ax.set_xlabel("Mean distance to nearest IPRESS (km)")
    ax.set_ylabel("Number of districts")
    ax.set_title("Q2 — Distribution of mean distance to nearest health facility",
                 fontweight="bold")
    ax.legend()
    fig.tight_layout()
    saved.append(_save(fig, "q2a_distance_distribution"))

    # --- 2b. % far centres by department ---
    if "pct_centres_far" in df.columns and df["pct_centres_far"].notna().sum() > 5:
        dept_far = (
            df.groupby(dept_col)["pct_centres_far"]
            .mean()
            .dropna()
            .sort_values(ascending=False)
            .head(15)
        )
        fig, ax = plt.subplots(figsize=(9, 5))
        colors = [ACCENT if v >= dept_far.quantile(0.75) else BLUE
                  for v in dept_far.values]
        ax.barh(range(len(dept_far)), dept_far.values, color=colors)
        ax.set_yticks(range(len(dept_far)))
        ax.set_yticklabels(
            [str(d)[:22] for d in dept_far.index], fontsize=9
        )
        ax.set_xlabel("Mean % of populated centres > 10 km from nearest IPRESS")
        ax.set_title("Q2 — Departments with highest share of poorly-served\n"
                     "populated centres (> 10 km from nearest facility)",
                     fontweight="bold")
        ax.xaxis.set_major_formatter(mticker.PercentFormatter())
        fig.tight_layout()
        saved.append(_save(fig, "q2b_pct_far_centres_dept"))

    # --- 2c. Scatter: district population vs mean distance ---
    if "pop_total" in df.columns:
        sub = df[[dist_col, "pop_total", dept_col]].dropna()
        sub = sub[sub["pop_total"] > 0]
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.scatter(
            sub["pop_total"] / 1000,
            sub[dist_col] / 1000,
            alpha=0.35, s=18, color=BLUE, linewidths=0,
        )
        ax.set_xscale("log")
        ax.set_xlabel("District population (thousands, log scale)")
        ax.set_ylabel("Mean distance to nearest IPRESS (km)")
        ax.set_title("Q2 — Distance to nearest facility vs district population",
                     fontweight="bold")
        # Reference line
        ax.axhline(10, color=ACCENT, lw=1.2, ls="--", label="10 km threshold")
        ax.legend(fontsize=8)
        fig.tight_layout()
        saved.append(_save(fig, "q2c_distance_vs_population"))

    return saved


# ---------------------------------------------------------------------------
# Q3 – Most/least underserved districts
# ---------------------------------------------------------------------------
def plot_q3_underservice(df: pd.DataFrame) -> list[Path]:
    """Charts addressing Q3 (composite index)."""
    dept_col = _dept_label(df)
    saved = []

    idx_col = "baseline_index"
    if idx_col not in df.columns or df[idx_col].notna().sum() == 0:
        print("  [skip Q3] baseline_index not available.")
        return saved

    # --- 3a. Top 20 most underserved districts ---
    top20 = df.nlargest(20, idx_col)[["distrito", dept_col, idx_col]].copy()
    top20["label"] = (
        top20["distrito"].str[:18] + "\n(" + top20[dept_col].str[:10] + ")"
    )

    fig, ax = plt.subplots(figsize=(9, 7))
    bars = ax.barh(range(len(top20)), top20[idx_col].values,
                   color=ACCENT, edgecolor="white", linewidth=0.4)
    ax.set_yticks(range(len(top20)))
    ax.set_yticklabels(top20["label"].values, fontsize=7.5)
    ax.set_xlabel("Baseline underservice index [0–1]")
    ax.set_title("Q3 — Top 20 most underserved districts\n(baseline composite index)",
                 fontweight="bold")
    ax.set_xlim(0, 1)
    for bar, val in zip(bars, top20[idx_col].values):
        ax.text(val + 0.01, bar.get_y() + bar.get_height() / 2,
                f"{val:.3f}", va="center", fontsize=7.5)
    fig.tight_layout()
    saved.append(_save(fig, "q3a_top20_underserved"))

    # --- 3b. Box plot of underservice index by department ---
    dept_median = (
        df.groupby(dept_col)[idx_col]
        .median()
        .sort_values(ascending=False)
    )
    n_depts = min(15, len(dept_median))
    order = dept_median.head(n_depts).index.tolist()
    sub = df[df[dept_col].isin(order)]

    fig, ax = plt.subplots(figsize=(11, 5))
    sns.boxplot(
        data=sub, x=dept_col, y=idx_col, order=order,
        palette=sns.color_palette("Reds_r", n_depts),
        flierprops={"marker": ".", "alpha": 0.4, "markersize": 3},
        ax=ax,
    )
    ax.set_xticklabels(
        [str(l.get_text())[:14] for l in ax.get_xticklabels()],
        rotation=35, ha="right", fontsize=8,
    )
    ax.set_xlabel("")
    ax.set_ylabel("Baseline underservice index")
    ax.set_title("Q3 — Underservice index distribution by department\n"
                 "(top 15 departments by median, most underserved first)",
                 fontweight="bold")
    fig.tight_layout()
    saved.append(_save(fig, "q3b_underservice_by_dept"))

    # --- 3c. Heatmap: metric correlations ---
    metric_cols = [
        "density_ipress_per100km2", "density_renipress_per100km2",
        "total_emergencias", "emergencias_per_facility",
        "mean_dist_nearest_m", "pct_centres_far",
        "baseline_index",
    ]
    avail = [c for c in metric_cols if c in df.columns and df[c].notna().sum() > 10]
    if len(avail) >= 3:
        corr = df[avail].corr(method="spearman")
        short = {
            "density_ipress_per100km2":      "IPRESS\ndensity",
            "density_renipress_per100km2":   "RENIPRESS\ndensity",
            "total_emergencias":             "Total\nemergencies",
            "emergencias_per_facility":      "Emerg. per\nfacility",
            "mean_dist_nearest_m":           "Mean\ndistance",
            "pct_centres_far":               "% far\ncentres",
            "baseline_index":                "Baseline\nindex",
        }
        corr.index   = [short.get(c, c) for c in corr.index]
        corr.columns = [short.get(c, c) for c in corr.columns]

        fig, ax = plt.subplots(figsize=(8, 6))
        mask = np.triu(np.ones_like(corr, dtype=bool), k=1)
        sns.heatmap(
            corr, mask=mask, annot=True, fmt=".2f",
            cmap="RdBu_r", vmin=-1, vmax=1, center=0,
            linewidths=0.5, ax=ax,
            annot_kws={"size": 8},
        )
        ax.set_title("Q3 — Spearman correlations between access metrics",
                     fontweight="bold")
        fig.tight_layout()
        saved.append(_save(fig, "q3c_metric_correlations"))

    return saved


# ---------------------------------------------------------------------------
# Q4 – Sensitivity: baseline vs alternative specification
# ---------------------------------------------------------------------------
def plot_q4_sensitivity(df: pd.DataFrame) -> list[Path]:
    """Charts addressing Q4 (robustness of index to methodological choices)."""
    dept_col = _dept_label(df)
    saved = []

    b_col, a_col = "baseline_index", "alternative_index"
    if b_col not in df.columns or a_col not in df.columns:
        print("  [skip Q4] Index columns not available.")
        return saved

    sub = df[[b_col, a_col, "distrito", dept_col]].dropna()

    # --- 4a. Scatter: baseline vs alternative ---
    rho, pval = spearmanr(sub[b_col], sub[a_col])

    fig, ax = plt.subplots(figsize=(7, 6))
    ax.scatter(sub[b_col], sub[a_col],
               alpha=0.35, s=14, color=BLUE, linewidths=0)
    # Diagonal reference
    lims = [0, 1]
    ax.plot(lims, lims, color=GREY, lw=1, ls="--", label="x = y")
    ax.set_xlabel("Baseline underservice index")
    ax.set_ylabel("Alternative underservice index")
    ax.set_title(
        f"Q4 — Baseline vs alternative index\n"
        f"Spearman ρ = {rho:.3f}  (p {'< 0.001' if pval < 0.001 else f'= {pval:.3f}'})",
        fontweight="bold",
    )
    ax.legend(fontsize=8)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    fig.tight_layout()
    saved.append(_save(fig, "q4a_baseline_vs_alternative"))

    # --- 4b. Rank-change chart: top 20 by baseline, track their alternative rank ---
    n_rank = 20
    ranked = df[[b_col, a_col, "distrito"]].dropna().copy()
    ranked["rank_baseline"]    = ranked[b_col].rank(ascending=False).astype(int)
    ranked["rank_alternative"] = ranked[a_col].rank(ascending=False).astype(int)
    top_base = ranked.nsmallest(n_rank, "rank_baseline")

    fig, ax = plt.subplots(figsize=(8, 6))
    for _, row in top_base.iterrows():
        rb, ra = row["rank_baseline"], row["rank_alternative"]
        color = ACCENT if abs(rb - ra) > 5 else BLUE
        ax.plot([0, 1], [rb, ra], color=color, alpha=0.7, lw=1.5,
                marker="o", markersize=4)
        ax.text(-0.03, rb, str(int(rb)), va="center", ha="right", fontsize=7.5)
        ax.text(1.03,  ra, str(int(ra)), va="center", ha="left",  fontsize=7.5)

    ax.set_xlim(-0.15, 1.15)
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["Baseline\nranking", "Alternative\nranking"], fontsize=9)
    ax.invert_yaxis()
    ax.set_ylabel("Underservice rank (1 = most underserved)")
    ax.set_title(f"Q4 — Rank stability: top {n_rank} districts under baseline spec\n"
                 "(red lines = rank shift > 5 positions)",
                 fontweight="bold")
    ax.yaxis.set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    fig.tight_layout()
    saved.append(_save(fig, "q4b_rank_change"))

    # --- 4c. Department-level agreement ---
    dept_b = df.groupby(dept_col)[b_col].median().rename("baseline")
    dept_a = df.groupby(dept_col)[a_col].median().rename("alternative")
    dept_both = pd.concat([dept_b, dept_a], axis=1).dropna()

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(dept_both["baseline"], dept_both["alternative"],
               s=50, color=BLUE, alpha=0.7, linewidths=0)
    for dept, row in dept_both.iterrows():
        ax.text(row["baseline"] + 0.002, row["alternative"],
                str(dept)[:10], fontsize=6.5, alpha=0.75)
    rho2, _ = spearmanr(dept_both["baseline"], dept_both["alternative"])
    ax.set_xlabel("Baseline index (department median)")
    ax.set_ylabel("Alternative index (department median)")
    ax.set_title(f"Q4 — Department-level agreement between specifications\n"
                 f"Spearman ρ = {rho2:.3f}",
                 fontweight="bold")
    lims2 = [0, max(dept_both.max()) * 1.05]
    ax.plot(lims2, lims2, color=GREY, lw=1, ls="--")
    ax.set_xlim(lims2); ax.set_ylim(lims2)
    fig.tight_layout()
    saved.append(_save(fig, "q4c_dept_agreement"))

    return saved


# ---------------------------------------------------------------------------
# NumPy-2-safe GeoJSON serialiser
# (geopandas __geo_interface__ / to_json use np.array(copy=False) which
#  raises on NumPy ≥ 2.0 with fiona < 2.  shapely.mapping avoids that path.)
# ---------------------------------------------------------------------------
def _gdf_to_geojson(gdf: gpd.GeoDataFrame) -> dict:
    """Convert a GeoDataFrame to a plain GeoJSON dict without __geo_interface__."""
    from shapely.geometry import mapping

    features = []
    prop_cols = [c for c in gdf.columns if c != gdf.geometry.name]
    for _, row in gdf.iterrows():
        geom = row[gdf.geometry.name]
        if geom is None or geom.is_empty:
            continue
        props = {}
        for c in prop_cols:
            v = row[c]
            # JSON must be serialisable
            if isinstance(v, float) and np.isnan(v):
                props[c] = None
            elif hasattr(v, "item"):          # numpy scalar
                props[c] = v.item()
            else:
                props[c] = v
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": mapping(geom),
        })
    return {"type": "FeatureCollection", "features": features}


# ---------------------------------------------------------------------------
# Geodata loader  (merges geometry with computed metrics)
# ---------------------------------------------------------------------------
def load_geodata(df: pd.DataFrame | None = None) -> gpd.GeoDataFrame | None:
    """
    Merge district_metrics with districts_summary geometry.
    Returns None if the GeoPackage is missing.
    """
    gpkg = PROCESSED / "districts_summary.gpkg"
    if not gpkg.exists():
        print(f"  [warn] {gpkg.name} not found — choropleth maps skipped.")
        return None

    gdf = gpd.read_file(gpkg)

    if df is not None and not df.empty:
        # ubigeo may be stored as int in shapefile but str in metrics
        for frame in [gdf, df]:
            if "ubigeo" in frame.columns:
                frame["ubigeo"] = frame["ubigeo"].astype(str).str.zfill(6)

        metric_cols = [c for c in df.columns if c not in gdf.columns or c == "ubigeo"]
        gdf = gdf.merge(df[metric_cols], on="ubigeo", how="left")

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")

    print(f"  Geodata: {len(gdf):,} districts, CRS={gdf.crs.to_epsg()}")
    return gdf


# ---------------------------------------------------------------------------
# Static choropleths  (matplotlib / geopandas)
# ---------------------------------------------------------------------------
_CHOROPLETH_SPECS = [
    # (column, title, colormap, unit_label, log_scale)
    ("density_ipress_per100km2",  "IPRESS density\n(per 100 km²)",   "YlOrRd", "facilities", False),
    ("total_emergencias",         "Emergency volume\n(total)",         "Blues",  "emergencies", True),
    ("mean_dist_nearest_m",       "Mean distance to\nnearest IPRESS",  "RdPu",  "metres",       True),
    ("baseline_index",            "Underservice index\n(baseline)",    "Reds",  "0–1 score",   False),
]


def plot_choropleth_maps(
    gdf: gpd.GeoDataFrame,
    df_metrics: pd.DataFrame | None = None,
) -> list[Path]:
    """
    2 × 2 grid of static district-level choropleth maps.
    Each panel maps one metric; missing values shown in light grey.
    """
    saved = []

    fig, axes = plt.subplots(2, 2, figsize=(16, 18))
    fig.suptitle(
        "Emergency healthcare access across Peruvian districts",
        fontsize=14, fontweight="bold", y=1.005,
    )

    for ax, (col, title, cmap_name, unit, log) in zip(
        axes.flat, _CHOROPLETH_SPECS
    ):
        if col not in gdf.columns or gdf[col].notna().sum() == 0:
            ax.set_title(f"{title}\n(no data)", fontsize=9)
            ax.axis("off")
            continue

        s = gdf[col].copy()

        # Colour normalisation
        valid = s.dropna()
        if log and (valid > 0).any():
            vmin = max(valid[valid > 0].quantile(0.02), 1e-6)
            vmax = valid.quantile(0.98)
            norm = mcolors.LogNorm(vmin=vmin, vmax=vmax)
        else:
            vmin = valid.quantile(0.02)
            vmax = valid.quantile(0.98)
            norm = mcolors.Normalize(vmin=vmin, vmax=vmax)

        cmap = cm.get_cmap(cmap_name)
        missing_color = "#D5D8DC"

        # Split into rows with and without values
        has_val = gdf[s.notna()]
        no_val  = gdf[s.isna()]

        if not no_val.empty:
            no_val.plot(ax=ax, color=missing_color, linewidth=0.05)
        if not has_val.empty:
            has_val.plot(
                ax=ax, column=col, cmap=cmap_name,
                norm=norm, linewidth=0.05, edgecolor="white",
            )

        # Colorbar
        sm = cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        cbar = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02, shrink=0.7)
        cbar.set_label(unit, fontsize=7)
        cbar.ax.tick_params(labelsize=6)

        # Legend patch for missing
        if not no_val.empty:
            patch = mpatches.Patch(color=missing_color, label="No data")
            ax.legend(handles=[patch], fontsize=6, loc="lower left")

        ax.set_title(title, fontsize=10, fontweight="bold")
        ax.axis("off")

    fig.tight_layout()
    saved.append(_save(fig, "geo_choropleths"))
    return saved


# ---------------------------------------------------------------------------
# Interactive Folium map
# ---------------------------------------------------------------------------
_FOLIUM_COLS = {
    "baseline_index":            ("Underservice index (baseline)",  "YlOrRd"),
    "density_ipress_per100km2":  ("IPRESS density / 100 km²",       "Blues"),
    "mean_dist_nearest_m":       ("Mean dist. to nearest IPRESS (m)","PuRd"),
    "total_emergencias":         ("Total emergencies",               "BuPu"),
}

_TOOLTIP_COLS = [
    "distrito", "departamen", "ubigeo",
    "baseline_index", "density_ipress_per100km2",
    "mean_dist_nearest_m", "total_emergencias",
    "n_ipress_minsa", "n_renipress_susalud",
]


def plot_folium_interactive(
    gdf: gpd.GeoDataFrame,
    centros: gpd.GeoDataFrame | None = None,
) -> list[Path]:
    """
    Build a multi-layer Folium map:
      • Choropleth layers for each metric (togglable)
      • Optional populated-centre markers (sub-sampled for performance)
      • District tooltip on hover
    Saved as HTML to output/figures/.
    """
    saved = []

    # Centre map on Peru
    m = folium.Map(
        location=[-9.5, -75.0],
        zoom_start=6,
        tiles="CartoDB positron",
        control_scale=True,
    )

    # Convert to WGS84, simplify polygons to keep HTML size manageable
    geo = gdf.to_crs("EPSG:4326").copy()
    geo["ubigeo"] = geo["ubigeo"].astype(str).str.zfill(6)
    geo["geometry"] = geo["geometry"].simplify(tolerance=0.01, preserve_topology=True)

    # Tooltip fields (keep only those present)
    tt_cols = [c for c in _TOOLTIP_COLS if c in geo.columns]
    tooltip_fields  = tt_cols
    tooltip_aliases = [c.replace("_", " ").title() + ":" for c in tt_cols]

    # Build GeoJSON once (reused by all layers)
    geo_json = _gdf_to_geojson(geo)
    tooltip_json = _gdf_to_geojson(geo[tt_cols + ["geometry"]])

    # --- Choropleth layers ---
    # Choropleth must be added directly to the Map (not inside a FeatureGroup).
    # The `show` kwarg controls which layer is visible on load.
    first = True
    for col, (layer_name, palette) in _FOLIUM_COLS.items():
        if col not in geo.columns or geo[col].notna().sum() == 0:
            continue

        data_series = geo[["ubigeo", col]].dropna()

        choropleth = folium.Choropleth(
            geo_data=geo_json,
            data=data_series,
            columns=["ubigeo", col],
            key_on="feature.properties.ubigeo",
            fill_color=palette,
            fill_opacity=0.75,
            line_opacity=0.15,
            line_color="white",
            nan_fill_color="#EEEEEE",
            nan_fill_opacity=0.4,
            legend_name=layer_name,
            name=layer_name,
            show=first,
            highlight=True,
        )
        choropleth.add_to(m)
        first = False

    # Invisible GeoJson layer for tooltips on hover (one shared layer)
    folium.GeoJson(
        tooltip_json,
        name="District tooltips",
        style_function=lambda _: {"fillOpacity": 0, "weight": 0},
        tooltip=folium.GeoJsonTooltip(
            fields=tooltip_fields,
            aliases=tooltip_aliases,
            localize=True,
            sticky=False,
            labels=True,
            style=(
                "background-color: white; color: #333; "
                "font-family: Arial; font-size: 11px; padding: 6px;"
            ),
        ),
        highlight_function=lambda _: {"weight": 2, "color": "#333333"},
        show=True,
    ).add_to(m)

    # --- Populated centres layer (sub-sampled, up to 300 points) ---
    if centros is not None and not centros.empty:
        cc = centros.to_crs("EPSG:4326").copy()
        if len(cc) > 300:
            cc = cc.sample(300, random_state=42)

        cc_fg = folium.FeatureGroup(name="Populated centres (sample)", show=False)
        for _, row in cc.iterrows():
            if row.geometry is None:
                continue
            pop = row.get("poblacion", "?")
            dist_km = (
                f"{row['dist_nearest_m'] / 1000:.1f} km"
                if "dist_nearest_m" in cc.columns and pd.notna(row.get("dist_nearest_m"))
                else "N/A"
            )
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=3,
                color="#2980B9",
                fill=True,
                fill_color="#2980B9",
                fill_opacity=0.6,
                weight=0.5,
                tooltip=(
                    f"<b>{row.get('nombre_centro_poblado', 'Centro')}</b><br>"
                    f"Population: {pop}<br>"
                    f"Dist. nearest IPRESS: {dist_km}"
                ),
            ).add_to(cc_fg)
        cc_fg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    out = FIGURES / "interactive_map.html"
    m.save(str(out))
    print(f"  Saved → {out.name}  ({out.stat().st_size:,} bytes)")
    saved.append(out)
    return saved


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_visualization_pipeline(df: pd.DataFrame | None = None) -> list[Path]:
    """
    Generate all static figures and geospatial maps.

    Parameters
    ----------
    df : DataFrame, optional
        Pre-loaded district_metrics table. Loaded from disk if None.

    Returns
    -------
    List of Path objects for every saved figure / map.
    """
    print("=== Visualization Pipeline ===\n")
    df = load_metrics(df)
    print(f"  Loaded metrics: {len(df):,} districts\n")

    all_saved: list[Path] = []

    print("[Q1] Facility and emergency availability …")
    all_saved += plot_q1_facility_availability(df)

    print("\n[Q2] Spatial access (distance to nearest facility) …")
    all_saved += plot_q2_spatial_access(df)

    print("\n[Q3] Combined underservice index …")
    all_saved += plot_q3_underservice(df)

    print("\n[Q4] Sensitivity: baseline vs alternative …")
    all_saved += plot_q4_sensitivity(df)

    print("\n[Geo] Loading geodata …")
    gdf = load_geodata(df)

    if gdf is not None:
        print("[Geo] Static choropleth maps …")
        all_saved += plot_choropleth_maps(gdf, df)

        print("[Geo] Interactive Folium map …")
        centros_path = PROCESSED / "centros_nearest_facility.gpkg"
        centros = gpd.read_file(centros_path) if centros_path.exists() else None
        all_saved += plot_folium_interactive(gdf, centros)

    print(f"\n=== Done — {len(all_saved)} outputs saved to {FIGURES} ===")
    return all_saved


if __name__ == "__main__":
    run_visualization_pipeline()
