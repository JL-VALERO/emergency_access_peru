"""
Emergency Healthcare Access Inequality in Peru
Streamlit application — 4 tabs
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT      = Path(__file__).resolve().parent
FIGURES   = ROOT / "output" / "figures"
TABLES    = ROOT / "output" / "tables"
PROCESSED = ROOT / "data" / "processed"

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Emergency Access in Peru",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Cached loaders
# ---------------------------------------------------------------------------
@st.cache_data
def load_metrics() -> pd.DataFrame:
    path = TABLES / "district_metrics.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


@st.cache_data
def load_geodata() -> gpd.GeoDataFrame | None:
    path = PROCESSED / "districts_summary.gpkg"
    if not path.exists():
        return None
    gdf = gpd.read_file(path)
    df  = load_metrics()
    if not df.empty and "ubigeo" in df.columns:
        gdf["ubigeo"] = gdf["ubigeo"].astype(str).str.zfill(6)
        df["ubigeo"]  = df["ubigeo"].astype(str).str.zfill(6)
        extra = [c for c in df.columns if c not in gdf.columns or c == "ubigeo"]
        gdf = gdf.merge(df[extra], on="ubigeo", how="left")
    return gdf


def _fig(name: str) -> Path:
    return FIGURES / name


def _img(name: str):
    path = _fig(name)
    if path.exists():
        st.image(str(path), use_container_width=True)
    else:
        st.warning(f"Figure not found: {name}")


# ---------------------------------------------------------------------------
# Global header
# ---------------------------------------------------------------------------
st.title("🏥 Emergency Healthcare Access Inequality in Peru")
st.caption(
    "District-level analysis combining MINSA IPRESS, SUSALUD RENIPRESS, "
    "emergency production data, and IGN populated-centre locations."
)

tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Methodology & Data",
    "📊 Static Analysis",
    "🗺️ Geospatial Results",
    "🌐 Interactive Exploration",
])

# ===========================================================================
# TAB 1 — Methodology & Data
# ===========================================================================
with tab1:
    st.header("Methodology & Data Sources")

    st.subheader("Research questions")
    questions = {
        "Q1": "Which districts have lower/higher facility and emergency care availability?",
        "Q2": "Which districts show weaker populated-centre access to emergency services?",
        "Q3": "Which districts appear most/least underserved when combining all factors?",
        "Q4": "How sensitive are results to alternative methodological definitions?",
    }
    for code, text in questions.items():
        st.markdown(f"**{code}** — {text}")

    st.divider()

    st.subheader("Data sources")
    sources = [
        {
            "Dataset": "Centros Poblados (IGN)",
            "Provider": "datosabiertos.gob.pe",
            "Format": "Shapefile (.shp)",
            "Use": "Populated-centre locations for spatial access measurement",
        },
        {
            "Dataset": "DISTRITOS boundaries",
            "Provider": "d2cml-ai / Data-Science-Python repo",
            "Format": "Shapefile (.shp)",
            "Use": "District polygon layer for spatial joins and mapping",
        },
        {
            "Dataset": "IPRESS MINSA",
            "Provider": "datosabiertos.gob.pe",
            "Format": "CSV",
            "Use": "MINSA-registered health facility locations",
        },
        {
            "Dataset": "RENIPRESS SUSALUD",
            "Provider": "datosabiertos.gob.pe",
            "Format": "CSV",
            "Use": "National IPRESS registry — broader facility coverage",
        },
        {
            "Dataset": "Emergencias C1 SUSALUD",
            "Provider": "datos.susalud.gob.pe",
            "Format": "CSV",
            "Use": "Emergency care production per IPRESS (volume metric)",
        },
    ]
    st.dataframe(pd.DataFrame(sources), use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("Processing pipeline")
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Step 1 — Ingestion (`data_loader.py`)**")
        st.markdown(
            "- All five datasets downloaded programmatically\n"
            "- Raw files cached to `data/raw/`\n"
            "- Encoding and HTTP failures handled with fallback warnings"
        )
        st.markdown("**Step 2 — Cleaning (`cleaning.py`)**")
        st.markdown(
            "- Column names snake_cased uniformly\n"
            "- All spatial layers re-projected to **EPSG:4326** (WGS 84)\n"
            "- Invalid / empty geometries dropped\n"
            "- Ubigeo zero-padded to 6 characters\n"
            "- Processed outputs saved to `data/processed/` as GeoPackage / Parquet"
        )
        st.markdown("**Step 3 — Spatial joins (`geospatial.py`)**")
        st.markdown(
            "- IPRESS / RENIPRESS → district polygon join (point-in-polygon)\n"
            "- Populated centres → nearest IPRESS via `sjoin_nearest`\n"
            "  — distances computed in **UTM 18S (EPSG:32718)** for metric accuracy\n"
            "- District summary layer aggregates counts and emergency volumes"
        )

    with col_b:
        st.markdown("**Step 4 — Metrics (`metrics.py`)**")
        st.markdown(
            "- **A. Facility density**: facilities per 100 km² *(baseline)*; "
            "per 10 000 population *(alternative)*\n"
            "- **B. Emergency activity**: total emergencies; emergencies per facility\n"
            "- **C. Spatial access**: mean / P75 distance to nearest facility; "
            "share of centres > 10 km away\n"
            "- **D. Composite underservice index**: min-max normalised, "
            "inverted so **1 = most underserved**"
        )
        st.markdown("**Baseline vs alternative specifications**")
        st.markdown(
            "| Dimension | Baseline | Alternative |\n"
            "|---|---|---|\n"
            "| Facility denominator | Area (km²) | Population |\n"
            "| Index weighting | Equal weights | Rank-normalised |\n"
            "| Distance summary | Mean | Pop-weighted mean |"
        )

    st.divider()

    st.subheader("Methodological choices and limitations")
    st.markdown(
        "- Districts with **zero populated-centre matches** (1 442 / 1 873) "
        "have no spatial-access metrics; this reflects missing Centros Poblados data "
        "rather than genuine absence of population.\n"
        "- RENIPRESS SUSALUD and Emergencias C1 required **manual download** "
        "(government portals returned HTTP 418 during automated ingestion).\n"
        "- Emergency volume data combines all available periods; "
        "districts with **no IPRESS** receive `NaN` for the facility-load metric.\n"
        "- The composite index treats all component metrics as equally informative "
        "in the baseline specification — a strong assumption relaxed in the alternative."
    )

    df = load_metrics()
    if not df.empty:
        st.divider()
        st.subheader("Dataset at a glance")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Districts", f"{len(df):,}")
        c2.metric("Departments", f"{df['departamen'].nunique()}")
        dists_with_ipress = int((df["n_ipress_minsa"] > 0).sum()) if "n_ipress_minsa" in df.columns else "—"
        c3.metric("Districts with ≥1 IPRESS", dists_with_ipress)
        median_idx = f"{df['baseline_index'].median():.3f}" if "baseline_index" in df.columns else "—"
        c4.metric("Median underservice index", median_idx)


# ===========================================================================
# TAB 2 — Static Analysis
# ===========================================================================
with tab2:
    st.header("Static Analysis")

    # --- Q1 ---
    st.subheader("Q1 — Facility and emergency care availability")

    st.markdown(
        "The histograms below show how facility density (IPRESS and RENIPRESS per 100 km²) "
        "is distributed across all 1 873 districts. The skew to the right reveals that "
        "a small number of districts — typically urban centres — concentrate most facilities, "
        "while the majority of districts have near-zero density."
    )
    _img("q1a_density_distribution.png")

    st.markdown(
        "Ranking departments by median district-level facility density highlights the "
        "geographic disparities: coastal and Lima-metropolitan departments rank highest; "
        "remote Amazonian and high-Andean departments fall at the bottom."
    )
    _img("q1b_dept_facility_ranking.png")

    st.markdown(
        "Emergency volume (total care episodes per district) varies enormously across "
        "departments. High variance within departments indicates that a single large "
        "urban IPRESS often dominates emergency activity for the whole department."
    )
    _img("q1c_emergency_volume_dept.png")

    st.divider()

    # --- Q2 ---
    st.subheader("Q2 — Populated-centre access to emergency services")

    st.markdown(
        "The distance distribution (below) shows that a substantial share of populated "
        "centres are more than 10 km from any IPRESS. The median distance and the "
        "proportion beyond the 10 km threshold are the two spatial-access indicators "
        "used in the composite index."
    )
    _img("q2a_distance_distribution.png")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            "Departments with the **highest share of poorly-served centres** "
            "(> 10 km from nearest IPRESS). These are predominantly Andean and "
            "Amazonian departments with dispersed settlement patterns."
        )
        _img("q2b_pct_far_centres_dept.png")
    with col2:
        st.markdown(
            "Distance to the nearest facility vs district population (log scale). "
            "Smaller, rural districts tend to have both lower population and longer "
            "distances, confirming the urban–rural access gap."
        )
        _img("q2c_distance_vs_population.png")

    st.divider()

    # --- Q3 ---
    st.subheader("Q3 — Combined underservice index")

    st.markdown(
        "The composite underservice index combines facility density, emergency volume, "
        "and spatial access into a single [0–1] score (1 = most underserved). "
        "The 20 most underserved districts are shown below, coloured by their index score."
    )
    _img("q3a_top20_underserved.png")

    col3, col4 = st.columns(2)
    with col3:
        st.markdown(
            "Distribution of the baseline index across the 15 worst-scoring "
            "departments. Wide boxes signal high within-department inequality — "
            "some districts are well-served while neighbours are not."
        )
        _img("q3b_underservice_by_dept.png")
    with col4:
        st.markdown(
            "Spearman correlations between all component metrics. "
            "Facility density metrics are strongly negatively correlated with "
            "distance metrics (as expected), validating the index construction."
        )
        _img("q3c_metric_correlations.png")

    st.divider()

    # --- Q4 preview in this tab ---
    st.subheader("Q4 — Sensitivity preview")
    st.markdown(
        "The scatter below plots the baseline index against the alternative "
        "(rank-normalised) index at the district level. High Spearman ρ indicates "
        "the ranking of most underserved districts is robust to the weighting choice. "
        "Full sensitivity analysis is available in the **Geospatial Results** tab."
    )
    _img("q4a_baseline_vs_alternative.png")


# ===========================================================================
# TAB 3 — Geospatial Results
# ===========================================================================
with tab3:
    st.header("Geospatial Results")

    st.subheader("District-level choropleth maps")
    st.markdown(
        "Four metrics mapped simultaneously across all 1 873 Peruvian districts. "
        "Light grey = no data. Log scales applied to emergency volume and distance "
        "to reduce the influence of extreme values in Lima."
    )
    _img("geo_choropleths.png")

    st.divider()
    st.subheader("Q4 — Baseline vs alternative specification sensitivity")

    st.markdown(
        "The rank-change plot (left) tracks each of the top-20 most underserved "
        "districts under the baseline specification and shows where they land under "
        "the alternative. Red lines indicate a rank shift > 5 positions — the "
        "handful of unstable cases are typically districts that score high on "
        "area-based density but low on population-based density (or vice-versa)."
    )

    col_l, col_r = st.columns(2)
    with col_l:
        _img("q4b_rank_change.png")
    with col_r:
        st.markdown(
            "At the department level both specifications agree closely (ρ ≈ 1). "
            "This confirms that the identification of the most underserved "
            "departments is not sensitive to whether equal-weight or rank-normalised "
            "aggregation is used."
        )
        _img("q4c_dept_agreement.png")

    st.divider()
    st.subheader("District comparison table")
    df = load_metrics()
    if not df.empty:
        display_cols = [
            "departamen", "distrito", "ubigeo",
            "n_ipress_minsa", "n_renipress_susalud",
            "density_ipress_per100km2",
            "total_emergencias",
            "mean_dist_nearest_m",
            "baseline_index", "baseline_index_pct",
            "alternative_index",
        ]
        present = [c for c in display_cols if c in df.columns]

        dept_options = ["All"] + sorted(df["departamen"].dropna().unique().tolist())
        sel_dept = st.selectbox("Filter by department", dept_options, key="dept_sel")

        view = df if sel_dept == "All" else df[df["departamen"] == sel_dept]
        view = view[present].copy()

        sort_col = st.selectbox(
            "Sort by",
            [c for c in ["baseline_index", "density_ipress_per100km2",
                          "mean_dist_nearest_m", "total_emergencias"] if c in present],
            key="sort_sel",
        )
        view = view.sort_values(sort_col, ascending=False)

        # Format floats for readability
        fmt = {
            "density_ipress_per100km2": "{:.3f}",
            "total_emergencias": "{:,.0f}",
            "mean_dist_nearest_m": "{:,.0f}",
            "baseline_index": "{:.4f}",
            "baseline_index_pct": "{:.1f}",
            "alternative_index": "{:.4f}",
        }
        st.dataframe(
            view.style.format({k: v for k, v in fmt.items() if k in view.columns}),
            use_container_width=True,
            height=420,
        )
        st.caption(f"{len(view):,} districts shown")
    else:
        st.info("Run the metrics pipeline first to populate this table.")


# ===========================================================================
# TAB 4 — Interactive Exploration
# ===========================================================================
with tab4:
    st.header("Interactive Exploration")

    # --- Folium map ---
    st.subheader("Interactive district map")
    st.markdown(
        "Use the **layer control** (top-right) to switch between the four metric "
        "layers. Hover over any district to see its name, ubigeo, facility counts, "
        "and index scores. Zoom and pan as usual."
    )

    html_path = FIGURES / "interactive_map.html"
    if html_path.exists():
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        components.html(html_content, height=600, scrolling=False)
    else:
        st.warning(
            "Interactive map not found. "
            "Run `src/visualization.run_visualization_pipeline()` to generate it."
        )

    st.divider()

    # --- Scenario comparison ---
    st.subheader("Baseline vs alternative scenario comparison")
    st.markdown(
        "Select a department to compare how each district ranks under the **baseline** "
        "(equal-weight, area-based density) vs the **alternative** "
        "(rank-normalised, population-based density) underservice specification."
    )

    df = load_metrics()
    if not df.empty:
        dept_options_t4 = sorted(df["departamen"].dropna().unique().tolist())
        sel_dept_t4 = st.selectbox("Department", dept_options_t4, key="dept_t4")

        sub = df[df["departamen"] == sel_dept_t4].copy()
        sub["ubigeo"] = sub["ubigeo"].astype(str).str.zfill(6)

        if not sub.empty:
            # Side-by-side ranking tables
            c_base, c_alt = st.columns(2)

            rank_cols_base = ["distrito", "baseline_index", "baseline_index_pct"]
            rank_cols_alt  = ["distrito", "alternative_index", "alternative_index_pct"]
            present_base = [c for c in rank_cols_base if c in sub.columns]
            present_alt  = [c for c in rank_cols_alt  if c in sub.columns]

            with c_base:
                st.markdown("**Baseline ranking** (equal-weight, area-based density)")
                base_view = (
                    sub[present_base]
                    .sort_values("baseline_index", ascending=False)
                    .reset_index(drop=True)
                )
                base_view.index += 1
                st.dataframe(
                    base_view.style.format({
                        "baseline_index": "{:.4f}",
                        "baseline_index_pct": "{:.1f}",
                    }),
                    use_container_width=True,
                    height=350,
                )

            with c_alt:
                st.markdown("**Alternative ranking** (rank-normalised, pop-based density)")
                alt_view = (
                    sub[present_alt]
                    .sort_values("alternative_index", ascending=False)
                    .reset_index(drop=True)
                )
                alt_view.index += 1
                st.dataframe(
                    alt_view.style.format({
                        "alternative_index": "{:.4f}",
                        "alternative_index_pct": "{:.1f}",
                    }),
                    use_container_width=True,
                    height=350,
                )

            # Rank-change summary
            if "baseline_index" in sub.columns and "alternative_index" in sub.columns:
                sub["rank_b"] = sub["baseline_index"].rank(
                    ascending=False, method="min"
                ).astype(int)
                sub["rank_a"] = sub["alternative_index"].rank(
                    ascending=False, method="min"
                ).astype(int)
                sub["rank_shift"] = (sub["rank_b"] - sub["rank_a"]).abs()

                st.markdown("**Rank-change summary for this department**")
                mc1, mc2, mc3 = st.columns(3)
                mc1.metric(
                    "Districts with rank shift > 5",
                    int((sub["rank_shift"] > 5).sum()),
                )
                mc2.metric(
                    "Max rank shift",
                    int(sub["rank_shift"].max()),
                )
                most_changed = sub.loc[sub["rank_shift"].idxmax(), "distrito"]
                mc3.metric("Most changed district", str(most_changed)[:25])

                # Metric-level breakdown for selected district
                st.markdown("**Explore a specific district**")
                dist_options = sorted(sub["distrito"].dropna().unique().tolist())
                sel_dist = st.selectbox("District", dist_options, key="dist_t4")
                row = sub[sub["distrito"] == sel_dist].iloc[0]

                metric_labels = {
                    "density_ipress_per100km2":    "IPRESS / 100 km²",
                    "density_renipress_per100km2": "RENIPRESS / 100 km²",
                    "density_ipress_per10kpop":    "IPRESS / 10k pop",
                    "total_emergencias":           "Total emergencies",
                    "emergencias_per_facility":    "Emergencies per facility",
                    "mean_dist_nearest_m":         "Mean dist. nearest (m)",
                    "pct_centres_far":             "% centres > 10 km",
                    "baseline_index":              "Baseline underservice index",
                    "alternative_index":           "Alternative underservice index",
                    "baseline_index_pct":          "Baseline percentile rank",
                    "alternative_index_pct":       "Alternative percentile rank",
                }
                avail = {k: v for k, v in metric_labels.items() if k in row.index}
                metrics_df = pd.DataFrame({
                    "Metric": list(avail.values()),
                    "Value": [
                        f"{row[k]:,.3f}" if pd.notna(row[k]) else "—"
                        for k in avail
                    ],
                })
                st.dataframe(metrics_df, use_container_width=True, hide_index=True)
    else:
        st.info("Run the metrics pipeline first to populate this section.")
