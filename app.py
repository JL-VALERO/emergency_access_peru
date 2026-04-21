"""
Emergency Healthcare Access Inequality in Peru — Streamlit application (4 tabs)
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
    df = pd.read_csv(path)
    df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
    return df


@st.cache_data
def load_geodata() -> gpd.GeoDataFrame | None:
    path = PROCESSED / "districts_summary.gpkg"
    if not path.exists():
        return None
    gdf = gpd.read_file(path)
    df  = load_metrics()
    if not df.empty and "ubigeo" in df.columns:
        gdf["ubigeo"] = gdf["ubigeo"].astype(str).str.zfill(6)
        extra = [c for c in df.columns if c not in gdf.columns or c == "ubigeo"]
        gdf = gdf.merge(df[extra], on="ubigeo", how="left")
    return gdf


def _img(name: str, caption: str = ""):
    path = FIGURES / name
    if path.exists():
        st.image(str(path), use_container_width=True, caption=caption or None)
    else:
        st.warning(f"Figure not found: {name}")


def _why(text: str):
    """Render a 'why this visual' callout."""
    st.caption(f"**Why this chart?** {text}")


# ---------------------------------------------------------------------------
# Global header
# ---------------------------------------------------------------------------
st.title("🏥 Emergency Healthcare Access Inequality in Peru")
st.caption(
    "District-level analysis combining MINSA IPRESS, SUSALUD RENIPRESS, "
    "emergency production data, and IGN populated-centre locations — 1 873 districts, 25 departments."
)

tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Data & Methodology",
    "📊 Static Analysis",
    "🗺️ Geospatial Results",
    "🌐 Interactive Exploration",
])

# ===========================================================================
# TAB 1 — Data & Methodology
# ===========================================================================
with tab1:
    st.header("Data & Methodology")

    # --- Problem statement ---
    st.subheader("Problem statement")
    st.markdown(
        """
        Peru's emergency healthcare system covers a vast and geographically fragmented
        territory spanning coastal desert, the Andes, and the Amazon basin. While national
        statistics suggest adequate numbers of registered health facilities overall, these
        figures mask extreme within-country inequality: a district in metropolitan Lima may
        have dozens of IPRESS within a few kilometres, while a rural district in Amazonas or
        Puno may have none within a 50 km radius.

        This inequality is not merely inconvenient — in emergency medicine, distance is a
        determinant of survival. Patients requiring emergency care in underserved districts
        face travel times that make timely treatment impossible for time-sensitive conditions
        such as obstetric emergencies, trauma, and acute cardiovascular events.

        This project provides a **reproducible, district-level evidence base** for identifying
        which of Peru's 1 873 districts are most underserved in emergency healthcare access,
        combining three complementary dimensions: facility availability, emergency service
        utilisation, and physical distance from populated centres to the nearest registered
        facility. The results are intended to support resource-allocation decisions by health
        planners and policymakers.
        """
    )

    st.divider()

    # --- Data sources ---
    st.subheader("Data sources")
    sources = [
        {
            "Dataset": "Centros Poblados (IGN)",
            "Provider": "datosabiertos.gob.pe",
            "Format": "Shapefile",
            "Role in analysis": "Origin layer for nearest-facility distance calculation",
            "Auto-download": "⚠ Manual",
        },
        {
            "Dataset": "DISTRITOS boundaries",
            "Provider": "d2cml-ai/Data-Science-Python (GitHub)",
            "Format": "Shapefile",
            "Role in analysis": "Polygon framework for all spatial joins and maps",
            "Auto-download": "✅ Auto",
        },
        {
            "Dataset": "IPRESS MINSA",
            "Provider": "datosabiertos.gob.pe",
            "Format": "CSV",
            "Role in analysis": "MINSA-registered facility locations for density & distance",
            "Auto-download": "⚠ Manual",
        },
        {
            "Dataset": "RENIPRESS SUSALUD",
            "Provider": "datosabiertos.gob.pe",
            "Format": "CSV",
            "Role in analysis": "Broader national registry — preferred target for nearest-facility search",
            "Auto-download": "⚠ Manual",
        },
        {
            "Dataset": "Emergencias C1 SUSALUD",
            "Provider": "datos.susalud.gob.pe",
            "Format": "CSV",
            "Role in analysis": "Emergency care production per IPRESS — district volume metric",
            "Auto-download": "⚠ Manual",
        },
    ]
    st.dataframe(pd.DataFrame(sources), use_container_width=True, hide_index=True)
    st.caption(
        "⚠ Manual = portal returned HTTP 418 during automated download. "
        "Place files in `data/raw/` before running the pipeline."
    )

    st.divider()

    # --- Data cleaning decisions ---
    st.subheader("Data cleaning decisions")
    st.markdown(
        "All cleaning logic lives in `src/cleaning.py`. "
        "Each decision below documents both the action taken and the reason for it."
    )

    cleaning_decisions = [
        (
            "Column name standardisation",
            "Every column name is converted to **snake_case** (spaces → underscores, "
            "non-word characters stripped, camelCase split). This normalises inconsistencies "
            "across dataset vintages, e.g. `NOMBDIST`, `NombDist`, and `Nombre Distrito` all "
            "become `nombre_distrito`."
        ),
        (
            "CRS harmonisation to EPSG:4326",
            "All spatial layers are re-projected to **WGS 84 (EPSG:4326)** before saving. "
            "If a layer arrives with no CRS declaration, EPSG:4326 is assumed and a warning "
            "is printed. This ensures a single consistent CRS across all joins and maps."
        ),
        (
            "Metric CRS for distance calculations",
            "Nearest-facility distances are computed after temporarily re-projecting both "
            "layers to **UTM Zone 18S (EPSG:32718)**. This is the standard metric CRS for "
            "most of Peru and returns distances in metres rather than decimal degrees."
        ),
        (
            "Invalid geometry removal",
            "Rows with `null` or empty geometries are dropped before any spatial operation. "
            "The count of dropped rows is printed. In practice this affects < 0.1 % of "
            "records in each layer."
        ),
        (
            "Ubigeo zero-padding to 6 characters",
            "The district code `ubigeo` is stored as a **6-character zero-padded string** "
            "(e.g. `'010101'`) across all layers. Sources deliver it as integers, plain "
            "strings, or mixed-length strings; all are coerced with `str.zfill(6)`. "
            "This is the join key across every dataset."
        ),
        (
            "Numeric coercion with NaN on failure",
            "Coordinate and count columns that arrive as strings are coerced with "
            "`pd.to_numeric(errors='coerce')`. Unparseable values become NaN rather than "
            "raising an error. Downstream metric functions handle NaN explicitly."
        ),
        (
            "Critical-field filtering",
            "IPRESS rows with a null `nombre_ipress` are dropped (a facility without a name "
            "has no usable anchor). Emergencias rows where both `codigo_ipress` and "
            "`nombre_ipress` are null are dropped (unattributable to any district)."
        ),
    ]

    for title, detail in cleaning_decisions:
        with st.expander(f"🔧 {title}"):
            st.markdown(detail)

    st.divider()

    # --- Methodological decisions & pipeline ---
    st.subheader("Methodological decisions")
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Metric A — Facility density**")
        st.markdown(
            "- *Baseline*: facilities per 100 km² (area-based)\n"
            "- *Alternative*: facilities per 10 000 population\n"
            "- Area recomputed in UTM 18S to avoid WGS 84 degree distortions"
        )
        st.markdown("**Metric B — Emergency activity**")
        st.markdown(
            "- *Baseline*: total emergency episodes per district\n"
            "- *Alternative*: emergencies per active facility (load proxy)"
        )
        st.markdown("**Metric C — Spatial access**")
        st.markdown(
            "- *Baseline*: mean distance (m) to nearest IPRESS\n"
            "- *Alternative*: population-weighted mean distance\n"
            "- Threshold for 'far': 10 km"
        )

    with col_b:
        st.markdown("**Metric D — Composite underservice index**")
        st.markdown(
            "Each component is min-max normalised and **inverted** so that "
            "**1 = most underserved** throughout. The composite score is the "
            "equal-weight mean of all available component scores."
        )
        st.markdown("**Baseline vs alternative specifications**")
        st.dataframe(
            pd.DataFrame({
                "Dimension":        ["Facility denominator", "Index normalisation", "Distance summary"],
                "Baseline":         ["Area (km²)",           "Min-max",             "Mean"],
                "Alternative":      ["Population",           "Rank-based percentile","Pop-weighted mean"],
            }),
            hide_index=True,
            use_container_width=True,
        )

    st.divider()

    # --- Limitations ---
    st.subheader("Limitations")
    limitations = [
        ("Incomplete spatial access data",
         "1 442 of 1 873 districts have no Centros Poblados records in the current run "
         "(manual download required). Distance metrics are NaN for those districts; the "
         "composite index for them is driven by facility density and emergency volume only."),
        ("Government portal availability",
         "Three datasets could not be retrieved automatically (HTTP 418). They must be "
         "downloaded manually via a browser. The pipeline handles their absence with "
         "warnings rather than hard failures."),
        ("Emergency data reflects reporting coverage, not true demand",
         "Emergencias C1 counts only episodes at registered SUSALUD facilities. "
         "Informal and community-level care is absent. Low volumes may signal low "
         "reporting coverage rather than low need."),
        ("Equal-weight composite assumption",
         "The baseline index weights density, activity, and access equally. "
         "A public health expert might weight spatial access more heavily in remote "
         "Amazonian districts. The alternative specification partly addresses this."),
        ("Cross-period emergency aggregation",
         "All reporting periods are summed into a single district total. "
         "Changes in facility registration across periods conflate volume changes "
         "with registry changes."),
    ]
    for title, detail in limitations:
        with st.expander(f"⚠ {title}"):
            st.markdown(detail)

    # --- Dataset at a glance ---
    df_kpi = load_metrics()
    if not df_kpi.empty:
        st.divider()
        st.subheader("Dataset at a glance")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Districts", f"{len(df_kpi):,}")
        c2.metric("Departments", f"{df_kpi['departamen'].nunique()}")
        n_with = int((df_kpi["n_ipress_minsa"] > 0).sum()) if "n_ipress_minsa" in df_kpi.columns else "—"
        c3.metric("Districts with ≥1 IPRESS", n_with)
        med = f"{df_kpi['baseline_index'].median():.3f}" if "baseline_index" in df_kpi.columns else "—"
        c4.metric("Median underservice index", med)


# ===========================================================================
# TAB 2 — Static Analysis
# ===========================================================================
with tab2:
    st.header("Static Analysis")
    st.markdown(
        "Charts are organised by the four research questions. "
        "Each figure is accompanied by an interpretation of its findings "
        "and an explanation of why that chart type was chosen."
    )

    # --- Q1 ---
    st.subheader("Q1 — Facility and emergency care availability")

    st.markdown(
        "The two histograms below show how IPRESS and RENIPRESS facility density "
        "(per 100 km²) is distributed across all 1 873 districts. "
        "The strong right skew confirms that a small number of urban districts "
        "concentrate most facilities while the majority of districts have near-zero density. "
        "The dashed line marks the national median."
    )
    _why(
        "A **histogram with a median reference line** was chosen over a bar chart or box plot "
        "because the goal is to show the *shape* of the distribution — specifically the "
        "degree of skewness — across all 1 873 districts at once. A bar chart would "
        "require aggregating to department level and would obscure within-department "
        "heterogeneity. A box plot was reserved for the emergency-volume comparison where "
        "group-level spread is the focus."
    )
    _img("q1a_density_distribution.png")

    st.markdown(
        "Ranking departments by their median district-level facility density shows which "
        "regions are systematically under-resourced. Remote Amazonian and high-Andean "
        "departments cluster at the bottom; Lima and coastal departments at the top. "
        "Districts below the national median are highlighted in red."
    )
    _why(
        "A **horizontal bar chart sorted by value** makes it easy to read department "
        "names alongside their scores and immediately identifies the top and bottom "
        "performers. Horizontal orientation was chosen over vertical because department "
        "names are long and would overlap on a vertical axis. Colour coding (red for "
        "bottom quartile) adds a second visual encoding without extra ink."
    )
    _img("q1b_dept_facility_ranking.png")

    st.markdown(
        "Emergency volume varies enormously within departments: a single large urban "
        "IPRESS often accounts for most of a department's recorded emergency activity, "
        "while smaller district facilities record near-zero volumes. High within-department "
        "variance indicates that simple department-level averages would be misleading."
    )
    _why(
        "A **box plot grouped by department** was chosen because the key finding is "
        "within-department *spread*, not just the central tendency. A bar chart of means "
        "would suppress this. The top-12 departments were selected to keep the chart "
        "readable; choosing by total volume ensures the highest-impact departments are shown."
    )
    _img("q1c_emergency_volume_dept.png")

    st.divider()

    # --- Q2 ---
    st.subheader("Q2 — Populated-centre access to emergency services")

    st.markdown(
        "The distance distribution below shows that the median district has populated "
        "centres on average more than 10 km from the nearest IPRESS — the standard "
        "policy threshold for 'acceptable' access. A substantial share of districts "
        "exceeds this threshold for most of their populated centres."
    )
    _why(
        "A **histogram with threshold reference lines** communicates both the distribution "
        "shape and the proportion of districts failing the policy standard in a single "
        "panel. An ECDF would show the same but is less intuitive for a policy audience."
    )
    _img("q2a_distance_distribution.png")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            "Departments with the highest share of populated centres more than 10 km from "
            "any IPRESS are predominantly Andean and Amazonian, consistent with their "
            "dispersed settlement patterns and limited road infrastructure."
        )
        _why(
            "A **ranked horizontal bar chart** (top 15 departments) focuses attention on the "
            "worst-performing regions without cluttering the view with all 25 departments. "
            "Red highlighting on the top quartile draws attention to the most urgent cases."
        )
        _img("q2b_pct_far_centres_dept.png")
    with col2:
        st.markdown(
            "Smaller, rural districts tend to have both lower population and longer distances "
            "to the nearest facility, confirming the urban–rural access gap. The log scale "
            "on the population axis prevents Lima districts from dominating the visual space."
        )
        _why(
            "A **log-scale scatter plot** was chosen because both population and distance "
            "span several orders of magnitude. A linear scale would compress the majority "
            "of districts into the lower-left corner, making the rural pattern invisible. "
            "The 10 km reference line anchors the viewer to the policy threshold."
        )
        _img("q2c_distance_vs_population.png")

    st.divider()

    # --- Q3 ---
    st.subheader("Q3 — Combined underservice index")

    st.markdown(
        "The composite underservice index combines facility density, emergency volume, "
        "and spatial access into a single [0–1] score where 1 = most underserved. "
        "The top 20 districts are shown with their exact scores."
    )
    _why(
        "A **horizontal bar chart of the top 20** was chosen over a full table or "
        "choropleth for this panel because it clearly ranks named districts while "
        "keeping the focus on relative magnitude rather than geography. The exact score "
        "labels on each bar add precision without requiring a separate table."
    )
    _img("q3a_top20_underserved.png")

    col3, col4 = st.columns(2)
    with col3:
        st.markdown(
            "Box plots of the baseline index grouped by the 15 worst-scoring departments. "
            "Wide interquartile ranges signal high within-department inequality — neighbouring "
            "districts may have very different levels of underservice."
        )
        _why(
            "**Box plots grouped by department** were chosen here (in contrast to the bar chart "
            "above) because the analytical question shifts from 'which district?' to "
            "'how much inequality exists *within* each department?'. The box plot's "
            "IQR directly answers that question; a bar chart of means would not."
        )
        _img("q3b_underservice_by_dept.png")
    with col4:
        st.markdown(
            "Spearman rank correlations between all component metrics. Strong negative "
            "correlations between density metrics and distance metrics confirm the "
            "index construction: districts with fewer facilities per km² are exactly "
            "those where populated centres must travel further to reach any IPRESS."
        )
        _why(
            "A **lower-triangle heatmap with annotations** compactly shows all pairwise "
            "relationships among seven metrics in a single panel. A scatter-plot matrix "
            "would show the same information but at 7× the space. Spearman correlation "
            "was used because several metrics have strongly skewed distributions."
        )
        _img("q3c_metric_correlations.png")

    st.divider()

    # --- Q4 ---
    st.subheader("Q4 — Sensitivity of results to methodological choices")

    st.markdown(
        "Each district is plotted as a point: x = baseline index, y = alternative index. "
        "Points near the diagonal indicate agreement between specifications. "
        "The Spearman ρ at the top of the chart quantifies overall rank agreement."
    )
    _why(
        "A **scatter plot with a diagonal reference line** is the standard diagnostic for "
        "comparing two ordinal scales. It simultaneously shows overall correlation "
        "(point cloud tightness), systematic bias (cloud above/below the diagonal), "
        "and outliers (points far from the diagonal). A simple correlation coefficient "
        "alone would miss the outlier pattern."
    )
    _img("q4a_baseline_vs_alternative.png")


# ===========================================================================
# TAB 3 — Geospatial Results
# ===========================================================================
with tab3:
    st.header("Geospatial Results")

    # --- Static choropleths ---
    st.subheader("District-level choropleth maps")
    st.markdown(
        "Four metrics mapped simultaneously across all 1 873 Peruvian districts. "
        "Light grey = no data available. Log colour scales applied to emergency volume "
        "and distance metrics to reduce the dominance of Lima-area extreme values."
    )
    _img("geo_choropleths.png")

    st.divider()

    # --- Supporting tables ---
    st.subheader("Supporting tables")

    df = load_metrics()
    if not df.empty:
        col_top, col_bot = st.columns(2)

        with col_top:
            st.markdown("**10 most underserved districts** (highest baseline index)")
            top10 = df.nlargest(10, "baseline_index")[
                ["distrito", "departamen", "baseline_index",
                 "density_ipress_per100km2", "mean_dist_nearest_m"]
            ].reset_index(drop=True)
            top10.index += 1
            st.dataframe(
                top10.style.format({
                    "baseline_index": "{:.3f}",
                    "density_ipress_per100km2": "{:.3f}",
                    "mean_dist_nearest_m": "{:,.0f}",
                }),
                use_container_width=True,
            )

        with col_bot:
            st.markdown("**10 best-served districts** (lowest baseline index)")
            bot10 = df.nsmallest(10, "baseline_index")[
                ["distrito", "departamen", "baseline_index",
                 "density_ipress_per100km2", "mean_dist_nearest_m"]
            ].reset_index(drop=True)
            bot10.index += 1
            st.dataframe(
                bot10.style.format({
                    "baseline_index": "{:.3f}",
                    "density_ipress_per100km2": "{:.3f}",
                    "mean_dist_nearest_m": "{:,.0f}",
                }),
                use_container_width=True,
            )

        st.divider()

        # Department summary table
        st.markdown("**Department-level summary** (median across districts)")
        dept_summary = (
            df.groupby("departamen")
            .agg(
                n_districts=("ubigeo", "count"),
                median_baseline_index=("baseline_index", "median"),
                median_density_ipress=("density_ipress_per100km2", "median"),
                median_dist_nearest_m=("mean_dist_nearest_m", "median"),
                total_emergencias=("total_emergencias", "sum"),
            )
            .sort_values("median_baseline_index", ascending=False)
            .reset_index()
        )
        st.dataframe(
            dept_summary.style.format({
                "median_baseline_index": "{:.3f}",
                "median_density_ipress": "{:.3f}",
                "median_dist_nearest_m": "{:,.0f}",
                "total_emergencias": "{:,.0f}",
            }),
            use_container_width=True,
            height=350,
        )
    else:
        st.info("Run the metrics pipeline first to populate these tables.")

    st.divider()

    # --- Q4 sensitivity maps ---
    st.subheader("Q4 — Specification sensitivity")
    st.markdown(
        "The rank-change slope chart (left) tracks each top-20 district under the baseline "
        "and shows where it lands under the alternative. Red lines = rank shift > 5. "
        "Instability is typically driven by districts that score high on area-based density "
        "but low on population-based density, or vice-versa."
    )
    col_l, col_r = st.columns(2)
    with col_l:
        _img("q4b_rank_change.png")
    with col_r:
        st.markdown(
            "At the department level both specifications agree closely (ρ ≈ 1). "
            "The identification of the most underserved departments is not sensitive "
            "to whether equal-weight or rank-normalised aggregation is used."
        )
        _img("q4c_dept_agreement.png")

    st.divider()

    # --- Multi-district comparison ---
    st.subheader("District-level comparison")
    st.markdown(
        "Select individual districts to compare their metric profiles side-by-side. "
        "Use the department filter to narrow the list first."
    )

    if not df.empty:
        dept_opts = ["All"] + sorted(df["departamen"].dropna().unique().tolist())
        sel_dept_t3 = st.selectbox("Filter by department", dept_opts, key="t3_dept")
        pool = df if sel_dept_t3 == "All" else df[df["departamen"] == sel_dept_t3]

        dist_opts = sorted(pool["distrito"].dropna().unique().tolist())
        sel_dists = st.multiselect(
            "Select districts to compare (2–8 recommended)",
            dist_opts,
            default=dist_opts[:3] if len(dist_opts) >= 3 else dist_opts,
            key="t3_multi",
        )

        if sel_dists:
            compare_cols = [
                "departamen", "distrito",
                "n_ipress_minsa", "n_renipress_susalud",
                "density_ipress_per100km2", "density_renipress_per100km2",
                "total_emergencias", "emergencias_per_facility",
                "mean_dist_nearest_m", "pct_centres_far",
                "baseline_index", "baseline_index_pct",
                "alternative_index", "alternative_index_pct",
            ]
            present = [c for c in compare_cols if c in df.columns]
            compare_df = (
                pool[pool["distrito"].isin(sel_dists)][present]
                .set_index("distrito")
            )
            st.dataframe(
                compare_df.style.format({
                    "density_ipress_per100km2":    "{:.3f}",
                    "density_renipress_per100km2": "{:.3f}",
                    "total_emergencias":           "{:,.0f}",
                    "emergencias_per_facility":    "{:,.1f}",
                    "mean_dist_nearest_m":         "{:,.0f}",
                    "pct_centres_far":             "{:.1f}",
                    "baseline_index":              "{:.4f}",
                    "baseline_index_pct":          "{:.1f}",
                    "alternative_index":           "{:.4f}",
                    "alternative_index_pct":       "{:.1f}",
                }).background_gradient(
                    subset=["baseline_index"],
                    cmap="Reds",
                ),
                use_container_width=True,
            )
        else:
            st.info("Select at least one district above.")
    else:
        st.info("Run the metrics pipeline first.")


# ===========================================================================
# TAB 4 — Interactive Exploration
# ===========================================================================
with tab4:
    st.header("Interactive Exploration")

    # --- Folium map ---
    st.subheader("Interactive district map")
    st.markdown(
        "Use the **layer control** (top-right corner of the map) to switch between "
        "four metric layers: underservice index, IPRESS density, mean distance to nearest "
        "facility, and total emergencies. **Hover** over any district to see its name, "
        "ubigeo, facility counts, and index scores. Zoom and pan freely."
    )

    html_path = FIGURES / "interactive_map.html"
    if html_path.exists():
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        components.html(html_content, height=600, scrolling=False)
        st.caption(
            "Map layers (toggle in top-right): Underservice index · IPRESS density · "
            "Mean distance to nearest IPRESS · Total emergencies. "
            "Populated-centre dots available as an optional layer."
        )
    else:
        st.warning(
            "Interactive map not found. "
            "Run `python -m src.visualization` to generate it."
        )

    st.divider()

    df = load_metrics()
    if not df.empty:

        # --- Multi-district comparison view ---
        st.subheader("Multi-district comparison")
        st.markdown(
            "Find a district on the map above, then look it up here for a full "
            "metric breakdown. Select multiple districts to compare them side-by-side "
            "across all access dimensions."
        )

        dept_opts_t4 = ["All"] + sorted(df["departamen"].dropna().unique().tolist())
        sel_dept_multi = st.selectbox("Filter by department", dept_opts_t4, key="t4_dept_multi")
        pool_multi = df if sel_dept_multi == "All" else df[df["departamen"] == sel_dept_multi]

        all_dists_multi = sorted(pool_multi["distrito"].dropna().unique().tolist())
        sel_multi = st.multiselect(
            "Select districts (use the map above to identify districts of interest)",
            all_dists_multi,
            default=all_dists_multi[:2] if len(all_dists_multi) >= 2 else all_dists_multi,
            key="t4_multi",
        )

        if sel_multi:
            multi_cols = [
                "departamen", "distrito",
                "n_ipress_minsa", "n_renipress_susalud",
                "density_ipress_per100km2", "density_ipress_per10kpop",
                "total_emergencias", "emergencias_per_facility",
                "mean_dist_nearest_m", "p75_dist_nearest_m", "pct_centres_far",
                "baseline_index", "baseline_index_pct",
                "alternative_index", "alternative_index_pct",
            ]
            pres = [c for c in multi_cols if c in df.columns]
            multi_df = (
                pool_multi[pool_multi["distrito"].isin(sel_multi)][pres]
                .set_index("distrito")
            )
            st.dataframe(
                multi_df.style.format({
                    "density_ipress_per100km2":  "{:.3f}",
                    "density_ipress_per10kpop":  "{:.3f}",
                    "total_emergencias":         "{:,.0f}",
                    "emergencias_per_facility":  "{:,.1f}",
                    "mean_dist_nearest_m":       "{:,.0f}",
                    "p75_dist_nearest_m":        "{:,.0f}",
                    "pct_centres_far":           "{:.1f}",
                    "baseline_index":            "{:.4f}",
                    "baseline_index_pct":        "{:.1f}",
                    "alternative_index":         "{:.4f}",
                    "alternative_index_pct":     "{:.1f}",
                }).background_gradient(subset=["baseline_index"], cmap="Reds"),
                use_container_width=True,
            )

        st.divider()

        # --- Baseline vs alternative scenario comparison ---
        st.subheader("Baseline vs alternative scenario comparison")
        st.markdown(
            "Select a department to compare how each of its districts ranks under the "
            "**baseline** (equal-weight, area-based density) vs the **alternative** "
            "(rank-normalised, population-based density) underservice specification. "
            "Differences reveal which districts are sensitive to the methodological choice."
        )

        dept_opts_t4b = sorted(df["departamen"].dropna().unique().tolist())
        sel_dept_t4 = st.selectbox("Department", dept_opts_t4b, key="t4_dept_scen")
        sub = df[df["departamen"] == sel_dept_t4].copy()

        if not sub.empty:
            c_base, c_alt = st.columns(2)

            with c_base:
                st.markdown("**Baseline ranking**  \n*(equal-weight, area-based density)*")
                base_v = (
                    sub[["distrito", "baseline_index", "baseline_index_pct"]]
                    .sort_values("baseline_index", ascending=False)
                    .reset_index(drop=True)
                )
                base_v.index += 1
                st.dataframe(
                    base_v.style.format({
                        "baseline_index": "{:.4f}",
                        "baseline_index_pct": "{:.1f}",
                    }),
                    use_container_width=True, height=320,
                )

            with c_alt:
                st.markdown("**Alternative ranking**  \n*(rank-normalised, pop-based density)*")
                alt_v = (
                    sub[["distrito", "alternative_index", "alternative_index_pct"]]
                    .sort_values("alternative_index", ascending=False)
                    .reset_index(drop=True)
                )
                alt_v.index += 1
                st.dataframe(
                    alt_v.style.format({
                        "alternative_index": "{:.4f}",
                        "alternative_index_pct": "{:.1f}",
                    }),
                    use_container_width=True, height=320,
                )

            # Rank-shift summary
            sub["rank_b"] = sub["baseline_index"].rank(ascending=False, method="min").astype(int)
            sub["rank_a"] = sub["alternative_index"].rank(ascending=False, method="min").astype(int)
            sub["rank_shift"] = (sub["rank_b"] - sub["rank_a"]).abs()

            st.markdown("**Rank-shift summary for this department**")
            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Districts with shift > 5", int((sub["rank_shift"] > 5).sum()))
            mc2.metric("Maximum rank shift", int(sub["rank_shift"].max()))
            mc3.metric("Most changed district", str(sub.loc[sub["rank_shift"].idxmax(), "distrito"])[:25])

            # Rank-shift detail table
            with st.expander("Show full rank-shift detail"):
                shift_tbl = (
                    sub[["distrito", "rank_b", "rank_a", "rank_shift",
                          "baseline_index", "alternative_index"]]
                    .sort_values("rank_shift", ascending=False)
                    .reset_index(drop=True)
                )
                shift_tbl.columns = [
                    "District", "Rank (baseline)", "Rank (alternative)",
                    "Shift", "Baseline index", "Alternative index",
                ]
                shift_tbl.index += 1
                st.dataframe(
                    shift_tbl.style.format({
                        "Baseline index": "{:.4f}",
                        "Alternative index": "{:.4f}",
                    }),
                    use_container_width=True,
                )

            st.divider()

            # --- Per-district deep-dive ---
            st.subheader("Per-district metric deep-dive")
            st.markdown(
                "Select a district to see every metric in detail. "
                "Identify districts of interest on the Folium map above, "
                "then look them up here."
            )
            dist_opts_t4 = sorted(sub["distrito"].dropna().unique().tolist())
            sel_dist = st.selectbox("District", dist_opts_t4, key="t4_dist_dive")
            row = sub[sub["distrito"] == sel_dist].iloc[0]

            metric_labels = {
                "n_ipress_minsa":              ("Facility counts",       "IPRESS MINSA count"),
                "n_renipress_susalud":         ("Facility counts",       "RENIPRESS SUSALUD count"),
                "density_ipress_per100km2":    ("Metric A — Density",    "IPRESS per 100 km² (baseline)"),
                "density_renipress_per100km2": ("Metric A — Density",    "RENIPRESS per 100 km²"),
                "density_ipress_per10kpop":    ("Metric A — Density",    "IPRESS per 10 000 pop (alternative)"),
                "total_emergencias":           ("Metric B — Activity",   "Total emergencies (baseline)"),
                "emergencias_per_facility":    ("Metric B — Activity",   "Emergencies per facility (alternative)"),
                "mean_dist_nearest_m":         ("Metric C — Access",     "Mean dist. to nearest IPRESS, m (baseline)"),
                "p75_dist_nearest_m":          ("Metric C — Access",     "P75 dist. to nearest IPRESS, m"),
                "pct_centres_far":             ("Metric C — Access",     "% centres > 10 km from IPRESS"),
                "wmean_dist_nearest_m":        ("Metric C — Access",     "Pop-weighted mean dist., m (alternative)"),
                "baseline_index":              ("Composite index",        "Baseline underservice index [0–1]"),
                "alternative_index":           ("Composite index",        "Alternative underservice index [0–1]"),
                "baseline_index_pct":          ("Composite index",        "Baseline percentile rank (100 = worst)"),
                "alternative_index_pct":       ("Composite index",        "Alternative percentile rank (100 = worst)"),
            }

            records = []
            for col, (group, label) in metric_labels.items():
                if col in row.index:
                    val = row[col]
                    records.append({
                        "Group": group,
                        "Metric": label,
                        "Value": f"{val:,.3f}" if pd.notna(val) else "—",
                    })

            deep_df = pd.DataFrame(records)
            st.dataframe(deep_df, use_container_width=True, hide_index=True)

    else:
        st.info("Run the metrics pipeline first to populate this section.")
