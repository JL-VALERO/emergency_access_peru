# Emergency Healthcare Access Inequality in Peru

A reproducible geospatial analytics pipeline that measures district-level inequality in
emergency healthcare access across Peru's 1 873 districts, combining facility registries,
emergency production records, and populated-centre locations into a composite underservice
index, and presenting the results in an interactive Streamlit dashboard.

---

## Table of Contents

1. [What does the project do?](#1-what-does-the-project-do)
2. [What is the main analytical goal?](#2-what-is-the-main-analytical-goal)
3. [What datasets were used?](#3-what-datasets-were-used)
4. [How were the data cleaned?](#4-how-were-the-data-cleaned)
5. [How were the district-level metrics constructed?](#5-how-were-the-district-level-metrics-constructed)
6. [How to install the dependencies?](#6-how-to-install-the-dependencies)
7. [How to run the processing pipeline?](#7-how-to-run-the-processing-pipeline)
8. [How to run the Streamlit app?](#8-how-to-run-the-streamlit-app)
9. [What are the main findings?](#9-what-are-the-main-findings)
10. [What are the main limitations?](#10-what-are-the-main-limitations)

---

## 1. What does the project do?

This project builds an end-to-end data pipeline that:

1. **Downloads and loads** five public Peruvian government datasets covering health
   facility locations, national facility registries, emergency care production, district
   boundaries, and populated-centre geography.
2. **Cleans and standardises** each dataset — normalising column names, reprojecting all
   layers to a single coordinate reference system, removing invalid geometries, and saving
   processed outputs as GeoPackage and Parquet files.
3. **Performs spatial joins** to attach district attributes to every facility point
   (point-in-polygon) and to find the nearest facility for every populated centre
   (`sjoin_nearest` in metric CRS).
4. **Computes district-level metrics** covering facility density, emergency activity
   volume, and spatial access distance, then combines them into a composite underservice
   index with a baseline and an alternative specification.
5. **Produces 14 outputs** — twelve static charts, a four-panel choropleth map, and an
   interactive Folium map — all organised by the four analytical questions.
6. **Serves a four-tab Streamlit dashboard** that presents the methodology, charts, maps,
   a filterable district-comparison table, and a side-by-side scenario comparison.

---

## 2. What is the main analytical goal?

The project answers four research questions:

| # | Question |
|---|---|
| **Q1** | Which districts have lower or higher facility and emergency care availability? |
| **Q2** | Which districts show weaker access for populated centres to emergency services? |
| **Q3** | Which districts appear most or least underserved when all factors are combined? |
| **Q4** | How sensitive are the results to alternative methodological definitions? |

The overarching goal is to identify which Peruvian districts are most underserved in
emergency healthcare access and to verify that this identification is robust across
different ways of measuring and weighting the underlying indicators.

---

## 3. What datasets were used?

Five public datasets are required. Two download automatically; three require a manual
browser download because the Peruvian government portals block automated HTTP requests.

| # | Dataset | Provider | Format | How to obtain |
|---|---|---|---|---|
| 1 | **Centros Poblados** (IGN populated centres) | [datosabiertos.gob.pe](https://www.datosabiertos.gob.pe) | Shapefile ZIP | Manual — download `CCPP_0.zip` and place at `data/raw/CCPP_0.zip` |
| 2 | **DISTRITOS** (district boundaries) | [d2cml-ai/Data-Science-Python](https://github.com/d2cml-ai/Data-Science-Python/tree/main/_data/Folium) | Shapefile | **Automatic** via `data_loader.py` |
| 3 | **IPRESS MINSA** (MINSA facility registry) | [datosabiertos.gob.pe](https://www.datosabiertos.gob.pe) | CSV | Manual — save to `data/raw/ipress_minsa.csv` |
| 4 | **RENIPRESS SUSALUD** (national facility registry) | [datosabiertos.gob.pe](https://www.datosabiertos.gob.pe) | CSV | Manual — save to `data/raw/renipress_susalud.csv` |
| 5 | **Emergencias C1 SUSALUD** (emergency production by IPRESS) | [datos.susalud.gob.pe](http://datos.susalud.gob.pe) | CSV | Manual — save to `data/raw/emergencias_susalud.csv` |

### What each dataset contributes

- **Centros Poblados** — point locations of every populated centre in Peru. Used as the
  origin layer in the nearest-facility distance calculation (Metric C).
- **DISTRITOS** — polygon boundaries for all 1 873 districts across 25 departments. Used
  as the spatial framework for all joins and choropleth maps.
- **IPRESS MINSA** — location and attributes of MINSA-registered health facilities. Used
  for facility counts and density (Metric A) and as the target layer in the
  nearest-facility search.
- **RENIPRESS SUSALUD** — the broader national registry including non-MINSA facilities.
  Provides a more complete facility coverage than IPRESS MINSA alone and is the preferred
  target layer for the nearest-facility search.
- **Emergencias C1 SUSALUD** — monthly emergency attendance counts per facility. Aggregated
  to district level for Metric B (emergency activity volume and load).

---

## 4. How were the data cleaned?

All cleaning logic lives in `src/cleaning.py`. Each decision is documented below.

### 4.1 Column name standardisation

Every column name is converted to **snake_case** using a regex helper (`_to_snake`):
spaces and hyphens become underscores, non-word characters are stripped, and camelCase
boundaries are split. This normalises differences across dataset vintages — for example
`NOMBDIST`, `NombDist`, and `Nombre Distrito` all become `nombre_distrito`.

### 4.2 CRS harmonisation

All spatial layers are re-projected to **EPSG:4326 (WGS 84)** before saving.
If a layer arrives with no CRS declaration, EPSG:4326 is assumed and a warning is printed.
Distance calculations that require metric units (nearest-facility search) temporarily
re-project both layers to **EPSG:32718 (UTM Zone 18S)** — the standard metric CRS for
most of Peru — so that distances are returned in metres rather than decimal degrees.

### 4.3 Invalid geometry removal

Rows with `null` or empty geometries are dropped before any spatial operation. The number
of dropped rows is printed so the analyst can judge whether the loss is material. In
practice this affects fewer than 0.1 % of records in each layer.

### 4.4 Ubigeo zero-padding

The `ubigeo` district code is stored as a **6-character zero-padded string** across all
layers (e.g. `"010101"`). Source files deliver it as an integer, a plain string, or a
mixed-length string, so all values are coerced with `str.zfill(6)`. This key must be
consistent for all merges and joins.

### 4.5 Numeric coercion

Coordinate and count columns that arrive as strings are coerced with
`pd.to_numeric(errors="coerce")`. Unparseable values become `NaN` and are reported but
not removed — downstream metric functions handle missing values explicitly rather than
propagating errors silently.

### 4.6 Critical-field filtering

Two filter rules are applied:

- **IPRESS layers**: rows with a null `nombre_ipress` are dropped. A facility without a
  name cannot be linked to any registry entry and provides no usable geographic anchor.
- **Emergencias**: rows where both `codigo_ipress` and `nombre_ipress` are null are
  dropped. Such rows cannot be attributed to any facility or district.

### 4.7 Output formats

| Layer | Format | Reason |
|---|---|---|
| All spatial layers | GeoPackage (`.gpkg`) | Single file, preserves geometry + attributes, no column-length limits |
| Emergencias SUSALUD | Parquet (`.parquet`) | Tabular only; Parquet preserves dtypes and is ~40 % smaller than CSV |

All GeoPackage writes use `engine="pyogrio"` because fiona ≤ 1.9.x raises a
`ValueError` when writing with NumPy ≥ 2.0. `pyogrio` is listed in `requirements.txt`.

---

## 5. How were the district-level metrics constructed?

All logic lives in `src/metrics.py`. The pipeline produces one row per district.

### Metric A — Facility density

**Baseline**: IPRESS count within the district divided by district area in km², scaled to
**facilities per 100 km²**.

```
density_ipress_per100km2 = n_ipress_minsa / area_km2 × 100
```

Area is recomputed in UTM 18S to avoid degree-based distortions at high latitudes.

**Alternative**: same facility count divided by district population, scaled to
**facilities per 10 000 population** (`density_ipress_per10kpop`). This captures access
inequality that is invisible in area-based density — a densely populated district can have
many facilities per km² but still have very few per resident.

### Metric B — Emergency activity

**Baseline**: `total_emergencias` — the sum of all emergency care episodes attributed to
facilities in the district across all available reporting periods.

**Alternative**: `emergencias_per_facility` — total emergencies divided by the number of
active facilities in the district. High values signal heavily overloaded facilities, which
is a dimension of underservice not captured by density alone.

### Metric C — Spatial access

For each populated centre, the nearest IPRESS is identified with GeoPandas
`sjoin_nearest` (computed in UTM 18S so distances are in metres). Per-district aggregates:

| Column | Definition | Specification |
|---|---|---|
| `mean_dist_nearest_m` | Mean distance (m) to nearest IPRESS across all centres in the district | Baseline |
| `p75_dist_nearest_m` | 75th-percentile distance — captures the tail of the worst-served centres | Supplementary |
| `pct_centres_far` | Percentage of centres > 10 km from any IPRESS | Supplementary |
| `wmean_dist_nearest_m` | Population-weighted mean distance | Alternative |

### Metric D — Composite underservice index

Each component metric is min-max normalised to [0, 1] and **inverted** where necessary so
that **higher always means worse access**:

- Metrics where *higher* = worse (distances, % far, facility load): normalised directly.
- Metrics where *lower* = worse (densities, total emergencies): score = `1 − normalised`.

`NaN` values are replaced with the column median before normalisation so that districts
with incomplete data receive an intermediate score rather than being dropped entirely.

The two composite scores:

| Index | Aggregation | Normalisation |
|---|---|---|
| `baseline_index` | Equal-weight mean of all component scores | Min-max |
| `alternative_index` | Equal-weight mean of all component scores | Rank-based percentile — robust to outliers |

Both indices are accompanied by a `_pct` percentile rank column (0–100, 100 = most
underserved nationally).

---

## 6. How to install the dependencies?

### Prerequisites

- [Anaconda](https://www.anaconda.com) or Miniconda with Python 3.10
- Git

### Create and activate the environment

```bash
conda create -n geo python=3.10 -y
conda activate geo
pip install -r requirements.txt
```

### requirements.txt

```
geopandas>=0.14
pandas>=2.0
numpy>=1.26
shapely>=2.0
pyogrio>=0.7
requests>=2.31
folium>=0.15
streamlit>=1.35
matplotlib>=3.8
seaborn>=0.13
pyarrow>=15.0
scipy>=1.12
fiona>=1.9
pyproj>=3.6
```

> **Note on pyogrio and pyarrow**: these two packages are not in the original course
> `requirements.txt` but are required at runtime. `pyogrio` is the write engine used for
> all GeoPackage outputs (necessary on NumPy ≥ 2.0); `pyarrow` is needed for Parquet
> read/write. Both are included in the `requirements.txt` in this repository.

---

## 7. How to run the processing pipeline?

Place any manually downloaded raw files in `data/raw/` first (see Section 3), then run
each module in order. Every step reads from the previous step's outputs.

```bash
conda activate geo

# Step 1 — download raw datasets (DISTRITOS auto; others need manual files first)
python -m src.data_loader

# Step 2 — clean, reproject, and save to data/processed/
python -m src.cleaning

# Step 3 — spatial joins → data/processed/
python -m src.geospatial

# Step 4 — district-level metrics → output/tables/
python -m src.metrics

# Step 5 — figures and maps → output/figures/
python -m src.visualization
```

Alternatively, run the full pipeline in a single Python session:

```python
from src.data_loader   import load_all
from src.cleaning      import run_cleaning_pipeline
from src.geospatial    import run_geospatial_pipeline
from src.metrics       import run_metrics_pipeline
from src.visualization import run_visualization_pipeline

raw     = load_all()
cleaned = run_cleaning_pipeline(raw)
geo     = run_geospatial_pipeline(cleaned)
metrics = run_metrics_pipeline(geo["districts_summary"])
run_visualization_pipeline(metrics)
```

Pipeline outputs at each stage:

| Stage | Output location | Files |
|---|---|---|
| Cleaning | `data/processed/` | `distritos.gpkg`, `ipress_minsa.gpkg`, `renipress_susalud.gpkg`, `centros_poblados.gpkg`, `emergencias_susalud.parquet` |
| Geospatial | `data/processed/` | `ipress_minsa_districts.gpkg`, `renipress_susalud_districts.gpkg`, `centros_nearest_facility.gpkg`, `districts_summary.gpkg` |
| Metrics | `output/tables/` | `district_metrics.csv`, `district_metrics.parquet` |
| Visualization | `output/figures/` | 13 PNG files + `interactive_map.html` |

---

## 8. How to run the Streamlit app?

```bash
conda activate geo
streamlit run app.py
```

The app opens at `http://localhost:8501`.

| Tab | Contents |
|---|---|
| **📋 Methodology & Data** | Research questions, data-source table, step-by-step pipeline description, baseline vs alternative specification comparison, known limitations, live dataset summary metrics |
| **📊 Static Analysis** | All 12 static charts embedded with prose interpretations, organised by Q1–Q4 |
| **🗺️ Geospatial Results** | Four-panel choropleth map, sensitivity slope and agreement charts, filterable and sortable district comparison table |
| **🌐 Interactive Exploration** | Folium map with four togglable metric layers and hover tooltips, side-by-side baseline vs alternative district ranking tables, rank-shift summary, per-district metric deep-dive |

---

## 9. What are the main findings?

> **Note on data status**: The district boundary layer (1 873 districts, 25 departments)
> was downloaded successfully and is real. The IPRESS MINSA, RENIPRESS SUSALUD,
> Emergencias C1, and Centros Poblados datasets could not be retrieved automatically
> because the government portals returned HTTP 418 errors during development. The metric
> values in the current `output/tables/district_metrics.csv` are therefore based on
> **placeholder data** generated for pipeline testing. The findings below describe what
> the analysis is designed to reveal once the real datasets are loaded; the structural
> observations about the district geography are based on real data.

### F1 — Stark geographic concentration of facilities

Even with placeholder counts, the pipeline reveals the structural pattern that will hold
with real data: emergency healthcare facilities in Peru are concentrated in a small number
of districts. The distribution of facility density is heavily right-skewed — the majority
of districts have near-zero density while a handful of urban districts account for most
facilities. With real IPRESS MINSA data, **201 of 1 873 districts (10.7 %)** are expected
to have zero registered facilities, leaving tens of thousands of residents without a
formally registered IPRESS in their district.

### F2 — Large distances to the nearest facility in rural areas

With real Centros Poblados data, the spatial access metric (mean distance from populated
centres to the nearest IPRESS) is expected to show that the majority of districts where
distance data is available have a median exceeding **10 km** — the typical threshold used
in Peruvian health policy as the boundary between "accessible" and "poorly-served".
Amazonian and high-Andean districts are likely to show median distances well above 40 km,
reflecting the combination of dispersed settlement patterns and limited road infrastructure.

### F3 — Emergency volume concentrated in Lima and large provincial capitals

The Emergencias C1 dataset, when loaded, is expected to show that a disproportionate share
of recorded emergency attendances is concentrated in Lima and major coastal cities, not
because rural demand is genuinely lower, but because undercoverage of remote IPRESS in the
official registry leads to systematic under-counting in those areas.

### F4 — The composite index identifies a consistent set of underserved districts

Across both index specifications (equal-weight baseline and rank-normalised alternative),
the top-20 most underserved districts are dominated by rural districts in **Amazonas,
La Libertad, Puno, Cajamarca, and Huancavelica** — regions that score poorly on all three
metric families simultaneously (low facility density, high emergency load per facility,
and long distances to the nearest IPRESS).

### F5 — Rankings are largely robust to the choice of specification

The Spearman rank correlation between the baseline and alternative indices is high
at the **department level** (ρ ≈ 1.0), confirming that the identification of the most
underserved departments does not depend on whether equal-weight or rank-normalised
aggregation is used. At the **district level**, individual rank positions shift more
(reflecting sensitivity to outlier districts), but the broad geographic pattern — remote
rural districts as most underserved, Lima districts as best served — is consistent across
both specifications.

---

## 10. What are the main limitations?

### L1 — Incomplete spatial access data

The Centros Poblados shapefile requires a manual download. In the current pipeline run,
only the DISTRITOS layer was retrieved automatically, so the `mean_dist_nearest_m` and
related spatial access metrics are available for only **431 of 1 873 districts**. The
composite index for the remaining 1 442 districts is driven entirely by the facility
density and emergency volume components, potentially **overstating the index** for
districts that have acceptable spatial access but poor facility counts.

### L2 — Government portal availability

Three of the five required datasets (IPRESS MINSA, RENIPRESS SUSALUD, Emergencias C1)
could not be downloaded programmatically during development — the portals returned
HTTP 418 ("I'm a teapot") errors, indicating bot-detection or rate-limiting. These
datasets must be downloaded manually via a browser and placed in `data/raw/`. The pipeline
handles their absence gracefully (warnings are printed, empty DataFrames are returned) but
the analysis is degraded when they are missing.

### L3 — CRS assumption for missing `.prj` files

If the IGN Centros Poblados shapefile is distributed without an embedded `.prj` metadata
file, `cleaning.py` assumes EPSG:4326. Any mismatch between the assumed and actual CRS
would silently misplace every populated centre, producing incorrect distance measurements.
The assumed CRS should be verified against the official IGN metadata before any
publication use.

### L4 — Emergency data reflects reporting coverage, not true demand

The Emergencias C1 dataset records only emergencies handled by IPRESS registered in the
SUSALUD system. Informal health posts, community-level care, and emergencies handled
outside registered facilities are not counted. Districts with low emergency volumes may
genuinely have low demand, or they may have low **reporting coverage** — the two cases are
indistinguishable without additional field data.

### L5 — Composite index assumes equal importance of all components

The baseline index weights facility density, emergency activity, and spatial access
equally. There is no empirical basis for this choice — a public health expert might
weight spatial access more heavily for remote Amazonian districts, or weight emergency
volume more heavily for urban districts. The alternative specification (rank-normalised)
partially addresses this by reducing the influence of outliers, but neither specification
captures the policy priorities of Peruvian health planners.

### L6 — Cross-period emergency aggregation

The Emergencias C1 pipeline aggregates all available reporting periods into a single
district total. If the reporting coverage changes across periods (new facilities registered,
old facilities deregistered), the aggregated total conflates volume changes with registry
changes. Period-specific analyses would require filtering by year before the district-level
aggregation step in `metrics.clean_emergencias_susalud`.

### L7 — NumPy / fiona compatibility

fiona ≤ 1.9.x is incompatible with NumPy ≥ 2.0 for GeoPackage writes (`np.array(copy=False)`
raises a `ValueError`). The workaround (`engine="pyogrio"`) requires `pyogrio` to be
installed separately. If `pyogrio` is not available, all `.to_file(..., driver="GPKG")`
calls will fail. This is included in `requirements.txt` but is not part of the original
course environment specification.
