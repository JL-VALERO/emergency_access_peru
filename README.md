# Emergency Healthcare Access Inequality in Peru

District-level analysis of emergency healthcare access across Peru's 1 873 districts,
combining facility registries, emergency production data, and populated-centre locations
to build a composite underservice index.

---

## Table of Contents

1. [Project objectives](#1-project-objectives)
2. [Repository structure](#2-repository-structure)
3. [Datasets](#3-datasets)
4. [Data cleaning decisions](#4-data-cleaning-decisions)
5. [Metrics construction](#5-metrics-construction)
6. [Installation](#6-installation)
7. [Running the pipeline](#7-running-the-pipeline)
8. [Running the Streamlit application](#8-running-the-streamlit-application)
9. [Output files](#9-output-files)
10. [Known limitations](#10-known-limitations)

---

## 1. Project objectives

This project answers four analytical questions about emergency healthcare access in Peru:

| # | Question |
|---|---|
| Q1 | Which districts have lower / higher facility and emergency care availability? |
| Q2 | Which districts show weaker populated-centre access to emergency services? |
| Q3 | Which districts appear most / least underserved when combining all factors? |
| Q4 | How sensitive are results to alternative methodological definitions? |

The analysis integrates five public datasets into a reproducible pipeline that ends in an
interactive Streamlit dashboard with static charts, static choropleth maps, and a Folium
interactive map.

---

## 2. Repository structure

```
emergency_access_peru/
├── app.py                     # Streamlit application (4 tabs)
├── requirements.txt
├── README.md
│
├── src/
│   ├── data_loader.py         # Step 1 — download and load all five raw datasets
│   ├── cleaning.py            # Step 2 — standardise, reproject, and save to processed/
│   ├── geospatial.py          # Step 3 — spatial joins and district summary layer
│   ├── metrics.py             # Step 4 — facility density, emergency activity,
│   │                          #           spatial access, and composite index
│   ├── visualization.py       # Steps 5–6 — static charts, choropleths, Folium map
│   └── utils.py               # Shared path constants
│
├── data/
│   ├── raw/                   # Downloaded source files (git-ignored for large binaries)
│   └── processed/             # GeoPackages and Parquet outputs of cleaning.py
│
├── output/
│   ├── figures/               # PNG charts + interactive_map.html
│   └── tables/                # district_metrics.csv / .parquet
│
└── video/
    └── link.txt               # Link to the explanatory video
```

---

## 3. Datasets

All five datasets are public. Three can be downloaded automatically by `data_loader.py`;
two require a manual browser download because the government portals block automated
requests (HTTP 418).

| # | Dataset | Provider | Format | Auto-download |
|---|---|---|---|---|
| 1 | **Centros Poblados** (IGN) | [datosabiertos.gob.pe](https://www.datosabiertos.gob.pe) | Shapefile (ZIP) | ⚠ Manual |
| 2 | **DISTRITOS** boundaries | [d2cml-ai / Data-Science-Python](https://github.com/d2cml-ai/Data-Science-Python) | Shapefile | ✅ Auto |
| 3 | **IPRESS MINSA** | [datosabiertos.gob.pe](https://www.datosabiertos.gob.pe) | CSV | ⚠ Manual |
| 4 | **RENIPRESS SUSALUD** | [datosabiertos.gob.pe](https://www.datosabiertos.gob.pe) | CSV | ⚠ Manual |
| 5 | **Emergencias C1 SUSALUD** | [datos.susalud.gob.pe](http://datos.susalud.gob.pe) | CSV | ⚠ Manual |

### Manual download instructions

1. **Centros Poblados** — download `CCPP_0.zip` from datosabiertos.gob.pe and place it at
   `data/raw/CCPP_0.zip`.
2. **IPRESS MINSA** — download the CSV and save it to `data/raw/ipress_minsa.csv`.
3. **RENIPRESS SUSALUD** — download the national IPRESS registry CSV and save it to
   `data/raw/renipress_susalud.csv`.
4. **Emergencias C1** — download from the SUSALUD open-data portal and save to
   `data/raw/emergencias_susalud.csv`.

After placing those files, re-run `python -m src.data_loader` — it will skip any file
already present and only process the new ones.

---

## 4. Data cleaning decisions

Implemented in `src/cleaning.py`. Each decision is documented below.

### 4.1 Column standardisation

All column names are converted to **snake_case** using a regex-based helper (`_to_snake`).
This normalises inconsistencies across shapefile and CSV vintages (e.g. `NOMBDIST` →
`nombre_distrito`, `Latitud` → `latitud`).

### 4.2 Coordinate reference system (CRS)

Every spatial layer is re-projected to **EPSG:4326 (WGS 84)** before saving. This ensures
a single consistent CRS across all downstream joins and maps. Distance calculations that
require metric units (nearest-facility search) are temporarily re-projected to
**EPSG:32718 (UTM Zone 18S)** — the standard metric CRS for most of Peru — then converted
back to WGS 84 for storage.

### 4.3 Invalid geometry removal

Rows with `null` or empty geometries are dropped before saving. These represent data-entry
errors in the source shapefiles and account for < 0.1 % of records in each layer.

### 4.4 Ubigeo zero-padding

The `ubigeo` district code is stored as a **6-character zero-padded string** (e.g.
`"010101"`) in all layers. Source files use inconsistent types (integer, string, mixed
length), so all are coerced to string and left-padded with `str.zfill(6)`. This is the
join key across every dataset.

### 4.5 Numeric coercion

Coordinate and count columns that arrive as strings are coerced to numeric with
`pd.to_numeric(errors="coerce")`. Unparseable values become `NaN` and are excluded from
spatial operations; a row count is printed so the analyst can judge impact.

### 4.6 Critical-field filtering

- **IPRESS layers**: rows with a null `nombre_ipress` are dropped — a facility without a
  name cannot be matched to any registry and provides no analytical value.
- **Emergencias**: rows with a null facility code *and* null facility name are dropped
  (both fields missing means the record cannot be attributed to any district).

### 4.7 Output formats

| Layer | Format | Reason |
|---|---|---|
| Centros Poblados | GeoPackage (`.gpkg`) | Preserves geometry + attributes in one file |
| DISTRITOS | GeoPackage (`.gpkg`) | Same |
| IPRESS MINSA | GeoPackage (`.gpkg`) | Facility points with geometry |
| RENIPRESS SUSALUD | GeoPackage (`.gpkg`) | Facility points with geometry |
| Emergencias SUSALUD | Parquet (`.parquet`) | Tabular only; Parquet is smaller than CSV and preserves dtypes |

All GeoPackage writes use `engine="pyogrio"` to avoid a known incompatibility between
fiona 1.9.x and NumPy ≥ 2.0.

---

## 5. Metrics construction

Implemented in `src/metrics.py`. The pipeline produces one row per district with the
columns described below.

### 5.1 Facility density (Metric A)

**Baseline**: number of MINSA IPRESS (or RENIPRESS) facilities within the district divided
by the district area in km², scaled to **facilities per 100 km²**.

```
density_ipress_per100km2 = n_ipress_minsa / area_km2 × 100
```

District area is recomputed in UTM 18S to avoid the distortions of WGS 84 degree-based
areas.

**Alternative**: same count divided by district population, scaled to
**facilities per 10 000 population** (`density_ipress_per10kpop`). This requires a
`pop_total` column aggregated from the Centros Poblados layer.

### 5.2 Emergency activity (Metric B)

**Baseline**: `total_emergencias` — the sum of all emergency-care episodes attributed to
facilities in the district across all available periods.

**Alternative**: `emergencias_per_facility` — total emergencies divided by the number of
active facilities. High values signal overloaded facilities, which is an independent
dimension of underservice.

### 5.3 Spatial access (Metric C)

For each populated centre the nearest IPRESS is found with GeoPandas `sjoin_nearest`
(distances computed in UTM 18S). Per-district aggregates:

| Column | Definition |
|---|---|
| `mean_dist_nearest_m` | Mean distance (m) from all centres in the district to their nearest IPRESS — **baseline** |
| `p75_dist_nearest_m` | 75th-percentile distance — captures the tail of poorly-served centres |
| `pct_centres_far` | Percentage of centres more than **10 km** from any IPRESS |
| `wmean_dist_nearest_m` | Population-weighted mean distance — **alternative** |

### 5.4 Composite underservice index (Metric D)

Each component metric is min-max normalised to [0, 1] and **inverted** where necessary so
that **1 always represents worse access**:

- Metrics where *higher* = worse (distances, % far centres, facility load): normalised
  directly.
- Metrics where *lower* = worse (densities, total emergencies): `score = 1 − normalised`.

The composite scores are the **mean** of all available component scores.

| Index | Weighting | Normalisation |
|---|---|---|
| `baseline_index` | Equal weight across all components | Min-max |
| `alternative_index` | Equal weight | Rank-based (percentile), making it robust to outliers |

Both indices are accompanied by a `_pct` column giving the district's percentile rank
(0–100, where 100 = most underserved nationally).

---

## 6. Installation

### Prerequisites

- [Anaconda](https://www.anaconda.com) or Miniconda
- Python 3.10
- Git

### Create the environment

```bash
conda create -n geo python=3.10 -y
conda activate geo
pip install -r requirements.txt
```

> **Note**: `pyogrio` and `pyarrow` are installed automatically via `requirements.txt`
> but are not listed in the original course file — they are required to write GeoPackage
> files on NumPy ≥ 2.0 and to read/write Parquet files respectively.

---

## 7. Running the pipeline

Run each module in order. Each step reads from the previous step's outputs.

```bash
conda activate geo

# Step 1 — download raw datasets (DISTRITOS auto; others need manual placement first)
python -m src.data_loader

# Step 2 — clean and save to data/processed/
python -m src.cleaning

# Step 3 — spatial joins → data/processed/*_districts.gpkg
python -m src.geospatial

# Step 4 — compute metrics → output/tables/district_metrics.*
python -m src.metrics

# Step 5/6 — generate all figures → output/figures/
python -m src.visualization
```

Or run the full pipeline from a Python session:

```python
from src.data_loader  import load_all
from src.cleaning     import run_cleaning_pipeline
from src.geospatial   import run_geospatial_pipeline
from src.metrics      import run_metrics_pipeline
from src.visualization import run_visualization_pipeline

raw     = load_all()
cleaned = run_cleaning_pipeline(raw)
geo     = run_geospatial_pipeline(cleaned)
metrics = run_metrics_pipeline(geo["districts_summary"])
run_visualization_pipeline(metrics)
```

---

## 8. Running the Streamlit application

```bash
conda activate geo
streamlit run app.py
```

The app opens at `http://localhost:8501` and contains four tabs:

| Tab | Contents |
|---|---|
| **Methodology & Data** | Research questions, data sources, pipeline overview, specification table, limitations |
| **Static Analysis** | All 12 static charts with prose interpretations (Q1–Q4) |
| **Geospatial Results** | Choropleth maps, sensitivity charts, filterable district table |
| **Interactive Exploration** | Folium map with layer control, baseline vs alternative side-by-side ranking, per-district metric deep-dive |

---

## 9. Output files

```
output/
├── figures/
│   ├── q1a_density_distribution.png      # Facility density histograms
│   ├── q1b_dept_facility_ranking.png     # Department ranking bar chart
│   ├── q1c_emergency_volume_dept.png     # Emergency volume box plots
│   ├── q2a_distance_distribution.png     # Distance-to-facility histogram
│   ├── q2b_pct_far_centres_dept.png      # % far centres by department
│   ├── q2c_distance_vs_population.png    # Distance vs population scatter
│   ├── q3a_top20_underserved.png         # Top-20 underserved districts
│   ├── q3b_underservice_by_dept.png      # Underservice box plots by dept
│   ├── q3c_metric_correlations.png       # Spearman correlation heatmap
│   ├── q4a_baseline_vs_alternative.png   # Baseline vs alternative scatter
│   ├── q4b_rank_change.png               # Rank-change slope chart
│   ├── q4c_dept_agreement.png            # Department-level agreement
│   ├── geo_choropleths.png               # 2×2 static choropleth maps
│   └── interactive_map.html             # Folium interactive map (~15 MB)
│
└── tables/
    ├── district_metrics.csv              # Full metrics table (1 873 districts × 29 cols)
    └── district_metrics.parquet         # Same, in Parquet format
```

---

## 10. Known limitations

- **Missing spatial-access data**: 1 442 of 1 873 districts have no populated-centre
  records in the current dataset (the Centros Poblados download was blocked during
  development). Distance-based metrics are `NaN` for those districts and are excluded
  from the composite index component, which biases the index toward facility-density
  signals for the affected districts.

- **Emergency data currency**: the Emergencias C1 dataset covers multiple reporting
  periods but does not have a fixed annual cadence. The pipeline aggregates all available
  periods; seasonal or year-specific analyses are not supported without additional
  filtering.

- **CRS assumption for Centros Poblados**: if the IGN shapefile is distributed without an
  embedded `.prj` file, `cleaning.py` assumes EPSG:4326. This should be verified against
  the official metadata before any publication use.

- **NumPy / fiona compatibility**: fiona ≤ 1.9.x is incompatible with NumPy ≥ 2.0 for
  GeoPackage writes. The workaround (`engine="pyogrio"`) requires `pyogrio` to be
  installed separately; this is included in `requirements.txt`.
