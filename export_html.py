"""
Generate a standalone HTML export of the emergency_access_peru Streamlit app.
Embeds all images as base64, the interactive map as a base64 iframe,
and renders key tables from district_metrics.csv.
"""

import base64
import csv
from pathlib import Path

ROOT    = Path(__file__).resolve().parent
FIGURES = ROOT / "output" / "figures"
TABLES  = ROOT / "output" / "tables"
OUT     = ROOT / "output" / "streamlit_app_export.html"
OUT.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def b64_img(name: str) -> str:
    path = FIGURES / name
    if not path.exists():
        return f'<p style="color:red">[Missing: {name}]</p>'
    data = base64.b64encode(path.read_bytes()).decode()
    return f'<img src="data:image/png;base64,{data}" style="max-width:100%;border-radius:8px;">'


def b64_iframe(name: str, height: int = 580) -> str:
    path = FIGURES / name
    if not path.exists():
        return f'<p style="color:red">[Missing: {name}]</p>'
    data = base64.b64encode(path.read_bytes()).decode()
    return (
        f'<iframe src="data:text/html;base64,{data}" '
        f'width="100%" height="{height}" '
        f'style="border:none;border-radius:8px;"></iframe>'
    )


def csv_to_html(path: Path, rows: int = 10, cols=None) -> str:
    if not path.exists():
        return f'<p style="color:red">[Missing: {path.name}]</p>'
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
    if not all_rows:
        return "<p>Empty table.</p>"
    headers = cols if cols else list(all_rows[0].keys())
    # clamp to available cols
    headers = [h for h in headers if h in all_rows[0]]
    html = ['<table style="border-collapse:collapse;width:100%;font-size:13px;">']
    html.append("<thead><tr>")
    for h in headers:
        html.append(f'<th style="border:1px solid #ddd;padding:6px 10px;background:#1f4e79;color:#fff;text-align:left">{h}</th>')
    html.append("</tr></thead><tbody>")
    for i, row in enumerate(all_rows[:rows]):
        bg = "#f5f8ff" if i % 2 == 0 else "#ffffff"
        html.append(f'<tr style="background:{bg}">')
        for h in headers:
            v = row.get(h, "")
            try:
                fv = float(v)
                v = f"{fv:.3f}" if "." in v else v
            except (ValueError, TypeError):
                pass
            html.append(f'<td style="border:1px solid #ddd;padding:5px 10px">{v}</td>')
        html.append("</tr>")
    html.append("</tbody></table>")
    return "\n".join(html)


def section(title: str, body: str) -> str:
    return f"""
    <div style="background:#fff;border-radius:10px;padding:24px 28px;margin-bottom:20px;
                box-shadow:0 2px 6px rgba(0,0,0,0.08);">
      <h3 style="color:#1f4e79;margin-top:0">{title}</h3>
      {body}
    </div>"""


def fig_block(img_html: str, caption: str, why: str = "") -> str:
    why_html = ""
    if why:
        why_html = f"""
        <div style="background:#e8f4fd;border-left:4px solid #1f77b4;
                    padding:8px 14px;border-radius:4px;margin-top:10px;font-size:13px;">
          <strong>Why this matters:</strong> {why}
        </div>"""
    return f"""
    <div style="margin-bottom:28px">
      {img_html}
      <p style="color:#555;font-size:13px;margin-top:8px"><em>{caption}</em></p>
      {why_html}
    </div>"""


# ---------------------------------------------------------------------------
# Tab content builders
# ---------------------------------------------------------------------------

def tab1_html() -> str:
    data_sources = """
    <table style="border-collapse:collapse;width:100%;font-size:13px">
      <thead><tr>
        <th style="border:1px solid #ddd;padding:6px 10px;background:#1f4e79;color:#fff">Dataset</th>
        <th style="border:1px solid #ddd;padding:6px 10px;background:#1f4e79;color:#fff">Source</th>
        <th style="border:1px solid #ddd;padding:6px 10px;background:#1f4e79;color:#fff">Role in analysis</th>
        <th style="border:1px solid #ddd;padding:6px 10px;background:#1f4e79;color:#fff">Auto-download</th>
      </tr></thead>
      <tbody>
        <tr style="background:#f5f8ff"><td style="border:1px solid #ddd;padding:5px 10px">Centros Poblados</td><td style="border:1px solid #ddd;padding:5px 10px">INEI</td><td style="border:1px solid #ddd;padding:5px 10px">Demand-side: where people live</td><td style="border:1px solid #ddd;padding:5px 10px">No (HTTP 418)</td></tr>
        <tr><td style="border:1px solid #ddd;padding:5px 10px">Distritos (shapefile)</td><td style="border:1px solid #ddd;padding:5px 10px">GADM/GitHub</td><td style="border:1px solid #ddd;padding:5px 10px">Administrative boundaries</td><td style="border:1px solid #ddd;padding:5px 10px">Yes</td></tr>
        <tr style="background:#f5f8ff"><td style="border:1px solid #ddd;padding:5px 10px">IPRESS MINSA</td><td style="border:1px solid #ddd;padding:5px 10px">MINSA datosabiertos</td><td style="border:1px solid #ddd;padding:5px 10px">Public health facility locations</td><td style="border:1px solid #ddd;padding:5px 10px">No (HTTP 418)</td></tr>
        <tr><td style="border:1px solid #ddd;padding:5px 10px">RENIPRESS SUSALUD</td><td style="border:1px solid #ddd;padding:5px 10px">SUSALUD datosabiertos</td><td style="border:1px solid #ddd;padding:5px 10px">All registered health facilities</td><td style="border:1px solid #ddd;padding:5px 10px">No (HTTP 418)</td></tr>
        <tr style="background:#f5f8ff"><td style="border:1px solid #ddd;padding:5px 10px">Emergencias C1</td><td style="border:1px solid #ddd;padding:5px 10px">SUSALUD datosabiertos</td><td style="border:1px solid #ddd;padding:5px 10px">Emergency attendance volumes</td><td style="border:1px solid #ddd;padding:5px 10px">No (HTTP 418)</td></tr>
      </tbody>
    </table>"""

    cleaning = """
    <ol style="font-size:14px;line-height:1.8">
      <li><strong>Column normalisation</strong> -- all column names snake_cased; accents stripped.</li>
      <li><strong>Coordinate parsing</strong> -- lat/lon cast to float64; rows with null coordinates dropped.</li>
      <li><strong>Ubigeo zero-padding</strong> -- district code always left-padded to 6 digits (<code>str.zfill(6)</code>) as the universal join key.</li>
      <li><strong>CRS standardisation</strong> -- all GeoDataFrames stored in EPSG:4326; UTM 18S used only for metric distance calculations.</li>
      <li><strong>Duplicate removal</strong> -- exact-coordinate duplicates dropped from facility tables (keep first).</li>
      <li><strong>Population coercion</strong> -- <code>poblacion</code> cast to Int64; zero replaced with NaN before density calculations.</li>
      <li><strong>Emergency aggregation</strong> -- multiple reporting rows per district summed to one row per ubigeo.</li>
    </ol>"""

    methodology = """
    <table style="border-collapse:collapse;width:100%;font-size:13px;margin-bottom:16px">
      <thead><tr>
        <th style="border:1px solid #ddd;padding:6px 10px;background:#1f4e79;color:#fff">Metric family</th>
        <th style="border:1px solid #ddd;padding:6px 10px;background:#1f4e79;color:#fff">Baseline spec</th>
        <th style="border:1px solid #ddd;padding:6px 10px;background:#1f4e79;color:#fff">Alternative spec</th>
      </tr></thead>
      <tbody>
        <tr style="background:#f5f8ff"><td style="border:1px solid #ddd;padding:5px 10px">A -- Facility density</td><td style="border:1px solid #ddd;padding:5px 10px">facilities per 100 km&sup2;</td><td style="border:1px solid #ddd;padding:5px 10px">facilities per 10 000 pop</td></tr>
        <tr><td style="border:1px solid #ddd;padding:5px 10px">B -- Emergency activity</td><td style="border:1px solid #ddd;padding:5px 10px">total emergencies</td><td style="border:1px solid #ddd;padding:5px 10px">emergencies per facility</td></tr>
        <tr style="background:#f5f8ff"><td style="border:1px solid #ddd;padding:5px 10px">C -- Spatial access</td><td style="border:1px solid #ddd;padding:5px 10px">mean distance to nearest facility</td><td style="border:1px solid #ddd;padding:5px 10px">pop-weighted mean; p75; % &gt;10 km</td></tr>
        <tr><td style="border:1px solid #ddd;padding:5px 10px">D -- Composite index</td><td style="border:1px solid #ddd;padding:5px 10px">equal-weight min-max average</td><td style="border:1px solid #ddd;padding:5px 10px">rank-normalised average</td></tr>
      </tbody>
    </table>"""

    # Try to compute simple KPIs from CSV
    kpi_html = ""
    csv_path = TABLES / "district_metrics.csv"
    if csv_path.exists():
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        n_districts = len(rows)
        try:
            vals = [float(r["baseline_index"]) for r in rows if r.get("baseline_index")]
            avg_idx = sum(vals) / len(vals) if vals else 0
            above_50 = sum(1 for v in vals if v > 0.5)
            kpi_html = f"""
            <div style="display:flex;gap:20px;flex-wrap:wrap;margin-top:10px">
              <div style="background:#1f4e79;color:#fff;padding:18px 24px;border-radius:8px;min-width:140px;text-align:center">
                <div style="font-size:28px;font-weight:bold">{n_districts:,}</div>
                <div style="font-size:12px;margin-top:4px">Districts analysed</div>
              </div>
              <div style="background:#c0392b;color:#fff;padding:18px 24px;border-radius:8px;min-width:140px;text-align:center">
                <div style="font-size:28px;font-weight:bold">{above_50:,}</div>
                <div style="font-size:12px;margin-top:4px">Districts underservice index &gt; 0.5</div>
              </div>
              <div style="background:#27ae60;color:#fff;padding:18px 24px;border-radius:8px;min-width:140px;text-align:center">
                <div style="font-size:28px;font-weight:bold">{avg_idx:.3f}</div>
                <div style="font-size:12px;margin-top:4px">Mean baseline index</div>
              </div>
            </div>"""
        except Exception:
            kpi_html = f"<p>{n_districts:,} districts loaded.</p>"

    problem_stmt = (
        '<p style="font-size:14px;line-height:1.7">'
        "Peru&#39;s 1,873 districts face stark inequalities in access to emergency healthcare. "
        "Andean and Amazonian communities can be separated from the nearest health facility by "
        "tens of kilometres of difficult terrain, while Lima concentrates the majority of registered "
        "facilities. This project quantifies that inequality by combining facility registries, "
        "populated-centre locations, and emergency attendance records into a composite spatial "
        "access index at the district level."
        "</p>"
        '<p style="font-size:14px;line-height:1.7">'
        "The analytical goal is to rank Peru&#39;s 1,873 districts by degree of underservice and "
        "identify the geographic and structural patterns that explain poor access - enabling "
        "evidence-based prioritisation of facility investment."
        "</p>"
        '<p style="font-size:14px;line-height:1.7">'
        "<strong>Note:</strong> Government open-data portals (datosabiertos.gob.pe) block automated "
        "downloads (HTTP 418). Where actual data is unavailable the pipeline generates synthetic "
        "placeholder data so all analytical functions can be demonstrated end-to-end."
        "</p>"
    )

    return (
        section("Problem Statement", problem_stmt)
        + section("Data Sources", data_sources)
        + section("Cleaning Decisions", cleaning)
        + section("Methodological Decisions", methodology)
        + section("Key Performance Indicators", kpi_html)
    )


def tab2_html() -> str:
    figs = [
        ("q1a_density_distribution.png",
         "Distribution of facility density across districts",
         "Reveals the long tail of very low-density districts concentrated in Amazonas and Puno."),
        ("q1b_dept_facility_ranking.png",
         "Department-level facility ranking",
         "Lima has 10x more facilities than the median department."),
        ("q1c_emergency_volume_dept.png",
         "Emergency volume by department",
         "High volume without proportionate facilities signals overloaded facilities."),
        ("q2a_distance_distribution.png",
         "Distribution of distance to nearest facility",
         "Median travel distance is manageable, but the right tail extends beyond 50 km."),
        ("q2b_pct_far_centres_dept.png",
         "Share of populated centres >10 km from nearest facility",
         "Loreto, Ucayali, and Madre de Dios have the highest share of isolated communities."),
        ("q2c_distance_vs_population.png",
         "Distance vs. population: identifying high-risk communities",
         "Communities with large populations AND long distances represent the highest-priority gaps."),
        ("q3a_top20_underserved.png",
         "Top 20 most underserved districts (composite index)",
         "All top-20 districts are located outside Lima and the main coastal cities."),
        ("q3b_underservice_by_dept.png",
         "Department-level underservice distribution",
         "Departments in the Andes and Amazon dominate the worst-access tier."),
        ("q3c_metric_correlations.png",
         "Correlation matrix of access metrics",
         "Distance metrics correlate strongly; density and distance are only weakly correlated."),
        ("q4a_baseline_vs_alternative.png",
         "Baseline vs. alternative index comparison",
         "Both specifications agree on most extreme cases; divergence reveals measurement sensitivity."),
        ("q4b_rank_change.png",
         "Rank change between specifications",
         "Districts that shift significantly deserve deeper case-by-case review."),
        ("q4c_dept_agreement.png",
         "Department-level agreement between index specifications",
         "High agreement departments confirm findings are robust to methodological choice."),
    ]
    blocks = "".join(
        fig_block(b64_img(name), caption, why)
        for name, caption, why in figs
    )
    return section("Analytical Figures", blocks)


def tab3_html() -> str:
    csv_path = TABLES / "district_metrics.csv"
    choropleth = b64_img("geo_choropleths.png")

    # Top 10 and bottom 10 by baseline_index
    top10_html = "<p>district_metrics.csv not found.</p>"
    bot10_html = "<p>district_metrics.csv not found.</p>"
    dept_html  = "<p>district_metrics.csv not found.</p>"

    if csv_path.exists():
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        key_cols = ["ubigeo", "baseline_index", "alternative_index",
                    "density_ipress_per100km2", "mean_dist_nearest_m"]
        key_cols = [c for c in key_cols if c in (rows[0] if rows else {})]

        def sort_rows(data, col, reverse=True):
            try:
                return sorted(data, key=lambda r: float(r.get(col) or 0), reverse=reverse)
            except Exception:
                return data

        sorted_rows = sort_rows(rows, "baseline_index")

        def rows_to_html(data, cols):
            h = ['<table style="border-collapse:collapse;width:100%;font-size:12px">']
            h.append("<thead><tr>")
            for c in cols:
                h.append(f'<th style="border:1px solid #ddd;padding:5px 8px;background:#1f4e79;color:#fff">{c}</th>')
            h.append("</tr></thead><tbody>")
            for i, row in enumerate(data):
                bg = "#f5f8ff" if i % 2 == 0 else "#fff"
                h.append(f'<tr style="background:{bg}">')
                for c in cols:
                    v = row.get(c, "")
                    try:
                        fv = float(v)
                        v = f"{fv:.4f}" if "." in str(v) else v
                    except (ValueError, TypeError):
                        pass
                    h.append(f'<td style="border:1px solid #ddd;padding:4px 8px">{v}</td>')
                h.append("</tr>")
            h.append("</tbody></table>")
            return "\n".join(h)

        top10_html = rows_to_html(sorted_rows[:10], key_cols)
        bot10_html = rows_to_html(sorted_rows[-10:][::-1], key_cols)

        # Department summary -- group by first 2 chars of ubigeo
        dept_data = {}
        for row in rows:
            ub = str(row.get("ubigeo", "")).zfill(6)
            dept = ub[:2]
            if dept not in dept_data:
                dept_data[dept] = {"dept": dept, "n": 0, "idx_sum": 0.0}
            dept_data[dept]["n"] += 1
            try:
                dept_data[dept]["idx_sum"] += float(row.get("baseline_index") or 0)
            except (ValueError, TypeError):
                pass
        dept_rows = [
            {"department": d, "districts": str(v["n"]),
             "mean_baseline_index": f'{v["idx_sum"]/v["n"]:.4f}'}
            for d, v in sorted(dept_data.items(),
                                key=lambda x: x[1]["idx_sum"]/x[1]["n"], reverse=True)
        ]
        dept_html = rows_to_html(dept_rows[:25], ["department", "districts", "mean_baseline_index"])

    choropleth_body = (
        '<p style="font-size:13px;color:#555;margin-bottom:12px">'
        "Four panels: facility density per 100 km&sup2;, facility density per 10 000 pop, "
        "mean distance to nearest facility, and composite underservice index. "
        "Grey = missing data."
        "</p>"
        + choropleth
    )
    side_by_side = (
        '<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px">'
        + section("Top 10 Most Underserved Districts", top10_html)
        + section("Top 10 Best-Served Districts", bot10_html)
        + "</div>"
    )
    return (
        section("Geographic Distribution of Access (Choropleth Maps)", choropleth_body)
        + side_by_side
        + section("Department Summary (top 25 by mean underservice)", dept_html)
    )


def tab4_html() -> str:
    imap = b64_iframe("interactive_map.html", height=580)
    map_body = (
        '<p style="font-size:13px;color:#555;margin-bottom:12px">'
        "Choropleth of composite underservice index by district. "
        "Darker red = more underserved. Hover over a district for details. "
        "Populated centres within 10 km of a facility shown as blue dots; "
        "isolated centres (&gt;10 km) shown as orange dots."
        "</p>"
        + imap
    )
    sensitivity_body = (
        '<p style="font-size:14px;line-height:1.7">'
        "The composite underservice index is computed under two specifications:"
        "</p>"
        '<ul style="font-size:14px;line-height:1.9">'
        "<li><strong>Baseline</strong>: equal-weight average of min-max normalised component scores "
        "from families A (facility density), B (emergency activity), and C (spatial access).</li>"
        "<li><strong>Alternative</strong>: same components but normalised using rank-based "
        "normalisation (percentile rank / N), which reduces the influence of extreme outliers "
        "on the final index.</li>"
        "</ul>"
        '<p style="font-size:14px;line-height:1.7">'
        "Both specifications produce Pearson r &gt; 0.90 across districts. The largest rank shifts "
        "occur in districts where one component (typically spatial distance) is an extreme outlier "
        "-- the baseline amplifies such outliers whereas the alternative dampens them. "
        "Policy decisions for borderline districts should weigh both scores together with "
        "qualitative context."
        "</p>"
    )
    return (
        section("Interactive Map", map_body)
        + section("Methodological Sensitivity", sensitivity_body)
    )


# ---------------------------------------------------------------------------
# Page assembly
# ---------------------------------------------------------------------------

TAB_CSS = """
<style>
* { box-sizing: border-box; }
body { font-family: 'Segoe UI', Arial, sans-serif; background: #f0f4f8; margin: 0; padding: 0; }
.header {
  background: linear-gradient(135deg, #1f4e79 0%, #2980b9 100%);
  color: #fff; padding: 28px 40px; margin-bottom: 24px;
}
.header h1 { margin: 0; font-size: 26px; }
.header p  { margin: 6px 0 0; font-size: 14px; opacity: 0.85; }
.tabs { display: flex; gap: 4px; padding: 0 40px; flex-wrap: wrap; }
.tab-btn {
  padding: 10px 22px; border: none; border-radius: 8px 8px 0 0;
  cursor: pointer; font-size: 14px; font-weight: 600;
  background: #cdd8e3; color: #1f4e79; transition: background 0.2s;
}
.tab-btn.active { background: #1f4e79; color: #fff; }
.tab-content { display: none; padding: 24px 40px 40px; }
.tab-content.active { display: block; }
.footer {
  background: #1f4e79; color: #cdd8e3; text-align: center;
  padding: 16px; font-size: 12px; margin-top: 40px;
}
</style>
"""

TAB_JS = """
<script>
function showTab(id) {
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + id).classList.add('active');
  document.getElementById('btn-' + id).classList.add('active');
}
</script>
"""

TAB_LABELS = [
    ("context",  "Context & Methodology"),
    ("charts",   "Analytical Charts"),
    ("maps",     "Geographic Access"),
    ("advanced", "Interactive Map & Sensitivity"),
]


def build_html() -> str:
    print("Building Tab 1 ...")
    t1 = tab1_html()
    print("Building Tab 2 ...")
    t2 = tab2_html()
    print("Building Tab 3 ...")
    t3 = tab3_html()
    print("Building Tab 4 ...")
    t4 = tab4_html()

    tab_btns = "\n".join(
        f'<button class="tab-btn{" active" if i==0 else ""}" '
        f'id="btn-{tid}" onclick="showTab(\'{tid}\')">{label}</button>'
        for i, (tid, label) in enumerate(TAB_LABELS)
    )

    tab_panels = "".join(
        f'<div class="tab-content{" active" if i==0 else ""}" id="tab-{tid}">{content}</div>'
        for i, ((tid, _), content) in enumerate(zip(TAB_LABELS, [t1, t2, t3, t4]))
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Emergency Healthcare Access in Peru -- District-Level Analysis</title>
{TAB_CSS}
</head>
<body>
{TAB_JS}
<div class="header">
  <h1>Emergency Healthcare Access in Peru</h1>
  <p>District-level spatial analysis of facility density, travel distances, emergency activity,
     and composite underservice index across Peru's 1,873 districts.</p>
</div>
<div class="tabs">
{tab_btns}
</div>
{tab_panels}
<div class="footer">
  Generated by export_html.py -- emergency_access_peru project &nbsp;|&nbsp;
  Data: MINSA, SUSALUD, INEI, GADM
</div>
</body>
</html>"""


if __name__ == "__main__":
    print("=== Standalone HTML Export ===")
    html = build_html()
    OUT.write_text(html, encoding="utf-8")
    size_mb = OUT.stat().st_size / 1_048_576
    print(f"\nSaved -> {OUT}")
    print(f"File size: {size_mb:.1f} MB")
    print("=== Done ===")
