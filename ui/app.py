from pathlib import Path
import streamlit as st
import plotly.express as px

from utils import (
    safe_read_csv,
    safe_read_text,
    safe_read_json,
    zip_dir,
    fmt_bytes,
    st_table,
    list_token_files,
)

st.set_page_config(page_title="DataDesc", layout="wide")

st.markdown(
    """
    <style>
      .small-note { color: rgba(49, 51, 63, 0.70); font-size: 0.92rem; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
      .block { padding: 12px; border-radius: 14px; background: rgba(240, 242, 246, 0.55); }
    </style>
    """,
    unsafe_allow_html=True,
)

ROOT = Path(__file__).resolve().parents[1]

# ---------------- Sidebar ----------------
st.sidebar.title("DataDesc")
st.sidebar.caption("Descriptive statistics browser (CSV/Excel outputs)")

out_dir = st.sidebar.text_input("Output directory", value="output")
output_root = ROOT / out_dir
total_dir = output_root / "_total"

st.sidebar.divider()
st.sidebar.subheader("Export")

if output_root.exists():
    st.sidebar.download_button(
        "Download output.zip",
        data=zip_dir(output_root),
        file_name="datadesc_output.zip",
        mime="application/zip",
    )
else:
    st.sidebar.info("No output directory found yet.")

st.sidebar.divider()
show_hidden = st.sidebar.checkbox("Show hidden files", value=False)

st.title("DataDesc UI")
st.caption(f"Reading results from: {total_dir.as_posix()}")

idx = safe_read_csv(total_dir / "datasets_index.csv")
if idx is None:
    st.error("Missing `_total/datasets_index.csv`.")
    st.markdown(
        f"""
        Expected path:

        `{(output_root / "_total" / "datasets_index.csv").as_posix()}`

        Run the pipeline first:

        ```bash
        python main.py
        ```
        """
    )
    st.stop()

totals_json = safe_read_json(total_dir / "totals.json") or {}
totals_md = safe_read_text(total_dir / "totals.md") or ""
master_md = safe_read_text(total_dir / "master_summary.md") or ""

sources_table = safe_read_csv(total_dir / "sources_table.csv")
global_dtype = safe_read_csv(total_dir / "global_dtype_counts.csv")
global_missing = safe_read_csv(total_dir / "global_missing_hotspots.csv")
global_high_missing = safe_read_csv(total_dir / "global_high_missing_columns.csv")
global_high_card = safe_read_csv(total_dir / "global_high_cardinality_columns.csv")
global_constant = safe_read_csv(total_dir / "global_constant_columns.csv")
global_temporal = safe_read_csv(total_dir / "global_temporal_coverage.csv")
global_warnings = safe_read_csv(total_dir / "global_quality_warnings.csv")

datasets_processed = idx.height
total_rows = totals_json.get("total_rows", "")
total_cols = totals_json.get("total_columns", "")

m1, m2, m3, m4 = st.columns(4)
m1.metric("Datasets", datasets_processed)
m2.metric("Total rows (sum)", "" if total_rows == "" else f"{int(total_rows):,}")
m3.metric("Total columns (sum)", "" if total_cols == "" else f"{int(total_cols):,}")
m4.metric("Output folder", out_dir)

tabs = st.tabs([
    "Overview",
    "Datasets",
    "Dataset drilldown",
    "Missingness",
    "Schema",
    "Temporal",
    "Quality",
    "Files",
])

# ---------------- Overview ----------------
with tabs[0]:
    st.subheader("Totals")
    if totals_md.strip():
        st.markdown(totals_md)
    else:
        st.info("`totals.md` not found (expected from pipeline).")

    st.subheader("Master summary")
    if master_md.strip():
        st.markdown(master_md)
    else:
        st.warning("`master_summary.md` not found. Ensure `generate_total_summary(...)` runs in the pipeline.")

    st.subheader("Global artifacts status")
    expected = [
        "sources_table.csv",
        "global_dtype_counts.csv",
        "global_missing_hotspots.csv",
        "global_high_missing_columns.csv",
        "global_high_cardinality_columns.csv",
        "global_constant_columns.csv",
        "global_temporal_coverage.csv",
        "global_quality_warnings.csv",
        "master_summary.md",
        "master_summary.json",
    ]
    rows = []
    for name in expected:
        rows.append({"file": name, "exists": (total_dir / name).exists()})
    st_table(st, safe_read_csv(total_dir / "sources_table.csv"), height=260)
    st.dataframe(rows, use_container_width=True, height=260)

with tabs[1]:
    st.subheader("Datasets table")

    base = sources_table if sources_table is not None else idx

    c1, c2, c3, c4 = st.columns([2.2, 1.2, 1.2, 1.4])
    query = c1.text_input("Search (name/path/sheet)", "")
    min_rows = c2.number_input("Min rows", value=0, step=1000)
    max_rows = c3.number_input("Max rows", value=0, step=10000, help="0 means no maximum")
    sort_by = c4.selectbox(
        "Sort by",
        options=[c for c in ["rows", "columns", "missing_cell_pct", "duplicate_row_pct", "dataset_name", "sheet_name"] if c in base.columns] + [base.columns[0]],
        index=0
    )

    df = base

    # search
    if query.strip():
        q = query.strip().lower()
        search_cols = [c for c in ["dataset_name", "source_path", "sheet_name"] if c in df.columns]
        if search_cols:
            expr = None
            import polars as pl
            for c in search_cols:
                e = pl.col(c).cast(pl.Utf8, strict=False).str.to_lowercase().str.contains(q)
                expr = e if expr is None else (expr | e)
            df = df.filter(expr)

    # numeric filters
    import polars as pl
    if "rows" in df.columns:
        df = df.filter(pl.col("rows").cast(pl.Int64, strict=False) >= int(min_rows))
        if int(max_rows) > 0:
            df = df.filter(pl.col("rows").cast(pl.Int64, strict=False) <= int(max_rows))

    if sort_by in df.columns:
        desc = sort_by in ["rows", "columns", "missing_cell_pct", "duplicate_row_pct"]
        df = df.sort(sort_by, descending=desc)

    st_table(st, df, height=520)

    st.divider()
    st.subheader("Rows per dataset (top 30)")
    if "rows" in df.columns and "dataset_id" in df.columns:
        top = df.sort("rows", descending=True).head(30)
        pdf = top.to_pandas() if hasattr(top, "to_pandas") else None
        if pdf is None:
            pdf = __import__("polars").DataFrame(top.to_dicts()).to_pandas()

        hover_cols = [c for c in ["dataset_name", "sheet_name", "source_path", "missing_cell_pct", "duplicate_row_pct"] if c in pdf.columns]
        fig = px.bar(pdf, x="dataset_id", y="rows", hover_data=hover_cols)
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No `rows` / `dataset_id` columns available for plotting.")

with tabs[2]:
    st.subheader("Dataset drilldown")

    choices = []
    for r in idx.iter_rows(named=True):
        dn = r.get("dataset_name", "") or ""
        sh = (r.get("sheet_name") or "").strip()
        if len(dn) > 60:
            dn = dn[:60] + "..."
        if len(sh) > 40:
            sh = sh[:40] + "..."

        label = "%s | %s | %s" % (
            r.get("dataset_id"),
            dn,
            sh if sh else "sheet: (none)",
        )
        choices.append((label, r))

    if not choices:
        st.info("No datasets found.")
        st.stop()

    selected_label = st.selectbox("Select dataset", options=[c[0] for c in choices])
    row = dict([c for c in choices if c[0] == selected_label][0][1])

    raw = str(row.get("dataset_dir", "") or "")
    p = Path(raw)

    # If it's not absolute, resolve it from repo root
    if not p.is_absolute():
        p = (ROOT / p).resolve()

    # If it still doesn't exist, try resolving relative to output_root
    if not p.exists():
        p2 = (output_root / Path(raw).name).resolve()
        if p2.exists():
            p = p2

    dataset_dir = p
    st.markdown(f"**Dataset folder:** `{dataset_dir.as_posix()}`")
    st.markdown(f"<div class='small-note mono'>{row.get('source_path','')}</div>", unsafe_allow_html=True)

    # per-dataset artifacts
    report_md = safe_read_text(dataset_dir / "report.md")
    schema_csv = safe_read_csv(dataset_dir / "schema.csv")
    miss_csv = safe_read_csv(dataset_dir / "missingness.csv")
    num_csv = safe_read_csv(dataset_dir / "numeric_summary.csv")
    cat_csv = safe_read_csv(dataset_dir / "categorical_summary.csv")
    cat_cols_csv = safe_read_csv(dataset_dir / "categorical_columns.csv")
    ll_csv = safe_read_csv(dataset_dir / "listlike_profile.csv")
    kd_csv = safe_read_csv(dataset_dir / "key_duplicates.csv")
    corr_csv = safe_read_csv(dataset_dir / "correlations.csv")
    uniq_csv = safe_read_csv(dataset_dir / "uniqueness.csv")
    text_csv = safe_read_csv(dataset_dir / "text_profile.csv")
    dt_csv = safe_read_csv(dataset_dir / "datetime_profile.csv")
    rowm_csv = safe_read_csv(dataset_dir / "row_missingness.csv")
    warn_md = safe_read_text(dataset_dir / "quality_warnings.md")
    preview_csv = safe_read_csv(dataset_dir / "preview.csv")

    c1, c2, c3 = st.columns(3)
    c1.metric("Rows", row.get("rows", ""))
    c2.metric("Columns", row.get("columns", ""))
    c3.metric("Memory (est)", fmt_bytes(row.get("memory_bytes_estimate", "")))

    drill_tabs = st.tabs([
        "Report", "Preview", "Schema", "Missingness",
        "Numeric", "Categorical", "Categorical columns",
        "List-like", "Key duplicates",
        "Correlations", "Uniqueness",
        "Text", "Datetime", "Row missingness", "Warnings", "Files"
    ])

    with drill_tabs[0]:
        if report_md:
            st.markdown(report_md)
        else:
            st.info("No report.md found.")

    with drill_tabs[1]:
        st_table(st, preview_csv, height=420)

    with drill_tabs[2]:
        st_table(st, schema_csv, height=520)
        if schema_csv is not None and not schema_csv.is_empty() and "dtype" in schema_csv.columns:
            g = schema_csv.group_by("dtype").agg(__import__("polars").len().alias("count")).sort("count", descending=True)
            pdf = g.to_pandas() if hasattr(g, "to_pandas") else None
            if pdf is None:
                pdf = __import__("polars").DataFrame(g.to_dicts()).to_pandas()
            fig = px.bar(pdf, x="dtype", y="count")
            fig.update_layout(height=360)
            st.plotly_chart(fig, use_container_width=True)

    with drill_tabs[3]:
        st_table(st, miss_csv, height=520)
        if miss_csv is not None and not miss_csv.is_empty() and {"column", "missing_pct"}.issubset(set(miss_csv.columns)):
            top = miss_csv.sort("missing_pct", descending=True).head(30)
            pdf = top.to_pandas() if hasattr(top, "to_pandas") else None
            if pdf is None:
                pdf = __import__("polars").DataFrame(top.to_dicts()).to_pandas()
            fig = px.bar(pdf, x="column", y="missing_pct")
            fig.update_layout(xaxis_tickangle=-45, height=420)
            st.plotly_chart(fig, use_container_width=True)

    with drill_tabs[4]:
        st_table(st, num_csv, height=520)

    with drill_tabs[5]:
        st_table(st, cat_csv, height=520)

    with drill_tabs[6]:
        st_table(st, cat_cols_csv, height=520)

    with drill_tabs[7]:
        st_table(st, ll_csv, height=520)
        token_files = list_token_files(dataset_dir)
        if token_files:
            st.divider()
            st.markdown("**List-like token files**")
            names = [p.name for p in token_files]
            chosen = st.selectbox("Select token file", options=names)
            tf = [p for p in token_files if p.name == chosen][0]
            st_table(st, safe_read_csv(tf), height=420)
        else:
            st.info("No listlike_tokens__*.csv files found.")

    with drill_tabs[8]:
        st_table(st, kd_csv, height=420)
        top_files = sorted([p for p in dataset_dir.glob("key_duplicates_top__*.csv") if p.is_file()])
        if top_files:
            st.divider()
            st.markdown("**Top repeated keys files**")
            names = [p.name for p in top_files]
            chosen = st.selectbox("Select file", options=names)
            fp = [p for p in top_files if p.name == chosen][0]
            st_table(st, safe_read_csv(fp), height=420)

    with drill_tabs[9]:
        st_table(st, corr_csv, height=520)

    with drill_tabs[10]:
        st_table(st, uniq_csv, height=520)

    with drill_tabs[11]:
        st_table(st, text_csv, height=520)

    with drill_tabs[12]:
        st_table(st, dt_csv, height=520)

    with drill_tabs[13]:
        st_table(st, rowm_csv, height=520)

    with drill_tabs[14]:
        if warn_md:
            st.markdown(warn_md)
        else:
            st.info("No quality_warnings.md found.")

    with drill_tabs[15]:
        files = []
        for p in sorted(dataset_dir.glob("*")):
            if p.is_file():
                if not show_hidden and p.name.startswith("."):
                    continue
                files.append({"file": p.name, "size": fmt_bytes(p.stat().st_size)})
        st.dataframe(files, use_container_width=True, height=520)

# ---------------- Missingness ----------------
with tabs[3]:
    st.subheader("Missingness (global)")
    st_table(st, global_missing, height=460)

    if global_missing is not None and not global_missing.is_empty() and "missing_pct" in global_missing.columns:
        pdf = global_missing.to_pandas() if hasattr(global_missing, "to_pandas") else None
        if pdf is None:
            pdf = __import__("polars").DataFrame(global_missing.to_dicts()).to_pandas()
        fig = px.histogram(pdf, x="missing_pct", nbins=25)
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Very high missingness (>=80%)")
    st_table(st, global_high_missing, height=360)

# ---------------- Schema ----------------
with tabs[4]:
    st.subheader("Global dtype composition")
    st_table(st, global_dtype, height=420)

    if global_dtype is not None and not global_dtype.is_empty() and {"dtype", "count"}.issubset(set(global_dtype.columns)):
        pdf = global_dtype.to_pandas() if hasattr(global_dtype, "to_pandas") else None
        if pdf is None:
            pdf = __import__("polars").DataFrame(global_dtype.to_dicts()).to_pandas()
        fig = px.pie(pdf, values="count", names="dtype")
        fig.update_layout(height=420)
        st.plotly_chart(fig, use_container_width=True)

# ---------------- Temporal ----------------
with tabs[5]:
    st.subheader("Temporal coverage")
    st_table(st, global_temporal, height=460)

    if global_temporal is not None and not global_temporal.is_empty():
        cols = set(global_temporal.columns)
        if {"min_year", "max_year"}.issubset(cols):
            pdf = global_temporal.to_pandas() if hasattr(global_temporal, "to_pandas") else None
            if pdf is None:
                pdf = __import__("polars").DataFrame(global_temporal.to_dicts()).to_pandas()
            fig = px.scatter(pdf, x="min_year", y="max_year", hover_data=[c for c in ["dataset_name", "sheet_name", "datetime_cols_detected"] if c in pdf.columns])
            fig.update_layout(height=420)
            st.plotly_chart(fig, use_container_width=True)

# ---------------- Quality ----------------
with tabs[6]:
    st.subheader("Quality signals")

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### High-cardinality columns")
        st_table(st, global_high_card, height=460)

    with c2:
        st.markdown("### Constant columns")
        st_table(st, global_constant, height=460)

    st.markdown("### Aggregated warnings")
    st_table(st, global_warnings, height=420)

# ---------------- Files ----------------
with tabs[7]:
    st.subheader("Output folder contents")

    if not output_root.exists():
        st.info("No output folder found.")
    else:
        rows = []
        for p in sorted(output_root.rglob("*")):
            if p.is_file():
                rel = str(p.relative_to(output_root))
                if not show_hidden and any(part.startswith(".") for part in rel.split("/")):
                    continue
                rows.append({"file": rel, "size": fmt_bytes(p.stat().st_size)})

        st.dataframe(rows, use_container_width=True, height=650)
