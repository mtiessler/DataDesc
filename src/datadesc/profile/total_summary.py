import json
from pathlib import Path
import polars as pl

from datadesc.writer import ensure_dir, write_text, write_json
from datadesc.report_html import render_report_html


def _safe_read_csv(path):
    p = Path(path)
    if not p.exists():
        return pl.DataFrame()
    try:
        return pl.read_csv(str(p), ignore_errors=True)
    except Exception:
        return pl.DataFrame()


def _safe_read_json(path):
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _df_to_md(df, max_rows=30):
    if df is None or df.is_empty():
        return "(none)\n"

    df = reveal_df(df.head(max_rows))
    cols = df.columns
    rows = df.rows()

    out = []
    out.append("| " + " | ".join(cols) + " |")
    out.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for r in rows:
        out.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")

    if df.height >= max_rows:
        out.append("")
        out.append("_table truncated_")
    out.append("")
    return "\n".join(out)


def reveal_df(df):
    # keep markdown readable: avoid long strings
    if df is None or df.is_empty():
        return df
    out = df
    for c in out.columns:
        try:
            out = out.with_columns(
                pl.when(pl.col(c).cast(pl.Utf8, strict=False).str.len_chars() > 80)
                .then(pl.col(c).cast(pl.Utf8, strict=False).str.slice(0, 77) + pl.lit("..."))
                .otherwise(pl.col(c))
                .alias(c)
            )
        except Exception:
            pass
    return out


def _count_types(schema_df):
    # returns (n_numeric, n_text, n_bool, n_other)
    if schema_df.is_empty() or "dtype" not in schema_df.columns:
        return 0, 0, 0, 0

    dts = [str(x).lower() for x in schema_df["dtype"].to_list()]

    n_numeric = sum(("int" in d or "float" in d or "decimal" in d) for d in dts)
    n_text = sum(("utf8" in d or "string" in d or "categorical" in d) for d in dts)
    n_bool = sum(("bool" in d) for d in dts)

    known = n_numeric + n_text + n_bool
    n_other = max(0, len(dts) - known)
    return int(n_numeric), int(n_text), int(n_bool), int(n_other)


def _high_uniqueness_candidates(schema_df):
    # purely descriptive: "high-uniqueness columns" (often IDs)
    if schema_df.is_empty():
        return []

    need = {"column", "unique", "non_null", "null_pct"}
    if not need.issubset(set(schema_df.columns)):
        return []

    out = []
    rows = schema_df.select(["column", "unique", "non_null", "null_pct"]).to_dicts()
    for r in rows:
        col = r["column"]
        u = float(r["unique"]) if r["unique"] is not None else 0.0
        nn = float(r["non_null"]) if r["non_null"] is not None else 0.0
        npct = float(r["null_pct"]) if r["null_pct"] is not None else 100.0
        ratio = (u / nn) if nn > 0 else 0.0

        if nn >= 50 and npct <= 30 and ratio >= 0.98:
            out.append(col)

    # stable, small
    res = []
    seen = set()
    for c in out:
        if c not in seen:
            seen.add(c)
            res.append(c)
    return res[:8]


def _high_cardinality_columns(schema_df):
    # purely descriptive: unique_ratio over non-null for text-like columns
    if schema_df.is_empty():
        return pl.DataFrame()

    need = {"column", "dtype", "unique", "non_null", "null_pct"}
    if not need.issubset(set(schema_df.columns)):
        return pl.DataFrame()

    sdf = schema_df.select(["column", "dtype", "unique", "non_null", "null_pct"]).with_columns([
        (pl.col("unique").cast(pl.Float64, strict=False) / pl.col("non_null").cast(pl.Float64, strict=False)).alias("unique_ratio")
    ])

    # focus on text-like columns
    sdf = sdf.filter(
        pl.col("dtype").cast(pl.Utf8, strict=False).str.to_lowercase().str.contains("utf8|string|categorical")
    )

    # high-card: many unique values relative to non-null
    sdf = sdf.filter(
        (pl.col("non_null") >= 200) &
        (pl.col("null_pct") <= 50) &
        (pl.col("unique_ratio") >= 0.50)
    ).sort("unique_ratio", descending=True)

    return sdf


def generate_total_summary(output_root, log):
    out_root = Path(output_root)
    total_dir = out_root / "_total"
    ensure_dir(total_dir)

    idx_path = total_dir / "datasets_index.csv"
    if not idx_path.exists():
        log.warning("Missing _total/datasets_index.csv; cannot build master summary.")
        return

    idx = pl.read_csv(str(idx_path), ignore_errors=True)

    dataset_rows = []
    dtype_rows = []
    missing_hotspots = []
    temporal_rows = []
    const_rows = []
    high_card_rows = []
    warn_rows = []

    for r in idx.iter_rows(named=True):
        dataset_id = r.get("dataset_id")
        dataset_dir = Path(r.get("dataset_dir"))
        source_path = r.get("source_path", "")
        dataset_name = r.get("dataset_name", "")
        sheet_name = r.get("sheet_name", "")

        overview = _safe_read_json(dataset_dir / "overview.json")
        schema = _safe_read_csv(dataset_dir / "schema.csv")
        miss = _safe_read_csv(dataset_dir / "missingness.csv")
        dist = _safe_read_csv(dataset_dir / "distribution_shape.csv")
        dtp = _safe_read_csv(dataset_dir / "datetime_profile.csv")
        rowm = _safe_read_csv(dataset_dir / "row_missingness.csv")
        twarn = _safe_read_json(dataset_dir / "quality_warnings.json")

        # dtype counts (global)
        if not schema.is_empty() and "dtype" in schema.columns:
            g = schema.group_by("dtype").agg(pl.len().alias("n"))
            for dt, n in g.iter_rows():
                dtype_rows.append({"dataset_id": dataset_id, "dtype": str(dt), "count": int(n)})

        n_numeric, n_text, n_bool, n_other = _count_types(schema)
        highuniq = _high_uniqueness_candidates(schema)

        # temporal coverage derived from datetime_profile (full column extraction)
        min_year = ""
        max_year = ""
        datetime_cols = 0
        if not dtp.is_empty() and {"min_year", "max_year", "year_hits"}.issubset(set(dtp.columns)):
            # count datetime-like columns that actually produced year hits
            try:
                datetime_cols = int(dtp.filter(pl.col("year_hits").cast(pl.Int64, strict=False) > 0).height)
            except Exception:
                datetime_cols = 0

            try:
                mins = dtp.select(pl.col("min_year").cast(pl.Int64, strict=False)).to_series().drop_nulls()
                maxs = dtp.select(pl.col("max_year").cast(pl.Int64, strict=False)).to_series().drop_nulls()
                if mins.len() > 0:
                    min_year = int(mins.min())
                if maxs.len() > 0:
                    max_year = int(maxs.max())
            except Exception:
                pass

        if min_year != "" or max_year != "" or datetime_cols > 0:
            temporal_rows.append({
                "dataset_id": dataset_id,
                "dataset_name": dataset_name,
                "sheet_name": sheet_name,
                "datetime_cols_detected": int(datetime_cols),
                "min_year": min_year,
                "max_year": max_year,
            })

        # missing hotspots (top 10 per dataset)
        if not miss.is_empty() and {"column", "missing_pct"}.issubset(set(miss.columns)):
            topm = miss.sort("missing_pct", descending=True).head(10)
            for mr in topm.iter_rows(named=True):
                missing_hotspots.append({
                    "dataset_id": dataset_id,
                    "dataset_name": dataset_name,
                    "sheet_name": sheet_name,
                    "column": mr.get("column"),
                    "missing_pct": mr.get("missing_pct"),
                })

        # constant numeric columns from distribution_shape
        if not dist.is_empty() and {"column", "is_constant"}.issubset(set(dist.columns)):
            try:
                const = dist.filter(pl.col("is_constant") == True).select(["column"]).head(100)
                for cr in const.iter_rows(named=True):
                    const_rows.append({
                        "dataset_id": dataset_id,
                        "dataset_name": dataset_name,
                        "sheet_name": sheet_name,
                        "column": cr.get("column"),
                    })
            except Exception:
                pass

        # high-cardinality text columns (pure descriptive)
        hc = _high_cardinality_columns(schema)
        if not hc.is_empty():
            for hcr in hc.head(30).iter_rows(named=True):
                high_card_rows.append({
                    "dataset_id": dataset_id,
                    "dataset_name": dataset_name,
                    "sheet_name": sheet_name,
                    "column": hcr.get("column"),
                    "dtype": hcr.get("dtype"),
                    "non_null": hcr.get("non_null"),
                    "unique": hcr.get("unique"),
                    "unique_ratio": hcr.get("unique_ratio"),
                    "null_pct": hcr.get("null_pct"),
                })

        # row-level missingness quantiles (take first row of row_missingness.csv)
        row_missing_summary = {}
        if not rowm.is_empty():
            # the file repeats quantiles on all rows; take first safely
            try:
                rr = rowm.head(1).to_dicts()[0]
                row_missing_summary = {
                    "row_min_null_pct": rr.get("min_null_pct", ""),
                    "row_p50_null_pct": rr.get("p50_null_pct", ""),
                    "row_p90_null_pct": rr.get("p90_null_pct", ""),
                    "row_p99_null_pct": rr.get("p99_null_pct", ""),
                    "row_max_null_pct": rr.get("max_null_pct", ""),
                }
            except Exception:
                row_missing_summary = {}

        # warnings (already descriptive)
        ws = twarn.get("warnings", []) if isinstance(twarn, dict) else []
        if ws:
            for w in ws[:100]:
                warn_rows.append({
                    "dataset_id": dataset_id,
                    "dataset_name": dataset_name,
                    "sheet_name": sheet_name,
                    "warning": str(w),
                })

        dataset_rows.append({
            "dataset_id": dataset_id,
            "source_path": source_path,
            "dataset_name": dataset_name,
            "sheet_name": sheet_name,
            "rows": int(r.get("rows") or 0),
            "rows_sample": int(r.get("rows_sample") or 0),
            "sampled": bool(r.get("sampled") or False),
            "columns": int(r.get("columns") or 0),
            "missing_cell_pct": float(r.get("missing_cell_pct") or 0.0),
            "duplicate_row_pct": float(r.get("duplicate_row_pct") or 0.0),
            "memory_bytes_estimate": int(r.get("memory_bytes_estimate") or 0),
            "numeric_cols_est": int(n_numeric),
            "text_cols_est": int(n_text),
            "bool_cols_est": int(n_bool),
            "other_cols_est": int(n_other),
            "datetime_cols_detected": int(datetime_cols),
            "min_year": min_year,
            "max_year": max_year,
            "high_uniqueness_cols": ", ".join(highuniq),
            **row_missing_summary,
        })

    # -----------------------
    # Write global tables
    # -----------------------
    sources_table = pl.DataFrame(dataset_rows).sort("rows", descending=True)
    sources_table.write_csv(str(total_dir / "sources_table.csv"))
    log.info("Wrote _total/sources_table.csv")

    dtype_df = pl.DataFrame(dtype_rows) if dtype_rows else pl.DataFrame({"dtype": [], "count": []})
    if not dtype_df.is_empty():
        global_dtype = dtype_df.group_by("dtype").agg(pl.col("count").sum().alias("count")).sort("count", descending=True)
    else:
        global_dtype = pl.DataFrame({"dtype": [], "count": []})
    global_dtype.write_csv(str(total_dir / "global_dtype_counts.csv"))
    log.info("Wrote _total/global_dtype_counts.csv")

    miss_df = pl.DataFrame(missing_hotspots) if missing_hotspots else pl.DataFrame({"dataset_id": [], "column": [], "missing_pct": []})
    if not miss_df.is_empty():
        global_miss = miss_df.sort("missing_pct", descending=True)
    else:
        global_miss = miss_df
    global_miss.head(500).write_csv(str(total_dir / "global_missing_hotspots.csv"))
    log.info("Wrote _total/global_missing_hotspots.csv")

    # high missing columns (>=80%)
    if not global_miss.is_empty():
        high_missing = global_miss.filter(pl.col("missing_pct").cast(pl.Float64, strict=False) >= 80).head(500)
    else:
        high_missing = pl.DataFrame({"dataset_id": [], "column": [], "missing_pct": []})
    high_missing.write_csv(str(total_dir / "global_high_missing_columns.csv"))
    log.info("Wrote _total/global_high_missing_columns.csv")

    const_df = pl.DataFrame(const_rows) if const_rows else pl.DataFrame({"dataset_id": [], "column": []})
    const_df.write_csv(str(total_dir / "global_constant_columns.csv"))
    log.info("Wrote _total/global_constant_columns.csv")

    hc_df = pl.DataFrame(high_card_rows) if high_card_rows else pl.DataFrame({"dataset_id": [], "column": [], "unique_ratio": []})
    hc_df.write_csv(str(total_dir / "global_high_cardinality_columns.csv"))
    log.info("Wrote _total/global_high_cardinality_columns.csv")

    temp_df = pl.DataFrame(temporal_rows) if temporal_rows else pl.DataFrame({"dataset_id": [], "min_year": [], "max_year": []})
    temp_df.write_csv(str(total_dir / "global_temporal_coverage.csv"))
    log.info("Wrote _total/global_temporal_coverage.csv")

    warn_df = pl.DataFrame(warn_rows) if warn_rows else pl.DataFrame({"dataset_id": [], "warning": []})
    warn_df.write_csv(str(total_dir / "global_quality_warnings.csv"))
    log.info("Wrote _total/global_quality_warnings.csv")

    totals = _safe_read_json(total_dir / "totals.json")

    # -----------------------
    # Build thesis-grade narrative
    # -----------------------
    # headline metrics
    ds_count = int(totals.get("datasets_processed", sources_table.height) or sources_table.height)
    total_rows = totals.get("total_rows", "")
    total_cols = totals.get("total_columns", "")

    # dataset quality snapshots
    top_by_rows = sources_table.select([
        "dataset_id", "dataset_name", "sheet_name", "rows", "rows_sample", "sampled", "columns",
        "missing_cell_pct", "duplicate_row_pct",
        "numeric_cols_est", "text_cols_est", "datetime_cols_detected",
        "min_year", "max_year"
    ]).sort("rows", descending=True)

    # aggregate missingness and duplicates distribution
    miss_stats = sources_table.select([
        pl.col("missing_cell_pct").mean().alias("avg_missing_cell_pct"),
        pl.col("missing_cell_pct").median().alias("p50_missing_cell_pct"),
        pl.col("missing_cell_pct").quantile(0.90, "nearest").alias("p90_missing_cell_pct"),
        pl.col("duplicate_row_pct").mean().alias("avg_duplicate_row_pct"),
        pl.col("duplicate_row_pct").median().alias("p50_duplicate_row_pct"),
        pl.col("duplicate_row_pct").quantile(0.90, "nearest").alias("p90_duplicate_row_pct"),
    ])

    # global schema composition percentages
    total_cols_counted = int(global_dtype.select(pl.col("count").sum()).item()) if not global_dtype.is_empty() else 0
    dtype_pct = global_dtype
    if total_cols_counted > 0:
        dtype_pct = dtype_pct.with_columns((pl.col("count") / total_cols_counted * 100.0).alias("pct")).select(["dtype", "count", "pct"])

    # -----------------------
    # Write master_summary.json
    # -----------------------
    master_json = {
        "totals": totals,
        "datasets": sources_table.to_dicts(),
        "global_dtype_counts": global_dtype.to_dicts(),
        "global_dtype_counts_pct": dtype_pct.to_dicts() if not dtype_pct.is_empty() else [],
        "missingness_stats": miss_stats.to_dicts()[0] if not miss_stats.is_empty() else {},
        "top_missing_hotspots": global_miss.head(50).to_dicts() if not global_miss.is_empty() else [],
        "high_missing_columns": high_missing.to_dicts() if not high_missing.is_empty() else [],
        "high_cardinality_columns": hc_df.head(200).to_dicts() if not hc_df.is_empty() else [],
        "constant_columns": const_df.to_dicts() if not const_df.is_empty() else [],
        "temporal_coverage": temp_df.to_dicts() if not temp_df.is_empty() else [],
        "quality_warnings": warn_df.head(200).to_dicts() if not warn_df.is_empty() else [],
        "notes": [
            "All outputs are computed automatically and are strictly descriptive (no manual mapping and no domain semantics required).",
            "Temporal coverage is derived by scanning datetime-like columns and extracting 4-digit years; it is a descriptive proxy for coverage, not a semantic guarantee.",
            "High-uniqueness and high-cardinality columns are flagged as descriptive signals relevant to data preparation and modeling decisions.",
        ],
    }
    write_json(total_dir / "master_summary.json", master_json)
    log.info("Wrote _total/master_summary.json")

    # -----------------------
    # Write master_summary.md
    # -----------------------
    md = []
    md.append("# Master Summary (Descriptive Statistics)\n")

    md.append("## Dataset inventory\n")
    md.append("- **Datasets processed**: %s" % str(ds_count))
    md.append("- **Total rows (sum across datasets)**: %s" % str(total_rows))
    md.append("- **Total columns (sum across datasets)**: %s" % str(total_cols))
    md.append("")

    md.append("## Per-dataset overview (top 20 by rows)\n")
    md.append(_df_to_md(top_by_rows, max_rows=20))

    md.append("## Schema composition (global)\n")
    if not dtype_pct.is_empty():
        md.append(_df_to_md(dtype_pct, max_rows=30))
    else:
        md.append("(no dtype information)\n")

    md.append("## Missingness and duplicates (global distribution)\n")
    if not miss_stats.is_empty():
        md.append(_df_to_md(miss_stats, max_rows=10))
    else:
        md.append("(no missingness/duplicate stats)\n")

    md.append("## Missingness hotspots (top 30 columns)\n")
    if not global_miss.is_empty():
        md.append(_df_to_md(global_miss.select(["dataset_id", "sheet_name", "column", "missing_pct"]).head(30), max_rows=30))
    else:
        md.append("(none)\n")

    md.append("## Columns with very high missingness (>= 80%)\n")
    if not high_missing.is_empty():
        md.append(_df_to_md(high_missing.select(["dataset_id", "sheet_name", "column", "missing_pct"]).head(50), max_rows=50))
    else:
        md.append("(none)\n")

    md.append("## High-cardinality text columns (descriptive signal)\n")
    if not hc_df.is_empty():
        md.append(_df_to_md(hc_df.select(["dataset_id", "sheet_name", "column", "unique_ratio", "non_null", "unique", "null_pct"]).head(50), max_rows=50))
    else:
        md.append("(none)\n")

    md.append("## Constant numeric columns (descriptive signal)\n")
    if not const_df.is_empty():
        md.append(_df_to_md(const_df.select(["dataset_id", "sheet_name", "column"]).head(50), max_rows=50))
    else:
        md.append("(none)\n")

    md.append("## Temporal coverage (from datetime-like columns)\n")
    if not temp_df.is_empty():
        md.append(_df_to_md(temp_df.sort("min_year"), max_rows=50))
    else:
        md.append("(no datetime-like columns detected)\n")

    md.append("## Quality warnings (aggregated)\n")
    if not warn_df.is_empty():
        md.append(_df_to_md(warn_df.select(["dataset_id", "sheet_name", "warning"]).head(50), max_rows=50))
    else:
        md.append("(none)\n")

    md.append("## Notes\n")
    for n in master_json["notes"]:
        md.append("- " + n)
    md.append("")

    write_text(total_dir / "master_summary.md", "\n".join(md))
    log.info("Wrote _total/master_summary.md")

    try:
        render_report_html(out_root, master_json, idx)
        log.info("Wrote _total/report.html")
    except Exception as e:
        log.exception("Failed to write _total/report.html: %s", str(e))
