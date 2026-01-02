import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_json, write_text


class QualityWarningsProfiler(BaseProfiler):
    name = "quality_warnings"

    def run(self, ctx):
        out_dir = ctx["out_dir"]
        ov = ctx.get("overview") or {}
        warnings = []

        rows = int(ov.get("rows", 0) or 0)
        cols = int(ov.get("columns", 0) or 0)
        miss_pct = float(ov.get("missing_cell_pct", 0.0) or 0.0)
        dup_pct = float(ov.get("duplicate_row_pct", 0.0) or 0.0)

        if rows == 0 or cols == 0:
            warnings.append("Dataset is empty (0 rows or 0 columns).")
        if miss_pct >= 50:
            warnings.append("High overall missingness (>= 50% missing cells).")
        if dup_pct >= 10:
            warnings.append("High duplicate row rate (>= 10%).")

        # Column-level warnings from missingness.csv if present
        miss_path = out_dir / "missingness.csv"
        if miss_path.exists():
            try:
                m = pl.read_csv(str(miss_path), ignore_errors=True)
                if "missing_pct" in m.columns and "column" in m.columns:
                    top = m.filter(pl.col("missing_pct") >= 80).head(20)
                    for r in top.iter_rows(named=True):
                        warnings.append("Column '%s' has >= 80%% missing (%.2f%%)." % (r["column"], float(r["missing_pct"])))
            except Exception:
                pass

        # Constant numeric columns from distribution_shape.csv if present
        dist_path = out_dir / "distribution_shape.csv"
        if dist_path.exists():
            try:
                d = pl.read_csv(str(dist_path), ignore_errors=True)
                if "is_constant" in d.columns and "column" in d.columns:
                    const = d.filter(pl.col("is_constant") == True).head(20)
                    for r in const.iter_rows(named=True):
                        warnings.append("Column '%s' is constant (unique<=1)." % r["column"])
            except Exception:
                pass

        write_json(out_dir / "quality_warnings.json", {"warnings": warnings})

        md = []
        md.append("# Quality Warnings\n")
        if warnings:
            for w in warnings:
                md.append("- " + w)
        else:
            md.append("- None detected.")
        md.append("")
        write_text(out_dir / "quality_warnings.md", "\n".join(md))

        ctx["log"].info("Wrote quality_warnings.json and quality_warnings.md")
