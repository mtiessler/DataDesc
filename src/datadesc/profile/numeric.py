import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv, write_text


class NumericProfiler(BaseProfiler):
    name = "numeric"

    def run(self, ctx):
        df = ctx["df"]
        num_cols = [c for c, t in zip(df.columns, df.dtypes) if t.is_numeric()]

        if not num_cols:
            write_text(ctx["out_dir"] / "numeric_summary.csv", "")
            ctx["log"].info("No numeric columns; wrote empty numeric_summary.csv")
            return

        rows = []
        for c in num_cols:
            s = df[c]
            rows.append({
                "column": c,
                "count": int(df.height - s.null_count()),
                "mean": float(s.mean()) if df.height else None,
                "std": float(s.std()) if df.height else None,
                "min": float(s.min()) if df.height else None,
                "25%": float(s.quantile(0.25, "nearest")) if df.height else None,
                "50%": float(s.quantile(0.50, "nearest")) if df.height else None,
                "75%": float(s.quantile(0.75, "nearest")) if df.height else None,
                "max": float(s.max()) if df.height else None,
            })

        out = pl.DataFrame(rows).sort("column")
        write_csv(ctx["out_dir"] / "numeric_summary.csv", out)
        ctx["log"].info("Wrote numeric_summary.csv")
