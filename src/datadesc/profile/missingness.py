import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv


class MissingnessProfiler(BaseProfiler):
    name = "missingness"

    def run(self, ctx):
        df = ctx["df"]
        rows = []

        lf = ctx.get("lf")
        if lf is not None:
            try:
                cols = lf.columns
                null_exprs = [pl.col(c).null_count().alias(c) for c in cols]
                counts = lf.select(null_exprs).collect(streaming=True).row(0)
                total_rows = ctx.get("rows_total")
                if total_rows is None:
                    total_rows = int(lf.select(pl.len()).collect(streaming=True).item())

                for c, nulls in zip(cols, counts):
                    nulls = int(nulls or 0)
                    pct = float((nulls / total_rows * 100.0) if total_rows else 0.0)
                    rows.append({"column": c, "missing": nulls, "missing_pct": pct})

                out = pl.DataFrame(rows).sort(["missing", "column"], descending=[True, False])
                write_csv(ctx["out_dir"] / "missingness.csv", out)
                ctx["log"].info("Wrote missingness.csv (full scan)")
                return
            except Exception:
                rows = []

        for c in df.columns:
            nulls = int(df[c].null_count())
            pct = float((nulls / df.height * 100.0) if df.height else 0.0)
            rows.append({"column": c, "missing": nulls, "missing_pct": pct})

        out = pl.DataFrame(rows).sort(["missing", "column"], descending=[True, False])
        write_csv(ctx["out_dir"] / "missingness.csv", out)
        ctx["log"].info("Wrote missingness.csv")
