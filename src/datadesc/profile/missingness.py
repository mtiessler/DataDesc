import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv


class MissingnessProfiler(BaseProfiler):
    name = "missingness"

    def run(self, ctx):
        df = ctx["df"]
        rows = []

        for c in df.columns:
            nulls = int(df[c].null_count())
            pct = float((nulls / df.height * 100.0) if df.height else 0.0)
            rows.append({"column": c, "missing": nulls, "missing_pct": pct})

        out = pl.DataFrame(rows).sort(["missing", "column"], descending=[True, False])
        write_csv(ctx["out_dir"] / "missingness.csv", out)
        ctx["log"].info("Wrote missingness.csv")
