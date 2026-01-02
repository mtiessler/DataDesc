import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv


class SchemaProfiler(BaseProfiler):
    name = "schema"

    def run(self, ctx):
        df = ctx["df"]

        rows = []
        for c in df.columns:
            s = df[c]
            nulls = int(s.null_count())
            rows.append({
                "column": c,
                "dtype": str(s.dtype),
                "non_null": int(df.height - nulls),
                "null": nulls,
                "null_pct": float((nulls / df.height * 100.0) if df.height else 0.0),
                "unique": int(s.n_unique()),
            })

        out = pl.DataFrame(rows).sort("column")
        write_csv(ctx["out_dir"] / "schema.csv", out)
        ctx["log"].info("Wrote schema.csv")
