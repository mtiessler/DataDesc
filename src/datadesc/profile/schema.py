import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv


class SchemaProfiler(BaseProfiler):
    name = "schema"

    def run(self, ctx):
        df = ctx["df"]
        lf = ctx.get("lf")
        unique_mode = str(ctx.get("config", {}).get("schema_unique_mode", "approx")).lower()

        if lf is not None and unique_mode in ("approx", "exact"):
            try:
                schema = lf.schema
                cols = list(schema.keys())
                null_exprs = [pl.col(c).null_count().alias(c) for c in cols]
                if unique_mode == "approx":
                    uniq_exprs = [pl.col(c).approx_n_unique().alias(c + "__unique") for c in cols]
                else:
                    uniq_exprs = [pl.col(c).n_unique().alias(c + "__unique") for c in cols]

                stats = lf.select(null_exprs + uniq_exprs).collect(streaming=True).row(0)
                total_rows = ctx.get("rows_total")
                if total_rows is None:
                    total_rows = int(lf.select(pl.len()).collect(streaming=True).item())

                rows = []
                for i, c in enumerate(cols):
                    nulls = int(stats[i] or 0)
                    unique = stats[i + len(cols)]
                    unique = int(unique) if unique is not None else 0
                    rows.append({
                        "column": c,
                        "dtype": str(schema[c]),
                        "non_null": int(total_rows - nulls),
                        "null": nulls,
                        "null_pct": float((nulls / total_rows * 100.0) if total_rows else 0.0),
                        "unique": unique,
                    })

                out = pl.DataFrame(rows).sort("column")
                write_csv(ctx["out_dir"] / "schema.csv", out)
                ctx["log"].info("Wrote schema.csv (%s unique, full scan)", unique_mode)
                return
            except Exception:
                pass

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
