import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv, write_text


class RowMissingnessProfiler(BaseProfiler):
    name = "row_missingness"

    def run(self, ctx):
        df = ctx["df"]
        if df.height == 0 or df.width == 0:
            write_text(ctx["out_dir"] / "row_missingness.csv", "")
            ctx["log"].info("Empty dataset; wrote empty row_missingness.csv")
            return

        # Count nulls per row
        null_exprs = [pl.col(c).is_null().cast(pl.Int32) for c in df.columns]
        row_nulls = df.select(pl.sum_horizontal(null_exprs).alias("nulls"))

        ncols = df.width
        row_pct = row_nulls.with_columns(
            (pl.col("nulls") / ncols * 100.0).alias("null_pct")
        )

        # Summary thresholds
        thresholds = [0, 10, 25, 50, 75, 90, 100]
        rows = []
        for t in thresholds:
            cnt = row_pct.filter(pl.col("null_pct") >= t).height
            rows.append({"threshold_null_pct_ge": t, "rows": int(cnt)})

        # quantiles
        q = row_pct.select([
            pl.col("null_pct").min().alias("min_null_pct"),
            pl.col("null_pct").max().alias("max_null_pct"),
            pl.col("null_pct").mean().alias("mean_null_pct"),
            pl.col("null_pct").quantile(0.50, "nearest").alias("p50_null_pct"),
            pl.col("null_pct").quantile(0.90, "nearest").alias("p90_null_pct"),
            pl.col("null_pct").quantile(0.99, "nearest").alias("p99_null_pct"),
        ]).row(0)

        out = pl.DataFrame(rows).with_columns([
            pl.lit(float(q[0]) if q[0] is not None else 0.0).alias("min_null_pct"),
            pl.lit(float(q[1]) if q[1] is not None else 0.0).alias("max_null_pct"),
            pl.lit(float(q[2]) if q[2] is not None else 0.0).alias("mean_null_pct"),
            pl.lit(float(q[3]) if q[3] is not None else 0.0).alias("p50_null_pct"),
            pl.lit(float(q[4]) if q[4] is not None else 0.0).alias("p90_null_pct"),
            pl.lit(float(q[5]) if q[5] is not None else 0.0).alias("p99_null_pct"),
        ])

        write_csv(ctx["out_dir"] / "row_missingness.csv", out)
        ctx["log"].info("Wrote row_missingness.csv")
