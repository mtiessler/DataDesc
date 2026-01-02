import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv


class UniquenessProfiler(BaseProfiler):
    name = "uniqueness"

    def run(self, ctx):
        df = ctx["df"]
        cap = int(ctx["config"].get("max_unique_sample", 200000))

        work = df
        if df.height > cap:
            work = df.sample(n=cap, seed=42)

        rows = []
        for c in work.columns:
            s = work[c]
            rows.append({
                "column": c,
                "unique_non_null": int(s.drop_nulls().n_unique()),
                "unique_including_null_as_value": int(s.fill_null("__NULL__").n_unique()) if s.dtype == pl.Utf8 else int(s.fill_null(-999999999).n_unique()),
            })

        out = pl.DataFrame(rows).sort("column")
        write_csv(ctx["out_dir"] / "uniqueness.csv", out)
        ctx["log"].info("Wrote uniqueness.csv (sampled=%s)", "yes" if work is not df else "no")
