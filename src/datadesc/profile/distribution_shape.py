import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv, write_text


class DistributionShapeProfiler(BaseProfiler):
    name = "distribution_shape"

    def run(self, ctx):
        df = ctx["df"]
        num_cols = [c for c, t in zip(df.columns, df.dtypes) if t.is_numeric()]

        if not num_cols:
            write_text(ctx["out_dir"] / "distribution_shape.csv", "")
            ctx["log"].info("No numeric columns; wrote empty distribution_shape.csv")
            return

        rows = []
        for c in num_cols:
            s = df[c]
            nn = int(df.height - s.null_count())
            if nn <= 0:
                rows.append({
                    "column": c,
                    "non_null": 0,
                    "unique": int(s.n_unique()),
                    "unique_ratio": 0.0,
                    "zero_pct": "",
                    "approx_skew": "",
                    "is_constant": True,
                })
                continue

            unique = int(s.n_unique())
            unique_ratio = float(unique / nn) if nn else 0.0

            # zero pct (only if comparable)
            zero_pct = ""
            try:
                zero_cnt = df.select((pl.col(c) == 0).sum()).item()
                zero_pct = float(zero_cnt / nn * 100.0) if nn else 0.0
            except Exception:
                zero_pct = ""

            # approximate skew = (mean - median)/std
            approx_skew = ""
            try:
                mean = s.mean()
                median = s.median()
                std = s.std()
                if std and std != 0:
                    approx_skew = float((mean - median) / std)
                else:
                    approx_skew = 0.0
            except Exception:
                approx_skew = ""

            is_constant = (unique <= 1)

            rows.append({
                "column": c,
                "non_null": nn,
                "unique": unique,
                "unique_ratio": unique_ratio,
                "zero_pct": zero_pct,
                "approx_skew": approx_skew,
                "is_constant": is_constant,
            })

        out = pl.DataFrame(rows).sort("column")
        write_csv(ctx["out_dir"] / "distribution_shape.csv", out)
        ctx["log"].info("Wrote distribution_shape.csv")
