import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv, write_text


class TextProfileProfiler(BaseProfiler):
    name = "text_profile"

    def run(self, ctx):
        df = ctx["df"]

        text_cols = []
        for c, t in zip(df.columns, df.dtypes):
            if t == pl.Utf8 or t == pl.Categorical:
                text_cols.append(c)

        if not text_cols:
            write_text(ctx["out_dir"] / "text_profile.csv", "")
            ctx["log"].info("No text columns; wrote empty text_profile.csv")
            return

        rows = []
        for c in text_cols:
            s = df[c].cast(pl.Utf8, strict=False)

            # nulls/empties/whitespace
            nulls = int(df[c].null_count())
            non_null = int(df.height - nulls)

            empty_cnt = 0
            ws_cnt = 0
            try:
                empty_cnt = df.select((pl.col(c).cast(pl.Utf8, strict=False) == "").sum()).item()
            except Exception:
                empty_cnt = 0

            try:
                ws_cnt = df.select(
                    pl.col(c).cast(pl.Utf8, strict=False).str.strip_chars().eq("").sum()
                ).item()
            except Exception:
                ws_cnt = 0

            # length stats
            avg_len = ""
            min_len = ""
            max_len = ""
            p50 = ""
            p90 = ""

            try:
                lens = s.drop_nulls().str.len_chars()
                if lens.len() > 0:
                    avg_len = float(lens.mean())
                    min_len = int(lens.min())
                    max_len = int(lens.max())
                    p50 = float(lens.quantile(0.50, "nearest"))
                    p90 = float(lens.quantile(0.90, "nearest"))
            except Exception:
                pass

            rows.append({
                "column": c,
                "non_null": non_null,
                "null": nulls,
                "empty_string": int(empty_cnt),
                "whitespace_only": int(ws_cnt),
                "avg_len": avg_len,
                "min_len": min_len,
                "p50_len": p50,
                "p90_len": p90,
                "max_len": max_len,
            })

        out = pl.DataFrame(rows).sort("column")
        write_csv(ctx["out_dir"] / "text_profile.csv", out)
        ctx["log"].info("Wrote text_profile.csv")
