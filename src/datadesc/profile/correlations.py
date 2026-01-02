import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv, write_text, write_json


class CorrelationProfiler(BaseProfiler):
    name = "correlations"

    def run(self, ctx):
        df = ctx["df"]
        max_cols = int(ctx["config"].get("max_corr_cols", 80))

        num_cols = [c for c, t in zip(df.columns, df.dtypes) if t.is_numeric()]
        note = None

        if len(num_cols) < 2:
            note = "Not enough numeric columns for correlation."
            write_text(ctx["out_dir"] / "correlations.csv", "")
        elif len(num_cols) > max_cols:
            note = "Skipped correlation: %d numeric columns exceeds limit (%d)." % (len(num_cols), max_cols)
            write_text(ctx["out_dir"] / "correlations.csv", "")
        else:
            cols = num_cols
            rows = []
            for a in cols:
                row = {"column": a}
                for b in cols:
                    try:
                        v = df.select(pl.corr(pl.col(a), pl.col(b))).item()
                    except Exception:
                        v = None
                    row[b] = v
                rows.append(row)

            out = pl.DataFrame(rows)
            write_csv(ctx["out_dir"] / "correlations.csv", out)

        if note:
            write_json(ctx["out_dir"] / "correlations_note.json", {"note": note})
            ctx["log"].info("Correlation note: %s", note)
        else:
            ctx["log"].info("Wrote correlations.csv")
