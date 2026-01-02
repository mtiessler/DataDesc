import re
import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv, write_text


YEAR_RE = r"(19\d{2}|20\d{2})"


class DatetimeProfileProfiler(BaseProfiler):
    name = "datetime_profile"

    def run(self, ctx):
        df = ctx["df"]
        if df.width == 0:
            write_text(ctx["out_dir"] / "datetime_profile.csv", "")
            ctx["log"].info("Empty dataset; wrote empty datetime_profile.csv")
            return

        # Candidate columns by name (purely automatic)
        candidates = []
        for c in df.columns:
            name = str(c).lower()
            if any(k in name for k in ["year", "date", "datum", "start", "end", "from", "to", "published", "created", "updated"]):
                candidates.append(c)

        if not candidates:
            write_text(ctx["out_dir"] / "datetime_profile.csv", "")
            ctx["log"].info("No datetime-like columns; wrote empty datetime_profile.csv")
            return

        rows = []
        for c in candidates:
            # Extract 4-digit years from the full column (robust across formats)
            try:
                year_expr = (
                    pl.col(c)
                    .cast(pl.Utf8, strict=False)
                    .str.extract(YEAR_RE, 1)
                    .cast(pl.Int32, strict=False)
                )

                stats = df.select([
                    year_expr.min().alias("min_year"),
                    year_expr.max().alias("max_year"),
                    year_expr.drop_nulls().len().alias("year_hits"),
                ]).row(0)

                rows.append({
                    "column": c,
                    "min_year": stats[0] if stats[0] is not None else "",
                    "max_year": stats[1] if stats[1] is not None else "",
                    "year_hits": int(stats[2]) if stats[2] is not None else 0,
                })
            except Exception:
                rows.append({"column": c, "min_year": "", "max_year": "", "year_hits": 0})

        out = pl.DataFrame(rows).sort("column")
        write_csv(ctx["out_dir"] / "datetime_profile.csv", out)
        ctx["log"].info("Wrote datetime_profile.csv")
