import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv, write_text


SEPS = [";", "|", ","]
LONG_TEXT_MAXLEN = 500          # skip columns that look like long prose
MIN_ROWS_WITH_SEP_PCT = 20.0    # detect list-like if >= 20% rows contain a separator


class ListLikeProfiler(BaseProfiler):
    name = "listlike"

    def run(self, ctx):
        df = ctx["df"]
        out_dir = ctx["out_dir"]
        log = ctx["log"]

        if df.height == 0 or df.width == 0:
            write_text(out_dir / "listlike_profile.csv", "")
            log.info("Empty dataset; wrote empty listlike_profile.csv")
            return

        text_cols = []
        for c, t in zip(df.columns, df.dtypes):
            if t == pl.Utf8 or t == pl.Categorical:
                text_cols.append(c)

        if not text_cols:
            write_text(out_dir / "listlike_profile.csv", "")
            log.info("No text columns; wrote empty listlike_profile.csv")
            return

        rows = []

        for c in text_cols:
            s = pl.col(c).cast(pl.Utf8, strict=False)

            # skip long text fields
            try:
                mx = df.select(s.str.len_chars().max()).item()
                if mx is not None and int(mx) > LONG_TEXT_MAXLEN:
                    continue
            except Exception:
                pass

            best_sep = None
            best_pct = 0.0

            for sep in SEPS:
                try:
                    pct = df.select(s.str.contains(sep).mean()).item()
                    if pct is None:
                        continue
                    pct = float(pct) * 100.0
                    if pct > best_pct:
                        best_pct = pct
                        best_sep = sep
                except Exception:
                    continue

            if best_sep is None or best_pct < MIN_ROWS_WITH_SEP_PCT:
                continue

            # item count stats for list-like cells
            avg_items = ""
            p50_items = ""
            p90_items = ""
            max_items = ""
            try:
                items = df.select(
                    pl.when(s.is_null())
                    .then(None)
                    .otherwise(s.str.split(best_sep).list.len())
                    .alias("n_items")
                )
                q = items.select([
                    pl.col("n_items").drop_nulls().mean().alias("avg"),
                    pl.col("n_items").drop_nulls().quantile(0.50, "nearest").alias("p50"),
                    pl.col("n_items").drop_nulls().quantile(0.90, "nearest").alias("p90"),
                    pl.col("n_items").drop_nulls().max().alias("max"),
                ]).row(0)
                avg_items, p50_items, p90_items, max_items = q
            except Exception:
                pass

            rows.append({
                "column": c,
                "separator": best_sep,
                "rows_with_separator_pct": float(best_pct),
                "avg_items": avg_items,
                "p50_items": p50_items,
                "p90_items": p90_items,
                "max_items": max_items,
            })

            # token frequency top 30
            try:
                tokens = (
                    df.select(s.alias("raw"))
                    .with_columns(pl.col("raw").str.split(best_sep).alias("tok"))
                    .select(pl.col("tok").explode().cast(pl.Utf8, strict=False).str.strip_chars().alias("token"))
                    .filter(pl.col("token").is_not_null() & (pl.col("token") != ""))
                    .with_columns(pl.col("token").str.slice(0, 120))
                )
                vc = tokens.group_by("token").agg(pl.len().alias("count")).sort("count", descending=True).head(30)
                vc.write_csv(str(out_dir / ("listlike_tokens__%s.csv" % c)))
            except Exception:
                pass

        if not rows:
            write_text(out_dir / "listlike_profile.csv", "")
            log.info("No list-like columns detected; wrote empty listlike_profile.csv")
            return

        out = pl.DataFrame(rows).sort("rows_with_separator_pct", descending=True)
        write_csv(out_dir / "listlike_profile.csv", out)
        log.info("Wrote listlike_profile.csv")
