import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv, write_json, write_text


class KeyDuplicatesProfiler(BaseProfiler):
    name = "key_duplicates"

    def run(self, ctx):
        df = ctx["df"]
        out_dir = ctx["out_dir"]
        log = ctx["log"]

        schema_path = out_dir / "schema.csv"
        if not schema_path.exists():
            write_text(out_dir / "key_duplicates.csv", "")
            log.info("No schema.csv; wrote empty key_duplicates.csv")
            return

        try:
            sch = pl.read_csv(str(schema_path), ignore_errors=True)
        except Exception:
            write_text(out_dir / "key_duplicates.csv", "")
            log.info("Failed to read schema.csv; wrote empty key_duplicates.csv")
            return

        need = {"column", "unique", "non_null", "null_pct"}
        if not need.issubset(set(sch.columns)):
            write_text(out_dir / "key_duplicates.csv", "")
            log.info("schema.csv missing fields; wrote empty key_duplicates.csv")
            return

        sch = sch.with_columns([
            (pl.col("unique").cast(pl.Float64, strict=False) / pl.col("non_null").cast(pl.Float64, strict=False)).alias("unique_ratio")
        ])

        # Purely statistical selection of key-like columns
        candidates = (
            sch.filter(
                (pl.col("non_null") >= 50) &
                (pl.col("null_pct") <= 30) &
                (pl.col("unique_ratio") >= 0.98)
            )
            .sort("unique_ratio", descending=True)
            .select("column")
            .to_series()
            .to_list()
        )

        # Add mild generic hint: columns containing "id" / "doi" / "orcid" etc (NOT dataset-specific)
        generic_hint = []
        for c in df.columns:
            n = str(c).lower()
            if any(k in n for k in [" id", "id ", "_id", "id_", "doi", "orcid", "uuid", "guid", "issn", "isbn"]):
                generic_hint.append(c)

        merged = []
        for c in candidates + generic_hint:
            if c in df.columns and c not in merged:
                merged.append(c)
        merged = merged[:5]

        if not merged:
            write_text(out_dir / "key_duplicates.csv", "")
            log.info("No key candidates; wrote empty key_duplicates.csv")
            return

        rows = []
        summary = {"candidates": merged, "by_key": []}

        for key in merged:
            try:
                s = df[key]
                nn = int(df.height - s.null_count())
                if nn < 10:
                    continue

                grp = (
                    df.filter(pl.col(key).is_not_null())
                    .group_by(key)
                    .agg(pl.len().alias("rows_per_key"))
                    .sort("rows_per_key", descending=True)
                )

                unique_keys = int(grp.height)
                total_rows = int(df.height)
                avg_rows_per_key = float(total_rows / unique_keys) if unique_keys else 0.0
                max_rows_per_key = int(grp.select(pl.col("rows_per_key").max()).item())

                repeated_keys = int(grp.filter(pl.col("rows_per_key") > 1).height)
                repeated_rows = int(grp.filter(pl.col("rows_per_key") > 1).select(pl.col("rows_per_key").sum()).item() or 0)

                rows.append({
                    "key_column": key,
                    "unique_keys": unique_keys,
                    "avg_rows_per_key": avg_rows_per_key,
                    "max_rows_per_key": max_rows_per_key,
                    "repeated_keys": repeated_keys,
                    "rows_in_repeated_keys": repeated_rows,
                    "repeated_keys_pct": float(repeated_keys / unique_keys * 100.0) if unique_keys else 0.0,
                })

                # write top repeated keys
                top = grp.head(10).select([
                    pl.col(key).cast(pl.Utf8, strict=False).alias("key"),
                    "rows_per_key"
                ])
                top.write_csv(str(out_dir / ("key_duplicates_top__%s.csv" % str(key))))

                summary["by_key"].append({
                    "key_column": key,
                    "unique_keys": unique_keys,
                    "avg_rows_per_key": avg_rows_per_key,
                    "max_rows_per_key": max_rows_per_key,
                })

            except Exception as e:
                log.exception("key_duplicates failed for %s: %s", key, str(e))

        if not rows:
            write_text(out_dir / "key_duplicates.csv", "")
            log.info("No key duplicate results; wrote empty key_duplicates.csv")
            return

        out = pl.DataFrame(rows).sort("avg_rows_per_key", descending=True)
        write_csv(out_dir / "key_duplicates.csv", out)
        write_json(out_dir / "key_duplicates_summary.json", summary)
        log.info("Wrote key_duplicates.csv and key_duplicates_summary.json")
