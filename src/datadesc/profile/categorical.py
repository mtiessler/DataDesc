import polars as pl
from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_csv, write_text


class CategoricalProfiler(BaseProfiler):
    name = "categorical"

    def run(self, ctx):
        df = ctx["df"]
        out_dir = ctx["out_dir"]
        log = ctx["log"]

        if df.height == 0 or df.width == 0:
            write_text(out_dir / "categorical_summary.csv", "")
            write_text(out_dir / "categorical_columns.csv", "")
            log.info("Empty dataset; wrote empty categorical outputs")
            return

        # read list-like detections (if present)
        listlike_cols = set()
        ll_path = out_dir / "listlike_profile.csv"
        if ll_path.exists():
            try:
                ll = pl.read_csv(str(ll_path), ignore_errors=True)
                if not ll.is_empty() and "column" in ll.columns:
                    listlike_cols = set([str(x) for x in ll["column"].to_list() if x is not None])
            except Exception:
                listlike_cols = set()

        # read schema stats (if present) for unique_ratio
        unique_ratio = {}
        schema_path = out_dir / "schema.csv"
        if schema_path.exists():
            try:
                sch = pl.read_csv(str(schema_path), ignore_errors=True)
                need = {"column", "unique", "non_null", "null_pct"}
                if not sch.is_empty() and need.issubset(set(sch.columns)):
                    sch = sch.with_columns([
                        (pl.col("unique").cast(pl.Float64, strict=False) / pl.col("non_null").cast(pl.Float64, strict=False)).alias("unique_ratio")
                    ])
                    for r in sch.select(["column", "unique_ratio", "null_pct", "non_null"]).to_dicts():
                        c = r.get("column")
                        if c is None:
                            continue
                        unique_ratio[str(c)] = {
                            "unique_ratio": r.get("unique_ratio"),
                            "null_pct": r.get("null_pct"),
                            "non_null": r.get("non_null"),
                        }
            except Exception:
                unique_ratio = {}

        # Parameters (kept internal; still "automatic")
        top_k = int(ctx.get("config", {}).get("top_k_categorical", 20) or 20)
        long_text_maxlen = 500
        high_unique_ratio = 0.98

        # Candidate categorical columns: Utf8/Categorical/Boolean (optional)
        cat_cols = []
        for c, t in zip(df.columns, df.dtypes):
            if t == pl.Utf8 or t == pl.Categorical or t == pl.Boolean:
                cat_cols.append(c)

        if not cat_cols:
            write_text(out_dir / "categorical_summary.csv", "")
            write_text(out_dir / "categorical_columns.csv", "")
            log.info("No categorical columns; wrote empty categorical outputs")
            return

        results = []
        columns_report = []

        for c in cat_cols:
            reason = None

            # skip list-like columns
            if c in listlike_cols:
                reason = "skipped_listlike"

            # skip long text (abstract-like)
            if reason is None:
                try:
                    mx = df.select(pl.col(c).cast(pl.Utf8, strict=False).str.len_chars().max()).item()
                    if mx is not None and int(mx) > long_text_maxlen:
                        reason = "skipped_long_text"
                except Exception:
                    pass

            # skip high uniqueness (ID-like)
            if reason is None and c in unique_ratio:
                ur = unique_ratio[c].get("unique_ratio")
                nn = unique_ratio[c].get("non_null")
                npct = unique_ratio[c].get("null_pct")
                try:
                    ur = float(ur) if ur is not None else None
                    nn = float(nn) if nn is not None else 0.0
                    npct = float(npct) if npct is not None else 100.0
                    if nn >= 50 and npct <= 50 and ur is not None and ur >= high_unique_ratio:
                        reason = "skipped_high_uniqueness"
                except Exception:
                    pass

            columns_report.append({
                "column": c,
                "dtype": str(df[c].dtype),
                "decision": "skipped" if reason else "profiled",
                "reason": reason or "",
            })

            if reason:
                continue

            # compute top categories
            try:
                s = df[c].cast(pl.Utf8, strict=False)

                # Replace empty/whitespace with empty marker for clearer stats
                s_clean = (
                    s.fill_null("<NULL>")
                    .str.replace_all(r"^\s+$", "", literal=False)
                    .fill_null("<NULL>")
                )

                vc = s_clean.value_counts().sort("count", descending=True).head(top_k)

                # Polars returns columns: [c, "count"] (with c having the original column name)
                # Normalize output columns
                if c not in vc.columns:
                    # rare, but guard
                    continue

                vc = vc.rename({c: "value"})
                vc = vc.with_columns([
                    pl.lit(c).alias("column"),
                    (pl.col("count").cast(pl.Float64, strict=False) / float(df.height) * 100.0).alias("pct_of_rows"),
                ]).select(["column", "value", "count", "pct_of_rows"])

                results.append(vc)

            except Exception as e:
                log.exception("categorical profiler failed for %s: %s", c, str(e))
                columns_report[-1]["decision"] = "skipped"
                columns_report[-1]["reason"] = "error"

        # Write outputs
        if results:
            out = pl.concat(results, how="vertical")
            write_csv(out_dir / "categorical_summary.csv", out)
            log.info("Wrote categorical_summary.csv")
        else:
            write_text(out_dir / "categorical_summary.csv", "")
            log.info("No categorical results; wrote empty categorical_summary.csv")

        write_csv(out_dir / "categorical_columns.csv", pl.DataFrame(columns_report))
        log.info("Wrote categorical_columns.csv")
