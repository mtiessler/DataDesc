from datadesc.profile.base import BaseProfiler
from datadesc.writer import write_json


class OverviewProfiler(BaseProfiler):
    name = "overview"

    def run(self, ctx):
        df = ctx["df"]

        rows_sample = df.height
        rows_total = ctx.get("rows_total")
        if rows_total is None:
            rows_total = rows_sample
        cols = df.width

        missing_cells = 0
        if cols:
            for c in df.columns:
                missing_cells += df.select(df[c].null_count()).item()

        total_cells = int(rows_sample * cols)
        dup_rows = df.is_duplicated().sum() if rows_sample else 0

        try:
            mem_bytes = int(df.estimated_size())
        except Exception:
            mem_bytes = 0

        ov = {
            "rows": int(rows_total),
            "rows_total": int(rows_total),
            "rows_sample": int(rows_sample),
            "sampled": bool(ctx.get("sampled", False)),
            "columns": int(cols),
            "missing_cells": int(missing_cells),
            "missing_cell_pct": (missing_cells / total_cells * 100.0) if total_cells else 0.0,
            "duplicate_rows": int(dup_rows),
            "duplicate_row_pct": (dup_rows / rows_sample * 100.0) if rows_sample else 0.0,
            "memory_bytes_estimate": mem_bytes,
        }

        ctx["overview"] = ov
        write_json(ctx["out_dir"] / "overview.json", {
            "source_path": str(ctx["source_path"]),
            "dataset_name": ctx["dataset_name"],
            "sheet_name": ctx["sheet_name"],
            **ov,
        })
        ctx["log"].info("Wrote overview.json")
