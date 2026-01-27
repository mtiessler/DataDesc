from pathlib import Path
import hashlib
import polars as pl

from datadesc.discover import discover_sources
from datadesc.loaders import load_datasets
from datadesc.writer import ensure_dir, write_json, write_text, write_csv
from datadesc.profile import get_profilers
from datadesc.profile.total_summary import generate_total_summary


def _count_rows_lazy(lf):
    try:
        return int(lf.select(pl.len()).collect(streaming=True).item())
    except Exception:
        return None


def _collect_sample(lf, cap, strategy, seed):
    if cap is None or int(cap) <= 0:
        return lf.collect(streaming=True)
    if strategy == "random":
        return lf.sample(n=int(cap), seed=int(seed)).collect(streaming=True)
    return lf.head(int(cap)).collect(streaming=True)


def dataset_id(path, sheet):
    extra = "sheet=%s" % (sheet if sheet is not None else "None")
    key = (str(Path(path).resolve()) + "::" + extra).encode("utf-8")
    return hashlib.sha1(key).hexdigest()[:8]


def slug(s):
    s = (s or "").strip()
    if not s:
        return "dataset"
    out = []
    for ch in s:
        if ch.isalnum() or ch in ("-", "_"):
            out.append(ch)
        elif ch.isspace():
            out.append("_")
    return "".join(out)[:60] or "dataset"


def write_report_md(out_dir, dataset_title, overview, notes):
    md = []
    md.append("# %s\n" % dataset_title)
    md.append("## Overview\n")

    if overview:
        for k in ["rows", "rows_sample", "sampled", "columns", "missing_cells", "missing_cell_pct", "duplicate_rows", "duplicate_row_pct", "memory_bytes_estimate"]:
            md.append("- **%s**: %s" % (k, overview.get(k)))
    else:
        md.append("- (overview unavailable)")

    if notes:
        md.append("\n## Notes\n")
        for n in notes:
            md.append("- %s" % n)

    md.append("")
    write_text(out_dir / "report.md", "\n".join(md))


def run_pipeline(inputs, output_dir, config, log):
    out_root = Path(output_dir)
    ensure_dir(out_root)

    files = discover_sources(inputs, log)
    profilers = get_profilers()
    log.info("Enabled profilers: %s", ", ".join([p.name for p in profilers]))

    totals = {
        "files_found": int(len(files)),
        "datasets_processed": 0,
        "total_rows": 0,
        "total_columns": 0,
    }
    index_rows = []

    for f in files:
        for ds in load_datasets(f, log, config=config):
            lf = ds.get("lf")
            df = ds.get("df")
            name = ds["name"]
            sheet = ds["sheet"]
            sid = dataset_id(ds["path"], sheet)

            sheet_slug = slug(sheet) if sheet is not None else "sheet-None"
            out_dir = out_root / ("%s__%s__%s" % (sid, slug(name), sheet_slug))
            ensure_dir(out_dir)

            log.info("Processing: %s (sheet=%s)", ds["path"].name, sheet if sheet is not None else "None")

            sample_rows = int(config.get("sample_rows", 200000))
            sample_strategy = str(config.get("sample_strategy", "head")).lower()
            sample_seed = int(config.get("sample_seed", 42))
            sampled = False
            rows_total = None

            if lf is not None:
                rows_total = _count_rows_lazy(lf)
                if rows_total is not None and rows_total > sample_rows:
                    sampled = True
                df = _collect_sample(lf, sample_rows, sample_strategy, sample_seed)
            else:
                if df is None:
                    df = pl.DataFrame()
                rows_total = df.height
                if ds.get("truncated"):
                    sampled = True

            ctx = {
                "df": df,
                "lf": lf,
                "out_dir": out_dir,
                "source_path": ds["path"],
                "dataset_name": name,
                "sheet_name": sheet,
                "config": config,
                "log": log,
                "overview": None,
                "rows_total": rows_total,
                "sampled": sampled,
            }

            notes = []
            if sampled:
                if rows_total is not None and rows_total > 0:
                    notes.append(
                        "Profiled on a sample of %d rows (of %d total) using strategy=%s."
                        % (int(df.height), int(rows_total), sample_strategy)
                    )
                else:
                    notes.append(
                        "Profiled on a sample of %d rows using strategy=%s."
                        % (int(df.height), sample_strategy)
                    )
            for p in profilers:
                try:
                    p.run(ctx)
                except Exception as e:
                    msg = "Profiler '%s' failed: %s" % (p.name, str(e))
                    log.exception(msg)
                    notes.append(msg)

            preview_rows = int(config.get("preview_rows", 20))
            try:
                prev = df.head(preview_rows)
                write_csv(out_dir / "preview.csv", prev)
                log.info("Wrote preview.csv")
            except Exception as e:
                log.exception("Failed to write preview.csv: %s", str(e))
                notes.append("Failed to write preview.csv: %s" % str(e))

            title = name if sheet is None else "%s (sheet: %s)" % (name, sheet)
            write_report_md(out_dir, title, ctx.get("overview"), notes)

            ov = ctx.get("overview") or {"rows": 0, "columns": 0}
            totals["datasets_processed"] += 1
            totals["total_rows"] += int(ov.get("rows", 0))
            totals["total_columns"] += int(ov.get("columns", 0))

            index_rows.append({
                "dataset_id": sid,
                "dataset_dir": str(out_dir),
                "source_path": str(ds["path"]),
                "dataset_name": name,
                "sheet_name": sheet if sheet is not None else "",
                "rows": int(ov.get("rows", 0)),
                "rows_sample": int(ov.get("rows_sample", 0)),
                "sampled": bool(ov.get("sampled", False)),
                "columns": int(ov.get("columns", 0)),
                "missing_cell_pct": float((ctx.get("overview") or {}).get("missing_cell_pct", 0.0)),
                "duplicate_row_pct": float((ctx.get("overview") or {}).get("duplicate_row_pct", 0.0)),
                "memory_bytes_estimate": int((ctx.get("overview") or {}).get("memory_bytes_estimate", 0)),
            })

    total_dir = out_root / "_total"
    ensure_dir(total_dir)

    pl.DataFrame(index_rows).write_csv(str(total_dir / "datasets_index.csv"))
    write_json(total_dir / "totals.json", totals)

    md = []
    md.append("# Total Summary\n")
    for k, v in totals.items():
        md.append("- **%s**: %s" % (k, v))
    md.append("")
    write_text(total_dir / "totals.md", "\n".join(md))

    try:
        generate_total_summary(out_root, log)
    except Exception as e:
        log.exception("Failed to generate total thesis summary: %s", str(e))

    log.info("Done. Datasets processed: %d", totals["datasets_processed"])
    log.info("Output written to: %s", out_root.resolve())
    return totals
