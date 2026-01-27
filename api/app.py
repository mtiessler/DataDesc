from __future__ import annotations

import csv
import json
import shutil
import sys
import uuid
from pathlib import Path
from typing import List

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from datadesc.config import default_config
from datadesc.logger import setup_logging
from datadesc.profile.pipeline import run_pipeline

APP_NAME = "DataDesc API"
UPLOAD_ROOT = ROOT / "api_uploads"
OUTPUT_ROOT = ROOT / "api_outputs"

UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/reports", StaticFiles(directory=OUTPUT_ROOT), name="reports")


def _csv_stats(path: Path):
    rows = 0
    cols = 0
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            header = next(reader, [])
            cols = len(header)
            for _ in reader:
                rows += 1
    except Exception:
        return {"rows": 0, "cols": 0}
    return {"rows": rows, "cols": cols}


def _json_stats(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"keys": 0, "items": 0}
    if isinstance(data, dict):
        items = 0
        for v in data.values():
            if isinstance(v, list):
                items += len(v)
        return {"keys": len(data), "items": items}
    if isinstance(data, list):
        return {"keys": 0, "items": len(data)}
    return {"keys": 0, "items": 0}


def _file_stats(path: Path):
    size = path.stat().st_size if path.exists() else 0
    ext = path.suffix.lower()
    stats = {"size": size, "rows": "", "cols": "", "keys": "", "items": ""}
    if ext == ".csv":
        cs = _csv_stats(path)
        stats["rows"] = cs.get("rows", 0)
        stats["cols"] = cs.get("cols", 0)
    elif ext == ".json":
        js = _json_stats(path)
        stats["keys"] = js.get("keys", 0)
        stats["items"] = js.get("items", 0)
    return stats


def _safe_job_path(job_id: str, rel_path: str) -> Path:
    job_root = (OUTPUT_ROOT / job_id).resolve()
    target = (job_root / rel_path).resolve()
    if not str(target).startswith(str(job_root)):
        raise HTTPException(status_code=400, detail="Invalid path.")
    return target


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/profile")
def profile(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded.")

    job_id = uuid.uuid4().hex[:12]
    upload_dir = UPLOAD_ROOT / job_id
    output_dir = OUTPUT_ROOT / job_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    saved = []
    for f in files:
        filename = Path(f.filename).name
        if not filename:
            continue
        dest = upload_dir / filename
        with dest.open("wb") as out:
            shutil.copyfileobj(f.file, out)
        saved.append(filename)

    if not saved:
        raise HTTPException(status_code=400, detail="No valid files found.")

    log = setup_logging("INFO")
    cfg = default_config()

    run_pipeline(inputs=[str(upload_dir)], output_dir=str(output_dir), config=cfg, log=log)

    return JSONResponse(
        {
            "message": "Report ready",
            "job_id": job_id,
            "result_url": f"/reports/{job_id}/_total/report.html",
            "download_url": f"/jobs/{job_id}/download",
        }
    )


@app.get("/jobs/{job_id}")
def job_status(job_id: str):
    out_dir = OUTPUT_ROOT / job_id
    total_md = out_dir / "_total" / "master_summary.md"
    total_json = out_dir / "_total" / "master_summary.json"
    total_html = out_dir / "_total" / "report.html"
    return {
        "job_id": job_id,
        "exists": out_dir.exists(),
        "ready": total_md.exists(),
        "result_url": f"/reports/{job_id}/_total/report.html" if total_html.exists() else "",
        "summary_url": f"/jobs/{job_id}/summary" if total_json.exists() else "",
        "files_url": f"/jobs/{job_id}/files",
    }


@app.get("/jobs/{job_id}/summary")
def job_summary(job_id: str):
    out_dir = OUTPUT_ROOT / job_id
    total_json = out_dir / "_total" / "master_summary.json"
    if not total_json.exists():
        raise HTTPException(status_code=404, detail="Summary not found.")
    try:
        payload = json.loads(total_json.read_text(encoding="utf-8"))
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to read summary.")

    # add relative dataset_dir for frontend
    datasets = payload.get("datasets", []) if isinstance(payload, dict) else []
    cleaned = []
    for d in datasets:
        if not isinstance(d, dict):
            continue
        rel = ""
        try:
            rel = str(Path(d.get("dataset_dir", "")).resolve().relative_to(out_dir.resolve()))
        except Exception:
            rel = ""
        row = dict(d)
        row["dataset_dir_rel"] = rel
        cleaned.append(row)
    if isinstance(payload, dict):
        payload["datasets"] = cleaned
        payload["_reports_base"] = f"/reports/{job_id}"

    return JSONResponse(payload)


@app.get("/jobs/{job_id}/files")
def job_files(job_id: str):
    out_dir = OUTPUT_ROOT / job_id
    total_dir = out_dir / "_total"
    if not out_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found.")

    global_files = sorted([p for p in total_dir.glob("*") if p.is_file()])
    global_rows = []
    for p in global_files:
        st = _file_stats(p)
        global_rows.append({
            "path": str(p.relative_to(total_dir)),
            "size": st["size"],
            "rows": st["rows"],
            "cols": st["cols"],
            "keys": st["keys"],
            "items": st["items"],
        })

    dataset_sections = []
    for d in out_dir.iterdir():
        if not d.is_dir() or d.name == "_total":
            continue
        files = sorted([p for p in d.glob("*") if p.is_file()])
        rows = []
        for p in files:
            st = _file_stats(p)
            rows.append({
                "path": str(p.relative_to(d)),
                "size": st["size"],
                "rows": st["rows"],
                "cols": st["cols"],
                "keys": st["keys"],
                "items": st["items"],
            })
        dataset_sections.append({
            "dir": d.name,
            "files": rows,
        })

    return JSONResponse({
        "global": global_rows,
        "datasets": dataset_sections,
    })


@app.get("/jobs/{job_id}/file")
def job_file(job_id: str, path: str):
    if not path:
        raise HTTPException(status_code=400, detail="Missing path.")
    target = _safe_job_path(job_id, path)
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(target)


@app.get("/jobs/{job_id}/download")
def download(job_id: str):
    out_dir = OUTPUT_ROOT / job_id
    if not out_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found.")

    zip_path = OUTPUT_ROOT / f"{job_id}.zip"
    if zip_path.exists():
        zip_path.unlink()

    shutil.make_archive(str(zip_path.with_suffix("")), "zip", out_dir)
    return FileResponse(zip_path, filename=f"datadesc_{job_id}.zip")
