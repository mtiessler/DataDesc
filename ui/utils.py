import io
import json
import zipfile
from pathlib import Path
import polars as pl


def safe_read_csv(path):
    path = Path(path)
    if not path.exists():
        return None
    try:
        return pl.read_csv(str(path), ignore_errors=True)
    except Exception:
        return None


def safe_read_text(path):
    path = Path(path)
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        try:
            return path.read_text()
        except Exception:
            return None


def safe_read_json(path):
    txt = safe_read_text(path)
    if not txt:
        return None
    try:
        return json.loads(txt)
    except Exception:
        return None


def zip_dir(folder):
    folder = Path(folder)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in folder.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(folder))
    buf.seek(0)
    return buf


def fmt_bytes(n):
    try:
        n = int(n)
    except Exception:
        return ""
    if n < 1024:
        return str(n) + " B"
    for unit in ["KB", "MB", "GB", "TB"]:
        n = n / 1024.0
        if n < 1024:
            return "%.2f %s" % (n, unit)
    return "%.2f PB" % n


def df_to_pandas_safe(df):
    try:
        return df.to_pandas()
    except Exception:
        return None


def st_table(st, df, height=520):
    if df is None:
        st.info("Not available.")
        return
    if hasattr(df, "is_empty") and df.is_empty():
        st.info("Empty.")
        return

    pdf = df_to_pandas_safe(df)
    st.dataframe(pdf if pdf is not None else df.to_dicts(), use_container_width=True, height=height)


def list_token_files(dataset_dir):
    dataset_dir = Path(dataset_dir)
    files = sorted([p for p in dataset_dir.glob("listlike_tokens__*.csv") if p.is_file()])
    return files
