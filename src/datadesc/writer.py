import json
from pathlib import Path


def ensure_dir(p):
    Path(p).mkdir(parents=True, exist_ok=True)


def write_json(path, obj):
    Path(path).write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_text(path, text):
    Path(path).write_text(text, encoding="utf-8")


def write_csv(path, df):
    # Polars DataFrame
    df.write_csv(str(path))
