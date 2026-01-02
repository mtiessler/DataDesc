import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from datadesc.logger import setup_logging
from datadesc.profile.pipeline import run_pipeline


def parse_args():
    p = argparse.ArgumentParser(prog="DataDesc", description="Generate descriptive stats for CSV/Excel datasets (Polars).")
    p.add_argument("--inputs", nargs="+", default=["data"], help="Input directories and/or files (default: data).")
    p.add_argument("--output", default="output", help="Output directory (default: output).")
    p.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    p.add_argument("--top-k", type=int, default=10, help="Top-K categorical values per column.")
    p.add_argument("--max-corr-cols", type=int, default=80, help="Max numeric columns for correlations.")
    p.add_argument("--preview-rows", type=int, default=20, help="Preview rows to store.")
    p.add_argument("--max-unique-sample", type=int, default=200000, help="Sample cap for uniqueness checks.")
    return p.parse_args()


def main():
    args = parse_args()
    log = setup_logging(args.log_level)

    config = {
        "top_k": args.top_k,
        "max_corr_cols": args.max_corr_cols,
        "preview_rows": args.preview_rows,
        "max_unique_sample": args.max_unique_sample,
    }

    run_pipeline(
        inputs=args.inputs,
        output_dir=args.output,
        config=config,
        log=log,
    )


if __name__ == "__main__":
    main()
