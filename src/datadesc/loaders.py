import polars as pl
from openpyxl import load_workbook


def load_csv(path, log):
    log.info("Loading CSV: %s", path)
    return pl.read_csv(path)


def excel_to_frames(path, log):
    log.info("Loading Excel: %s", path)
    wb = load_workbook(filename=path, read_only=True, data_only=True)
    out = {}

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = ws.iter_rows(values_only=True)

        try:
            header = next(rows)
        except StopIteration:
            out[sheet_name] = pl.DataFrame()
            continue

        header = [str(x) if x is not None else "" for x in header]
        data = list(rows)

        # Normalize row lengths
        n = len(header)
        norm = []
        for r in data:
            r = list(r) if r is not None else []
            if len(r) < n:
                r = r + [None] * (n - len(r))
            elif len(r) > n:
                r = r[:n]
            norm.append(r)

        if n == 0:
            out[sheet_name] = pl.DataFrame()
        else:
            cols = list(zip(*norm)) if norm else [[] for _ in range(n)]
            out[sheet_name] = pl.DataFrame({header[i]: list(cols[i]) for i in range(n)})

    return out


def load_datasets(path, log):
    path = path.resolve()
    suffix = path.suffix.lower()

    if suffix == ".csv":
        df = load_csv(path, log)
        yield {"path": path, "name": path.stem, "sheet": None, "df": df}
        return

    if suffix in (".xlsx", ".xls"):
        sheets = excel_to_frames(path, log)
        for sheet_name, df in sheets.items():
            yield {"path": path, "name": path.stem, "sheet": str(sheet_name), "df": df}
        return

    log.warning("Unsupported file type: %s", path)
