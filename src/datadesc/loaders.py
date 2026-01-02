import polars as pl
from openpyxl import load_workbook


def load_csv(path, log):
    log.info("Loading CSV: %s", path)
    return pl.read_csv(path)


def _make_unique_headers(header):
    seen = {}
    out = []
    for i, h in enumerate(header):
        h = "" if h is None else str(h).strip()
        if not h:
            h = "col_%d" % (i + 1)

        if h not in seen:
            seen[h] = 0
            out.append(h)
        else:
            seen[h] += 1
            out.append("%s__%d" % (h, seen[h]))
    return out


def _sheet_to_frame(ws, log, path, sheet_name):
    rows_iter = ws.iter_rows(values_only=True)

    # Find first non-empty row as header
    header = None
    for r in rows_iter:
        if r is None:
            continue
        if any(x is not None and str(x).strip() != "" for x in r):
            header = list(r)
            break

    if header is None:
        log.warning("Empty sheet: %s (sheet=%s)", path.name, sheet_name)
        return pl.DataFrame()

    header = _make_unique_headers(header)
    n = len(header)

    data = []
    for r in rows_iter:
        if r is None:
            continue
        r = list(r)
        if len(r) < n:
            r = r + [None] * (n - len(r))
        elif len(r) > n:
            r = r[:n]
        data.append(r)

    # If there are no data rows, still return empty DF with columns
    if not data:
        return pl.DataFrame(schema=header)

    # Column-wise data
    cols = list(zip(*data))
    columns = {}
    for i in range(n):
        values = list(cols[i])

        # strict=False allows mixed types within a column
        try:
            columns[header[i]] = pl.Series(header[i], values, strict=False)
        except Exception as e:
            # worst case: coerce everything to string
            log.warning(
                "Column conversion fallback to string: file=%s sheet=%s col=%s err=%s",
                path.name, sheet_name, header[i], str(e)
            )
            columns[header[i]] = pl.Series(header[i], [None if v is None else str(v) for v in values], strict=False)

    return pl.DataFrame(columns)


def excel_to_frames(path, log):
    log.info("Loading Excel: %s", path)
    wb = load_workbook(filename=path, read_only=True, data_only=True)

    out = {}
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        try:
            out[sheet_name] = _sheet_to_frame(ws, log, path, sheet_name)
        except Exception as e:
            log.exception("Failed to parse sheet: file=%s sheet=%s err=%s", path.name, sheet_name, str(e))
            out[sheet_name] = pl.DataFrame()

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
