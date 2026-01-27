from __future__ import annotations

import csv
import html
import json
from pathlib import Path


def _escape(val):
    return html.escape(str(val)) if val is not None else ""


def _relpath_or_empty(path, root):
    try:
        return str(Path(path).resolve().relative_to(Path(root).resolve()))
    except Exception:
        return ""


def _csv_stats(path):
    rows = 0
    cols = 0
    try:
        with Path(path).open("r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            header = next(reader, [])
            cols = len(header)
            for _ in reader:
                rows += 1
    except Exception:
        return {"rows": 0, "cols": 0}
    return {"rows": rows, "cols": cols}


def _json_stats(path):
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
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


def _file_stats(path):
    p = Path(path)
    size = p.stat().st_size if p.exists() else 0
    ext = p.suffix.lower()
    stats = {"size": size, "rows": "", "cols": "", "keys": "", "items": ""}
    if ext == ".csv":
        cs = _csv_stats(p)
        stats["rows"] = cs.get("rows", 0)
        stats["cols"] = cs.get("cols", 0)
    elif ext == ".json":
        js = _json_stats(p)
        stats["keys"] = js.get("keys", 0)
        stats["items"] = js.get("items", 0)
    return stats


def render_report_html(out_root, summary, datasets_index):
    out_root = Path(out_root)
    total_dir = out_root / "_total"
    total_dir.mkdir(parents=True, exist_ok=True)

    totals = summary.get("totals", {}) if isinstance(summary, dict) else {}
    missing_stats = summary.get("missingness_stats", {})

    def fmt_num(x):
        try:
            return f"{int(x):,}"
        except Exception:
            try:
                return f"{float(x):,.2f}"
            except Exception:
                return str(x) if x is not None else ""

    def pct(x):
        try:
            return f"{float(x):.2f}%"
        except Exception:
            return ""

    headline = {
        "datasets": fmt_num(totals.get("datasets_processed")),
        "rows": fmt_num(totals.get("total_rows")),
        "cols": fmt_num(totals.get("total_columns")),
        "avg_missing": pct(missing_stats.get("avg_missing_cell_pct")),
    }

    payload = dict(summary) if isinstance(summary, dict) else {}
    datasets = payload.get("datasets", [])
    cleaned = []
    for d in datasets:
        if not isinstance(d, dict):
            continue
        rel_dir = _relpath_or_empty(d.get("dataset_dir"), out_root)
        row = dict(d)
        row["dataset_dir_rel"] = rel_dir
        cleaned.append(row)
    payload["datasets"] = cleaned
    payload["_base"] = ".."  # from _total/report.html to job root

    # Build artifacts section
    global_files = sorted([p for p in total_dir.glob("*") if p.is_file()])
    global_rows = []
    for p in global_files:
        st = _file_stats(p)
        global_rows.append({
            "path": _relpath_or_empty(p, total_dir),
            "size": st["size"],
            "rows": st["rows"],
            "cols": st["cols"],
            "keys": st["keys"],
            "items": st["items"],
        })

    dataset_artifacts = []
    for d in cleaned:
        rel_dir = d.get("dataset_dir_rel")
        if not rel_dir:
            continue
        pdir = out_root / rel_dir
        files = sorted([p for p in pdir.glob("*") if p.is_file()])
        rows = []
        for p in files:
            st = _file_stats(p)
            rows.append({
                "path": _relpath_or_empty(p, pdir),
                "size": st["size"],
                "rows": st["rows"],
                "cols": st["cols"],
                "keys": st["keys"],
                "items": st["items"],
            })
        dataset_artifacts.append({
            "dataset_id": d.get("dataset_id"),
            "dataset_name": d.get("dataset_name"),
            "sheet_name": d.get("sheet_name"),
            "dir": rel_dir,
            "files": rows,
        })

    def render_file_table(rows, base_path=""):
        if not rows:
            return "<p class='muted'>No files.</p>"
        trs = []
        for r in rows:
            link = f"{base_path}/{r['path']}" if base_path else r["path"]
            trs.append(
                "<tr>"
                f"<td><a class='link' href='{_escape(link)}' target='_blank'>{_escape(r['path'])}</a></td>"
                f"<td>{fmt_num(r['size'])}</td>"
                f"<td>{fmt_num(r['rows'])}</td>"
                f"<td>{fmt_num(r['cols'])}</td>"
                f"<td>{fmt_num(r['keys'])}</td>"
                f"<td>{fmt_num(r['items'])}</td>"
                "</tr>"
            )
        return (
            "<table><thead><tr>"
            "<th>File</th><th>Size (B)</th><th>Rows</th><th>Cols</th><th>JSON Keys</th><th>JSON Items</th>"
            "</tr></thead><tbody>" + "".join(trs) + "</tbody></table>"
        )

    global_files_html = render_file_table(global_rows, base_path=".")

    dataset_sections = []
    for d in dataset_artifacts:
        title = d.get("dataset_name") or d.get("dataset_id") or "dataset"
        sheet = d.get("sheet_name") or ""
        summary_line = f"{_escape(title)}{(' | ' + _escape(sheet)) if sheet else ''}"
        dataset_sections.append(
            "<details class='card' open>"
            f"<summary class='mini'>{summary_line}</summary>"
            + render_file_table(d.get("files", []), base_path=f"../{d.get('dir')}")
            + "</details>"
        )

    artifacts_html = (
        "<div class='section card'><h2>All generated files</h2>"
        "<h3>Global outputs (_total)</h3>"
        + global_files_html
        + "<h3 style='margin-top:16px;'>Per-dataset outputs</h3>"
        + "".join(dataset_sections)
        + "</div>"
    )

    summary_json = json.dumps(payload).replace("</", "<\\/")

    template = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DataDesc Report</title>
  <style>
    :root {
      --bg: #0b0d13; --card: #151a26; --muted: #a5acba; --accent: #8ef0c4; --accent-2: #f7b35b;
      --border: rgba(255,255,255,0.08); --text: #f7f6f2; --shadow: 0 14px 40px rgba(0,0,0,0.35);
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: ui-sans-serif, system-ui; background: var(--bg); color: var(--text); }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 48px 28px 80px; }
    .hero { display: flex; justify-content: space-between; align-items: center; gap: 24px; }
    h1 { margin: 0; font-size: 2.4rem; }
    h2 { margin: 0 0 12px; font-size: 1.4rem; }
    h3 { margin: 0 0 10px; }
    .muted { color: var(--muted); }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin: 28px 0; }
    .card { background: var(--card); border-radius: 16px; padding: 18px; border: 1px solid var(--border); box-shadow: var(--shadow); }
    .metric { font-size: 1.3rem; font-weight: 600; }
    .section { margin-top: 28px; }
    .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
    canvas { width: 100%; height: 220px; }
    table { width: 100%; border-collapse: collapse; font-size: 0.95rem; }
    th, td { text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--border); }
    .filters { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 12px; }
    input, select, button { background: #0f1320; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 10px; }
    button { cursor: pointer; }
    .row { cursor: pointer; }
    .row:hover { background: rgba(255,255,255,0.03); }
    .drill { padding: 12px 8px; background: #0f1320; border: 1px solid var(--border); border-radius: 12px; margin: 10px 0; }
    .pill { display: inline-block; padding: 4px 10px; border-radius: 999px; background: rgba(255,255,255,0.06); font-size: 12px; }
    .flex { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }
    .tabs { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 10px; }
    .tab { padding: 6px 12px; border: 1px solid var(--border); border-radius: 999px; background: transparent; }
    .tab.active { background: rgba(255,255,255,0.08); }
    .tab-panel { margin-top: 12px; }
    .two-col { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; }
    .mini { font-size: 0.85rem; }
    .link { color: var(--accent-2); text-decoration: none; }
    .link:hover { text-decoration: underline; }
    summary { cursor: pointer; margin-bottom: 12px; }
    @media (max-width: 720px) {
      .hero { flex-direction: column; align-items: flex-start; }
    }
    @media print {
      body { background: #fff; color: #000; }
      .card { box-shadow: none; }
      button { display: none; }
      .filters { display: none; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div>
        <h1>DataDesc Report</h1>
        <div class="muted">Professional profiling summary</div>
      </div>
      <div class="flex">
        <button onclick="window.print()">Export PDF</button>
        <span class="pill">Generated by DataDesc</span>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <div class="muted">Datasets</div>
        <div class="metric">__DATASETS__</div>
      </div>
      <div class="card">
        <div class="muted">Total rows</div>
        <div class="metric">__ROWS__</div>
      </div>
      <div class="card">
        <div class="muted">Total columns</div>
        <div class="metric">__COLS__</div>
      </div>
      <div class="card">
        <div class="muted">Avg missing</div>
        <div class="metric">__AVG_MISSING__</div>
      </div>
    </div>

    <div class="section card">
      <h2>Interactive charts</h2>
      <div class="charts">
        <div class="card">
          <div class="muted">Schema composition</div>
          <canvas id="chart-dtypes"></canvas>
        </div>
        <div class="card">
          <div class="muted">Missingness distribution</div>
          <canvas id="chart-missing"></canvas>
        </div>
        <div class="card">
          <div class="muted">Top datasets by rows</div>
          <canvas id="chart-rows"></canvas>
        </div>
      </div>
    </div>

    <div class="section card">
      <h2>Datasets</h2>
      <div class="filters">
        <input id="search" placeholder="Search by name" />
        <select id="sort">
          <option value="rows">Rows (desc)</option>
          <option value="columns">Columns (desc)</option>
          <option value="missing_cell_pct">Missing % (desc)</option>
        </select>
        <input id="minRows" type="number" placeholder="Min rows" />
      </div>
      <div id="table"></div>
      <div id="drilldown"></div>
    </div>

    __ARTIFACTS__
  </div>

  <script id="summary-data" type="application/json">__SUMMARY_JSON__</script>
  <script>
    const SUMMARY = JSON.parse(document.getElementById('summary-data').textContent || '{}');

    const datasets = Array.isArray(SUMMARY.datasets) ? SUMMARY.datasets : [];
    const dtypeCounts = SUMMARY.global_dtype_counts_pct || SUMMARY.global_dtype_counts || [];
    const missStats = SUMMARY.missingness_stats || {};

    const chartDtypes = document.getElementById("chart-dtypes");
    const chartMissing = document.getElementById("chart-missing");
    const chartRows = document.getElementById("chart-rows");

    function sizeCanvas(canvas) {
      const rect = canvas.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      const ctx = canvas.getContext('2d');
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      return { ctx, width: rect.width, height: rect.height };
    }

    function drawBars(canvas, labels, values, color) {
      if (!canvas || !labels.length) return;
      const { ctx, width, height } = sizeCanvas(canvas);
      if (width < 10 || height < 10) return;
      const pad = 30;
      const max = Math.max(...values, 1);
      const barW = (width - pad * 2) / values.length;
      ctx.clearRect(0,0,width,height);
      ctx.strokeStyle = "rgba(255,255,255,0.12)";
      ctx.beginPath();
      ctx.moveTo(pad, height - pad);
      ctx.lineTo(width - pad, height - pad);
      ctx.stroke();
      values.forEach((v, i) => {
        const x = pad + i * barW + 6;
        const barH = (height - pad * 2) * (v / max);
        ctx.fillStyle = color;
        ctx.fillRect(x, height - pad - barH, barW - 12, barH);
      });
      ctx.fillStyle = "rgba(255,255,255,0.6)";
      ctx.font = "10px sans-serif";
      labels.forEach((l, i) => {
        const x = pad + i * barW + 6;
        ctx.fillText(l.slice(0,6), x, height - 10);
      });
    }

    function drawLine(canvas, labels, values, color) {
      if (!canvas || !labels.length) return;
      const { ctx, width, height } = sizeCanvas(canvas);
      if (width < 10 || height < 10) return;
      const pad = 30;
      const max = Math.max(...values, 1);
      const step = (width - pad * 2) / (values.length - 1 || 1);
      ctx.clearRect(0,0,width,height);
      ctx.strokeStyle = "rgba(255,255,255,0.12)";
      ctx.beginPath();
      ctx.moveTo(pad, height - pad);
      ctx.lineTo(width - pad, height - pad);
      ctx.stroke();
      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.beginPath();
      values.forEach((v, i) => {
        const x = pad + i * step;
        const y = height - pad - (height - pad * 2) * (v / max);
        if (i === 0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
      });
      ctx.stroke();
      ctx.fillStyle = "rgba(255,255,255,0.6)";
      ctx.font = "10px sans-serif";
      labels.forEach((l, i) => {
        const x = pad + i * step - 6;
        ctx.fillText(l, x, height - 10);
      });
    }

    function drawHeatmap(canvas, labels, matrix) {
      if (!canvas || !labels.length) return;
      const { ctx, width, height } = sizeCanvas(canvas);
      const size = Math.min(width, height) - 40;
      const cell = size / labels.length;
      ctx.clearRect(0,0,width,height);
      matrix.forEach((row, i) => {
        row.forEach((v, j) => {
          const x = 30 + j * cell;
          const y = 10 + i * cell;
          const c = Math.floor((v + 1) / 2 * 255);
          ctx.fillStyle = `rgb(${c}, ${120}, ${255 - c})`;
          ctx.fillRect(x, y, cell, cell);
        });
      });
    }

    function drawHistogram(canvas, values, bins = 10, color = '#8ef0c4') {
      if (!canvas || !values.length) return;
      const { ctx, width, height } = sizeCanvas(canvas);
      const min = Math.min(...values);
      const max = Math.max(...values);
      const binSize = (max - min) / bins || 1;
      const counts = Array.from({length: bins}, () => 0);
      values.forEach(v => {
        const idx = Math.min(bins - 1, Math.floor((v - min) / binSize));
        counts[idx] += 1;
      });
      const pad = 30;
      const barW = (width - pad * 2) / bins;
      const maxCount = Math.max(...counts, 1);
      ctx.clearRect(0,0,width,height);
      counts.forEach((c, i) => {
        const x = pad + i * barW + 2;
        const barH = (height - pad * 2) * (c / maxCount);
        ctx.fillStyle = color;
        ctx.fillRect(x, height - pad - barH, barW - 4, barH);
      });
    }

    function fmtNum(val) {
      if (val === null || val === undefined || val === "") return "-";
      const num = Number(val);
      if (Number.isNaN(num)) return val;
      return new Intl.NumberFormat().format(num);
    }

    function parseCSV(text) {
      const rows = [];
      let row = [];
      let col = "";
      let inQuotes = false;
      for (let i = 0; i < text.length; i++) {
        const c = text[i];
        if (c === '"') {
          if (inQuotes && text[i + 1] === '"') {
            col += '"';
            i++;
          } else {
            inQuotes = !inQuotes;
          }
        } else if (c === ',' && !inQuotes) {
          row.push(col);
          col = "";
        } else if ((c === '\n' || c === '\r') && !inQuotes) {
          if (col.length || row.length) {
            row.push(col);
            rows.push(row);
            row = [];
            col = "";
          }
        } else {
          col += c;
        }
      }
      if (col.length || row.length) {
        row.push(col);
        rows.push(row);
      }
      if (!rows.length) return [];
      const headers = rows.shift().map(h => h.trim());
      return rows.filter(r => r.length).map(r => {
        const obj = {};
        headers.forEach((h, idx) => obj[h] = r[idx]);
        return obj;
      });
    }

    async function fetchCSV(path) {
      const res = await fetch(path);
      if (!res.ok) return [];
      const text = await res.text();
      if (!text.trim()) return [];
      return parseCSV(text);
    }

    function renderTable(rows) {
      const table = document.createElement("table");
      table.innerHTML = `
        <thead>
          <tr>
            <th>Name</th><th>Rows</th><th>Columns</th><th>Missing %</th><th>Sampled</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map(r => `
            <tr class="row" data-id="${r.dataset_id}">
              <td>${r.dataset_name || r.dataset_id}</td>
              <td>${fmtNum(r.rows)}</td>
              <td>${fmtNum(r.columns)}</td>
              <td>${(r.missing_cell_pct ?? 0).toFixed ? (r.missing_cell_pct).toFixed(2) + '%' : '-'}</td>
              <td>${r.sampled ? 'yes' : 'no'}</td>
            </tr>`).join('')}
        </tbody>
      `;
      return table;
    }

    function renderDrilldown(row) {
      const el = document.getElementById("drilldown");
      if (!el) return;
      const tabs = ["Overview","Schema","Missingness","Numeric","Categorical","Text","Warnings","Correlations"];
      el.innerHTML = `
        <div class="drill">
          <div class="flex">
            <strong>${row.dataset_name || row.dataset_id}</strong>
            <span class="pill">${row.sheet_name || 'sheet: none'}</span>
            <span class="pill">${row.sampled ? 'sampled' : 'full'}</span>
          </div>
          <p class="muted">${row.source_path || ''}</p>
          <div class="tabs" id="tabs"></div>
          <div class="tab-panel" id="panel"></div>
        </div>
      `;
      const tabsEl = document.getElementById("tabs");
      const panel = document.getElementById("panel");
      tabs.forEach((t, idx) => {
        const b = document.createElement("button");
        b.className = "tab" + (idx === 0 ? " active" : "");
        b.textContent = t;
        b.onclick = () => loadTab(t, row, panel, tabsEl);
        tabsEl.appendChild(b);
      });
      loadTab("Overview", row, panel, tabsEl);
    }

    async function loadTab(tab, row, panel, tabsEl) {
      [...tabsEl.children].forEach(btn => btn.classList.toggle("active", btn.textContent === tab));
      const base = SUMMARY._base || "..";
      const dir = row.dataset_dir_rel || "";
      const prefix = `${base}/${dir}`;
      if (!dir) {
        panel.innerHTML = `<p class="muted">Dataset files not available.</p>`;
        return;
      }

      if (tab === "Overview") {
        panel.innerHTML = `
          <div class="two-col">
            <div><div class="muted">Rows</div><div class="metric">${fmtNum(row.rows)}</div></div>
            <div><div class="muted">Columns</div><div class="metric">${fmtNum(row.columns)}</div></div>
            <div><div class="muted">Missing %</div><div class="metric">${(row.missing_cell_pct ?? 0).toFixed ? row.missing_cell_pct.toFixed(2) + '%' : '-'}</div></div>
            <div><div class="muted">Duplicate %</div><div class="metric">${(row.duplicate_row_pct ?? 0).toFixed ? row.duplicate_row_pct.toFixed(2) + '%' : '-'}</div></div>
          </div>
        `;
        return;
      }

      if (tab == "Schema") {
        const data = await fetchCSV(`${prefix}/schema.csv`);
        panel.innerHTML = `
          <table>
            <thead><tr><th>Column</th><th>Type</th><th>Non-null</th><th>Unique</th></tr></thead>
            <tbody>
              ${data.slice(0, 30).map(r => `<tr><td>${r.column}</td><td>${r.dtype}</td><td>${fmtNum(r.non_null)}</td><td>${fmtNum(r.unique)}</td></tr>`).join('')}
            </tbody>
          </table>
        `;
        return;
      }

      if (tab === "Missingness") {
        const data = await fetchCSV(`${prefix}/missingness.csv`);
        panel.innerHTML = `
          <div class="card"><canvas id="miss-chart"></canvas></div>
          <table>
            <thead><tr><th>Column</th><th>Missing</th><th>Missing %</th></tr></thead>
            <tbody>
              ${data.slice(0, 30).map(r => `<tr><td>${r.column}</td><td>${fmtNum(r.missing)}</td><td>${Number(r.missing_pct || 0).toFixed(2)}%</td></tr>`).join('')}
            </tbody>
          </table>
        `;
        const labels = data.slice(0, 20).map(d => d.column);
        const values = data.slice(0, 20).map(d => Number(d.missing_pct || 0));
        drawBars(document.getElementById("miss-chart"), labels, values, '#7ba3ff');
        return;
      }

      if (tab === "Numeric") {
        const data = await fetchCSV(`${prefix}/numeric_summary.csv`);
        const preview = await fetchCSV(`${prefix}/preview.csv`);
        const numericCols = data.map(r => r.column).filter(Boolean);
        const select = numericCols.map(c => `<option value="${c}">${c}</option>`).join('');
        panel.innerHTML = `
          <div class="filters"><select id="numcol">${select}</select></div>
          <div class="card"><canvas id="hist"></canvas></div>
          <table>
            <thead><tr><th>Column</th><th>Min</th><th>25%</th><th>50%</th><th>75%</th><th>Max</th></tr></thead>
            <tbody>
              ${data.slice(0, 30).map(r => `<tr><td>${r.column}</td><td>${r.min}</td><td>${r['25%']}</td><td>${r['50%']}</td><td>${r['75%']}</td><td>${r.max}</td></tr>`).join('')}
            </tbody>
          </table>
        `;
        const draw = () => {
          const col = document.getElementById('numcol').value;
          const values = preview.map(r => Number(r[col])).filter(v => Number.isFinite(v));
          drawHistogram(document.getElementById('hist'), values, 12, '#8ef0c4');
        };
        document.getElementById('numcol').addEventListener('change', draw);
        draw();
        return;
      }

      if (tab === "Categorical") {
        const data = await fetchCSV(`${prefix}/categorical_summary.csv`);
        panel.innerHTML = `
          <table>
            <thead><tr><th>Column</th><th>Value</th><th>Count</th><th>% Rows</th></tr></thead>
            <tbody>
              ${data.slice(0, 40).map(r => `<tr><td>${r.column}</td><td>${r.value}</td><td>${fmtNum(r.count)}</td><td>${Number(r.pct_of_rows || 0).toFixed(2)}%</td></tr>`).join('')}
            </tbody>
          </table>
        `;
        return;
      }

      if (tab === "Text") {
        const data = await fetchCSV(`${prefix}/text_profile.csv`);
        panel.innerHTML = `
          <table>
            <thead><tr><th>Column</th><th>Non-null</th><th>Empty</th><th>Avg len</th><th>P90 len</th></tr></thead>
            <tbody>
              ${data.slice(0, 30).map(r => `<tr><td>${r.column}</td><td>${fmtNum(r.non_null)}</td><td>${fmtNum(r.empty_string)}</td><td>${r.avg_len}</td><td>${r.p90_len}</td></tr>`).join('')}
            </tbody>
          </table>
        `;
        return;
      }

      if (tab === "Warnings") {
        try {
          const res = await fetch(`${prefix}/quality_warnings.json`);
          const json = await res.json();
          const warnings = json.warnings || [];
          panel.innerHTML = warnings.length
            ? `<ul>${warnings.map(w => `<li>${w}</li>`).join('')}</ul>`
            : `<p class="muted">No warnings.</p>`;
        } catch {
          panel.innerHTML = `<p class="muted">No warnings.</p>`;
        }
        return;
      }

      if (tab === "Correlations") {
        const data = await fetchCSV(`${prefix}/correlations.csv`);
        if (!data.length) {
          panel.innerHTML = `<p class="muted">No correlation data.</p>`;
          return;
        }
        panel.innerHTML = `<div class="card"><canvas id="corr"></canvas></div>`;
        const labels = Object.keys(data[0]).filter(k => k !== 'column');
        const matrix = data.map(r => labels.map(l => Number(r[l] || 0)));
        drawHeatmap(document.getElementById("corr"), labels, matrix);
        return;
      }
    }

    function applyFilters() {
      const q = (document.getElementById('search').value || '').toLowerCase();
      const sort = document.getElementById('sort').value;
      const minRows = Number(document.getElementById('minRows').value || 0);
      let rows = datasets.filter(r => (r.dataset_name || '').toLowerCase().includes(q));
      rows = rows.filter(r => Number(r.rows || 0) >= minRows);
      rows = rows.sort((a,b) => Number(b[sort] || 0) - Number(a[sort] || 0));
      const tableWrap = document.getElementById('table');
      tableWrap.innerHTML = '';
      const table = renderTable(rows.slice(0, 50));
      tableWrap.appendChild(table);
      table.querySelectorAll('.row').forEach(rowEl => {
        rowEl.addEventListener('click', () => {
          const id = rowEl.getAttribute('data-id');
          const row = rows.find(r => r.dataset_id === id);
          if (row) renderDrilldown(row);
        });
      });
    }

    function init() {
      const dtypeLabels = dtypeCounts.slice(0, 8).map(d => d.dtype || '');
      const dtypeValues = dtypeCounts.slice(0, 8).map(d => Number(d.pct || d.count || 0));
      drawBars(chartDtypes, dtypeLabels, dtypeValues, '#8ef0c4');

      const missingLabels = ['Avg','P50','P90'];
      const missingValues = [
        Number(missStats.avg_missing_cell_pct || 0),
        Number(missStats.p50_missing_cell_pct || 0),
        Number(missStats.p90_missing_cell_pct || 0)
      ];
      drawLine(chartMissing, missingLabels, missingValues, '#7ba3ff');

      const top = datasets.slice(0, 8);
      const topLabels = top.map(d => (d.dataset_name || d.dataset_id || '').slice(0,6));
      const topValues = top.map(d => Number(d.rows || 0));
      drawBars(chartRows, topLabels, topValues, '#f7b35b');

      ['search','sort','minRows'].forEach(id => {
        document.getElementById(id).addEventListener('input', applyFilters);
        document.getElementById(id).addEventListener('change', applyFilters);
      });
      applyFilters();
    }

    window.addEventListener('load', () => {
      setTimeout(init, 200);
    });
  </script>
</body>
</html>
"""

    html_doc = (
        template.replace("__SUMMARY_JSON__", summary_json)
        .replace("__DATASETS__", _escape(headline["datasets"]))
        .replace("__ROWS__", _escape(headline["rows"]))
        .replace("__COLS__", _escape(headline["cols"]))
        .replace("__AVG_MISSING__", _escape(headline["avg_missing"]))
        .replace("__ARTIFACTS__", artifacts_html)
    )

    (total_dir / "report.html").write_text(html_doc, encoding="utf-8")
