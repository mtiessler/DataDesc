import { useEffect, useMemo, useRef, useState } from "react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  LineChart,
  Line,
  ScatterChart,
  Scatter,
  ZAxis
} from "recharts";

const parseCSV = (text) => {
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
    } else if ((c === "\n" || c === "\r") && !inQuotes) {
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
  const headers = rows.shift().map((h) => h.trim());
  return rows.filter((r) => r.length).map((r) => {
    const obj = {};
    headers.forEach((h, idx) => (obj[h] = r[idx]));
    return obj;
  });
};

const fetchCSV = async (url) => {
  const res = await fetch(url);
  if (!res.ok) return [];
  const text = await res.text();
  if (!text.trim()) return [];
  return parseCSV(text);
};

const fmtNum = (val) => {
  if (val === null || val === undefined || val === "") return "-";
  const num = Number(val);
  if (Number.isNaN(num)) return val;
  return Intl.NumberFormat().format(num);
};

const CanvasHeatmap = ({ matrix, labelsX, labelsY, title }) => {
  const canvasRef = useRef(null);
  const [tooltip, setTooltip] = useState(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !matrix || !labelsX?.length || !labelsY?.length) return;
    const rect = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    const ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    const paddingLeft = 80;
    const paddingTop = 20;
    const width = rect.width - paddingLeft - 20;
    const height = rect.height - paddingTop - 30;
    const cellW = width / labelsX.length;
    const cellH = height / labelsY.length;

    ctx.clearRect(0, 0, rect.width, rect.height);
    matrix.forEach((row, i) => {
      row.forEach((v, j) => {
        const x = paddingLeft + j * cellW;
        const y = paddingTop + i * cellH;
        const c = Math.floor((v + 1) / 2 * 255);
        ctx.fillStyle = `rgb(${c}, ${120}, ${255 - c})`;
        ctx.fillRect(x, y, cellW, cellH);
      });
    });

    ctx.fillStyle = "rgba(255,255,255,0.7)";
    ctx.font = "10px sans-serif";
    labelsX.forEach((l, i) => {
      const x = paddingLeft + i * cellW + 2;
      ctx.fillText(l.slice(0, 6), x, rect.height - 10);
    });
    labelsY.forEach((l, i) => {
      const y = paddingTop + i * cellH + 12;
      ctx.fillText(l.slice(0, 10), 8, y);
    });
  }, [matrix, labelsX, labelsY]);

  const onMove = (event) => {
    if (!canvasRef.current || !matrix) return;
    const rect = canvasRef.current.getBoundingClientRect();
    const paddingLeft = 80;
    const paddingTop = 20;
    const width = rect.width - paddingLeft - 20;
    const height = rect.height - paddingTop - 30;
    const cellW = width / labelsX.length;
    const cellH = height / labelsY.length;
    const x = event.clientX - rect.left - paddingLeft;
    const y = event.clientY - rect.top - paddingTop;
    const col = Math.floor(x / cellW);
    const row = Math.floor(y / cellH);
    if (row >= 0 && row < labelsY.length && col >= 0 && col < labelsX.length) {
      setTooltip({
        x: event.clientX - rect.left,
        y: event.clientY - rect.top,
        labelX: labelsX[col],
        labelY: labelsY[row],
        value: matrix[row][col]
      });
    } else {
      setTooltip(null);
    }
  };

  const tooltipStyle = {
    background: "rgba(8, 10, 18, 0.95)",
    border: "1px solid rgba(255,255,255,0.12)",
    borderRadius: "8px",
    color: "#f7f6f2",
    fontSize: "0.75rem"
  };

  return (
    <div className="heatmap-wrap">
      {title && <div className="muted">{title}</div>}
      <canvas ref={canvasRef} className="canvas" onMouseMove={onMove} onMouseLeave={() => setTooltip(null)} />
      {tooltip && (
        <div className="heatmap-tooltip" style={{ left: tooltip.x + 12, top: tooltip.y + 12 }}>
          <strong>{tooltip.labelY}</strong>
          <div>{tooltip.labelX}</div>
          <div>{Number(tooltip.value || 0).toFixed(2)}</div>
        </div>
      )}
    </div>
  );
};

const CanvasHistogram = ({ values }) => {
  const ref = useRef(null);
  useEffect(() => {
    const canvas = ref.current;
    if (!canvas || !values || !values.length) return;
    const rect = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    const ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    const min = Math.min(...values);
    const max = Math.max(...values);
    const bins = 14;
    const binSize = (max - min) / bins || 1;
    const counts = Array.from({ length: bins }, () => 0);
    values.forEach((v) => {
      const idx = Math.min(bins - 1, Math.floor((v - min) / binSize));
      counts[idx] += 1;
    });

    const pad = 30;
    const barW = (rect.width - pad * 2) / bins;
    const maxCount = Math.max(...counts, 1);
    ctx.clearRect(0, 0, rect.width, rect.height);
    counts.forEach((c, i) => {
      const x = pad + i * barW + 2;
      const barH = (rect.height - pad * 2) * (c / maxCount);
      ctx.fillStyle = "#8ef0c4";
      ctx.fillRect(x, rect.height - pad - barH, barW - 4, barH);
    });
  }, [values]);
  return <canvas ref={ref} className="canvas" />;
};

export default function ReportView({ jobId, apiBase, onBack }) {
  const [summary, setSummary] = useState(null);
  const [files, setFiles] = useState(null);
  const [status, setStatus] = useState("idle");
  const [selectedDataset, setSelectedDataset] = useState(null);
  const [tab, setTab] = useState("Overview");
  const [tabData, setTabData] = useState(null);
  const [downloadUrl, setDownloadUrl] = useState("");
  const [tabStatus, setTabStatus] = useState("idle");
  const [tabError, setTabError] = useState("");

  const reportsBase = summary?._reports_base
    ? `${apiBase}${summary._reports_base}`
    : `${apiBase}/reports/${jobId}`;

  const jobFileUrl = (relPath) =>
    `${apiBase}/jobs/${jobId}/file?path=${encodeURIComponent(relPath)}`;

  const resolveDatasetRel = (dataset) => {
    if (!dataset) return "";
    if (dataset.dataset_dir_rel) return dataset.dataset_dir_rel;
    const raw = dataset.dataset_dir || "";
    const parts = String(raw).split(/[\\/]/).filter(Boolean);
    if (parts.length) return parts[parts.length - 1];
    const id = dataset.dataset_id || "";
    if (files?.datasets && id) {
      const match = files.datasets.find((d) => String(d.dir || "").startsWith(id));
      if (match?.dir) return match.dir;
    }
    return "";
  };

  useEffect(() => {
    const load = async () => {
      setStatus("loading");
      try {
        const res = await fetch(`${apiBase}/jobs/${jobId}/summary`);
        const json = await res.json();
        setSummary(json);
        setDownloadUrl(`${apiBase}/jobs/${jobId}/download`);
        const filesRes = await fetch(`${apiBase}/jobs/${jobId}/files`);
        if (filesRes.ok) {
          setFiles(await filesRes.json());
        }
        setStatus("ready");
      } catch (err) {
        setStatus("error");
      }
    };
    if (jobId) load();
  }, [jobId, apiBase]);

  useEffect(() => {
    const loadTab = async () => {
      if (!selectedDataset) return;
      const rel = resolveDatasetRel(selectedDataset);
      if (!rel) return;
      const base = `${rel}`;
      setTabStatus("loading");
      setTabError("");
      const safeFetch = async (path) => {
        const res = await fetch(jobFileUrl(path));
        if (!res.ok) {
          throw new Error(`Failed to load ${path} (${res.status})`);
        }
        const text = await res.text();
        if (!text.trim()) return [];
        return parseCSV(text);
      };

      try {
        if (tab === "Schema") {
          setTabData(await safeFetch(`${base}/schema.csv`));
        } else if (tab === "Missingness") {
          setTabData(await safeFetch(`${base}/missingness.csv`));
        } else if (tab === "Numeric") {
          const data = await safeFetch(`${base}/numeric_summary.csv`);
          const preview = await safeFetch(`${base}/preview.csv`);
          setTabData({ data, preview });
        } else if (tab === "Categorical") {
          setTabData(await safeFetch(`${base}/categorical_summary.csv`));
        } else if (tab === "Text") {
          setTabData(await safeFetch(`${base}/text_profile.csv`));
        } else if (tab === "Correlations") {
          setTabData(await safeFetch(`${base}/correlations.csv`));
        } else if (tab === "Distribution") {
          setTabData(await safeFetch(`${base}/distribution_shape.csv`));
        } else if (tab === "Uniqueness") {
          setTabData(await safeFetch(`${base}/uniqueness.csv`));
        } else if (tab === "Keys") {
          setTabData(await safeFetch(`${base}/key_duplicates.csv`));
        } else if (tab === "Columns") {
          const schema = await safeFetch(`${base}/schema.csv`);
          const numeric = await safeFetch(`${base}/numeric_summary.csv`);
          const text = await safeFetch(`${base}/text_profile.csv`);
          const cat = await safeFetch(`${base}/categorical_summary.csv`);
          const uniq = await safeFetch(`${base}/uniqueness.csv`);

        const topCat = {};
        cat.forEach((r) => {
          if (!topCat[r.column]) topCat[r.column] = r;
        });

        const map = {};
        schema.forEach((r) => (map[r.column] = { ...r }));
        numeric.forEach((r) => (map[r.column] = { ...map[r.column], ...r }));
        text.forEach((r) => (map[r.column] = { ...map[r.column], ...r }));
        uniq.forEach((r) => (map[r.column] = { ...map[r.column], ...r }));
        Object.keys(topCat).forEach((c) => (map[c] = { ...map[c], top_value: topCat[c].value, top_count: topCat[c].count }));

          setTabData(Object.values(map));
        } else if (tab === "Warnings") {
          const res = await fetch(jobFileUrl(`${base}/quality_warnings.json`));
          setTabData(res.ok ? await res.json() : { warnings: [] });
        } else {
          setTabData(null);
        }
        setTabStatus("ready");
      } catch (err) {
        setTabError(err.message || "Failed to load tab data.");
        setTabData(null);
        setTabStatus("ready");
      }
    };
    loadTab();
  }, [selectedDataset, tab, reportsBase]);

  const datasets = summary?.datasets || [];
  const dtypeCounts = summary?.global_dtype_counts_pct || summary?.global_dtype_counts || [];
  const missStats = summary?.missingness_stats || {};
  const warnings = summary?.quality_warnings || [];
  const temporal = summary?.temporal_coverage || [];
  const hotspots = summary?.top_missing_hotspots || [];

  const dtypeSeries = dtypeCounts.slice(0, 8).map((d) => ({
    dtype: d.dtype,
    pct: d.pct ?? d.count ?? 0
  }));

  const missingSeries = [
    { label: "Avg", value: Number(missStats.avg_missing_cell_pct || 0) },
    { label: "P50", value: Number(missStats.p50_missing_cell_pct || 0) },
    { label: "P90", value: Number(missStats.p90_missing_cell_pct || 0) }
  ];

  const topSeries = datasets.slice(0, 8).map((d) => ({
    label: (d.dataset_name || d.dataset_id || "").slice(0, 10),
    rows: Number(d.rows || 0)
  }));

  const missingDistribution = useMemo(() => {
    const buckets = Array.from({ length: 10 }, (_, i) => ({ bucket: `${i * 10}-${i * 10 + 10}`, count: 0 }));
    datasets.forEach((d) => {
      const v = Number(d.missing_cell_pct || 0);
      const idx = Math.min(9, Math.floor(v / 10));
      buckets[idx].count += 1;
    });
    return buckets;
  }, [datasets]);

  const scatterData = datasets.map((d) => ({
    rows: Number(d.rows || 0),
    missing: Number(d.missing_cell_pct || 0),
    columns: Number(d.columns || 0)
  }));

  const temporalSeries = temporal.slice(0, 12).map((d) => ({
    name: (d.dataset_name || d.dataset_id || "").slice(0, 10),
    min: Number(d.min_year || 0),
    max: Number(d.max_year || 0)
  }));

  const hotspotDatasets = Array.from(new Set(hotspots.map((h) => h.dataset_name || h.dataset_id))).slice(0, 6);
  const hotspotColumns = Array.from(new Set(hotspots.map((h) => h.column))).slice(0, 8);
  const hotspotMatrix = hotspotDatasets.map((ds) =>
    hotspotColumns.map((col) => {
      const hit = hotspots.find((h) => (h.dataset_name || h.dataset_id) === ds && h.column === col);
      return Number(hit?.missing_pct || 0) / 100;
    })
  );

  const tabs = ["Overview", "Columns", "Schema", "Missingness", "Numeric", "Categorical", "Text", "Distribution", "Uniqueness", "Keys", "Warnings", "Correlations"];

  const numericCols = tabData?.data?.map((r) => r.column).filter(Boolean) || [];
  const [numericCol, setNumericCol] = useState("");
  useEffect(() => {
    if (numericCols.length) setNumericCol(numericCols[0]);
  }, [numericCols.length]);

  const histogramValues = useMemo(() => {
    if (!numericCol || !tabData?.preview) return [];
    return tabData.preview.map((r) => Number(r[numericCol])).filter((v) => Number.isFinite(v));
  }, [numericCol, tabData]);

  const tooltipStyle = {
    background: "rgba(8, 10, 18, 0.95)",
    border: "1px solid rgba(255,255,255,0.12)",
    borderRadius: "8px",
    color: "#f7f6f2",
    fontSize: "0.75rem"
  };

  if (status === "loading") {
    return <div className="report-wrap">Loading report...</div>;
  }

  if (status === "error") {
    return <div className="report-wrap">Failed to load report.</div>;
  }

  return (
    <div className="report-wrap">
      <div className="report-header">
        <div>
          <h1>DataDesc Report</h1>
          <p>Job ID: {jobId}</p>
        </div>
        <div className="report-actions">
          <button onClick={onBack}>Back</button>
          {downloadUrl && (
            <a className="ghost" href={downloadUrl}>
              Download ZIP
            </a>
          )}
        </div>
      </div>

      <section className="report-grid">
        <div className="report-card">
          <div className="muted">Datasets</div>
          <strong>{fmtNum(summary?.totals?.datasets_processed)}</strong>
        </div>
        <div className="report-card">
          <div className="muted">Total rows</div>
          <strong>{fmtNum(summary?.totals?.total_rows)}</strong>
        </div>
        <div className="report-card">
          <div className="muted">Total columns</div>
          <strong>{fmtNum(summary?.totals?.total_columns)}</strong>
        </div>
        <div className="report-card">
          <div className="muted">Avg missing</div>
          <strong>{Number(missStats.avg_missing_cell_pct || 0).toFixed(2)}%</strong>
        </div>
      </section>

      <section className="report-grid">
        <div className="report-card">
          <h3>Schema composition</h3>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={dtypeSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
              <XAxis dataKey="dtype" stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <YAxis stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar dataKey="pct" fill="#8ef0c4" radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="report-card">
          <h3>Missingness distribution</h3>
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={missingSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
              <XAxis dataKey="label" stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <YAxis stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <Tooltip contentStyle={tooltipStyle} />
              <Line type="monotone" dataKey="value" stroke="#7ba3ff" strokeWidth={3} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="report-card">
          <h3>Top datasets</h3>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={topSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
              <XAxis dataKey="label" stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <YAxis stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar dataKey="rows" fill="#f7b35b" radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="report-grid">
        <div className="report-card">
          <h3>Missingness buckets</h3>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={missingDistribution}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
              <XAxis dataKey="bucket" stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <YAxis stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar dataKey="count" fill="#7ba3ff" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="report-card">
          <h3>Rows vs missingness</h3>
          <ResponsiveContainer width="100%" height={240}>
            <ScatterChart>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
              <XAxis dataKey="rows" type="number" stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <YAxis dataKey="missing" type="number" stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <ZAxis dataKey="columns" range={[40, 200]} />
              <Tooltip cursor={{ strokeDasharray: "3 3" }} contentStyle={tooltipStyle} />
              <Scatter data={scatterData} fill="#8ef0c4" />
            </ScatterChart>
          </ResponsiveContainer>
        </div>

        <div className="report-card">
          <h3>Quality warnings</h3>
          <ul>
            {warnings.slice(0, 8).map((w, idx) => (
              <li key={idx}>{w.warning || w}</li>
            ))}
          </ul>
        </div>
      </section>

      <section className="report-grid">
        <div className="report-card">
          <h3>Temporal coverage</h3>
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={temporalSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
              <XAxis dataKey="name" stroke="rgba(255,255,255,0.6)" fontSize={10} />
              <YAxis stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <Tooltip contentStyle={tooltipStyle} />
              <Line type="monotone" dataKey="min" stroke="#8ef0c4" strokeWidth={2} />
              <Line type="monotone" dataKey="max" stroke="#f7b35b" strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="report-card">
          <h3>Missingness heatmap</h3>
          <CanvasHeatmap matrix={hotspotMatrix} labelsX={hotspotColumns} labelsY={hotspotDatasets} title="Top missing hotspots" />
        </div>

        <div className="report-card">
          <h3>Dataset spread</h3>
          <ResponsiveContainer width="100%" height={240}>
            <ScatterChart>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
              <XAxis dataKey="columns" type="number" stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <YAxis dataKey="rows" type="number" stroke="rgba(255,255,255,0.6)" fontSize={12} />
              <ZAxis dataKey="missing" range={[40, 200]} />
              <Tooltip cursor={{ strokeDasharray: "3 3" }} contentStyle={tooltipStyle} />
              <Scatter data={scatterData} fill="#7ba3ff" />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="report-card">
        <h3>Datasets</h3>
        <div className="dataset-grid">
          {datasets.map((d) => (
            <button
              key={d.dataset_id}
              className={selectedDataset?.dataset_id === d.dataset_id ? "dataset active" : "dataset"}
              onClick={() => {
                setSelectedDataset(d);
                setTab("Overview");
              }}
            >
              <div>{d.dataset_name || d.dataset_id}</div>
              <small>{fmtNum(d.rows)} rows</small>
            </button>
          ))}
        </div>
      </section>

      {selectedDataset && (
        <section className="report-card">
          <h3>{selectedDataset.dataset_name || selectedDataset.dataset_id}</h3>
          <div className="tabs">
            {tabs.map((t) => (
              <button key={t} className={tab === t ? "tab active" : "tab"} onClick={() => setTab(t)}>
                {t}
              </button>
            ))}
          </div>

          <div className="tab-panel">
            {tabStatus === "loading" && (
              <div className="muted">Loading {tab}…</div>
            )}
            {tabError && (
              <div className="muted">Error: {tabError}</div>
            )}
            {tabStatus === "ready" &&
              tab !== "Overview" &&
              (tabData === null ||
                (Array.isArray(tabData) && tabData.length === 0) ||
                (tabData?.data && Array.isArray(tabData.data) && tabData.data.length === 0)) && (
                <div className="muted">No data available for this tab.</div>
              )}
            {tab === "Overview" && (
              <div className="two-col">
                <div><div className="muted">Rows</div><strong>{fmtNum(selectedDataset.rows)}</strong></div>
                <div><div className="muted">Columns</div><strong>{fmtNum(selectedDataset.columns)}</strong></div>
                <div><div className="muted">Missing %</div><strong>{Number(selectedDataset.missing_cell_pct || 0).toFixed(2)}%</strong></div>
                <div><div className="muted">Duplicate %</div><strong>{Number(selectedDataset.duplicate_row_pct || 0).toFixed(2)}%</strong></div>
              </div>
            )}

            {tab === "Columns" && Array.isArray(tabData) && (
              <table>
                <thead>
                  <tr>
                    <th>Column</th><th>Type</th><th>Non-null</th><th>Null %</th><th>Unique</th>
                    <th>Mean</th><th>Std</th><th>Min</th><th>Median</th><th>Max</th>
                    <th>Top value</th><th>Top count</th><th>Avg len</th>
                  </tr>
                </thead>
                <tbody>
                  {tabData.slice(0, 50).map((r, idx) => (
                    <tr key={idx}>
                      <td>{r.column}</td>
                      <td>{r.dtype}</td>
                      <td>{fmtNum(r.non_null)}</td>
                      <td>{Number(r.null_pct || 0).toFixed(2)}%</td>
                      <td>{fmtNum(r.unique)}</td>
                      <td>{r.mean}</td>
                      <td>{r.std}</td>
                      <td>{r.min}</td>
                      <td>{r["50%"] || r["p50"]}</td>
                      <td>{r.max}</td>
                      <td>{r.top_value}</td>
                      <td>{fmtNum(r.top_count)}</td>
                      <td>{r.avg_len}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}

            {tab === "Schema" && Array.isArray(tabData) && (
              <table>
                <thead><tr><th>Column</th><th>Type</th><th>Non-null</th><th>Unique</th></tr></thead>
                <tbody>
                  {tabData.slice(0, 30).map((r, idx) => (
                    <tr key={idx}>
                      <td>{r.column}</td>
                      <td>{r.dtype}</td>
                      <td>{fmtNum(r.non_null)}</td>
                      <td>{fmtNum(r.unique)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}

            {tab === "Missingness" && Array.isArray(tabData) && (
              <>
                <ResponsiveContainer width="100%" height={240}>
                  <BarChart data={tabData.slice(0, 20)}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                    <XAxis dataKey="column" stroke="rgba(255,255,255,0.6)" fontSize={10} />
                    <YAxis stroke="rgba(255,255,255,0.6)" fontSize={12} />
                  <Tooltip contentStyle={tooltipStyle} />
                    <Bar dataKey="missing_pct" fill="#7ba3ff" />
                  </BarChart>
                </ResponsiveContainer>
                <table>
                  <thead><tr><th>Column</th><th>Missing</th><th>Missing %</th></tr></thead>
                  <tbody>
                    {tabData.slice(0, 30).map((r, idx) => (
                      <tr key={idx}>
                        <td>{r.column}</td>
                        <td>{fmtNum(r.missing)}</td>
                        <td>{Number(r.missing_pct || 0).toFixed(2)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </>
            )}

            {tab === "Numeric" && tabData && (
              <>
                <div className="filters">
                  <select value={numericCol} onChange={(e) => setNumericCol(e.target.value)}>
                    {numericCols.map((c) => (
                      <option key={c} value={c}>{c}</option>
                    ))}
                  </select>
                </div>
                <CanvasHistogram values={histogramValues} />
                <table>
                  <thead><tr><th>Column</th><th>Min</th><th>25%</th><th>50%</th><th>75%</th><th>Max</th></tr></thead>
                  <tbody>
                    {tabData.data?.slice(0, 30).map((r, idx) => (
                      <tr key={idx}>
                        <td>{r.column}</td>
                        <td>{r.min}</td>
                        <td>{r["25%"]}</td>
                        <td>{r["50%"]}</td>
                        <td>{r["75%"]}</td>
                        <td>{r.max}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </>
            )}

            {tab === "Categorical" && Array.isArray(tabData) && (
              <table>
                <thead><tr><th>Column</th><th>Value</th><th>Count</th><th>% Rows</th></tr></thead>
                <tbody>
                  {tabData.slice(0, 40).map((r, idx) => (
                    <tr key={idx}>
                      <td>{r.column}</td>
                      <td>{r.value}</td>
                      <td>{fmtNum(r.count)}</td>
                      <td>{Number(r.pct_of_rows || 0).toFixed(2)}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}

            {tab === "Text" && Array.isArray(tabData) && (
              <table>
                <thead><tr><th>Column</th><th>Non-null</th><th>Empty</th><th>Avg len</th><th>P90 len</th></tr></thead>
                <tbody>
                  {tabData.slice(0, 30).map((r, idx) => (
                    <tr key={idx}>
                      <td>{r.column}</td>
                      <td>{fmtNum(r.non_null)}</td>
                      <td>{fmtNum(r.empty_string)}</td>
                      <td>{r.avg_len}</td>
                      <td>{r.p90_len}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}

            {tab === "Distribution" && Array.isArray(tabData) && (
              <>
                <ResponsiveContainer width="100%" height={240}>
                  <BarChart data={tabData.slice(0, 20)}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                    <XAxis dataKey="column" stroke="rgba(255,255,255,0.6)" fontSize={10} />
                    <YAxis stroke="rgba(255,255,255,0.6)" fontSize={12} />
                    <Tooltip contentStyle={tooltipStyle} />
                    <Bar dataKey="approx_skew" fill="#f7b35b" />
                  </BarChart>
                </ResponsiveContainer>
                <table>
                  <thead><tr><th>Column</th><th>Unique ratio</th><th>Zero %</th><th>Skew</th></tr></thead>
                  <tbody>
                    {tabData.slice(0, 30).map((r, idx) => (
                      <tr key={idx}>
                        <td>{r.column}</td>
                        <td>{Number(r.unique_ratio || 0).toFixed(2)}</td>
                        <td>{Number(r.zero_pct || 0).toFixed(2)}%</td>
                        <td>{Number(r.approx_skew || 0).toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </>
            )}

            {tab === "Uniqueness" && Array.isArray(tabData) && (
              <table>
                <thead><tr><th>Column</th><th>Unique (non-null)</th><th>Unique+NULL</th></tr></thead>
                <tbody>
                  {tabData.slice(0, 30).map((r, idx) => (
                    <tr key={idx}>
                      <td>{r.column}</td>
                      <td>{fmtNum(r.unique_non_null)}</td>
                      <td>{fmtNum(r.unique_including_null_as_value)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}

            {tab === "Keys" && Array.isArray(tabData) && (
              <table>
                <thead><tr><th>Key column</th><th>Unique keys</th><th>Repeated %</th></tr></thead>
                <tbody>
                  {tabData.slice(0, 30).map((r, idx) => (
                    <tr key={idx}>
                      <td>{r.key_column}</td>
                      <td>{fmtNum(r.unique_keys)}</td>
                      <td>{Number(r.repeated_keys_pct || 0).toFixed(2)}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}

            {tab === "Warnings" && (
              <ul>
                {(tabData?.warnings || []).map((w, idx) => (
                  <li key={idx}>{w}</li>
                ))}
              </ul>
            )}

            {tab === "Correlations" && Array.isArray(tabData) && tabData.length > 0 && (
              <CanvasHeatmap
                labelsX={Object.keys(tabData[0]).filter((k) => k !== "column")}
                labelsY={Object.keys(tabData[0]).filter((k) => k !== "column")}
                matrix={tabData.map((r) => Object.keys(r).filter((k) => k !== "column").map((k) => Number(r[k] || 0)))}
                title="Correlation heatmap"
              />
            )}
          </div>
        </section>
      )}

      {files && (
        <section className="report-card">
          <h3>All generated files</h3>
          <h4>Global outputs</h4>
          <table>
            <thead><tr><th>File</th><th>Size</th><th>Rows</th><th>Cols</th><th>JSON Keys</th><th>JSON Items</th></tr></thead>
            <tbody>
              {files.global.map((f, idx) => (
                <tr key={idx}>
                  <td><a className="link" href={`${reportsBase}/_total/${f.path}`} target="_blank" rel="noreferrer">{f.path}</a></td>
                  <td>{fmtNum(f.size)}</td>
                  <td>{fmtNum(f.rows)}</td>
                  <td>{fmtNum(f.cols)}</td>
                  <td>{fmtNum(f.keys)}</td>
                  <td>{fmtNum(f.items)}</td>
                </tr>
              ))}
            </tbody>
          </table>

          <h4>Per-dataset outputs</h4>
          {files.datasets.map((d, idx) => (
            <details key={idx} open>
              <summary>{d.dir}</summary>
              <table>
                <thead><tr><th>File</th><th>Size</th><th>Rows</th><th>Cols</th><th>JSON Keys</th><th>JSON Items</th></tr></thead>
                <tbody>
                  {d.files.map((f, fidx) => (
                    <tr key={fidx}>
                      <td><a className="link" href={`${reportsBase}/${d.dir}/${f.path}`} target="_blank" rel="noreferrer">{f.path}</a></td>
                      <td>{fmtNum(f.size)}</td>
                      <td>{fmtNum(f.rows)}</td>
                      <td>{fmtNum(f.cols)}</td>
                      <td>{fmtNum(f.keys)}</td>
                      <td>{fmtNum(f.items)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </details>
          ))}
        </section>
      )}
    </div>
  );
}
