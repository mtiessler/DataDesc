import { useEffect, useMemo, useState } from "react";
import ReportView from "./ReportView.jsx";

const featureCards = [
  {
    title: "Instant Profiling",
    body: "Upload CSV/XLSX and get stats, quality warnings, and schema insights in seconds."
  },
  {
    title: "Scale Ready",
    body: "Sampling, streaming scans, and capped Excel parsing keep huge files responsive."
  },
  {
    title: "Research‑grade Outputs",
    body: "Deterministic reports, clean exports, and reproducible markdown summaries."
  }
];

const stats = [
  { label: "Profiler modules", value: "12+" },
  { label: "Outputs per dataset", value: "15 files" },
  { label: "Sampling default", value: "200k rows" }
];

const steps = [
  {
    label: "1. Upload",
    text: "Drag‑drop CSV/XLSX. We store the file and create a dataset job."
  },
  {
    label: "2. Profile",
    text: "Streaming + sampling stats. Schema, missingness, correlations, and warnings."
  },
  {
    label: "3. Report",
    text: "Download JSON/CSV/Markdown or open the interactive report browser."
  }
];

export default function App() {
  const defaultEndpoint =
    import.meta.env.VITE_API_ENDPOINT || "http://localhost:8000/profile";
  const [endpoint, setEndpoint] = useState(defaultEndpoint);
  const [files, setFiles] = useState([]);
  const [status, setStatus] = useState("idle");
  const [message, setMessage] = useState("");
  const [downloadUrl, setDownloadUrl] = useState("");
  const [jobId, setJobId] = useState("");
  const [reportLink, setReportLink] = useState("");

  const totalSize = useMemo(() => {
    return files.reduce((sum, f) => sum + f.size, 0);
  }, [files]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const job = params.get("job");
    if (job) {
      setJobId(job);
    }
  }, []);

  const onFiles = (list) => {
    const next = Array.from(list || []).filter(Boolean);
    setFiles(next);
    setStatus("idle");
    setMessage("");
    setDownloadUrl("");
    setJobId("");
    setReportLink("");
  };

  const onDrop = (event) => {
    event.preventDefault();
    onFiles(event.dataTransfer.files);
  };

  const onSubmit = async () => {
    if (!files.length) {
      setStatus("error");
      setMessage("Please add at least one file.");
      return;
    }

    let pendingTab = null;
    try {
      pendingTab = window.open("", "_blank", "noopener,noreferrer");
    } catch {
      pendingTab = null;
    }

    setStatus("loading");
    setMessage("Uploading and profiling…");
    setDownloadUrl("");

    try {
      const form = new FormData();
      files.forEach((file) => form.append("files", file));
      const res = await fetch(endpoint, { method: "POST", body: form });
      if (!res.ok) {
        throw new Error("Server responded with status " + res.status);
      }
      const payload = await res.json();
      setStatus("success");
      setMessage(payload.message || "Report ready.");
      const base = endpoint.replace(/\/profile\/?$/, "");
      if (payload.download_url) {
        const url = payload.download_url.startsWith("http")
          ? payload.download_url
          : base + payload.download_url;
        setDownloadUrl(url);
      }
      if (payload.job_id) {
        const nextUrl = new URL(window.location.href);
        nextUrl.searchParams.set("job", payload.job_id);
        setReportLink(nextUrl.toString());
        if (pendingTab && !pendingTab.closed) {
          try {
            pendingTab.document.open();
            pendingTab.document.write(
              `<!doctype html><html><head><title>Loading report...</title></head><body style="font-family:system-ui;background:#0b0d13;color:#f7f6f2;padding:24px;">Opening report...</body></html>`
            );
            pendingTab.document.close();
          } catch {}
          pendingTab.location = nextUrl.toString();
        } else {
          // popup blocked, show a manual link
          setMessage("Report ready. Click Open report in new tab.");
        }
      }
    } catch (err) {
      setStatus("error");
      setMessage(err.message || "Upload failed.");
      if (pendingTab && !pendingTab.closed) {
        pendingTab.close();
      }
    }
  };

  const apiBase = endpoint.replace(/\/profile\/?$/, "");

  if (jobId) {
    return (
      <ReportView
        jobId={jobId}
        apiBase={apiBase}
        onBack={() => {
          setJobId("");
          const nextUrl = new URL(window.location.href);
          nextUrl.searchParams.delete("job");
          window.history.replaceState({}, "", nextUrl);
        }}
      />
    );
  }

  return (
    <div className="page">
      <header className="hero">
        <nav className="nav">
          <div className="brand">
            <span className="brand-dot" />
            <span>DataDesc</span>
          </div>
          <div className="nav-links">
            <a href="#features">Features</a>
            <a href="#how">How it works</a>
            <a href="#upload">Upload</a>
          </div>
          <button className="nav-cta">Book a demo</button>
        </nav>

        <div className="hero-grid">
          <div className="hero-copy">
            <p className="eyebrow">Data profiling, re‑imagined</p>
            <h1>
              Turn any dataset into a <span>clean, readable report</span>
              <br /> in minutes.
            </h1>
            <p className="sub">
              DataDesc is a research‑grade profiler that surfaces quality risks, schema
              composition, and statistical summaries at scale.
            </p>
            <div className="hero-actions">
              <button className="primary">Get started</button>
              <button className="ghost">View sample report</button>
            </div>
            <div className="stats">
              {stats.map((item) => (
                <div key={item.label} className="stat-card">
                  <div className="stat-value">{item.value}</div>
                  <div className="stat-label">{item.label}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="hero-panel">
            <div className="panel-header">
              <div>
                <h3>Live profiler</h3>
                <p>Drop files. Generate stats. Export.</p>
              </div>
              <span className="pill">v1.0</span>
            </div>

            <div
              className="dropzone"
              onDrop={onDrop}
              onDragOver={(e) => e.preventDefault()}
            >
              <input
                type="file"
                id="file-input"
                multiple
                accept=".csv,.xlsx,.xls"
                onChange={(e) => onFiles(e.target.files)}
              />
              <label htmlFor="file-input">
                <span>Drag & drop CSV/XLSX</span>
                <span className="muted">or click to browse</span>
              </label>
            </div>

            <div className="file-list">
              {files.length ? (
                files.map((file) => (
                  <div key={file.name} className="file-item">
                    <div>
                      <div className="file-name">{file.name}</div>
                      <div className="file-meta">{(file.size / 1024 / 1024).toFixed(2)} MB</div>
                    </div>
                    <span className="file-tag">queued</span>
                  </div>
                ))
              ) : (
                <p className="muted">No files added yet.</p>
              )}
            </div>

            <div className="endpoint">
              <label>Backend endpoint</label>
              <input
                value={endpoint}
                onChange={(e) => setEndpoint(e.target.value)}
                placeholder="https://api.yourdomain.com/profile"
              />
            </div>

            <div className="panel-actions">
              <button className="primary" onClick={onSubmit}>
                {status === "loading" ? "Running…" : "Run profiling"}
              </button>
              <div className="muted">{(totalSize / 1024 / 1024).toFixed(2)} MB total</div>
            </div>

            {status !== "idle" && (
              <div className={`status ${status}`}>
                <span>{message}</span>
                <div className="status-links">
                  {reportLink && (
                    <a href={reportLink} target="_blank" rel="noreferrer">
                      Open report
                    </a>
                  )}
                  {downloadUrl && <a href={downloadUrl}>Download zip</a>}
                </div>
              </div>
            )}
          </div>
        </div>
      </header>

      <section id="features" className="section">
        <div className="section-head">
          <p className="eyebrow">Why teams pick DataDesc</p>
          <h2>Beautiful outputs, production‑grade signals.</h2>
        </div>
        <div className="feature-grid">
          {featureCards.map((card) => (
            <article key={card.title} className="feature-card">
              <h3>{card.title}</h3>
              <p>{card.body}</p>
            </article>
          ))}
        </div>
      </section>

      <section id="how" className="section how">
        <div className="section-head">
          <p className="eyebrow">How it works</p>
          <h2>Streaming pipeline, deliberate UX.</h2>
        </div>
        <div className="steps">
          {steps.map((step) => (
            <div key={step.label} className="step">
              <div className="step-label">{step.label}</div>
              <p>{step.text}</p>
            </div>
          ))}
        </div>
      </section>

      <section id="upload" className="section callout">
        <div>
          <p className="eyebrow">Ship it</p>
          <h2>Deploy the profiler behind your own API.</h2>
          <p>
            Pair this React frontend with a lightweight FastAPI or Flask endpoint. Send files to
            `/profile`, return a `result_url`, and DataDesc will handle the rest.
          </p>
        </div>
        <div className="callout-actions">
          <button className="ghost">See API spec</button>
          <button className="primary">Deploy on AWS</button>
        </div>
      </section>

      <footer className="footer">
        <div>
          <strong>DataDesc</strong>
          <p>Automated dataset profiling for teams that move fast.</p>
        </div>
        <div className="footer-links">
          <a href="#features">Features</a>
          <a href="#how">How it works</a>
          <a href="#upload">Upload</a>
        </div>
      </footer>
    </div>
  );
}
