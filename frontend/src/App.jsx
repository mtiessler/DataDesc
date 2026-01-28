import { useEffect, useMemo, useState } from "react";
import ReportView from "./ReportView.jsx";

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
      <header className="hero compact">
        <div className="hero-grid">
          <div className="hero-copy">
            <p className="eyebrow">DataDesc Profiler</p>
            <h1>
              Upload files. Get a report.
            </h1>
            <p className="sub">
              Drop CSV/XLSX files to generate the full profiling report in seconds.
            </p>
          </div>

          <div className="hero-panel">
            <div className="panel-header">
              <div>
                <h3>Live profiler</h3>
                <p>Upload files and generate reports.</p>
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
    </div>
  );
}
