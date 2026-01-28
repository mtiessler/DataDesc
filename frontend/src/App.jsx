import { useEffect, useMemo, useState } from "react";
import ReportView from "./ReportView.jsx";

function getConsent() {
  try {
    return localStorage.getItem("dd_cookie_consent");
  } catch {
    return null;
  }
}

function setConsent(value) {
  try {
    localStorage.setItem("dd_cookie_consent", value);
  } catch {
    // ignore
  }
}

export default function App() {
  const defaultEndpoint =
    import.meta.env.VITE_API_ENDPOINT || "http://localhost:8000/profile";
  const gaId = import.meta.env.VITE_GA_ID || "";
  const [endpoint, setEndpoint] = useState(defaultEndpoint);
  const [files, setFiles] = useState([]);
  const [status, setStatus] = useState("idle");
  const [message, setMessage] = useState("");
  const [downloadUrl, setDownloadUrl] = useState("");
  const [jobId, setJobId] = useState("");
  const [reportLink, setReportLink] = useState("");
  const [cookieConsent, setCookieConsent] = useState(getConsent());

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

  useEffect(() => {
    if (!gaId) return;
    if (cookieConsent !== "accepted") return;
    if (window.gtag) return;

    const script = document.createElement("script");
    script.async = true;
    script.src = `https://www.googletagmanager.com/gtag/js?id=${gaId}`;
    document.head.appendChild(script);

    window.dataLayer = window.dataLayer || [];
    function gtag() {
      window.dataLayer.push(arguments);
    }
    window.gtag = gtag;
    gtag("js", new Date());
    gtag("config", gaId, { anonymize_ip: true });
  }, [gaId, cookieConsent]);

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
        setMessage("Report ready. Click Open report in new tab.");
      }
    } catch (err) {
      setStatus("error");
      setMessage(err.message || "Upload failed.");
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
            <h1>Upload files. Get a report.</h1>
            <p className="sub">
              Secure, automated profiling for CSV and Excel. Files stay private to your
              workspace and are processed immediately.
            </p>
            <div className="trust-row">
              <span>Secure processing</span>
              <span>Fast results</span>
              <span>No manual setup</span>
            </div>
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

      <section className="section">
        <div className="section-head">
          <p className="eyebrow">What is DataDesc</p>
          <h2>Fast, automatic profiling for CSV and Excel.</h2>
          <p className="sub">
            DataDesc generates descriptive statistics, schema insights, missingness analysis,
            and quality warnings in seconds. It is built for teams who want clarity without
            setup overhead.
          </p>
        </div>
        <div className="section-grid">
          <div className="feature-card">
            <h3>Descriptive statistics</h3>
            <p>Mean, median, quartiles, distributions, correlations, and uniqueness signals.</p>
          </div>
          <div className="feature-card">
            <h3>Data quality checks</h3>
            <p>Missingness hotspots, duplicates, constant columns, and anomaly flags.</p>
          </div>
          <div className="feature-card">
            <h3>Professional reports</h3>
            <p>Interactive dashboards with charts, drilldowns, and exportable outputs.</p>
          </div>
        </div>
      </section>

      <section className="section security">
        <div className="section-head">
          <p className="eyebrow">Security</p>
          <h2>Built for sensitive data.</h2>
          <p className="sub">
            Files are processed on your server and never leave your environment unless you
            choose to export or share. You control retention and access policies.
          </p>
        </div>
        <div className="section-grid">
          <div className="feature-card">
            <h3>Private processing</h3>
            <p>Uploads are handled by your backend and stored under your own output directory.</p>
          </div>
          <div className="feature-card">
            <h3>Minimal exposure</h3>
            <p>Reports are generated as static assets; you decide who can access them.</p>
          </div>
          <div className="feature-card">
            <h3>Configurable retention</h3>
            <p>Keep outputs for auditing or wipe them automatically after delivery.</p>
          </div>
        </div>
      </section>

      <section className="section">
        <div className="section-head">
          <p className="eyebrow">How it works</p>
          <h2>Upload → Profile → Share.</h2>
        </div>
        <div className="section-grid">
          <div className="feature-card">
            <h3>1. Upload</h3>
            <p>Drop CSV/XLSX files into the profiler.</p>
          </div>
          <div className="feature-card">
            <h3>2. Profile</h3>
            <p>Automated statistics and quality checks run immediately.</p>
          </div>
          <div className="feature-card">
            <h3>3. Report</h3>
            <p>Open the interactive report or export as ZIP/PDF.</p>
          </div>
        </div>
      </section>

      <footer className="footer">
        <div>
          <strong>DataDesc</strong>
          <p>Secure, automated data profiling for modern teams.</p>
        </div>
        <div className="footer-links">
          <span>Security-first by design</span>
          <span>Fast, reproducible outputs</span>
          <span>Built on Polars + FastAPI</span>
        </div>
      </footer>

      {!cookieConsent && (
        <div className="cookie-banner">
          <div>
            <strong>Cookies</strong>
            <p>We use analytics cookies to understand usage and improve the product.</p>
          </div>
          <div className="cookie-actions">
            <button
              className="ghost"
              onClick={() => {
                setConsent("declined");
                setCookieConsent("declined");
              }}
            >
              Decline
            </button>
            <button
              className="primary"
              onClick={() => {
                setConsent("accepted");
                setCookieConsent("accepted");
              }}
            >
              Accept
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
