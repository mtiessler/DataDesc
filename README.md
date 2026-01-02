# DataDesc 🚀

**Automated data profiling for CSV and Excel files** — powered by Polars for high performance on datasets of any size.

Generate comprehensive, research-grade descriptive statistics in seconds. Perfect for data scientists, analysts, and researchers who need instant insights into their datasets.

---

## 🎯 What is DataDesc?

DataDesc is a **high-performance data profiling toolkit** that automatically analyzes your CSV and Excel files to produce:

- **Comprehensive statistical summaries** (mean, median, quartiles, correlations)
- **Data quality reports** (missing values, duplicates, outliers)
- **Schema analysis** (data types, uniqueness, cardinality)
- **Temporal coverage** (automatic date/year extraction)
- **Interactive web dashboard** (built with Streamlit)
- **Aggregated insights** across all your datasets

All powered by **[Polars](https://pola.rs/)** — the next-generation DataFrame library that's **10-100x faster than pandas** for data processing.

---

## ⚡ Why Polars?

DataDesc leverages **Polars** under the hood for exceptional performance:

- **High speed**: Process millions of rows in seconds with multi-threaded execution
- **Memory efficient**: Handle datasets larger than RAM with lazy evaluation
- **Type safety**: Strong typing prevents silent errors and data corruption
- **Modern API**: Expressive, chainable operations with clear error messages
- **Zero-copy operations**: Minimize memory overhead during transformations

Whether you're analyzing 100 rows or 100 million rows, DataDesc with Polars ensures fast, reliable profiling.

---

## 🎯 Perfect For

- **Data Scientists** — Get instant EDA (Exploratory Data Analysis) without writing code
- **Data Engineers** — Validate pipeline outputs and monitor data quality
- **Researchers** — Generate reproducible dataset documentation for publications
- **Analysts** — Quickly understand new datasets before diving into analysis
- **ML Engineers** — Profile training data and detect data drift
- **Data Teams** — Standardize data quality checks across projects

---

## ✨ Key Features

### 📊 Automated Profiling (Per Dataset)
- ✅ **Overview metrics**: rows, columns, memory usage, duplicate detection
- ✅ **Schema analysis**: data types, null counts, uniqueness ratios
- ✅ **Numeric statistics**: min, max, mean, std, quartiles, distribution shape
- ✅ **Categorical analysis**: top-K values, frequency tables, cardinality detection
- ✅ **Missing data analysis**: cell-level and row-level missingness patterns
- ✅ **Correlation matrices**: Pearson correlations with safety limits for wide datasets
- ✅ **Text profiling**: length statistics, empty/whitespace detection
- ✅ **Temporal analysis**: automatic year extraction from date columns
- ✅ **List-like detection**: identify delimiter-separated values (semicolon, pipe, comma)
- ✅ **Key/ID detection**: find potential primary keys and duplicate patterns
- ✅ **Quality warnings**: automated flagging of data quality issues

### 🌍 Global Aggregation
- 📈 **Master summary**: aggregate statistics across all datasets
- 📋 **Cross-dataset insights**: missing data hotspots, schema composition
- ⏰ **Temporal coverage**: min/max years across all datasets
- ⚠️ **Quality dashboard**: aggregated warnings and anomalies

### 🖥️ Interactive Web UI
- 🎨 **Streamlit dashboard**: browse results visually with charts and tables
- 🔍 **Dataset drilldown**: explore individual dataset details
- 📊 **Interactive charts**: Plotly visualizations for distributions and correlations
- 💾 **Export functionality**: download complete results as ZIP

### 🏗️ Production-Ready
- ✅ **Deterministic outputs**: reproducible results for CI/CD pipelines
- ✅ **File-based architecture**: easy versioning and auditing
- ✅ **Excel multi-sheet support**: each sheet analyzed separately
- ✅ **Nested directory support**: recursive dataset discovery
- ✅ **Configurable parameters**: customize top-K limits, correlation thresholds
- ✅ **Robust error handling**: graceful degradation for malformed data

---

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/DataDesc.git
cd DataDesc

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Run Analysis

```bash
# Place your CSV/Excel files in the data/ folder
# (nested directories are supported)

# Run the profiling pipeline
python main.py --inputs data --output output

# Launch the web UI
streamlit run ui/app.py
```

### View Results

Open http://localhost:8501 in your browser to explore:
- Dataset inventory and metrics
- Schema composition across all files
- Missing data patterns
- Quality warnings and anomalies
- Interactive charts and tables

---

## 📁 Project Structure

```
DataDesc/
├── data/                    # Your input datasets (gitignored)
│   ├── customers.csv
│   ├── sales.xlsx          # Multi-sheet support
│   └── analytics/
│       └── metrics.csv
├── output/                  # Generated reports (gitignored)
│   ├── _total/             # Aggregate summaries
│   │   ├── master_summary.md
│   │   ├── datasets_index.csv
│   │   └── sources_table.csv
│   └── {dataset_hash}__name__sheet/  # Per-dataset results
│       ├── overview.json
│       ├── schema.csv
│       ├── numeric_summary.csv
│       ├── categorical_summary.csv
│       ├── missingness.csv
│       ├── correlations.csv
│       └── report.md
├── src/datadesc/           # Core profiling engine
│   ├── profile/            # Profiler modules
│   ├── loaders.py          # CSV/Excel readers (Polars)
│   └── writer.py           # Output generators
├── ui/                     # Streamlit dashboard
│   └── app.py
├── main.py                 # CLI entry point
└── requirements.txt        # Dependencies (polars, streamlit, plotly)
```

---

## 🎛️ Advanced Usage

### Custom Configuration

```bash
python main.py \
  --inputs data external_data \
  --output reports \
  --top-k 20 \
  --max-corr-cols 100 \
  --preview-rows 50 \
  --log-level DEBUG
```

**Parameters:**
- `--inputs`: Multiple input directories (supports wildcards)
- `--output`: Custom output directory
- `--top-k`: Number of top categorical values (default: 10)
- `--max-corr-cols`: Max columns for correlation analysis (default: 80)
- `--preview-rows`: Rows in data preview (default: 20)
- `--log-level`: Logging verbosity (DEBUG, INFO, WARNING, ERROR)

### Python API

```python
from datadesc.profile.pipeline import run_pipeline
from datadesc.logger import setup_logging

log = setup_logging("INFO")

config = {
    "top_k": 15,
    "max_corr_cols": 100,
    "preview_rows": 25,
}

results = run_pipeline(
    inputs=["data", "external"],
    output_dir="output",
    config=config,
    log=log
)

print(f"Processed {results['datasets_processed']} datasets")
```

---

## 📊 Output Examples

### Master Summary (master_summary.md)
```markdown
# Master Summary (Descriptive Statistics)

## Dataset inventory
- **Datasets processed**: 47
- **Total rows (sum across datasets)**: 12,458,392
- **Total columns (sum across datasets)**: 1,247

## Schema composition (global)
| dtype    | count | pct    |
|----------|-------|--------|
| Utf8     | 542   | 43.46% |
| Int64    | 389   | 31.19% |
| Float64  | 251   | 20.13% |
| Boolean  | 65    | 5.21%  |

## Temporal coverage (from datetime-like columns)
| dataset_id | min_year | max_year |
|------------|----------|----------|
| a3f2b1c8   | 2018     | 2024     |
| 5e9d4a2f   | 2015     | 2023     |
```

### Per-Dataset Report (report.md)
```markdown
# Customer Database (sheet: Main)

## Overview
- **rows**: 125,847
- **columns**: 23
- **missing_cell_pct**: 8.42%
- **duplicate_row_pct**: 0.13%
- **memory_bytes_estimate**: 24,567,893
```

---

## 🔮 Roadmap

- [ ] **Extended format support**: Parquet, JSON, SQL databases
- [ ] **Custom profilers**: Plugin system for domain-specific checks
- [ ] **Dataset versioning**: Track changes over time
- [ ] **Data drift detection**: Compare dataset versions
- [ ] **Export formats**: PDF reports, LaTeX tables, Jupyter notebooks
- [ ] **Cloud integration**: S3, Azure Blob, Google Cloud Storage
- [ ] **Scheduled profiling**: Automated pipeline monitoring
- [ ] **API mode**: REST/GraphQL endpoints for programmatic access

---

## 🤝 Contributing

Contributions welcome! Whether it's:
- 🐛 Bug reports
- 💡 Feature requests
- 📖 Documentation improvements
- 🔧 Code contributions

Please open an issue or submit a pull request.

---

## 📄 License

MIT License — free for commercial and academic use.

---

## 🔍 Keywords

data profiling, exploratory data analysis, EDA, dataset analysis, CSV profiling, Excel analysis, data quality, missing data analysis, Polars, pandas alternative, data validation, schema detection, automated EDA, data science tools, data engineering, research data, reproducible research, data documentation, data catalog, data lineage

---