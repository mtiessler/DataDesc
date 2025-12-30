
# DataDesc

**DataDesc** is a project for generating high-quality descriptive statistics for data science datasets.  
It scans folders containing CSV and Excel files, produces structured per-dataset reports, and generates an aggregate summary across everything processed.

---

## Why DataDesc

In many data science projects, the first steps are repetitive:
- Count entities
- Inspect schema and types
- Measure missingness and duplicates
- Generate summary statistics
- Produce reproducible documentation

DataDesc standardizes these steps into a **deterministic, auditable pipeline** suitable for research projects, data platforms, and institutional reporting.

---

## Features

- Recursive discovery of datasets:
  - CSV (`.csv`)
  - Excel (`.xlsx`, `.xls`)
  
- Per dataset (and per Excel sheet):
  - Dataset overview (rows, columns, memory, duplicates, missingness)
  - Schema and inferred data types
  - Numeric descriptive statistics
  - Categorical summaries with top-K values
  - Missingness tables
  - Correlation matrices (with safety limits)
  - Data previews
  - Human-readable Markdown report
  
- Clean output structure:
  - One directory per dataset
  - One global summary directory
- Deterministic outputs suitable for CI, versioning, and automation

---

## Repository Structure

```

DataDesc/
data/                      # Input data (any nested structure)
raw/
external/
curated/
output/                    # Generated results (gitignored)
src/
datadesc/
**init**.py
cli.py
discover.py
io.py
profiling.py
reporting.py
utils.py
tests/
.gitignore
pyproject.toml
README.md

````

---

## Installation

```bash
python -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows

pip install -e .
````

---

## Usage

### 1. Add data

Place your CSV or Excel files anywhere under `data/`:

```
data/
  internal/
    people.xlsx
    publications.csv
  projects/
    eu_projects.xlsx
```

Nested directories are fully supported.

---

### 2. Run DataDesc

```bash
datadesc run --inputs data --output output
```

Multiple input roots are supported:

```bash
datadesc run --inputs data external_data --output output
```

---

## Output Layout

### Per-dataset output

Each file (and each Excel sheet) gets its own directory:

```
output/
  a91f3c2e__publications__sheet-None/
    overview.json
    schema.csv
    preview.csv
    missingness.csv
    numeric_summary.csv
    categorical_summary.csv
    correlations.csv
    report.md
```

### Aggregate output

```
output/
  _total/
    datasets_index.csv
    totals.json
    totals.md
```

This provides:

* A registry of all processed datasets
* Global totals (rows, columns, number of datasets)
* A concise summary for reporting and documentation

---

## Design Principles

* **Reproducible**: deterministic file-based outputs
* **Composable**: core logic usable as a Python library
* **UI-ready**: clear separation between computation and presentation
* **Safe by default**: guardrails for large or wide datasets
* **Research-grade**: suitable for institutional and academic contexts

---

## Planned Extensions

* Web UI with drag-and-drop dataset upload
* JSON-only API mode for frontend consumption
* Custom profiling plugins and domain-specific checks
* Dataset comparison and drift analysis
* Export to PDF / LaTeX summaries

---

## License

MIT (recommended for research and tooling projects)

---

**DataDesc** aims to make the *first mile* of data science projects fast, structured, and defensible.
