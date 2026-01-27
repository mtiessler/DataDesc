def default_config():
    return {
        "top_k": 10,
        "max_corr_cols": 80,
        "preview_rows": 20,
        "max_unique_sample": 200000,
        "sample_rows": 200000,
        "sample_strategy": "head",
        "sample_seed": 42,
        "excel_max_rows": 200000,
        "schema_unique_mode": "approx",
    }
