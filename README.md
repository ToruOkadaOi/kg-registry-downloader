# kg-registry-downloader

Interactive py script for downloading Neo4j products from [kghub.org](https://kghub.org)'s KG Registry.

## Features

- Fetches KG Registry parquet data with local caching
- Extracts and filters Neo4j-related products
- Interactive filtering (regex keywords, deduplication, URL validation)
- Async downloads with progress bars and retry logic
- Saves filtered metadata as CSV

## Contents

- `kg-downloader.py` – main download script
- `data/` – local downloaded/output data (ignored in git)
- `kg-reg-venv/` – local virtual environment (ignored in git)

## Quick start

1. Create and activate a virtual environment
2. Install dependencies:

```bash
pip install pyarrow pandas aiohttp rich
```

3. Run:

```bash
python kg-downloader.py
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KG_REGISTRY_PARQUET_URL` | `https://kghub.org/kg-registry/registry/parquet/resources.parquet` | Parquet URL |
| `KG_REGISTRY_CACHE_PATH` | `data/data_raw/kg_registry/resources.parquet` | Local cache path |
| `KG_NEO4J_OUT_DIR` | `data/data_raw/KG_neo4j_downloads` | Output directory |
| `KG_REGISTRY_FORCE_REFRESH` | `false` | Force refresh cached parquet |
