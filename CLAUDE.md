# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Dagster (`dg`) pipeline that scrapes Fandango.com for movie theater/showtime/seatmap data and lands it in Databricks (Unity Catalog volumes + SQL warehouse). Asset dependency chain: `franchises` → `theaters` → `showtimes_raw` → `showtimes` → `seatmaps_raw` → `seatmaps`.

## Commands

```bash
uv sync                 # install deps into .venv (uv is the package manager; pip -e ".[dev]" also works)
dg dev                  # start the Dagster webserver/daemon at http://localhost:3000
```

There is no configured lint, format, or test runner (`tests/` is an empty stub, `pyproject.toml` has no `[tool.ruff]`/`[tool.pytest]` sections) — don't assume `pytest`/`ruff` commands exist unless you add the tooling yourself.

To materialize/debug a single asset chain outside the webserver, edit and run `debug.py` (it calls `dagster.materialize` directly on a list of assets) — this is also wired up as the "Debug Dagster Job" launch config in VS Code.

`DAGSTER_ENV` (set in `.env`, e.g. `dev`) selects the top-level key read out of `src/box_office/config.yaml`; Databricks auth comes from the `main` profile in `~/.databrickscfg` (not from the commented-out `DATABRICKS_*` vars in `.env`).

## Architecture

### Asset layout (`src/box_office/defs/`)

Each pipeline stage lives in its own folder (`franchises/`, `theaters/`, `showtimes/`, `seatmaps/`) and is auto-loaded by `load_from_defs_folder` in [definitions.py](src/box_office/definitions.py) — there's no manual asset registry to update when adding a new folder/asset. Resources are wired centrally in [defs/resources.py](src/box_office/defs/resources.py).

Two recurring patterns across stages:

1. **Scrape → snapshot → SQL table** (franchises, theaters): a `*_snapshot` asset scrapes Fandango HTML with `requests`/`BeautifulSoup`, writes a Polars DataFrame as parquet to a Databricks volume path (`DatabricksResource.upload_polars`), and returns a `run_id`. A downstream asset then runs a `CREATE OR REPLACE TABLE ... AS SELECT * FROM read_files(...)` query (`DatabricksResource.submit_query`) to materialize `{catalog}.base.<name>`.

2. **Distributed scrape → raw JSON → notebook cleaning** (showtimes, seatmaps): a `*_raw` asset builds an `asyncio.Queue` of API requests and fans it out across many concurrent workers (see below), flushing raw JSON responses to a Databricks volume as `.jsonl`. A downstream asset (`showtimes`/`seatmaps`) then runs the corresponding notebook in `data_cleaning/` via `papermill` — these notebooks use Databricks Connect (`DatabricksSession`) + PySpark to reshape the raw JSON into typed tables in `{catalog}.base`. Both cleaning assets accept an optional `run_id` config override so a specific raw snapshot can be reprocessed without re-scraping.

Each asset module independently loads `config.yaml` at import time via `importlib.resources`, keyed by `os.getenv("DAGSTER_ENV")` — this happens at module scope (not inside the asset function), so config is fixed at process start.

### Scraping infrastructure (`src/box_office/resources/common/`)

- **`RequestNode`**: one concurrent worker that pulls requests off a shared `asyncio.Queue`, issues them with `curl_cffi` (Chrome-impersonated, routed through a proxy), and flushes responses to Databricks in chunks. On a 403/407 or too many consecutive failures it rotates to a new proxy and opens a new `AsyncSession`. `showtimes_raw` spins up `RequestNode`s directly; `seatmaps_raw` goes through `ClusterManager`, a thin wrapper that fans a queue out across N `RequestNode`s (`numNodes`/`chunkSize`/`timeout`/`maxRequestsFailedInRow` all come from `config.yaml`).
- **`ProxyResource`/`ProxyClient`**: manages a rotating pool of Webshare proxies loaded from a local `:`-delimited credentials file (`resources/common/Webshare 100 proxies.txt` — **not tracked in git, treat as a secret**). Proxies that get blocked go into a cooldown queue with a strike counter (`maxStrikes = 3`) before being retried.
- **`DatabricksResource`**: wraps `databricks.sdk.WorkspaceClient`. Handles file uploads to volumes (parquet/JSON/JSONL, always via `upload_raw_jsonl`/`upload_polars`/`upload_dict_as_json*` → `upload`) and SQL execution against a hardcoded warehouse id (`submit_query`/`query`, using `EXTERNAL_LINKS`/`ARROW_STREAM` disposition and reassembling Arrow chunks into a Polars DataFrame).

### Config (`src/box_office/config.yaml`)

YAML anchors (`&common`, `&franchises`, etc.) get merged into a `default` block and then per-environment blocks (currently only `dev`) override `catalog` via `<<: *default`. Snapshot paths are Databricks volume path templates containing `{catalog}`/`{run_id}` placeholders, formatted in each asset module.
