# Copilot Instructions — SSS Healthcheck

## Project summary

Internal service health check for the DBCA **Spatial Support System (SSS)**. The project has two runtime layers:

1. **`status.py`** — A legacy standalone [Quart](https://quart.palletsprojects.com/) app kept for backward compatibility with older routes and external consumers (`/json`, `/prtg`, `/legacy`, `/`, `/api/*`). It still queries external HTTP endpoints, but it is not the preferred place for new UI work.
2. **`healthcheck/`** — The supported health-check application package. It includes the background polling server (`healthcheckserver.py`), the socket IPC layer, configurable check types, and the Quart app extension (`healthcheckapp.py`) that hosts the current dashboard and health-check views.

The project is deployed as a Docker container (Python 3.13 / Alpine) behind a Kubernetes ingress that enforces SSO authentication on protected routes.

> **Legacy status view policy:** Views in `status.py` are considered legacy and will be removed later. Do not add new user-facing views, routes, or HTML/UI features there. Keep changes to `status.py` limited to compatibility fixes, required downstream integration, or necessary bug fixes while the old app remains in service.

---

## Environment setup

> **Always activate the local virtualenv before running any Python commands:**
>
> ```bash
> source .venv/bin/activate
> ```

Dependencies are managed with [uv](https://docs.astral.sh/uv/). To install or sync:

```bash
uv sync
```

To add a new dependency:

```bash
uv add packagename==x.y.z
```

Environment variables are loaded from a `.env` file via **python-dotenv**. Required variables include `USER_SSO`, `PASS_SSO` (for `status.py`) and `AUTH2_USER`, `AUTH2_PASSWORD`, `HEALTHCHECKSERVER_HOST` (for the background server). See `healthcheck/settings.py` for all settings.

---

## Project structure

```
healthcheck/
├── status.py                  # Legacy Quart compatibility app; no new views should be added here
├── test_status.py             # pytest tests for status.py compatibility behaviour
├── prtg_schema.json           # JSON Schema for the /prtg endpoint response format
├── pyproject.toml             # Project metadata, dependencies (uv), ruff config
├── uv.lock                    # Locked dependency versions — commit this file
├── hypercorn.toml             # Hypercorn ASGI server config (port 8080, 4 workers)
├── Dockerfile                 # Multi-stage build: builder (uv/pip) → runtime (Alpine)
├── healthcheck_liveness.sh    # Kubernetes liveness probe for the front-end app
├── healthcheckserver_liveness.sh  # Kubernetes liveness probe for the polling server
├── static/                    # Static assets served by Quart
├── templates/                 # Jinja2 templates (index.html etc.)
├── data_dir/                  # Runtime data directory for the polling server
├── kustomize/                 # Kubernetes manifests (base + overlays)
└── healthcheck/               # Supported package for health-check UI and polling logic
    ├── settings.py            # All environment-variable-driven configuration
    ├── healthcheck.py         # Core healthcheck logic and state
    ├── healthcheckapp.py      # Active Quart app extension for the supported dashboard/views
    ├── healthcheckserver.py   # Background polling server (runs on port 9080)
    ├── healthcheckclient.py   # Client that connects to the polling server
    ├── checks/                # Pluggable check types (httpstatus, jsonresponse, etc.)
    ├── socket/                # Unix socket IPC between app and polling server
    ├── serializers.py         # Response serialisation helpers
    ├── response.py            # Response model
    ├── locks.py / lists.py    # Async concurrency primitives
    └── utils.py               # Shared utilities
```

---

## Conventions

### Language and runtime

- Python **3.13+** is required.
- All async code uses `asyncio` / `async`/`await`. The Quart app and all route handlers are async.
- Use `httpx.AsyncClient` (via `get_session()` or `get_anonymous_session()`) for all outbound HTTP requests. Do not use `requests`.

### Linting

- **ruff** is configured in `pyproject.toml` with `line-length = 140`. Run before committing:

  ```bash
  ruff check .
  ruff format .
  ```

- Bare `except:` clauses are permitted (rule `E722` is ignored) for resilience in health-check probes.

### Testing

- Tests live in `test_status.py` and use **pytest** + **pytest-asyncio**.
- Run tests with:

  ```bash
  pytest -s
  ```

- Mock external HTTP calls with `unittest.mock.AsyncMock` and the `make_mock_client()` helper defined in the test file. Patch `status.get_session`, `status.get_anonymous_session`, `status.get_healthcheck`, `status.get_kb_layer`, or `status.get_kmi_layer` as appropriate.
- Add tests for any new route or helper function.

### Routes and legacy views in `status.py`

- `/readyz` and `/livez` — public Kubernetes probes, no auth.
- `/`, `/json`, `/legacy`, `/prtg`, `/api/*` — legacy compatibility endpoints retained for older consumers; protected at the ingress level by external SSO; no auth code inside the app itself.
- `/prtg` remains in place for downstream monitoring compatibility and returns JSON matching `prtg_schema.json` (`{"prtg": {"result": [...channels], "text": "...", "error": 0|1}}`).
- All aggregate routes call `get_healthcheck()` and transform the result; avoid duplicating HTTP calls.
- Cache-control headers (`max-age=60`) are applied when `CACHE_RESPONSE` is truthy.
- New UI work belongs in `healthcheck/healthcheckapp.py` and the supporting templates/views, not in `status.py`. Treat `status.py` as a compatibility layer that may be removed later.
- Keep changes in `status.py` minimal and backwards-compatible. Do not add new view handlers, dashboard HTML, or user-facing pages there unless a compatibility requirement explicitly demands it.

### Adding new checked sources

1. If a legacy `status.py` compatibility path must be updated, add the URL constant near the top of `status.py` only when required for backward compatibility.
2. Add the fetch/check logic inside `get_healthcheck()`, populating keys on the `d` dict.
3. Add a corresponding channel in `build_prtg_channels()` when the legacy `/prtg` output must continue to expose the metric.
4. Update `SAMPLE_HEALTHCHECK` in `test_status.py` and add test coverage.
5. Prefer adding new health-check logic and new features to the supported `healthcheck/` package rather than expanding the legacy `status.py` surface.

### Docker and deployment

- The Docker image uses a **multi-stage build**: a `builder_base` stage installs dependencies with uv, and the final Alpine stage copies only the virtualenv and application files.
- The container runs as a non-root user (`app`, uid 1000).
- Kubernetes manifests are managed with **Kustomize** under `kustomize/` (base + overlays pattern).
- Pre-commit hooks include **TruffleHog** for credential scanning. Install with `pre-commit install`.
