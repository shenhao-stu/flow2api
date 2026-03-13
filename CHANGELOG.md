# Changelog

This file documents changes made in this fork ([shenhao-stu/flow2api](https://github.com/shenhao-stu/flow2api)) relative to the upstream repository ([TheSmallHanCat/flow2api](https://github.com/TheSmallHanCat/flow2api)).

---

## Fork Changes

> All changes below are additions on top of the upstream codebase. The fork diverged from upstream at commit `fcd61c6` (2025-12-01).

---

### [2026-03-13] fix: auto-detect HTTPS scheme for plugin connection URL (`814f04c`)

- **`src/api/admin.py`**: The plugin connection URL displayed in the admin panel was hardcoded to `http://`. Now reads `X-Forwarded-Proto` header (set by reverse proxies like nginx/Render) and falls back to `request.url.scheme`, so HTTPS deployments correctly display an `https://` URL.

---

### [2026-03-13] feat: PostgreSQL support + Render deployment (`db99f5b`, merged from `cursor/render-f764`)

This group of commits adds full PostgreSQL compatibility and a ready-to-deploy Render configuration.

- **`src/core/pg_compat.py`** *(new)*: SQLite→PostgreSQL shim — translates SQLite-style DDL/DML (e.g. `INTEGER PRIMARY KEY`, `BOOLEAN DEFAULT 0`, `?` placeholders) to PostgreSQL syntax at runtime, enabling the existing codebase to run on both backends without rewriting queries.
- **`src/core/config.py`**: Added `DATABASE_URL` env-var support; auto-detects Postgres vs SQLite based on the URL scheme. Added `setting.toml` override for local development.
- **`src/core/database.py`**: Integrated asyncpg connection pool when Postgres is detected; falls back to aiosqlite for SQLite.
- **`src/api/admin.py`**: Minor compatibility fixes for Postgres query results.
- **`Dockerfile.render`** *(new)*: Minimal Dockerfile for Render (no headed browser, single-process).
- **`docker/entrypoint.render.sh`** *(new)*: Entrypoint script that waits for the Postgres service, runs migrations, then starts the app.
- **`render.yaml`** *(new)*: Render Blueprint — defines the web service + managed PostgreSQL database with all required env vars.
- **`requirements.txt`**: Added `asyncpg`.
- **`main.py`**: Graceful startup banner and port binding from env var.

---

### [2026-03-10] fix: improve browser captcha slot allocation (`eee5075`)

- **`src/services/browser_captcha.py`**: Reworked slot-allocation logic to prevent over-subscription of headed browser workers under concurrent load.

---

### [2026-03-10] feat: expand token project pooling and refine manage UI (`bb42e79`)

- **`src/services/token_manager.py`**: Extended project-level token pooling; tokens can now be grouped by project and selected with weighted round-robin.
- **`static/manage.html`**: UI refinements — improved token table layout, project badge display, and status indicators.

---

### [2026-03-10] feat: improve generation log status handling (`aa68501`)

- **`src/core/database.py`**: Added generation log status fields; new queries for filtering/paginating logs by status.
- **`src/core/models.py`**: Extended `GenerationLog` model with `status` and `error_message` fields.
- **`src/services/generation_handler.py`**: Generation pipeline now writes structured status updates (`queued` / `running` / `success` / `failed`) throughout the lifecycle.
- **`src/api/admin.py`**: New admin API endpoints for querying logs by status.
- **`static/manage.html`**: Log viewer now shows coloured status badges and supports status filtering.

---

### [2026-03-09] fix: enforce tier-based generation limits (`b44c70a`)

- **`src/core/account_tiers.py`** *(new)*: Defines per-tier concurrency and resolution limits (free / pro / ultra).
- **`src/services/generation_handler.py`**: Enforces tier limits before dispatching; returns a meaningful 429 response when exceeded.
- **`src/services/load_balancer.py`**: Exposes tier information to the generation handler.

---

### [2026-03-09] feat: support disabling cache cleanup (`8dcb50e`)

- **`src/services/file_cache.py`**: Added `disable_cache_cleanup` flag; when set, generated files are never auto-deleted.
- **`src/api/admin.py`**: New config endpoint to toggle cache cleanup.
- **`config/setting_example.toml`** / **`static/manage.html`**: Exposed the new toggle in the admin UI.

---

### [2026-03-08] fix: token always-expired bug + full PostgreSQL compatibility audit (`0a35d82`)

- Fixed a bug where tokens were incorrectly marked as expired due to timezone-naive vs timezone-aware datetime comparison.
- Full audit of all SQL queries for PostgreSQL compatibility (boolean literals, placeholder syntax, column types).

---

### [2026-03-08] refactor: remove local launch throttling (`d5b0688`)

- **`src/services/flow_client.py`** / **`src/services/generation_handler.py`**: Removed per-process launch throttling that was causing artificial latency; concurrency is now managed entirely by the load balancer.

---

### [2026-03-08] feat: tighten headed browser lifecycle (`58b388d`, `95b9219`, `bd7f93f`, `c2ec39e`, `f401f83`)

- **`src/services/browser_captcha.py`**: Headed browser sessions are now kept alive between requests (warm pool), with health-check pings and automatic restart on crash. Slot allocation and scheduling were also overhauled.
- **`src/core/config.py`**: Added `browser_keep_alive` and related config knobs.

---

### [2026-03-07] fix: sync R2V model keys and image limits (`a756954`)

- **`src/services/flow_client.py`**: Corrected the landscape model key for R2V (image-to-video) requests.
- **`src/services/generation_handler.py`**: Fixed max-image-count enforcement for R2V endpoints.

---

### [2026-03-06] feat: remote captcha concurrent scheduling + observability (`e3a2615`)

- **`src/services/flow_client.py`**: Major rewrite of the remote captcha dispatch path — added a weighted concurrency scheduler, retry backoff, and per-provider health tracking.
- **`src/services/concurrency_manager.py`** *(new)*: Generic async semaphore pool for managing per-provider concurrency budgets.
- **`src/api/admin.py`** / **`src/core/database.py`** / **`static/manage.html`**: New admin panel section showing live captcha provider stats (success rate, latency, active slots).

---

### [2026-03-06] fix: sync R2V request body with upstream V2 payload (`d4bb151`)

- **`src/services/flow_client.py`**: Updated R2V request payload structure to match the upstream Flow V2 API change.

---

### [2026-03-03] feat: token-level captcha proxy + headed Docker dual-image (`6d4cb8b`)

- **`src/services/browser_captcha.py`** / **`src/services/browser_captcha_personal.py`**: Each token can now have its own captcha proxy, independent of the global proxy setting.
- **`Dockerfile.headed`** *(new)*: Separate Docker image that includes Chromium for in-container headed browser captcha solving.
- **`docker-compose.headed.yml`** *(new)*: Docker Compose profile for headed-mode deployments.
- **`docker/entrypoint.headed.sh`** *(new)*: Entrypoint that launches the Chromium helper process alongside the API server.
- **`.github/workflows/docker-publish.yml`**: Updated CI to build and push both `latest` (headless) and `headed` Docker image tags.

---

### [2026-03-03] fix: browser captcha 500 self-healing + background startup (`cc6036a`)

- **`src/services/browser_captcha.py`** / **`src/services/browser_captcha_personal.py`**: Added a watchdog that detects HTTP 500 responses from the headed browser, automatically restarts the process, and retries the captcha request.
- **`src/services/flow_client.py`**: Browser captcha process is now started in the background at server startup instead of lazily on first request.
