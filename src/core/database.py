"""Database storage layer for Flow2API — supports SQLite and PostgreSQL."""
import os
import re
import json
import aiosqlite
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path
from .models import Token, TokenStats, Task, RequestLog, AdminConfig, ProxyConfig, GenerationConfig, CacheConfig, Project, CaptchaConfig, PluginConfig

# ---------------------------------------------------------------------------
# Detect backend from environment
# ---------------------------------------------------------------------------
_DATABASE_URL: Optional[str] = os.environ.get("DATABASE_URL", "")

# Neon / standard Postgres DSN uses postgresql:// or postgres://
_USE_POSTGRES: bool = bool(_DATABASE_URL and (
    _DATABASE_URL.startswith("postgresql://") or
    _DATABASE_URL.startswith("postgres://")
))


# ---------------------------------------------------------------------------
# Small helper: convert SQLite-style "?" placeholders to "$1, $2, …" for asyncpg
# ---------------------------------------------------------------------------
def _to_pg(sql: str, params: tuple) -> tuple[str, tuple]:
    """Replace every '?' in *sql* with $1, $2, … positional params."""
    counter = 0

    def _replace(_match):
        nonlocal counter
        counter += 1
        return f"${counter}"

    return re.sub(r"\?", _replace, sql), params


# ---------------------------------------------------------------------------
# Thin asyncpg connection context-manager (mirrors aiosqlite API just enough)
# ---------------------------------------------------------------------------
class _PgConn:
    """Wraps an asyncpg connection to look like an aiosqlite connection."""

    def __init__(self, conn):
        self._conn = conn
        self._tr = None

    async def execute(self, sql: str, params=()):
        pg_sql, pg_params = _to_pg(sql, tuple(params) if params else ())
        # asyncpg uses *args not a tuple
        if pg_params:
            return await self._conn.execute(pg_sql, *pg_params)
        return await self._conn.execute(pg_sql)

    async def fetch(self, sql: str, params=()):
        pg_sql, pg_params = _to_pg(sql, tuple(params) if params else ())
        if pg_params:
            return await self._conn.fetch(pg_sql, *pg_params)
        return await self._conn.fetch(pg_sql)

    async def fetchrow(self, sql: str, params=()):
        pg_sql, pg_params = _to_pg(sql, tuple(params) if params else ())
        if pg_params:
            return await self._conn.fetchrow(pg_sql, *pg_params)
        return await self._conn.fetchrow(pg_sql)

    async def fetchval(self, sql: str, params=()):
        pg_sql, pg_params = _to_pg(sql, tuple(params) if params else ())
        if pg_params:
            return await self._conn.fetchval(pg_sql, *pg_params)
        return await self._conn.fetchval(pg_sql)

    async def commit(self):
        pass  # handled by transaction context

    async def __aenter__(self):
        self._tr = self._conn.transaction()
        await self._tr.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self._tr.commit()
        else:
            await self._tr.rollback()


class _PgPool:
    """Wraps an asyncpg pool and provides connect() semantics."""

    def __init__(self, pool):
        self._pool = pool

    def acquire(self) -> "_PgAcquireCtx":
        return _PgAcquireCtx(self._pool)

    async def close(self):
        await self._pool.close()


class _PgAcquireCtx:
    def __init__(self, pool):
        self._pool = pool
        self._raw = None

    async def __aenter__(self) -> _PgConn:
        self._raw = await self._pool.acquire()
        self._conn = _PgConn(self._raw)
        await self._conn.__aenter__()
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        await self._conn.__aexit__(exc_type, exc, tb)
        await self._pool.release(self._raw)


# ---------------------------------------------------------------------------
# Database class — public API is identical regardless of backend
# ---------------------------------------------------------------------------
class Database:
    """Database manager — SQLite (default) or PostgreSQL (when DATABASE_URL is set)."""

    def __init__(self, db_path: str = None):
        self._pg_pool = None

        if _USE_POSTGRES:
            self.db_path = None
            self._dsn = _DATABASE_URL
        else:
            if db_path is None:
                data_dir = Path(__file__).parent.parent.parent / "data"
                data_dir.mkdir(exist_ok=True)
                db_path = str(data_dir / "flow.db")
            self.db_path = db_path
            self._dsn = None

    # ------------------------------------------------------------------
    # Internal: connection context managers
    # ------------------------------------------------------------------
    def _connect(self):
        """Return an async context manager yielding a connection/cursor-like object."""
        if _USE_POSTGRES:
            return self._pg_pool.acquire()
        return _SqliteCtx(self.db_path)

    async def _init_pg_pool(self):
        """Create the asyncpg connection pool (called once at startup)."""
        import asyncpg
        dsn = self._dsn
        # asyncpg requires 'postgresql://' not 'postgres://'
        if dsn.startswith("postgres://"):
            dsn = "postgresql://" + dsn[len("postgres://"):]
        self._pg_pool = _PgPool(await asyncpg.create_pool(dsn, min_size=2, max_size=10))

    async def close(self):
        """Close the connection pool (call on shutdown)."""
        if self._pg_pool:
            await self._pg_pool.close()

    # ------------------------------------------------------------------
    # Introspection helpers (schema-dependent)
    # ------------------------------------------------------------------
    def db_exists(self) -> bool:
        if _USE_POSTGRES:
            return True  # remote DB is always "existing"
        return Path(self.db_path).exists()

    async def _table_exists(self, db, table_name: str) -> bool:
        if _USE_POSTGRES:
            row = await db.fetchrow(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name=?",
                (table_name,),
            )
            return row is not None
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        result = await cursor.fetchone()
        return result is not None

    async def _column_exists(self, db, table_name: str, column_name: str) -> bool:
        if _USE_POSTGRES:
            row = await db.fetchrow(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=? AND column_name=?",
                (table_name, column_name),
            )
            return row is not None
        try:
            cursor = await db.execute(f"PRAGMA table_info({table_name})")
            columns = await cursor.fetchall()
            return any(col[1] == column_name for col in columns)
        except Exception:
            return False

    # ------------------------------------------------------------------
    # SQL dialect helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _pk_autoincrement() -> str:
        """Primary-key auto-increment syntax."""
        if _USE_POSTGRES:
            return "SERIAL PRIMARY KEY"
        return "INTEGER PRIMARY KEY AUTOINCREMENT"

    @staticmethod
    def _pk_default_1() -> str:
        """Singleton PK with default value 1."""
        if _USE_POSTGRES:
            return "INTEGER PRIMARY KEY DEFAULT 1"
        return "INTEGER PRIMARY KEY DEFAULT 1"

    @staticmethod
    def _returning_id(table: str = "") -> str:
        """Clause to retrieve the new auto-generated id."""
        if _USE_POSTGRES:
            return "RETURNING id"
        return ""  # sqlite uses lastrowid

    # ------------------------------------------------------------------
    # fetch helpers that work for both backends
    # ------------------------------------------------------------------
    async def _fetchone(self, db, sql: str, params=()):
        """Return one row as dict or None."""
        if _USE_POSTGRES:
            row = await db.fetchrow(sql, params)
            return dict(row) if row else None
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(sql, params)
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def _fetchall(self, db, sql: str, params=()):
        """Return list of dicts."""
        if _USE_POSTGRES:
            rows = await db.fetch(sql, params)
            return [dict(r) for r in rows]
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(sql, params)
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def _fetchscalar(self, db, sql: str, params=()):
        """Return a single scalar value."""
        if _USE_POSTGRES:
            return await db.fetchval(sql, params)
        cursor = await db.execute(sql, params)
        row = await cursor.fetchone()
        return row[0] if row else None

    async def _execute_returning_id(self, db, sql: str, params=()):
        """Execute an INSERT and return the new row id."""
        if _USE_POSTGRES:
            pg_sql, pg_params = _to_pg(sql + " RETURNING id", tuple(params))
            val = await db._conn.fetchval(pg_sql, *pg_params)
            return val
        cursor = await db.execute(sql, params)
        await db.commit()
        return cursor.lastrowid

    # ------------------------------------------------------------------
    # Schema: CREATE TABLE helpers (unified SQL)
    # ------------------------------------------------------------------
    def _ddl_tokens(self) -> str:
        pk = self._pk_autoincrement()
        return f"""
            CREATE TABLE IF NOT EXISTS tokens (
                id {pk},
                st TEXT UNIQUE NOT NULL,
                at TEXT,
                at_expires TIMESTAMP,
                email TEXT NOT NULL,
                name TEXT,
                remark TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP,
                use_count INTEGER DEFAULT 0,
                credits INTEGER DEFAULT 0,
                user_paygate_tier TEXT,
                current_project_id TEXT,
                current_project_name TEXT,
                image_enabled BOOLEAN DEFAULT TRUE,
                video_enabled BOOLEAN DEFAULT TRUE,
                image_concurrency INTEGER DEFAULT -1,
                video_concurrency INTEGER DEFAULT -1,
                captcha_proxy_url TEXT,
                ban_reason TEXT,
                banned_at TIMESTAMP
            )"""

    def _ddl_projects(self) -> str:
        pk = self._pk_autoincrement()
        return f"""
            CREATE TABLE IF NOT EXISTS projects (
                id {pk},
                project_id TEXT UNIQUE NOT NULL,
                token_id INTEGER NOT NULL,
                project_name TEXT NOT NULL,
                tool_name TEXT DEFAULT 'PINHOLE',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (token_id) REFERENCES tokens(id)
            )"""

    def _ddl_token_stats(self) -> str:
        pk = self._pk_autoincrement()
        return f"""
            CREATE TABLE IF NOT EXISTS token_stats (
                id {pk},
                token_id INTEGER NOT NULL,
                image_count INTEGER DEFAULT 0,
                video_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                last_success_at TIMESTAMP,
                last_error_at TIMESTAMP,
                today_image_count INTEGER DEFAULT 0,
                today_video_count INTEGER DEFAULT 0,
                today_error_count INTEGER DEFAULT 0,
                today_date DATE,
                consecutive_error_count INTEGER DEFAULT 0,
                FOREIGN KEY (token_id) REFERENCES tokens(id)
            )"""

    def _ddl_tasks(self) -> str:
        pk = self._pk_autoincrement()
        return f"""
            CREATE TABLE IF NOT EXISTS tasks (
                id {pk},
                task_id TEXT UNIQUE NOT NULL,
                token_id INTEGER NOT NULL,
                model TEXT NOT NULL,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'processing',
                progress INTEGER DEFAULT 0,
                result_urls TEXT,
                error_message TEXT,
                scene_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (token_id) REFERENCES tokens(id)
            )"""

    def _ddl_request_logs(self) -> str:
        pk = self._pk_autoincrement()
        return f"""
            CREATE TABLE IF NOT EXISTS request_logs (
                id {pk},
                token_id INTEGER,
                operation TEXT NOT NULL,
                request_body TEXT,
                response_body TEXT,
                status_code INTEGER NOT NULL,
                duration FLOAT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (token_id) REFERENCES tokens(id)
            )"""

    # ------------------------------------------------------------------
    # _ensure_config_rows
    # ------------------------------------------------------------------
    async def _ensure_config_rows(self, db, config_dict: dict = None):
        """Ensure all singleton config rows exist (id=1)."""

        async def _count(table: str) -> int:
            if _USE_POSTGRES:
                return await db.fetchval(f"SELECT COUNT(*) FROM {table}")
            cursor = await db.execute(f"SELECT COUNT(*) FROM {table}")
            row = await cursor.fetchone()
            return row[0]

        # admin_config
        if await _count("admin_config") == 0:
            admin_username = "admin"
            admin_password = "admin"
            api_key = "han1234"
            error_ban_threshold = 3
            if config_dict:
                g = config_dict.get("global", {})
                admin_username = g.get("admin_username", admin_username)
                admin_password = g.get("admin_password", admin_password)
                api_key = g.get("api_key", api_key)
                error_ban_threshold = config_dict.get("admin", {}).get("error_ban_threshold", error_ban_threshold)
            await db.execute("""
                INSERT INTO admin_config (id, username, password, api_key, error_ban_threshold)
                VALUES (1, ?, ?, ?, ?)
            """, (admin_username, admin_password, api_key, error_ban_threshold))

        # proxy_config
        if await _count("proxy_config") == 0:
            proxy_enabled = False
            proxy_url = None
            media_proxy_enabled = False
            media_proxy_url = None
            if config_dict:
                pc = config_dict.get("proxy", {})
                proxy_enabled = pc.get("proxy_enabled", False)
                proxy_url = pc.get("proxy_url", "") or None
                media_proxy_enabled = pc.get("media_proxy_enabled", pc.get("image_io_proxy_enabled", False))
                media_proxy_url = pc.get("media_proxy_url", pc.get("image_io_proxy_url", "")) or None
            await db.execute("""
                INSERT INTO proxy_config (id, enabled, proxy_url, media_proxy_enabled, media_proxy_url)
                VALUES (1, ?, ?, ?, ?)
            """, (proxy_enabled, proxy_url, media_proxy_enabled, media_proxy_url))

        # generation_config
        if await _count("generation_config") == 0:
            image_timeout = 300
            video_timeout = 1500
            if config_dict:
                gc = config_dict.get("generation", {})
                image_timeout = gc.get("image_timeout", image_timeout)
                video_timeout = gc.get("video_timeout", video_timeout)
            await db.execute("""
                INSERT INTO generation_config (id, image_timeout, video_timeout)
                VALUES (1, ?, ?)
            """, (image_timeout, video_timeout))

        # cache_config
        if await _count("cache_config") == 0:
            cache_enabled = False
            cache_timeout = 7200
            cache_base_url = None
            if config_dict:
                cc = config_dict.get("cache", {})
                cache_enabled = cc.get("enabled", False)
                cache_timeout = cc.get("timeout", cache_timeout)
                cache_base_url = cc.get("base_url", "") or None
            await db.execute("""
                INSERT INTO cache_config (id, cache_enabled, cache_timeout, cache_base_url)
                VALUES (1, ?, ?, ?)
            """, (cache_enabled, cache_timeout, cache_base_url))

        # debug_config
        if await _count("debug_config") == 0:
            debug_enabled = False
            log_requests = True
            log_responses = True
            mask_token = True
            if config_dict:
                dc = config_dict.get("debug", {})
                debug_enabled = dc.get("enabled", False)
                log_requests = dc.get("log_requests", True)
                log_responses = dc.get("log_responses", True)
                mask_token = dc.get("mask_token", True)
            await db.execute("""
                INSERT INTO debug_config (id, enabled, log_requests, log_responses, mask_token)
                VALUES (1, ?, ?, ?, ?)
            """, (debug_enabled, log_requests, log_responses, mask_token))

        # captcha_config
        if await _count("captcha_config") == 0:
            captcha_method = "browser"
            yescaptcha_api_key = ""
            yescaptcha_base_url = "https://api.yescaptcha.com"
            remote_browser_base_url = ""
            remote_browser_api_key = ""
            remote_browser_timeout = 60
            if config_dict:
                cap = config_dict.get("captcha", {})
                captcha_method = cap.get("captcha_method", captcha_method)
                yescaptcha_api_key = cap.get("yescaptcha_api_key", yescaptcha_api_key)
                yescaptcha_base_url = cap.get("yescaptcha_base_url", yescaptcha_base_url)
                remote_browser_base_url = cap.get("remote_browser_base_url", remote_browser_base_url)
                remote_browser_api_key = cap.get("remote_browser_api_key", remote_browser_api_key)
                remote_browser_timeout = cap.get("remote_browser_timeout", remote_browser_timeout)
            try:
                remote_browser_timeout = max(5, int(remote_browser_timeout))
            except Exception:
                remote_browser_timeout = 60
            await db.execute("""
                INSERT INTO captcha_config (
                    id, captcha_method, yescaptcha_api_key, yescaptcha_base_url,
                    remote_browser_base_url, remote_browser_api_key, remote_browser_timeout
                ) VALUES (1, ?, ?, ?, ?, ?, ?)
            """, (captcha_method, yescaptcha_api_key, yescaptcha_base_url,
                  remote_browser_base_url, remote_browser_api_key, remote_browser_timeout))

        # plugin_config
        if await _count("plugin_config") == 0:
            await db.execute("""
                INSERT INTO plugin_config (id, connection_token, auto_enable_on_update)
                VALUES (1, '', TRUE)
            """)

        if not _USE_POSTGRES:
            await db.commit()

    # ------------------------------------------------------------------
    # check_and_migrate_db
    # ------------------------------------------------------------------
    async def check_and_migrate_db(self, config_dict: dict = None):
        """Check database integrity and perform migrations if needed."""
        if _USE_POSTGRES:
            await self._pg_migrate(config_dict)
        else:
            await self._sqlite_migrate(config_dict)

    async def _sqlite_migrate(self, config_dict: dict = None):
        async with aiosqlite.connect(self.db_path) as db:
            print("Checking database integrity and performing migrations...")
            await self._create_missing_tables_sqlite(db)
            await self._add_missing_columns_sqlite(db)
            await self._ensure_config_rows(db, config_dict=config_dict)
            await db.commit()
            print("Database migration check completed.")

    async def _pg_migrate(self, config_dict: dict = None):
        async with self._pg_pool.acquire() as db:
            print("Checking PostgreSQL database integrity and performing migrations...")
            await self._create_missing_tables_pg(db)
            await self._add_missing_columns_pg(db)
            await self._ensure_config_rows(db, config_dict=config_dict)
            print("Database migration check completed.")

    # --- SQLite: create missing tables ---
    async def _create_missing_tables_sqlite(self, db):
        tables = {
            "cache_config": """CREATE TABLE cache_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                cache_enabled BOOLEAN DEFAULT 0,
                cache_timeout INTEGER DEFAULT 7200,
                cache_base_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            "proxy_config": """CREATE TABLE proxy_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                enabled BOOLEAN DEFAULT 0,
                proxy_url TEXT,
                media_proxy_enabled BOOLEAN DEFAULT 0,
                media_proxy_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            "captcha_config": """CREATE TABLE captcha_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                captcha_method TEXT DEFAULT 'browser',
                yescaptcha_api_key TEXT DEFAULT '',
                yescaptcha_base_url TEXT DEFAULT 'https://api.yescaptcha.com',
                capmonster_api_key TEXT DEFAULT '',
                capmonster_base_url TEXT DEFAULT 'https://api.capmonster.cloud',
                ezcaptcha_api_key TEXT DEFAULT '',
                ezcaptcha_base_url TEXT DEFAULT 'https://api.ez-captcha.com',
                capsolver_api_key TEXT DEFAULT '',
                capsolver_base_url TEXT DEFAULT 'https://api.capsolver.com',
                remote_browser_base_url TEXT DEFAULT '',
                remote_browser_api_key TEXT DEFAULT '',
                remote_browser_timeout INTEGER DEFAULT 60,
                website_key TEXT DEFAULT '6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV',
                page_action TEXT DEFAULT 'IMAGE_GENERATION',
                browser_proxy_enabled BOOLEAN DEFAULT 0,
                browser_proxy_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            "plugin_config": """CREATE TABLE plugin_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                connection_token TEXT DEFAULT '',
                auto_enable_on_update BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        }
        for name, ddl in tables.items():
            if not await self._table_exists(db, name):
                print(f"  ✓ Creating missing table: {name}")
                await db.execute(ddl)

    async def _add_missing_columns_sqlite(self, db):
        additions = {
            "tokens": [
                ("at", "TEXT"),
                ("at_expires", "TIMESTAMP"),
                ("credits", "INTEGER DEFAULT 0"),
                ("user_paygate_tier", "TEXT"),
                ("current_project_id", "TEXT"),
                ("current_project_name", "TEXT"),
                ("image_enabled", "BOOLEAN DEFAULT 1"),
                ("video_enabled", "BOOLEAN DEFAULT 1"),
                ("image_concurrency", "INTEGER DEFAULT -1"),
                ("video_concurrency", "INTEGER DEFAULT -1"),
                ("captcha_proxy_url", "TEXT"),
                ("ban_reason", "TEXT"),
                ("banned_at", "TIMESTAMP"),
            ],
            "admin_config": [("error_ban_threshold", "INTEGER DEFAULT 3")],
            "proxy_config": [
                ("media_proxy_enabled", "BOOLEAN DEFAULT 0"),
                ("media_proxy_url", "TEXT"),
            ],
            "captcha_config": [
                ("browser_proxy_enabled", "BOOLEAN DEFAULT 0"),
                ("browser_proxy_url", "TEXT"),
                ("capmonster_api_key", "TEXT DEFAULT ''"),
                ("capmonster_base_url", "TEXT DEFAULT 'https://api.capmonster.cloud'"),
                ("ezcaptcha_api_key", "TEXT DEFAULT ''"),
                ("ezcaptcha_base_url", "TEXT DEFAULT 'https://api.ez-captcha.com'"),
                ("capsolver_api_key", "TEXT DEFAULT ''"),
                ("capsolver_base_url", "TEXT DEFAULT 'https://api.capsolver.com'"),
                ("browser_count", "INTEGER DEFAULT 1"),
                ("remote_browser_base_url", "TEXT DEFAULT ''"),
                ("remote_browser_api_key", "TEXT DEFAULT ''"),
                ("remote_browser_timeout", "INTEGER DEFAULT 60"),
            ],
            "token_stats": [
                ("today_image_count", "INTEGER DEFAULT 0"),
                ("today_video_count", "INTEGER DEFAULT 0"),
                ("today_error_count", "INTEGER DEFAULT 0"),
                ("today_date", "DATE"),
                ("consecutive_error_count", "INTEGER DEFAULT 0"),
            ],
            "plugin_config": [("auto_enable_on_update", "BOOLEAN DEFAULT 1")],
        }
        for table, cols in additions.items():
            if not await self._table_exists(db, table):
                continue
            for col_name, col_type in cols:
                if not await self._column_exists(db, table, col_name):
                    try:
                        await db.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                        print(f"  ✓ Added column '{col_name}' to {table}")
                    except Exception as e:
                        print(f"  ✗ Failed to add column '{col_name}': {e}")

    # --- PostgreSQL: create missing tables ---
    async def _create_missing_tables_pg(self, db):
        tables = {
            "cache_config": """CREATE TABLE IF NOT EXISTS cache_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                cache_enabled BOOLEAN DEFAULT FALSE,
                cache_timeout INTEGER DEFAULT 7200,
                cache_base_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            "proxy_config": """CREATE TABLE IF NOT EXISTS proxy_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                enabled BOOLEAN DEFAULT FALSE,
                proxy_url TEXT,
                media_proxy_enabled BOOLEAN DEFAULT FALSE,
                media_proxy_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            "captcha_config": """CREATE TABLE IF NOT EXISTS captcha_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                captcha_method TEXT DEFAULT 'browser',
                yescaptcha_api_key TEXT DEFAULT '',
                yescaptcha_base_url TEXT DEFAULT 'https://api.yescaptcha.com',
                capmonster_api_key TEXT DEFAULT '',
                capmonster_base_url TEXT DEFAULT 'https://api.capmonster.cloud',
                ezcaptcha_api_key TEXT DEFAULT '',
                ezcaptcha_base_url TEXT DEFAULT 'https://api.ez-captcha.com',
                capsolver_api_key TEXT DEFAULT '',
                capsolver_base_url TEXT DEFAULT 'https://api.capsolver.com',
                remote_browser_base_url TEXT DEFAULT '',
                remote_browser_api_key TEXT DEFAULT '',
                remote_browser_timeout INTEGER DEFAULT 60,
                website_key TEXT DEFAULT '6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV',
                page_action TEXT DEFAULT 'IMAGE_GENERATION',
                browser_proxy_enabled BOOLEAN DEFAULT FALSE,
                browser_proxy_url TEXT,
                browser_count INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            "plugin_config": """CREATE TABLE IF NOT EXISTS plugin_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                connection_token TEXT DEFAULT '',
                auto_enable_on_update BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        }
        for name, ddl in tables.items():
            if not await self._table_exists(db, name):
                print(f"  ✓ Creating missing table: {name}")
                await db.execute(ddl)

    async def _add_missing_columns_pg(self, db):
        additions = {
            "tokens": [
                ("at", "TEXT"),
                ("at_expires", "TIMESTAMP"),
                ("credits", "INTEGER DEFAULT 0"),
                ("user_paygate_tier", "TEXT"),
                ("current_project_id", "TEXT"),
                ("current_project_name", "TEXT"),
                ("image_enabled", "BOOLEAN DEFAULT TRUE"),
                ("video_enabled", "BOOLEAN DEFAULT TRUE"),
                ("image_concurrency", "INTEGER DEFAULT -1"),
                ("video_concurrency", "INTEGER DEFAULT -1"),
                ("captcha_proxy_url", "TEXT"),
                ("ban_reason", "TEXT"),
                ("banned_at", "TIMESTAMP"),
            ],
            "admin_config": [("error_ban_threshold", "INTEGER DEFAULT 3")],
            "proxy_config": [
                ("media_proxy_enabled", "BOOLEAN DEFAULT FALSE"),
                ("media_proxy_url", "TEXT"),
            ],
            "captcha_config": [
                ("browser_proxy_enabled", "BOOLEAN DEFAULT FALSE"),
                ("browser_proxy_url", "TEXT"),
                ("capmonster_api_key", "TEXT DEFAULT ''"),
                ("capmonster_base_url", "TEXT DEFAULT 'https://api.capmonster.cloud'"),
                ("ezcaptcha_api_key", "TEXT DEFAULT ''"),
                ("ezcaptcha_base_url", "TEXT DEFAULT 'https://api.ez-captcha.com'"),
                ("capsolver_api_key", "TEXT DEFAULT ''"),
                ("capsolver_base_url", "TEXT DEFAULT 'https://api.capsolver.com'"),
                ("browser_count", "INTEGER DEFAULT 1"),
                ("remote_browser_base_url", "TEXT DEFAULT ''"),
                ("remote_browser_api_key", "TEXT DEFAULT ''"),
                ("remote_browser_timeout", "INTEGER DEFAULT 60"),
            ],
            "token_stats": [
                ("today_image_count", "INTEGER DEFAULT 0"),
                ("today_video_count", "INTEGER DEFAULT 0"),
                ("today_error_count", "INTEGER DEFAULT 0"),
                ("today_date", "DATE"),
                ("consecutive_error_count", "INTEGER DEFAULT 0"),
            ],
            "plugin_config": [("auto_enable_on_update", "BOOLEAN DEFAULT TRUE")],
        }
        for table, cols in additions.items():
            if not await self._table_exists(db, table):
                continue
            for col_name, col_type in cols:
                if not await self._column_exists(db, table, col_name):
                    try:
                        await db.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                        print(f"  ✓ Added column '{col_name}' to {table}")
                    except Exception as e:
                        print(f"  ✗ Failed to add column '{col_name}': {e}")

    # ------------------------------------------------------------------
    # init_db — create all tables fresh
    # ------------------------------------------------------------------
    async def init_db(self):
        """Initialize database tables."""
        if _USE_POSTGRES:
            # ensure pool exists
            if self._pg_pool is None:
                await self._init_pg_pool()
            await self._init_db_pg()
        else:
            await self._init_db_sqlite()

    async def _init_db_sqlite(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(self._ddl_tokens())
            await db.execute(self._ddl_projects())
            await db.execute(self._ddl_token_stats())
            await db.execute(self._ddl_tasks())
            await db.execute(self._ddl_request_logs())
            await db.execute("""CREATE TABLE IF NOT EXISTS admin_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                username TEXT DEFAULT 'admin',
                password TEXT DEFAULT 'admin',
                api_key TEXT DEFAULT 'han1234',
                error_ban_threshold INTEGER DEFAULT 3,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS proxy_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                enabled BOOLEAN DEFAULT 0,
                proxy_url TEXT,
                media_proxy_enabled BOOLEAN DEFAULT 0,
                media_proxy_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS generation_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                image_timeout INTEGER DEFAULT 300,
                video_timeout INTEGER DEFAULT 1500,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS cache_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                cache_enabled BOOLEAN DEFAULT 0,
                cache_timeout INTEGER DEFAULT 7200,
                cache_base_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS debug_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                enabled BOOLEAN DEFAULT 0,
                log_requests BOOLEAN DEFAULT 1,
                log_responses BOOLEAN DEFAULT 1,
                mask_token BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS captcha_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                captcha_method TEXT DEFAULT 'browser',
                yescaptcha_api_key TEXT DEFAULT '',
                yescaptcha_base_url TEXT DEFAULT 'https://api.yescaptcha.com',
                capmonster_api_key TEXT DEFAULT '',
                capmonster_base_url TEXT DEFAULT 'https://api.capmonster.cloud',
                ezcaptcha_api_key TEXT DEFAULT '',
                ezcaptcha_base_url TEXT DEFAULT 'https://api.ez-captcha.com',
                capsolver_api_key TEXT DEFAULT '',
                capsolver_base_url TEXT DEFAULT 'https://api.capsolver.com',
                remote_browser_base_url TEXT DEFAULT '',
                remote_browser_api_key TEXT DEFAULT '',
                remote_browser_timeout INTEGER DEFAULT 60,
                website_key TEXT DEFAULT '6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV',
                page_action TEXT DEFAULT 'IMAGE_GENERATION',
                browser_proxy_enabled BOOLEAN DEFAULT 0,
                browser_proxy_url TEXT,
                browser_count INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS plugin_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                connection_token TEXT DEFAULT '',
                auto_enable_on_update BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_task_id ON tasks(task_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_token_st ON tokens(st)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_project_id ON projects(project_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_tokens_email ON tokens(email)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_tokens_is_active_last_used_at ON tokens(is_active, last_used_at)")
            await self._migrate_request_logs_sqlite(db)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_created_at ON request_logs(created_at DESC)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_token_id_created_at ON request_logs(token_id, created_at DESC)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_token_stats_token_id ON token_stats(token_id)")
            await db.commit()

    async def _init_db_pg(self):
        async with self._pg_pool.acquire() as db:
            await db.execute(self._ddl_tokens())
            await db.execute(self._ddl_projects())
            await db.execute(self._ddl_token_stats())
            await db.execute(self._ddl_tasks())
            await db.execute(self._ddl_request_logs())
            await db.execute("""CREATE TABLE IF NOT EXISTS admin_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                username TEXT DEFAULT 'admin',
                password TEXT DEFAULT 'admin',
                api_key TEXT DEFAULT 'han1234',
                error_ban_threshold INTEGER DEFAULT 3,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS proxy_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                enabled BOOLEAN DEFAULT FALSE,
                proxy_url TEXT,
                media_proxy_enabled BOOLEAN DEFAULT FALSE,
                media_proxy_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS generation_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                image_timeout INTEGER DEFAULT 300,
                video_timeout INTEGER DEFAULT 1500,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS cache_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                cache_enabled BOOLEAN DEFAULT FALSE,
                cache_timeout INTEGER DEFAULT 7200,
                cache_base_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS debug_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                enabled BOOLEAN DEFAULT FALSE,
                log_requests BOOLEAN DEFAULT TRUE,
                log_responses BOOLEAN DEFAULT TRUE,
                mask_token BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS captcha_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                captcha_method TEXT DEFAULT 'browser',
                yescaptcha_api_key TEXT DEFAULT '',
                yescaptcha_base_url TEXT DEFAULT 'https://api.yescaptcha.com',
                capmonster_api_key TEXT DEFAULT '',
                capmonster_base_url TEXT DEFAULT 'https://api.capmonster.cloud',
                ezcaptcha_api_key TEXT DEFAULT '',
                ezcaptcha_base_url TEXT DEFAULT 'https://api.ez-captcha.com',
                capsolver_api_key TEXT DEFAULT '',
                capsolver_base_url TEXT DEFAULT 'https://api.capsolver.com',
                remote_browser_base_url TEXT DEFAULT '',
                remote_browser_api_key TEXT DEFAULT '',
                remote_browser_timeout INTEGER DEFAULT 60,
                website_key TEXT DEFAULT '6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV',
                page_action TEXT DEFAULT 'IMAGE_GENERATION',
                browser_proxy_enabled BOOLEAN DEFAULT FALSE,
                browser_proxy_url TEXT,
                browser_count INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS plugin_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                connection_token TEXT DEFAULT '',
                auto_enable_on_update BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            # indexes
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_task_id ON tasks(task_id)",
                "CREATE INDEX IF NOT EXISTS idx_token_st ON tokens(st)",
                "CREATE INDEX IF NOT EXISTS idx_project_id ON projects(project_id)",
                "CREATE INDEX IF NOT EXISTS idx_tokens_email ON tokens(email)",
                "CREATE INDEX IF NOT EXISTS idx_tokens_is_active_last_used_at ON tokens(is_active, last_used_at)",
                "CREATE INDEX IF NOT EXISTS idx_request_logs_created_at ON request_logs(created_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_request_logs_token_id_created_at ON request_logs(token_id, created_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_token_stats_token_id ON token_stats(token_id)",
            ]:
                await db.execute(idx_sql)

    # ------------------------------------------------------------------
    # SQLite request_logs migration (legacy)
    # ------------------------------------------------------------------
    async def _migrate_request_logs_sqlite(self, db):
        try:
            has_model = await self._column_exists(db, "request_logs", "model")
            has_operation = await self._column_exists(db, "request_logs", "operation")
            if has_model and not has_operation:
                print("Migrating old request_logs schema...")
                await db.execute("ALTER TABLE request_logs RENAME TO request_logs_old")
                await db.execute(self._ddl_request_logs())
                await db.execute("""
                    INSERT INTO request_logs (token_id, operation, request_body, status_code, duration, created_at)
                    SELECT token_id,
                           model,
                           json_object('model', model, 'prompt', substr(prompt,1,100)),
                           CASE WHEN status='completed' THEN 200 WHEN status='failed' THEN 500 ELSE 0 END,
                           response_time, created_at
                    FROM request_logs_old
                """)
                await db.execute("DROP TABLE request_logs_old")
                print("request_logs migration complete.")
        except Exception as e:
            print(f"request_logs migration failed: {e}")

    # ------------------------------------------------------------------
    # init_config_from_toml
    # ------------------------------------------------------------------
    async def init_config_from_toml(self, config_dict: dict, is_first_startup: bool = True):
        if _USE_POSTGRES:
            async with self._pg_pool.acquire() as db:
                if is_first_startup:
                    await self._ensure_config_rows(db, config_dict)
                else:
                    await self._ensure_config_rows(db, config_dict=None)
        else:
            async with aiosqlite.connect(self.db_path) as db:
                if is_first_startup:
                    await self._ensure_config_rows(db, config_dict)
                else:
                    await self._ensure_config_rows(db, config_dict=None)
                await db.commit()

    # ------------------------------------------------------------------
    # reload_config_to_memory
    # ------------------------------------------------------------------
    async def reload_config_to_memory(self):
        from .config import config

        admin_config = await self.get_admin_config()
        if admin_config:
            config.set_admin_username_from_db(admin_config.username)
            config.set_admin_password_from_db(admin_config.password)
            config.api_key = admin_config.api_key

        cache_config = await self.get_cache_config()
        if cache_config:
            config.set_cache_enabled(cache_config.cache_enabled)
            config.set_cache_timeout(cache_config.cache_timeout)
            config.set_cache_base_url(cache_config.cache_base_url or "")

        generation_config = await self.get_generation_config()
        if generation_config:
            config.set_image_timeout(generation_config.image_timeout)
            config.set_video_timeout(generation_config.video_timeout)

        debug_config = await self.get_debug_config()
        if debug_config:
            config.set_debug_enabled(debug_config.enabled)

        captcha_config = await self.get_captcha_config()
        if captcha_config:
            config.set_captcha_method(captcha_config.captcha_method)
            config.set_yescaptcha_api_key(captcha_config.yescaptcha_api_key)
            config.set_yescaptcha_base_url(captcha_config.yescaptcha_base_url)
            config.set_capmonster_api_key(captcha_config.capmonster_api_key)
            config.set_capmonster_base_url(captcha_config.capmonster_base_url)
            config.set_ezcaptcha_api_key(captcha_config.ezcaptcha_api_key)
            config.set_ezcaptcha_base_url(captcha_config.ezcaptcha_base_url)
            config.set_capsolver_api_key(captcha_config.capsolver_api_key)
            config.set_capsolver_base_url(captcha_config.capsolver_base_url)
            config.set_remote_browser_base_url(captcha_config.remote_browser_base_url)
            config.set_remote_browser_api_key(captcha_config.remote_browser_api_key)
            config.set_remote_browser_timeout(captcha_config.remote_browser_timeout)

    # ==================================================================
    # Token operations
    # ==================================================================
    async def add_token(self, token: Token) -> int:
        if _USE_POSTGRES:
            async with self._pg_pool.acquire() as db:
                token_id = await self._execute_returning_id(db, """
                    INSERT INTO tokens (st, at, at_expires, email, name, remark, is_active,
                                       credits, user_paygate_tier, current_project_id, current_project_name,
                                       image_enabled, video_enabled, image_concurrency, video_concurrency, captcha_proxy_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (token.st, token.at, token.at_expires, token.email, token.name, token.remark,
                      token.is_active, token.credits, token.user_paygate_tier,
                      token.current_project_id, token.current_project_name,
                      token.image_enabled, token.video_enabled,
                      token.image_concurrency, token.video_concurrency, token.captcha_proxy_url))
                await db.execute("INSERT INTO token_stats (token_id) VALUES (?)", (token_id,))
                return token_id
        else:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("""
                    INSERT INTO tokens (st, at, at_expires, email, name, remark, is_active,
                                       credits, user_paygate_tier, current_project_id, current_project_name,
                                       image_enabled, video_enabled, image_concurrency, video_concurrency, captcha_proxy_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (token.st, token.at, token.at_expires, token.email, token.name, token.remark,
                      token.is_active, token.credits, token.user_paygate_tier,
                      token.current_project_id, token.current_project_name,
                      token.image_enabled, token.video_enabled,
                      token.image_concurrency, token.video_concurrency, token.captcha_proxy_url))
                await db.commit()
                token_id = cursor.lastrowid
                await db.execute("INSERT INTO token_stats (token_id) VALUES (?)", (token_id,))
                await db.commit()
                return token_id

    async def get_token(self, token_id: int) -> Optional[Token]:
        row = await self._query_one("SELECT * FROM tokens WHERE id = ?", (token_id,))
        return Token(**row) if row else None

    async def get_token_by_st(self, st: str) -> Optional[Token]:
        row = await self._query_one("SELECT * FROM tokens WHERE st = ?", (st,))
        return Token(**row) if row else None

    async def get_token_by_email(self, email: str) -> Optional[Token]:
        row = await self._query_one("SELECT * FROM tokens WHERE email = ?", (email,))
        return Token(**row) if row else None

    async def get_all_tokens(self) -> List[Token]:
        rows = await self._query_many("SELECT * FROM tokens ORDER BY created_at DESC")
        return [Token(**r) for r in rows]

    async def get_all_tokens_with_stats(self) -> List[Dict[str, Any]]:
        return await self._query_many("""
            SELECT t.*,
                   COALESCE(ts.image_count, 0) AS image_count,
                   COALESCE(ts.video_count, 0) AS video_count,
                   COALESCE(ts.error_count, 0) AS error_count
            FROM tokens t
            LEFT JOIN token_stats ts ON ts.token_id = t.id
            ORDER BY t.created_at DESC
        """)

    async def get_dashboard_stats(self) -> Dict[str, int]:
        token_row = await self._query_one("""
            SELECT COUNT(*) AS total_tokens,
                   COALESCE(SUM(CASE WHEN is_active THEN 1 ELSE 0 END), 0) AS active_tokens
            FROM tokens
        """)
        stats_row = await self._query_one("""
            SELECT COALESCE(SUM(image_count), 0) AS total_images,
                   COALESCE(SUM(video_count), 0) AS total_videos,
                   COALESCE(SUM(error_count), 0) AS total_errors,
                   COALESCE(SUM(today_image_count), 0) AS today_images,
                   COALESCE(SUM(today_video_count), 0) AS today_videos,
                   COALESCE(SUM(today_error_count), 0) AS today_errors
            FROM token_stats
        """)
        td = token_row or {}
        sd = stats_row or {}
        return {
            "total_tokens": int(td.get("total_tokens") or 0),
            "active_tokens": int(td.get("active_tokens") or 0),
            "total_images": int(sd.get("total_images") or 0),
            "total_videos": int(sd.get("total_videos") or 0),
            "total_errors": int(sd.get("total_errors") or 0),
            "today_images": int(sd.get("today_images") or 0),
            "today_videos": int(sd.get("today_videos") or 0),
            "today_errors": int(sd.get("today_errors") or 0),
        }

    async def get_system_info_stats(self) -> Dict[str, int]:
        row = await self._query_one("""
            SELECT COUNT(*) AS total_tokens,
                   COALESCE(SUM(CASE WHEN is_active THEN 1 ELSE 0 END), 0) AS active_tokens,
                   COALESCE(SUM(CASE WHEN is_active THEN credits ELSE 0 END), 0) AS total_credits
            FROM tokens
        """)
        d = row or {}
        return {
            "total_tokens": int(d.get("total_tokens") or 0),
            "active_tokens": int(d.get("active_tokens") or 0),
            "total_credits": int(d.get("total_credits") or 0),
        }

    async def get_active_tokens(self) -> List[Token]:
        rows = await self._query_many("SELECT * FROM tokens WHERE is_active = TRUE ORDER BY last_used_at ASC")
        return [Token(**r) for r in rows]

    async def update_token(self, token_id: int, **kwargs):
        updates = []
        params = []
        for key, value in kwargs.items():
            if value is not None:
                updates.append(key)
                params.append(value)
        if not updates:
            return
        params.append(token_id)
        await self._update(
            f"UPDATE tokens SET {', '.join(f'{k} = ?' for k in updates)} WHERE id = ?",
            params,
        )

    async def delete_token(self, token_id: int):
        await self._update("DELETE FROM token_stats WHERE token_id = ?", (token_id,))
        await self._update("DELETE FROM projects WHERE token_id = ?", (token_id,))
        await self._update("DELETE FROM tokens WHERE id = ?", (token_id,))

    # ==================================================================
    # Project operations
    # ==================================================================
    async def add_project(self, project: Project) -> int:
        if _USE_POSTGRES:
            async with self._pg_pool.acquire() as db:
                return await self._execute_returning_id(db, """
                    INSERT INTO projects (project_id, token_id, project_name, tool_name, is_active)
                    VALUES (?, ?, ?, ?, ?)
                """, (project.project_id, project.token_id, project.project_name,
                      project.tool_name, project.is_active))
        else:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("""
                    INSERT INTO projects (project_id, token_id, project_name, tool_name, is_active)
                    VALUES (?, ?, ?, ?, ?)
                """, (project.project_id, project.token_id, project.project_name,
                      project.tool_name, project.is_active))
                await db.commit()
                return cursor.lastrowid

    async def get_project_by_id(self, project_id: str) -> Optional[Project]:
        row = await self._query_one("SELECT * FROM projects WHERE project_id = ?", (project_id,))
        return Project(**row) if row else None

    async def get_projects_by_token(self, token_id: int) -> List[Project]:
        rows = await self._query_many(
            "SELECT * FROM projects WHERE token_id = ? ORDER BY created_at DESC", (token_id,))
        return [Project(**r) for r in rows]

    async def delete_project(self, project_id: str):
        await self._update("DELETE FROM projects WHERE project_id = ?", (project_id,))

    # ==================================================================
    # Task operations
    # ==================================================================
    async def create_task(self, task: Task) -> int:
        if _USE_POSTGRES:
            async with self._pg_pool.acquire() as db:
                return await self._execute_returning_id(db, """
                    INSERT INTO tasks (task_id, token_id, model, prompt, status, progress, scene_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (task.task_id, task.token_id, task.model, task.prompt,
                      task.status, task.progress, task.scene_id))
        else:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("""
                    INSERT INTO tasks (task_id, token_id, model, prompt, status, progress, scene_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (task.task_id, task.token_id, task.model, task.prompt,
                      task.status, task.progress, task.scene_id))
                await db.commit()
                return cursor.lastrowid

    async def get_task(self, task_id: str) -> Optional[Task]:
        row = await self._query_one("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
        if row:
            if row.get("result_urls") and isinstance(row["result_urls"], str):
                row["result_urls"] = json.loads(row["result_urls"])
            return Task(**row)
        return None

    async def update_task(self, task_id: str, **kwargs):
        updates = []
        params = []
        for key, value in kwargs.items():
            if value is not None:
                if key == "result_urls" and isinstance(value, list):
                    value = json.dumps(value)
                updates.append(key)
                params.append(value)
        if not updates:
            return
        params.append(task_id)
        await self._update(
            f"UPDATE tasks SET {', '.join(f'{k} = ?' for k in updates)} WHERE task_id = ?",
            params,
        )

    # ==================================================================
    # Token stats operations
    # ==================================================================
    async def increment_token_stats(self, token_id: int, stat_type: str):
        if stat_type == "image":
            await self.increment_image_count(token_id)
        elif stat_type == "video":
            await self.increment_video_count(token_id)
        elif stat_type == "error":
            await self.increment_error_count(token_id)

    async def get_token_stats(self, token_id: int) -> Optional[TokenStats]:
        row = await self._query_one("SELECT * FROM token_stats WHERE token_id = ?", (token_id,))
        return TokenStats(**row) if row else None

    async def increment_image_count(self, token_id: int):
        from datetime import date
        today = str(date.today())
        row = await self._query_one("SELECT today_date FROM token_stats WHERE token_id = ?", (token_id,))
        if row and str(row.get("today_date") or "") != today:
            await self._update("""
                UPDATE token_stats SET image_count = image_count + 1,
                    today_image_count = 1, today_date = ? WHERE token_id = ?
            """, (today, token_id))
        else:
            await self._update("""
                UPDATE token_stats SET image_count = image_count + 1,
                    today_image_count = today_image_count + 1, today_date = ? WHERE token_id = ?
            """, (today, token_id))

    async def increment_video_count(self, token_id: int):
        from datetime import date
        today = str(date.today())
        row = await self._query_one("SELECT today_date FROM token_stats WHERE token_id = ?", (token_id,))
        if row and str(row.get("today_date") or "") != today:
            await self._update("""
                UPDATE token_stats SET video_count = video_count + 1,
                    today_video_count = 1, today_date = ? WHERE token_id = ?
            """, (today, token_id))
        else:
            await self._update("""
                UPDATE token_stats SET video_count = video_count + 1,
                    today_video_count = today_video_count + 1, today_date = ? WHERE token_id = ?
            """, (today, token_id))

    async def increment_error_count(self, token_id: int):
        from datetime import date
        today = str(date.today())
        row = await self._query_one("SELECT today_date FROM token_stats WHERE token_id = ?", (token_id,))
        if row and str(row.get("today_date") or "") != today:
            await self._update("""
                UPDATE token_stats SET error_count = error_count + 1,
                    consecutive_error_count = consecutive_error_count + 1,
                    today_error_count = 1, today_date = ?, last_error_at = CURRENT_TIMESTAMP
                WHERE token_id = ?
            """, (today, token_id))
        else:
            await self._update("""
                UPDATE token_stats SET error_count = error_count + 1,
                    consecutive_error_count = consecutive_error_count + 1,
                    today_error_count = today_error_count + 1, today_date = ?,
                    last_error_at = CURRENT_TIMESTAMP
                WHERE token_id = ?
            """, (today, token_id))

    async def reset_error_count(self, token_id: int):
        await self._update(
            "UPDATE token_stats SET consecutive_error_count = 0 WHERE token_id = ?",
            (token_id,),
        )

    # ==================================================================
    # Config operations
    # ==================================================================
    async def get_admin_config(self) -> Optional[AdminConfig]:
        row = await self._query_one("SELECT * FROM admin_config WHERE id = 1")
        return AdminConfig(**row) if row else None

    async def update_admin_config(self, **kwargs):
        updates = [k for k, v in kwargs.items() if v is not None]
        params = [v for v in kwargs.values() if v is not None]
        if updates:
            updates_with_ts = updates + ["updated_at"]
            params_with_ts = params + ["CURRENT_TIMESTAMP"]
            # use literal for updated_at
            set_clause = ", ".join(f"{k} = ?" for k in updates) + ", updated_at = CURRENT_TIMESTAMP"
            await self._update(f"UPDATE admin_config SET {set_clause} WHERE id = 1", params)

    async def get_proxy_config(self) -> Optional[ProxyConfig]:
        row = await self._query_one("SELECT * FROM proxy_config WHERE id = 1")
        return ProxyConfig(**row) if row else None

    async def update_proxy_config(
        self,
        enabled: bool,
        proxy_url: Optional[str] = None,
        media_proxy_enabled: Optional[bool] = None,
        media_proxy_url: Optional[str] = None,
    ):
        row = await self._query_one("SELECT * FROM proxy_config WHERE id = 1")
        if row:
            new_media_proxy_enabled = media_proxy_enabled if media_proxy_enabled is not None else row.get("media_proxy_enabled", False)
            new_media_proxy_url = media_proxy_url if media_proxy_url is not None else row.get("media_proxy_url")
            await self._update("""
                UPDATE proxy_config SET enabled = ?, proxy_url = ?,
                    media_proxy_enabled = ?, media_proxy_url = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """, (enabled, proxy_url, new_media_proxy_enabled, new_media_proxy_url))
        else:
            new_media_proxy_enabled = media_proxy_enabled if media_proxy_enabled is not None else False
            await self._update("""
                INSERT INTO proxy_config (id, enabled, proxy_url, media_proxy_enabled, media_proxy_url)
                VALUES (1, ?, ?, ?, ?)
            """, (enabled, proxy_url, new_media_proxy_enabled, media_proxy_url))

    async def get_generation_config(self) -> Optional[GenerationConfig]:
        row = await self._query_one("SELECT * FROM generation_config WHERE id = 1")
        return GenerationConfig(**row) if row else None

    async def update_generation_config(self, image_timeout: int, video_timeout: int):
        await self._update("""
            UPDATE generation_config SET image_timeout = ?, video_timeout = ?,
                updated_at = CURRENT_TIMESTAMP WHERE id = 1
        """, (image_timeout, video_timeout))

    async def get_cache_config(self) -> CacheConfig:
        row = await self._query_one("SELECT * FROM cache_config WHERE id = 1")
        return CacheConfig(**row) if row else CacheConfig(cache_enabled=False, cache_timeout=7200)

    async def update_cache_config(self, enabled: bool = None, timeout: int = None, base_url: Optional[str] = None):
        row = await self._query_one("SELECT * FROM cache_config WHERE id = 1")
        if row:
            new_enabled = enabled if enabled is not None else row.get("cache_enabled", False)
            new_timeout = timeout if timeout is not None else row.get("cache_timeout", 7200)
            new_base_url = row.get("cache_base_url")
            if base_url is not None:
                new_base_url = base_url or None
            await self._update("""
                UPDATE cache_config SET cache_enabled = ?, cache_timeout = ?, cache_base_url = ?,
                    updated_at = CURRENT_TIMESTAMP WHERE id = 1
            """, (new_enabled, new_timeout, new_base_url))
        else:
            await self._update("""
                INSERT INTO cache_config (id, cache_enabled, cache_timeout, cache_base_url)
                VALUES (1, ?, ?, ?)
            """, (enabled or False, timeout or 7200, base_url or None))

    async def get_debug_config(self):
        from .models import DebugConfig
        row = await self._query_one("SELECT * FROM debug_config WHERE id = 1")
        return DebugConfig(**row) if row else DebugConfig(enabled=False, log_requests=True, log_responses=True, mask_token=True)

    async def update_debug_config(
        self,
        enabled: bool = None,
        log_requests: bool = None,
        log_responses: bool = None,
        mask_token: bool = None,
    ):
        row = await self._query_one("SELECT * FROM debug_config WHERE id = 1")
        if row:
            new_enabled = enabled if enabled is not None else row.get("enabled", False)
            new_log_req = log_requests if log_requests is not None else row.get("log_requests", True)
            new_log_res = log_responses if log_responses is not None else row.get("log_responses", True)
            new_mask = mask_token if mask_token is not None else row.get("mask_token", True)
            await self._update("""
                UPDATE debug_config SET enabled = ?, log_requests = ?, log_responses = ?,
                    mask_token = ?, updated_at = CURRENT_TIMESTAMP WHERE id = 1
            """, (new_enabled, new_log_req, new_log_res, new_mask))
        else:
            await self._update("""
                INSERT INTO debug_config (id, enabled, log_requests, log_responses, mask_token)
                VALUES (1, ?, ?, ?, ?)
            """, (enabled or False, log_requests if log_requests is not None else True,
                  log_responses if log_responses is not None else True,
                  mask_token if mask_token is not None else True))

    async def get_captcha_config(self) -> CaptchaConfig:
        row = await self._query_one("SELECT * FROM captcha_config WHERE id = 1")
        return CaptchaConfig(**row) if row else CaptchaConfig()

    async def update_captcha_config(
        self,
        captcha_method: str = None,
        yescaptcha_api_key: str = None,
        yescaptcha_base_url: str = None,
        capmonster_api_key: str = None,
        capmonster_base_url: str = None,
        ezcaptcha_api_key: str = None,
        ezcaptcha_base_url: str = None,
        capsolver_api_key: str = None,
        capsolver_base_url: str = None,
        remote_browser_base_url: str = None,
        remote_browser_api_key: str = None,
        remote_browser_timeout: int = None,
        browser_proxy_enabled: bool = None,
        browser_proxy_url: str = None,
        browser_count: int = None,
    ):
        row = await self._query_one("SELECT * FROM captcha_config WHERE id = 1")
        cur = row or {}

        def _v(new, key, default):
            return new if new is not None else cur.get(key, default)

        new_method = _v(captcha_method, "captcha_method", "yescaptcha")
        new_yes_key = _v(yescaptcha_api_key, "yescaptcha_api_key", "")
        new_yes_url = _v(yescaptcha_base_url, "yescaptcha_base_url", "https://api.yescaptcha.com")
        new_cap_key = _v(capmonster_api_key, "capmonster_api_key", "")
        new_cap_url = _v(capmonster_base_url, "capmonster_base_url", "https://api.capmonster.cloud")
        new_ez_key = _v(ezcaptcha_api_key, "ezcaptcha_api_key", "")
        new_ez_url = _v(ezcaptcha_base_url, "ezcaptcha_base_url", "https://api.ez-captcha.com")
        new_cs_key = _v(capsolver_api_key, "capsolver_api_key", "")
        new_cs_url = _v(capsolver_base_url, "capsolver_base_url", "https://api.capsolver.com")
        new_rb_url = _v(remote_browser_base_url, "remote_browser_base_url", "")
        new_rb_key = _v(remote_browser_api_key, "remote_browser_api_key", "")
        new_rb_to = _v(remote_browser_timeout, "remote_browser_timeout", 60)
        new_proxy_en = _v(browser_proxy_enabled, "browser_proxy_enabled", False)
        new_proxy_url = _v(browser_proxy_url, "browser_proxy_url", None)
        new_bc = _v(browser_count, "browser_count", 1)
        new_rb_to = max(5, int(new_rb_to)) if new_rb_to is not None else 60

        if row:
            await self._update("""
                UPDATE captcha_config SET captcha_method=?, yescaptcha_api_key=?, yescaptcha_base_url=?,
                    capmonster_api_key=?, capmonster_base_url=?, ezcaptcha_api_key=?, ezcaptcha_base_url=?,
                    capsolver_api_key=?, capsolver_base_url=?,
                    remote_browser_base_url=?, remote_browser_api_key=?, remote_browser_timeout=?,
                    browser_proxy_enabled=?, browser_proxy_url=?, browser_count=?,
                    updated_at=CURRENT_TIMESTAMP WHERE id=1
            """, (new_method, new_yes_key, new_yes_url, new_cap_key, new_cap_url,
                  new_ez_key, new_ez_url, new_cs_key, new_cs_url,
                  (new_rb_url or "").strip(), (new_rb_key or "").strip(), new_rb_to,
                  new_proxy_en, new_proxy_url, new_bc))
        else:
            await self._update("""
                INSERT INTO captcha_config (id, captcha_method, yescaptcha_api_key, yescaptcha_base_url,
                    capmonster_api_key, capmonster_base_url, ezcaptcha_api_key, ezcaptcha_base_url,
                    capsolver_api_key, capsolver_base_url, remote_browser_base_url, remote_browser_api_key,
                    remote_browser_timeout, browser_proxy_enabled, browser_proxy_url, browser_count)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (new_method, new_yes_key, new_yes_url, new_cap_key, new_cap_url,
                  new_ez_key, new_ez_url, new_cs_key, new_cs_url,
                  (new_rb_url or "").strip(), (new_rb_key or "").strip(), new_rb_to,
                  new_proxy_en, new_proxy_url, new_bc))

    async def get_plugin_config(self) -> PluginConfig:
        row = await self._query_one("SELECT * FROM plugin_config WHERE id = 1")
        return PluginConfig(**row) if row else PluginConfig()

    async def update_plugin_config(self, connection_token: str, auto_enable_on_update: bool = True):
        row = await self._query_one("SELECT * FROM plugin_config WHERE id = 1")
        if row:
            await self._update("""
                UPDATE plugin_config SET connection_token = ?, auto_enable_on_update = ?,
                    updated_at = CURRENT_TIMESTAMP WHERE id = 1
            """, (connection_token, auto_enable_on_update))
        else:
            await self._update("""
                INSERT INTO plugin_config (id, connection_token, auto_enable_on_update)
                VALUES (1, ?, ?)
            """, (connection_token, auto_enable_on_update))

    # ==================================================================
    # Request log operations
    # ==================================================================
    async def add_request_log(self, log: RequestLog):
        await self._update("""
            INSERT INTO request_logs (token_id, operation, request_body, response_body, status_code, duration)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (log.token_id, log.operation, log.request_body, log.response_body,
              log.status_code, log.duration))

    async def get_logs(self, limit: int = 100, token_id: Optional[int] = None, include_payload: bool = False):
        payload_cols = "rl.request_body, rl.response_body," if include_payload else ""
        base = f"""
            SELECT rl.id, rl.token_id, rl.operation, {payload_cols}
                   rl.status_code, rl.duration, rl.created_at,
                   t.email as token_email, t.name as token_username
            FROM request_logs rl
            LEFT JOIN tokens t ON rl.token_id = t.id
        """
        if token_id:
            return await self._query_many(
                base + " WHERE rl.token_id = ? ORDER BY rl.created_at DESC LIMIT ?",
                (token_id, limit),
            )
        return await self._query_many(base + " ORDER BY rl.created_at DESC LIMIT ?", (limit,))

    async def get_log_detail(self, log_id: int) -> Optional[Dict[str, Any]]:
        return await self._query_one("""
            SELECT rl.id, rl.token_id, rl.operation, rl.request_body, rl.response_body,
                   rl.status_code, rl.duration, rl.created_at,
                   t.email as token_email, t.name as token_username
            FROM request_logs rl
            LEFT JOIN tokens t ON rl.token_id = t.id
            WHERE rl.id = ?
        """, (log_id,))

    async def clear_all_logs(self):
        await self._update("DELETE FROM request_logs")

    # ==================================================================
    # Generic low-level helpers (backend-transparent)
    # ==================================================================
    async def _query_one(self, sql: str, params=()) -> Optional[Dict[str, Any]]:
        if _USE_POSTGRES:
            async with self._pg_pool.acquire() as db:
                row = await db.fetchrow(sql, params)
                return dict(row) if row else None
        else:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(sql, params)
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def _query_many(self, sql: str, params=()) -> List[Dict[str, Any]]:
        if _USE_POSTGRES:
            async with self._pg_pool.acquire() as db:
                rows = await db.fetch(sql, params)
                return [dict(r) for r in rows]
        else:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(sql, params)
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def _update(self, sql: str, params=()):
        if _USE_POSTGRES:
            async with self._pg_pool.acquire() as db:
                await db.execute(sql, params)
        else:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(sql, params)
                await db.commit()


# ---------------------------------------------------------------------------
# SQLite context-manager wrapper (mirrors _PgAcquireCtx for _connect())
# ---------------------------------------------------------------------------
class _SqliteCtx:
    def __init__(self, path: str):
        self._path = path
        self._db = None

    async def __aenter__(self):
        self._db = await aiosqlite.connect(self._path)
        self._db.row_factory = aiosqlite.Row
        return self._db

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self._db.commit()
        await self._db.close()
