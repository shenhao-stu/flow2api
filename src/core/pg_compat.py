"""
pg_compat.py — aiosqlite-compatible shim over asyncpg.

When the DATABASE_URL environment variable points to a PostgreSQL DSN
(postgresql:// or postgres://) this module replaces aiosqlite transparently:

    from .pg_compat import compat as aiosqlite   # in database.py

The public surface used by database.py is:

    aiosqlite.connect(path_or_url)  -> async context manager
    connection.row_factory = aiosqlite.Row       (no-op for PG — rows are dicts)
    cursor = await connection.execute(sql, params)
    cursor.lastrowid
    await cursor.fetchone()   -> dict | None
    await cursor.fetchall()   -> list[dict]
    await connection.commit() -> no-op (auto-commit via per-statement transactions)
    aiosqlite.Row             -> sentinel object

SQL dialect differences handled automatically:
    ?  placeholders -> $1 $2 … for asyncpg
    INTEGER PRIMARY KEY AUTOINCREMENT  -> SERIAL PRIMARY KEY  (for PostgreSQL)
    BOOLEAN DEFAULT 0 / 1             -> BOOLEAN DEFAULT FALSE / TRUE
    sqlite_master                     -> information_schema.tables
    PRAGMA table_info(t)              -> information_schema.columns
    json_object(…)                    -> jsonb_build_object(…)
    CURRENT_TIMESTAMP                 -> CURRENT_TIMESTAMP  (same)
"""

import os
import re
import asyncio
from typing import Any, List, Optional, Tuple


# ── detect backend ────────────────────────────────────────────────────────────
_DATABASE_URL: str = os.environ.get("DATABASE_URL", "")
USE_POSTGRES: bool = bool(
    _DATABASE_URL and (
        _DATABASE_URL.startswith("postgresql://")
        or _DATABASE_URL.startswith("postgres://")
    )
)

# ── global asyncpg pool (lazy-initialized) ────────────────────────────────────
_pool = None
_pool_lock = asyncio.Lock()


async def _get_pool():
    global _pool
    if _pool is not None:
        return _pool
    async with _pool_lock:
        if _pool is not None:
            return _pool
        import asyncpg
        dsn = _DATABASE_URL
        if dsn.startswith("postgres://"):
            dsn = "postgresql://" + dsn[len("postgres://"):]
        # strip unsupported parameters (channel_binding is not a libpq kwarg)
        dsn = re.sub(r"[?&]channel_binding=[^&]*", "", dsn)
        dsn = re.sub(r"[?&]$", "", dsn)
        _pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
        return _pool


async def close_pool():
    """Call on application shutdown."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


# ── SQL dialect translation ───────────────────────────────────────────────────

def _translate_sql(sql: str) -> str:
    """Translate SQLite SQL to PostgreSQL SQL."""
    # ? → $1 $2 …
    counter = 0
    def _repl_placeholder(_m):
        nonlocal counter
        counter += 1
        return f"${counter}"
    sql = re.sub(r"\?", _repl_placeholder, sql)

    # DDL: AUTOINCREMENT → SERIAL
    sql = re.sub(r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT", "SERIAL PRIMARY KEY", sql, flags=re.IGNORECASE)

    # BOOLEAN defaults: only convert 0→FALSE / 1→TRUE when the column is
    # declared BOOLEAN.  A bare "INTEGER DEFAULT 0" must stay as-is.
    # Pattern: BOOLEAN [NOT NULL] DEFAULT 0|1
    sql = re.sub(
        r"(BOOLEAN(?:\s+NOT\s+NULL)?\s+DEFAULT\s+)0\b",
        r"\1FALSE",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(BOOLEAN(?:\s+NOT\s+NULL)?\s+DEFAULT\s+)1\b",
        r"\1TRUE",
        sql,
        flags=re.IGNORECASE,
    )
    # Also handle ADD COLUMN … BOOLEAN DEFAULT 0/1 inside ALTER TABLE
    sql = re.sub(
        r"(ADD\s+COLUMN(?:\s+IF\s+NOT\s+EXISTS)?\s+\w+\s+BOOLEAN\s+DEFAULT\s+)0\b",
        r"\1FALSE",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(ADD\s+COLUMN(?:\s+IF\s+NOT\s+EXISTS)?\s+\w+\s+BOOLEAN\s+DEFAULT\s+)1\b",
        r"\1TRUE",
        sql,
        flags=re.IGNORECASE,
    )

    # sqlite_master → information_schema.tables (but we handle introspection
    # via dedicated methods, so this is a safety net)
    sql = sql.replace("sqlite_master", "pg_catalog.pg_tables")

    # json_object → jsonb_build_object
    sql = re.sub(r"\bjson_object\b", "jsonb_build_object", sql, flags=re.IGNORECASE)

    # ALTER TABLE … ADD COLUMN … → ADD COLUMN IF NOT EXISTS …
    sql = re.sub(
        r"(ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN\s+)(?!IF\s+NOT\s+EXISTS\s+)",
        r"\1IF NOT EXISTS ",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


# ── cursor shim ───────────────────────────────────────────────────────────────

class _PgCursor:
    """Mimics the subset of aiosqlite Cursor used by database.py."""

    def __init__(self, rows, lastrowid=None):
        self._rows = rows  # list[asyncpg.Record] or None
        self.lastrowid: Optional[int] = lastrowid

    async def fetchone(self):
        if not self._rows:
            return None
        row = self._rows[0]
        return dict(row)

    async def fetchall(self):
        if not self._rows:
            return []
        return [dict(r) for r in self._rows]


# ── Row sentinel (database.py sets conn.row_factory = aiosqlite.Row) ──────────

class Row:
    """Sentinel — not actually used in the PG path."""


# ── connection shim ───────────────────────────────────────────────────────────

class _PgConnection:
    """Mimics aiosqlite Connection for the patterns used in database.py."""

    def __init__(self, conn):
        self._conn = conn          # raw asyncpg connection
        self._tr = None
        self.row_factory = None    # ignored — asyncpg always returns Records

    async def execute(self, sql: str, params=()) -> _PgCursor:
        sql_pg = _translate_sql(sql)
        params = tuple(params) if params else ()

        sql_upper = sql_pg.lstrip().upper()

        if sql_upper.startswith("INSERT"):
            # Detect RETURNING id; if not present, add it for AUTOINCREMENT tables
            has_returning = "RETURNING" in sql_upper
            if not has_returning:
                # Try to get lastrowid via RETURNING id
                sql_with_ret = sql_pg.rstrip().rstrip(";") + " RETURNING id"
                try:
                    if params:
                        row = await self._conn.fetchrow(sql_with_ret, *params)
                    else:
                        row = await self._conn.fetchrow(sql_with_ret)
                    lastrowid = row["id"] if row else None
                    return _PgCursor([row] if row else [], lastrowid=lastrowid)
                except Exception:
                    pass
            # Plain execute
            if params:
                await self._conn.execute(sql_pg, *params)
            else:
                await self._conn.execute(sql_pg)
            return _PgCursor([], lastrowid=None)

        elif sql_upper.startswith(("SELECT", "WITH")):
            if params:
                rows = await self._conn.fetch(sql_pg, *params)
            else:
                rows = await self._conn.fetch(sql_pg)
            return _PgCursor(rows)

        else:
            # UPDATE, DELETE, CREATE, ALTER, DROP, …
            if params:
                await self._conn.execute(sql_pg, *params)
            else:
                await self._conn.execute(sql_pg)
            return _PgCursor([])

    async def commit(self):
        pass  # autocommit via transaction

    async def __aenter__(self):
        self._tr = self._conn.transaction()
        await self._tr.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self._tr.commit()
        else:
            await self._tr.rollback()


# ── connect() context manager ─────────────────────────────────────────────────

class _PgConnectCtx:
    """Returned by connect(); used as `async with _PgConnectCtx() as conn:`."""

    def __init__(self):
        self._raw = None
        self._conn = None

    async def __aenter__(self) -> _PgConnection:
        pool = await _get_pool()
        self._raw = await pool.acquire()
        self._conn = _PgConnection(self._raw)
        await self._conn.__aenter__()
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        try:
            await self._conn.__aexit__(exc_type, exc, tb)
        finally:
            pool = await _get_pool()
            await pool.release(self._raw)


# ── introspection helpers (replace PRAGMA / sqlite_master) ────────────────────

async def pg_table_exists(conn: _PgConnection, table_name: str) -> bool:
    rows = await conn._conn.fetch(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = $1",
        table_name,
    )
    return bool(rows)


async def pg_column_exists(conn: _PgConnection, table_name: str, column_name: str) -> bool:
    rows = await conn._conn.fetch(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2",
        table_name,
        column_name,
    )
    return bool(rows)


# ── public compat object (drop-in for `import aiosqlite`) ─────────────────────

class _Compat:
    """
    Usage in database.py:

        from .pg_compat import compat as aiosqlite

    Then all existing `aiosqlite.connect(...)` calls work unchanged.
    """
    Row = Row

    def connect(self, path_ignored: str):
        if USE_POSTGRES:
            return _PgConnectCtx()
        import aiosqlite as _real
        return _real.connect(path_ignored)


compat = _Compat()
