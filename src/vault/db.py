"""Raw Linguistic Vault Database Manager.

Provides high-performance SQLite operations for sources, items, and payloads
with WAL concurrency, SHA-256 deduplication, foreign key enforcement, and FTS5 search.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def _utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def _sanitize_metadata(metadata: Optional[Union[Dict[str, Any], str]]) -> str:
    """Ensure metadata is valid JSON string."""
    if metadata is None:
        return "{}"
    if isinstance(metadata, str):
        try:
            json.loads(metadata)
            return metadata
        except json.JSONDecodeError:
            return json.dumps({"raw": metadata})
    return json.dumps(metadata)


class VaultManager:
    """Thread-safe manager for the Raw Linguistic Vault SQLite database."""

    def __init__(self, db_path: Union[str, Path] = "data/vault/raw_vault.db"):
        self.db_path_str = str(db_path)
        self.is_memory = self.db_path_str == ":memory:"
        if not self.is_memory:
            self.db_path = Path(self.db_path_str)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            self.db_path = None

        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self._tx_depth: int = 0

    def _get_connection(self) -> sqlite3.Connection:
        """Get or initialize the database connection with WAL mode and foreign keys."""
        if self._conn is None:
            conn = sqlite3.connect(
                self.db_path_str,
                check_same_thread=False,
                timeout=5.0,
            )
            conn.row_factory = sqlite3.Row
            # Enable foreign keys and busy timeout
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            if not self.is_memory:
                # WAL mode applies to disk-backed databases
                conn.execute("PRAGMA journal_mode = WAL;")
            self._conn = conn
        return self._conn

    @property
    def conn(self) -> sqlite3.Connection:
        """Active SQLite connection."""
        return self._get_connection()

    def _maybe_commit(self) -> None:
        """Commit only if not inside an explicit transaction block."""
        if self._tx_depth == 0 and self._conn is not None:
            self._conn.commit()

    def __enter__(self) -> "VaultManager":
        self._get_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            if exc_type is not None:
                self._conn.rollback()
            else:
                self._conn.commit()
            self.close()

    def close(self) -> None:
        """Close connection to database."""
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                finally:
                    self._conn = None

    @contextmanager
    def transaction(self):
        """Context manager for atomic database transactions with rollback on error."""
        with self._lock:
            conn = self.conn
            self._tx_depth += 1
            try:
                yield conn
                if self._tx_depth == 1:
                    conn.commit()
            except Exception:
                if self._tx_depth == 1:
                    conn.rollback()
                raise
            finally:
                self._tx_depth -= 1

    def init_db(self, schema_file: Optional[Union[str, Path]] = None) -> None:
        """Initialize the database schema idempotently."""
        if schema_file is None:
            schema_file = Path(__file__).parent / "schema.sql"
        schema_path = Path(schema_file)
        if not schema_path.is_file():
            raise FileNotFoundError(f"Schema file not found at {schema_path}")

        schema_sql = schema_path.read_text(encoding="utf-8")
        with self._lock:
            conn = self.conn
            conn.executescript(schema_sql)
            self._maybe_commit()

    def upsert_source(
        self,
        id: str,
        platform: str,
        language_iso1: str,
        name: str,
        url: str,
        metadata: Optional[Union[Dict[str, Any], str]] = None,
        created_at: Optional[str] = None,
    ) -> str:
        """Insert or update a source record."""
        meta_json = _sanitize_metadata(metadata)
        created_time = created_at or _utc_now_iso()

        sql = """
        INSERT INTO sources (id, platform, language_iso1, name, url, metadata_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            platform = excluded.platform,
            language_iso1 = excluded.language_iso1,
            name = excluded.name,
            url = excluded.url,
            metadata_json = excluded.metadata_json;
        """
        with self._lock:
            self.conn.execute(
                sql, (id, platform, language_iso1, name, url, meta_json, created_time)
            )
            self._maybe_commit()
        return id

    def upsert_item(
        self,
        id: str,
        source_id: str,
        title: str,
        url: str,
        published_at: Optional[str] = None,
        duration_seconds: Optional[int] = None,
        view_count: Optional[int] = None,
        metadata: Optional[Union[Dict[str, Any], str]] = None,
        created_at: Optional[str] = None,
    ) -> str:
        """Insert or update an item record (foreign key to sources)."""
        meta_json = _sanitize_metadata(metadata)
        created_time = created_at or _utc_now_iso()

        sql = """
        INSERT INTO items (id, source_id, title, url, published_at, duration_seconds, view_count, metadata_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            source_id = excluded.source_id,
            title = excluded.title,
            url = excluded.url,
            published_at = excluded.published_at,
            duration_seconds = excluded.duration_seconds,
            view_count = excluded.view_count,
            metadata_json = excluded.metadata_json;
        """
        with self._lock:
            self.conn.execute(
                sql,
                (
                    id,
                    source_id,
                    title,
                    url,
                    published_at,
                    duration_seconds,
                    view_count,
                    meta_json,
                    created_time,
                ),
            )
            self._maybe_commit()
        return id

    def insert_payload(
        self,
        item_id: str,
        payload_type: str,
        language_iso1: str,
        origin_type: str,
        format: str,
        raw_content: str,
        fetched_at: Optional[str] = None,
    ) -> str:
        """Atomically insert a payload with SHA-256 deduplication.
        
        Returns the SHA-256 digest payload id. Deduplicates identical content via INSERT OR IGNORE.
        """
        if not isinstance(raw_content, str):
            raise TypeError("raw_content must be a string")

        payload_id = hashlib.sha256(raw_content.encode("utf-8")).hexdigest()
        fetched_time = fetched_at or _utc_now_iso()

        sql = """
        INSERT OR IGNORE INTO payloads (
            id, item_id, payload_type, language_iso1, origin_type, format, raw_content, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """
        with self._lock:
            self.conn.execute(
                sql,
                (
                    payload_id,
                    item_id,
                    payload_type,
                    language_iso1,
                    origin_type,
                    format,
                    raw_content,
                    fetched_time,
                ),
            )
            self._maybe_commit()
        return payload_id

    def get_source(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve source by ID."""
        with self._lock:
            cur = self.conn.execute("SELECT * FROM sources WHERE id = ?;", (source_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_item(self, item_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve item by ID."""
        with self._lock:
            cur = self.conn.execute("SELECT * FROM items WHERE id = ?;", (item_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_payload(self, payload_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve payload by ID."""
        with self._lock:
            cur = self.conn.execute("SELECT * FROM payloads WHERE id = ?;", (payload_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_items_by_source(
        self,
        source_id: str,
        limit: Optional[int] = None,
        order_by_views: bool = False,
    ) -> List[Dict[str, Any]]:
        """Retrieve items belonging to a source."""
        query = "SELECT * FROM items WHERE source_id = ?"
        if order_by_views:
            query += " ORDER BY COALESCE(view_count, 0) DESC"
        else:
            query += " ORDER BY created_at DESC"
        if limit is not None:
            query += f" LIMIT {int(limit)}"

        with self._lock:
            cur = self.conn.execute(query, (source_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_payloads_by_item(self, item_id: str) -> List[Dict[str, Any]]:
        """Retrieve all payloads for a specific item."""
        with self._lock:
            cur = self.conn.execute(
                "SELECT * FROM payloads WHERE item_id = ? ORDER BY fetched_at ASC;",
                (item_id,),
            )
            return [dict(row) for row in cur.fetchall()]

    def search_text(
        self,
        query: str,
        language_iso1: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Search raw transcripts using FTS5 full-text search.
        
        Returns matching records with snippets, item metadata (title, URL), and rank.
        """
        cleaned_query = query.strip()
        if not cleaned_query:
            return []

        # If the query does not contain FTS operators, enclose multiple words in quotes or sanitize
        fts_query = cleaned_query
        has_operators = (
            '"' in cleaned_query
            or " NEAR/" in cleaned_query
            or " NEAR " in cleaned_query
            or " OR " in cleaned_query
            or " AND " in cleaned_query
            or " NOT " in cleaned_query
            or "*" in cleaned_query
        )
        if not has_operators:
            escaped = cleaned_query.replace('"', '""')
            fts_query = f'"{escaped}"'

        sql = """
        SELECT
            p.id AS payload_id,
            p.item_id,
            i.title AS item_title,
            i.url AS item_url,
            p.payload_type,
            p.language_iso1,
            p.origin_type,
            p.format,
            p.raw_content,
            p.fetched_at,
            snippet(payloads_fts, 2, '<b>', '</b>', '...', 15) AS snippet,
            bm25(payloads_fts) AS rank
        FROM payloads_fts
        JOIN payloads p ON payloads_fts.rowid = p.rowid
        LEFT JOIN items i ON p.item_id = i.id
        WHERE payloads_fts MATCH ?
        """
        params: List[Any] = [fts_query]

        if language_iso1:
            sql += " AND p.language_iso1 = ?"
            params.append(language_iso1)

        sql += " ORDER BY rank ASC LIMIT ?;"
        params.append(limit)

        with self._lock:
            try:
                cur = self.conn.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]
            except sqlite3.OperationalError:
                # Fallback to tokenized words if exact phrase syntax failed
                raw_words = [w for w in cleaned_query.replace('"', '').split() if w]
                fallback_query = " ".join(f'"{w}"' for w in raw_words)
                params[0] = fallback_query
                cur = self.conn.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    def get_vault_stats(self) -> Dict[str, Any]:
        """Return comprehensive metrics on sources, items, payloads, and breakdown by language/origin."""
        with self._lock:
            total_sources = self.conn.execute("SELECT COUNT(*) FROM sources;").fetchone()[0]
            total_items = self.conn.execute("SELECT COUNT(*) FROM items;").fetchone()[0]
            total_payloads = self.conn.execute("SELECT COUNT(*) FROM payloads;").fetchone()[0]

            # Sources by platform
            sources_by_platform = dict(
                self.conn.execute(
                    "SELECT platform, COUNT(*) FROM sources GROUP BY platform ORDER BY COUNT(*) DESC;"
                ).fetchall()
            )

            # Sources by language
            sources_by_language = dict(
                self.conn.execute(
                    "SELECT language_iso1, COUNT(*) FROM sources GROUP BY language_iso1 ORDER BY COUNT(*) DESC;"
                ).fetchall()
            )

            # Payloads by type
            payloads_by_type = dict(
                self.conn.execute(
                    "SELECT payload_type, COUNT(*) FROM payloads GROUP BY payload_type ORDER BY COUNT(*) DESC;"
                ).fetchall()
            )

            # Payloads by origin
            payloads_by_origin = dict(
                self.conn.execute(
                    "SELECT origin_type, COUNT(*) FROM payloads GROUP BY origin_type ORDER BY COUNT(*) DESC;"
                ).fetchall()
            )

            # Payloads by language
            payloads_by_language = dict(
                self.conn.execute(
                    "SELECT language_iso1, COUNT(*) FROM payloads GROUP BY language_iso1 ORDER BY COUNT(*) DESC;"
                ).fetchall()
            )

            # Duration and views
            totals_row = self.conn.execute(
                "SELECT SUM(duration_seconds), SUM(view_count) FROM items;"
            ).fetchone()
            total_duration_sec = totals_row[0] or 0
            total_views = totals_row[1] or 0

            return {
                "total_sources": total_sources,
                "total_items": total_items,
                "total_payloads": total_payloads,
                "sources_by_platform": sources_by_platform,
                "sources_by_language": sources_by_language,
                "payloads_by_type": payloads_by_type,
                "payloads_by_origin": payloads_by_origin,
                "payloads_by_language": payloads_by_language,
                "total_duration_seconds": total_duration_sec,
                "total_duration_hours": round(total_duration_sec / 3600.0, 2),
                "total_views": total_views,
            }
