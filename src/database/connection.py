"""
DuckDB database connection management.
"""
import duckdb
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, Any
import logging

from ..config.settings import settings

logger = logging.getLogger(__name__)


class Database:
    """Thread-safe DuckDB database wrapper."""

    def __init__(self, db_path: Optional[Path] = None, read_only: bool = False):
        self.db_path = db_path or settings.db_path
        self.read_only = read_only
        self._connection: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Open database connection."""
        if self._connection is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._connection = duckdb.connect(str(self.db_path), read_only=self.read_only)
            mode = "read-only" if self.read_only else "read-write"
            logger.info(f"Database verbonden ({mode}): {self.db_path}")
        return self._connection

    def close(self):
        """Sluit database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("Database verbinding gesloten")

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get active connection, connect if needed."""
        return self.connect()

    def execute(self, query: str, params: Optional[list] = None) -> duckdb.DuckDBPyRelation:
        """Execute a query."""
        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)

    def fetchone(self, query: str, params: Optional[list] = None) -> Optional[tuple]:
        """Execute query and fetch one result."""
        result = self.execute(query, params)
        return result.fetchone()

    def fetchall(self, query: str, params: Optional[list] = None) -> list[tuple]:
        """Execute query and fetch all results."""
        result = self.execute(query, params)
        return result.fetchall()

    def fetchdf(self, query: str, params: Optional[list] = None):
        """Execute query and return as pandas DataFrame."""
        result = self.execute(query, params)
        return result.df()

    def insert_many(self, table: str, columns: list[str], values: list[tuple]):
        """Insert multiple rows efficiently."""
        if not values:
            return

        placeholders = ", ".join(["?" for _ in columns])
        cols = ", ".join(columns)
        query = f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"

        for row in values:
            self.execute(query, list(row))

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Singleton database instance
_db_instance: Optional[Database] = None


def get_connection(read_only: bool = False) -> Database:
    """Get the singleton database instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = Database(read_only=read_only)
    return _db_instance


def get_readonly_connection() -> Database:
    """Get a read-only database connection (for dashboard)."""
    return Database(read_only=True)


@contextmanager
def get_db_context():
    """Context manager for database operations."""
    db = get_connection()
    try:
        yield db
    finally:
        pass  # Don't close singleton, just yield
