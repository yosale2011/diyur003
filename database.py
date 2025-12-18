"""
Database connection and utilities for DiyurCalc application.
Provides PostgreSQL connection wrapper and database utilities.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import psycopg2
import psycopg2.extras

from config import config
from logic import get_db_connection

logger = logging.getLogger(__name__)


class PostgresConnection:
    """Wrapper for PostgreSQL connection to provide SQLite-like interface."""

    def __init__(self, conn):
        self.conn = conn
        self._in_transaction = False

    def execute(self, query: str, params: tuple = ()) -> Any:
        """Execute a query and return a cursor-like object."""
        # Convert SQLite placeholders (?) to PostgreSQL (%s)
        query = query.replace("?", "%s")
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, params)
        return cursor

    def cursor(self, *args, **kwargs):
        """Allow raw access to cursors if needed (e.g. by logic.py functions)."""
        return self.conn.cursor(*args, **kwargs)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()


def get_conn() -> PostgresConnection:
    """Create and return a PostgreSQL database connection wrapped with SQLite-like interface."""
    pg_conn = get_db_connection()
    return PostgresConnection(pg_conn)





