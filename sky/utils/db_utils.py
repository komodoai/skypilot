"""Utils for sky databases."""
import contextlib
import sqlite3
import threading
from typing import Any, Callable, Optional
# import os
# from sqlalchemy import create_engine, MetaData, Table, text


# def get_komodo_db_resources():
#     if os.environ.get('DATABASE_URL') is None:
#         print("No DATABASE_URL found in the environment, running against local sqlite db")
#         return None, None, None, None, None
#     print(f"Running against a Komodo db")
#     # Create SQLAlchemy engine
#     engine = create_engine(os.environ['DATABASE_URL'], pool_pre_ping=True)

#     # Create MetaData instance
#     metadata = MetaData()

#     # Reflect tables from the database
#     clusters = Table('clusters', metadata, autoload_with=engine)
#     cluster_history = Table('cluster_history', metadata, autoload_with=engine)
#     config = Table('config', metadata, autoload_with=engine)
#     storage = Table('storage', metadata, autoload_with=engine)

#     return engine, clusters, cluster_history, config, storage

# engine, clusters, cluster_history, config, storage = get_komodo_db_resources()



@contextlib.contextmanager
def safe_cursor(db_path: str):
    """A newly created, auto-committing, auto-closing cursor."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        conn.commit()
        conn.close()


def add_column_to_table(
    cursor: 'sqlite3.Cursor',
    conn: 'sqlite3.Connection',
    table_name: str,
    column_name: str,
    column_type: str,
    copy_from: Optional[str] = None,
    value_to_replace_existing_entries: Optional[Any] = None,
):
    """Add a column to a table."""
    for row in cursor.execute(f'PRAGMA table_info({table_name})'):
        if row[1] == column_name:
            break
    else:
        try:
            add_column_cmd = (f'ALTER TABLE {table_name} '
                              f'ADD COLUMN {column_name} {column_type}')
            cursor.execute(add_column_cmd)
            if copy_from is not None:
                cursor.execute(f'UPDATE {table_name} '
                               f'SET {column_name} = {copy_from}')
            if value_to_replace_existing_entries is not None:
                cursor.execute(
                    f'UPDATE {table_name} '
                    f'SET {column_name} = (?) '
                    f'WHERE {column_name} IS NULL',
                    (value_to_replace_existing_entries,))
        except sqlite3.OperationalError as e:
            if 'duplicate column name' in str(e):
                # We may be trying to add the same column twice, when
                # running multiple threads. This is fine.
                pass
            else:
                raise
    conn.commit()


def rename_column(
    cursor: 'sqlite3.Cursor',
    conn: 'sqlite3.Connection',
    table_name: str,
    old_name: str,
    new_name: str,
):
    """Rename a column in a table."""
    # NOTE: This only works for sqlite3 >= 3.25.0. Be careful to use this.

    for row in cursor.execute(f'PRAGMA table_info({table_name})'):
        if row[1] == old_name:
            cursor.execute(f'ALTER TABLE {table_name} '
                           f'RENAME COLUMN {old_name} to {new_name}')
            break
    conn.commit()


class SQLiteConn(threading.local):
    """Thread-local connection to the sqlite3 database."""

    def __init__(self, db_path: str, create_table: Callable):
        super().__init__()
        self.db_path = db_path
        # NOTE: We use a timeout of 10 seconds to avoid database locked
        # errors. This is a hack, but it works.
        self.conn = sqlite3.connect(db_path, timeout=10)
        self.cursor = self.conn.cursor()
        create_table(self.cursor, self.conn)
