import sqlite3
import os
import logging

from unmanic.libs import common

# TODO: function to clean up orphans
# TODO: shouldn't have to create a new connection for every operation

logger = logging.getLogger("Unmanic.Plugin.kmarius_incremental_scan_db")

DB_PATH = os.path.join(common.get_home_dir(), ".unmanic",
                       "userdata", "kmarius_incremental_scan_db", "timestamps.db")

if not os.path.exists(os.path.dirname(DB_PATH)):
    os.makedirs(os.path.dirname(DB_PATH))


def check_column_exists(conn: sqlite3.Connection, table_name: str, column_name: str):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()

    return any(column[1] == column_name for column in columns)


# check the database table, create it if it doesn't exist.
# migration for the addition of a column consists of dropping the table
def check_database():
    conn = sqlite3.connect(DB_PATH)
    with conn:
        cursor = conn.cursor()
        if not check_column_exists(conn, "timestamps", "library_id"):
            logger.info(
                "Table 'timestamps' does not exists or is missing the 'library_id' column. (Re-)creating...")
            cursor.execute("DROP TABLE IF EXISTS timestamps")
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS timestamps(
                           library_id INTEGER NULL,
                           path TEXT NOT NULL,
                           mtime INTEGER NOT NULL,
                           PRIMARY KEY (library_id,path)
                        )''')
    conn.close()


check_database()


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    return conn


def store_timestamp(library_id: int, path: str, mtime: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
                INSERT INTO timestamps (library_id, path, mtime)
                VALUES (?, ?, ?) ON CONFLICT(library_id, path) DO
                UPDATE SET
                    mtime = excluded.mtime
                ''', (library_id, path, mtime))
    conn.commit()
    conn.close()


def store_timestamps(values: list[(int, str, int)]):
    conn = get_connection()
    cur = conn.cursor()
    cur.executemany('''
                    INSERT INTO timestamps (library_id, path, mtime)
                    VALUES (?, ?, ?) ON CONFLICT(library_id, path) DO
                    UPDATE SET
                        mtime = excluded.mtime
                    ''', values)
    conn.commit()
    conn.close()


def load_timestamp(library_id: int, path: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT mtime FROM timestamps WHERE library_id = ? AND path = ?", (library_id, path))
    row = cur.fetchone()
    mtime = row[0] if row else None
    conn.close()
    return mtime


# we only allow batch loading with fixed library_id
def load_timestamps(library_id: int, paths: list[str]):
    conn = get_connection()
    with conn:
        cur = conn.cursor()
        mtimes = []
        # there's better approaches for this, e.g. a long in (...) expression with all values, or a common-table-expression
        for path in paths:
            cur.execute(
                "SELECT mtime FROM timestamps WHERE library_id = ? AND path = ?", (library_id, path))
            row = cur.fetchone()
            mtimes.append(row[0] if row else None)
    conn.close()
    return mtimes
