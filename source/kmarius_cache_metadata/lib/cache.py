import sqlite3
import os
import json
import threading
import time
from typing import Optional

from unmanic.libs import common

from . import PLUGIN_ID

# TODO: a way to clean up orphans

DB_PATH = os.path.join(common.get_home_dir(), ".unmanic", "userdata", PLUGIN_ID, "metadata.db")

local = threading.local()


# NOTE: only reuse in short-lived threads like FileTester
def _get_connection(reuse_connection=False) -> sqlite3.Connection:
    if reuse_connection:
        if not hasattr(local, "connection"):
            local.connection = sqlite3.connect(DB_PATH)

        return local.connection
    else:
        return sqlite3.connect(DB_PATH)


def init(tables: list[str]):
    if not os.path.exists(os.path.dirname(DB_PATH)):
        os.makedirs(os.path.dirname(DB_PATH))

    conn = _get_connection()
    with conn:
        cur = conn.cursor()
        for table in tables:
            cur.execute(f'''
                           CREATE TABLE IF NOT EXISTS {table} (
                               path TEXT PRIMARY KEY,
                               mtime INTEGER NOT NULL,
                               last_update INTEGER NOT NULL,
                               data TEXT DEFAULT NULL
                           )''')
        conn.commit()
    conn.close()


def get(table: str, path: str, mtime: int = None, reuse_connection=False) -> Optional[dict]:
    conn = _get_connection(reuse_connection)
    cur = conn.cursor()
    if mtime:
        cur.execute(f"SELECT data FROM {table} WHERE path = ? AND mtime = ? LIMIT 1",
                    (path, mtime))
    else:
        cur.execute(f"SELECT data FROM {table} WHERE path = ? LIMIT 1",
                    (path,))
    row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return json.loads(row[0])


def put(table: str, path: str, mtime: int, data: dict, reuse_connection=False):
    conn = _get_connection(reuse_connection)
    cur = conn.cursor()
    last_update = int(time.time())
    cur.execute(f'''
                INSERT INTO {table} (path, mtime, last_update, data)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (path) DO
                UPDATE SET
                    (mtime, last_update, data) = (EXCLUDED.mtime, EXCLUDED.last_update, EXCLUDED.data)
                ''', (path, mtime, last_update, json.dumps(data)))
    conn.commit()