from pathlib import Path
import sqlite3

DB_PATH = Path("cache/data.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
