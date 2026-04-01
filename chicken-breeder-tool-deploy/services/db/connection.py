from pathlib import Path
import os
import sqlite3

railway_volume = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "").strip()

if railway_volume:
    CACHE_DIR = Path(railway_volume) / "cache"
else:
    CACHE_DIR = Path("cache")

CACHE_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = CACHE_DIR / "data.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
