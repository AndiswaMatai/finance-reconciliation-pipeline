"""Small helper around sqlite3 so every module shares one DB path/connection style."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "warehouse.db"


def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_schema(conn):
    schema_path = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
    with open(schema_path) as f:
        conn.executescript(f.read())
    conn.commit()
