"""
Bronze layer: idempotent ingestion of raw extracts into staging tables.

Idempotency: every load uses INSERT OR REPLACE keyed on the natural key
(transaction_id / posting_id / account_id+balance_date), so re-running the
pipeline against the same extract never creates duplicates - the same
guarantee required for safely re-playing a batch or event-driven feed.
"""
import csv
from datetime import datetime, timezone
from pathlib import Path

from db import get_connection, init_schema

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"


def _now():
    return datetime.now(timezone.utc).isoformat()


def load_transactions(conn):
    with open(RAW / "transactions.csv") as f:
        rows = list(csv.DictReader(f))
    conn.executemany(
        """INSERT OR REPLACE INTO stg_transactions
           (transaction_id, account_id, txn_date, amount, direction, source_system, load_ts)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [(r["transaction_id"], r["account_id"], r["txn_date"], float(r["amount"]),
          r["direction"], r["source_system"], _now()) for r in rows],
    )
    conn.commit()
    return len(rows)


def load_postings(conn):
    with open(RAW / "postings.csv") as f:
        rows = list(csv.DictReader(f))
    conn.executemany(
        """INSERT OR REPLACE INTO stg_postings
           (posting_id, transaction_id, account_id, posting_date, posted_amount, direction, load_ts)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [(r["posting_id"], r["transaction_id"], r["account_id"], r["posting_date"],
          float(r["posted_amount"]), r["direction"], _now()) for r in rows],
    )
    conn.commit()
    return len(rows)


def load_gl_balances(conn):
    with open(RAW / "gl_balances.csv") as f:
        rows = list(csv.DictReader(f))
    conn.executemany(
        """INSERT OR REPLACE INTO stg_gl_balances
           (account_id, balance_date, gl_balance, load_ts)
           VALUES (?, ?, ?, ?)""",
        [(r["account_id"], r["balance_date"], float(r["gl_balance"]), _now()) for r in rows],
    )
    conn.commit()
    return len(rows)


def run():
    conn = get_connection()
    init_schema(conn)
    counts = {
        "stg_transactions": load_transactions(conn),
        "stg_postings": load_postings(conn),
        "stg_gl_balances": load_gl_balances(conn),
    }
    conn.close()
    return counts


if __name__ == "__main__":
    print(run())
