"""
Embeds completeness, accuracy, and timeliness metrics against agreed
thresholds, and writes the result of every check to data_quality_results so
it can be pulled as audit/control evidence.
"""
from datetime import datetime, timezone

from db import get_connection

THRESHOLDS = {
    "transactions_completeness": 0.98,   # >=98% of transactions have a non-null account_id/amount
    "postings_coverage": 0.90,           # >=90% of transactions have a matching posting
    "amount_accuracy": 0.95,             # >=95% of postings match transaction amount exactly
}


def _record(conn, check_name, table_name, status, value, threshold):
    conn.execute(
        """INSERT INTO data_quality_results (check_name, table_name, status, metric_value, threshold, run_ts)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (check_name, table_name, status, value, threshold, datetime.now(timezone.utc).isoformat()),
    )


def check_completeness(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stg_transactions")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stg_transactions WHERE account_id IS NULL OR amount IS NULL")
    missing = cur.fetchone()[0]
    score = (total - missing) / total if total else 0
    status = "PASS" if score >= THRESHOLDS["transactions_completeness"] else "FAIL"
    _record(conn, "transactions_completeness", "stg_transactions", status, round(score, 4),
             THRESHOLDS["transactions_completeness"])
    return status, score


def check_postings_coverage(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stg_transactions")
    total = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM stg_transactions t "
        "WHERE EXISTS (SELECT 1 FROM stg_postings p WHERE p.transaction_id = t.transaction_id)"
    )
    matched = cur.fetchone()[0]
    score = matched / total if total else 0
    status = "PASS" if score >= THRESHOLDS["postings_coverage"] else "FAIL"
    _record(conn, "postings_coverage", "stg_postings", status, round(score, 4),
             THRESHOLDS["postings_coverage"])
    return status, score


def check_amount_accuracy(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM stg_postings p JOIN stg_transactions t "
        "ON p.transaction_id = t.transaction_id"
    )
    total = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM stg_postings p JOIN stg_transactions t "
        "ON p.transaction_id = t.transaction_id WHERE ABS(p.posted_amount - t.amount) < 0.01"
    )
    matched = cur.fetchone()[0]
    score = matched / total if total else 0
    status = "PASS" if score >= THRESHOLDS["amount_accuracy"] else "FAIL"
    _record(conn, "amount_accuracy", "stg_postings", status, round(score, 4),
             THRESHOLDS["amount_accuracy"])
    return status, score


def run():
    conn = get_connection()
    results = {
        "transactions_completeness": check_completeness(conn),
        "postings_coverage": check_postings_coverage(conn),
        "amount_accuracy": check_amount_accuracy(conn),
    }
    conn.commit()
    conn.close()
    return results


if __name__ == "__main__":
    for name, (status, score) in run().items():
        print(f"{name}: {status} ({score:.2%})")
