"""
Reconciliation engine: source (transactions) -> sub-ledger (postings) -> GL.

Flags three break types into reconciliation_breaks for exception reporting
and root-cause analysis, mirroring the production control:
  - MISSING_POSTING: a transaction with no corresponding posting
  - AMOUNT_BREAK:    posted amount differs from the source transaction
  - TIMING_BREAK:    posting landed on a different date than the transaction
"""
from datetime import datetime, timezone

from db import get_connection


def _record_break(conn, transaction_id, account_id, break_type, detail):
    conn.execute(
        """INSERT INTO reconciliation_breaks
           (transaction_id, account_id, break_type, detail, detected_ts)
           VALUES (?, ?, ?, ?, ?)""",
        (transaction_id, account_id, break_type, detail, datetime.now(timezone.utc).isoformat()),
    )


def reconcile_source_to_subledger(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT transaction_id, account_id, txn_date, amount FROM stg_transactions"
    )
    transactions = {row[0]: row[1:] for row in cur.fetchall()}

    cur.execute(
        "SELECT transaction_id, posting_date, posted_amount FROM stg_postings"
    )
    postings = {row[0]: row[1:] for row in cur.fetchall()}

    breaks = 0
    for txn_id, (account_id, txn_date, amount) in transactions.items():
        posting = postings.get(txn_id)
        if posting is None:
            _record_break(conn, txn_id, account_id, "MISSING_POSTING",
                          f"No posting found for transaction dated {txn_date}")
            breaks += 1
            continue

        posting_date, posted_amount = posting
        if abs(posted_amount - amount) >= 0.01:
            _record_break(conn, txn_id, account_id, "AMOUNT_BREAK",
                          f"Transaction amount {amount} vs posted amount {posted_amount}")
            breaks += 1
        if posting_date != txn_date:
            _record_break(conn, txn_id, account_id, "TIMING_BREAK",
                          f"Transaction dated {txn_date}, posted {posting_date}")
            breaks += 1

    conn.commit()
    return breaks


def reconcile_subledger_to_gl(conn):
    """Aggregate postings per account/day and compare to the GL balance feed."""
    cur = conn.cursor()
    cur.execute(
        "SELECT account_id, posting_date, "
        "SUM(CASE WHEN direction = 'DEBIT' THEN posted_amount ELSE -posted_amount END) "
        "FROM stg_postings GROUP BY account_id, posting_date"
    )
    subledger_net = {(r[0], r[1]): round(r[2], 2) for r in cur.fetchall()}

    cur.execute("SELECT account_id, balance_date, gl_balance FROM stg_gl_balances")
    gl = {(r[0], r[1]): r[2] for r in cur.fetchall()}

    variances = 0
    for key, gl_balance in gl.items():
        sub_net = subledger_net.get(key, 0.0)
        if abs(sub_net - gl_balance) >= 0.01:
            account_id, balance_date = key
            _record_break(conn, None, account_id, "GL_VARIANCE",
                          f"Sub-ledger net {sub_net} vs GL balance {gl_balance} on {balance_date}")
            variances += 1

    conn.commit()
    return variances


def run():
    conn = get_connection()
    result = {
        "source_to_subledger_breaks": reconcile_source_to_subledger(conn),
        "subledger_to_gl_variances": reconcile_subledger_to_gl(conn),
    }
    conn.close()
    return result


if __name__ == "__main__":
    print(run())
