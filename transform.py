"""
Silver/Gold layer: builds the conformed dim_account (Type 2 SCD) and
fact_transactions table from staged data.

apply_scd2() implements the same close-out / insert-new-version logic shown
in sql/scd2_dim_account.sql, expressed in Python because SQLite has no
MERGE statement. The intent and outcome match how it runs in Synapse/SQL
Server in production.
"""
from datetime import date

from db import get_connection

# Reference data that would normally arrive from an ERP / chart-of-accounts feed.
ACCOUNT_REFERENCE = [
    ("1001", "Trading Book - Equities", "ASSET"),
    ("1002", "Trading Book - FX", "ASSET"),
    ("2001", "Client Settlement Clearing", "LIABILITY"),
    ("3001", "Fee Income", "INCOME"),
    ("4001", "Operating Expense", "EXPENSE"),
]


def apply_scd2(conn):
    today = date.today().isoformat()
    cur = conn.cursor()
    changed_or_new = 0

    for account_id, name, acct_type in ACCOUNT_REFERENCE:
        cur.execute(
            "SELECT account_sk, account_name, account_type FROM dim_account "
            "WHERE account_id = ? AND is_current = 1", (account_id,),
        )
        current = cur.fetchone()

        if current is None:
            cur.execute(
                """INSERT INTO dim_account
                   (account_id, account_name, account_type, effective_from, effective_to, is_current)
                   VALUES (?, ?, ?, ?, NULL, 1)""",
                (account_id, name, acct_type, today),
            )
            changed_or_new += 1
        elif (current[1], current[2]) != (name, acct_type):
            cur.execute(
                "UPDATE dim_account SET effective_to = ?, is_current = 0 WHERE account_sk = ?",
                (today, current[0]),
            )
            cur.execute(
                """INSERT INTO dim_account
                   (account_id, account_name, account_type, effective_from, effective_to, is_current)
                   VALUES (?, ?, ?, ?, NULL, 1)""",
                (account_id, name, acct_type, today),
            )
            changed_or_new += 1

    conn.commit()
    return changed_or_new


def build_fact_transactions(conn):
    cur = conn.cursor()
    cur.execute(
        """SELECT account_id, account_sk FROM dim_account WHERE is_current = 1"""
    )
    account_sk_by_id = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT transaction_id, account_id, txn_date, amount, direction, source_system FROM stg_transactions")
    rows = cur.fetchall()

    insert_rows = [
        (txn_id, account_sk_by_id[account_id], txn_date, amount, direction, source_system)
        for (txn_id, account_id, txn_date, amount, direction, source_system) in rows
        if account_id in account_sk_by_id
    ]

    cur.executemany(
        """INSERT OR REPLACE INTO fact_transactions
           (transaction_id, account_sk, txn_date, amount, direction, source_system)
           VALUES (?, ?, ?, ?, ?, ?)""",
        insert_rows,
    )
    conn.commit()
    return len(insert_rows)


def run():
    conn = get_connection()
    dim_changes = apply_scd2(conn)
    fact_rows = build_fact_transactions(conn)
    conn.close()
    return {"dim_account_changes": dim_changes, "fact_transactions_rows": fact_rows}


if __name__ == "__main__":
    print(run())
