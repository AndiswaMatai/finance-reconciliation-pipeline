"""
Generates synthetic source → sub-ledger → GL data for the demo.
Deliberately injects a handful of breaks (timing, amount, missing posting)
so the reconciliation engine has something real to find.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

ACCOUNTS = [
    ("1001", "Trading Book - Equities", "ASSET"),
    ("1002", "Trading Book - FX", "ASSET"),
    ("2001", "Client Settlement Clearing", "LIABILITY"),
    ("3001", "Fee Income", "INCOME"),
    ("4001", "Operating Expense", "EXPENSE"),
]

start = datetime(2026, 5, 1)
transactions, postings, gl_balances = [], [], []

txn_id = 100000
for day in range(20):
    txn_date = start + timedelta(days=day)
    for _ in range(random.randint(8, 14)):
        txn_id += 1
        account_id, _, _ = random.choice(ACCOUNTS)
        amount = round(random.uniform(500, 250000), 2)
        direction = random.choice(["DEBIT", "CREDIT"])
        transactions.append({
            "transaction_id": f"TXN{txn_id}",
            "account_id": account_id,
            "txn_date": txn_date.strftime("%Y-%m-%d"),
            "amount": amount,
            "direction": direction,
            "source_system": random.choice(["CORE_BANKING", "PAYMENTS_HUB", "FX_ENGINE"]),
        })

# Build postings from transactions, injecting controlled breaks
posting_id = 500000
for t in transactions:
    posting_id += 1
    break_type = None
    posted_amount = t["amount"]
    posted_date = t["txn_date"]

    r = random.random()
    if r < 0.03:
        break_type = "MISSING_POSTING"
        continue  # no posting row created -> reconciliation should flag this
    elif r < 0.06:
        break_type = "AMOUNT_BREAK"
        posted_amount = round(t["amount"] + random.choice([-1, 1]) * random.uniform(1, 50), 2)
    elif r < 0.09:
        break_type = "TIMING_BREAK"
        posted_date = (datetime.strptime(t["txn_date"], "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    postings.append({
        "posting_id": f"PST{posting_id}",
        "transaction_id": t["transaction_id"],
        "account_id": t["account_id"],
        "posting_date": posted_date,
        "posted_amount": posted_amount,
        "direction": t["direction"],
    })

# Aggregate GL balances per account/day from postings (this is what the GL "should" show)
from collections import defaultdict
daily = defaultdict(float)
for p in postings:
    sign = 1 if p["direction"] == "DEBIT" else -1
    daily[(p["account_id"], p["posting_date"])] += sign * p["posted_amount"]

for (account_id, date), net in sorted(daily.items()):
    gl_balances.append({
        "account_id": account_id,
        "balance_date": date,
        "gl_balance": round(net, 2),
    })

# Write CSVs
with open(RAW / "transactions.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=transactions[0].keys())
    w.writeheader(); w.writerows(transactions)

with open(RAW / "postings.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=postings[0].keys())
    w.writeheader(); w.writerows(postings)

with open(RAW / "gl_balances.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=gl_balances[0].keys())
    w.writeheader(); w.writerows(gl_balances)

print(f"transactions: {len(transactions)} | postings: {len(postings)} | gl rows: {len(gl_balances)}")
print(f"injected breaks: ~{len(transactions) - len(postings)} missing postings + amount/timing breaks")
