"""
Generates realistic synthetic Cash and RADA datasets for the Finance
Reconciliation Engine. Deliberately injects mismatches, new records,
and cleared records so the reconciliation engine has real scenarios to handle.

These CSVs land in data/sample/ — in production, equivalent extracts arrive
in ADLS Gen2 (abfss://landing@...) and are picked up by the Bronze layer notebooks.

Run: python scripts/generate_sample_data.py
"""
import csv
import hashlib
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(2026)
OUT = Path(__file__).resolve().parent.parent / "data" / "sample"
OUT.mkdir(parents=True, exist_ok=True)

CURRENCIES = ["ZAR", "USD", "EUR", "GBP"]
BANKS = ["FIRNZAJJ", "ABSAZAJJ", "NEDSZAJJ", "SBZAZAJJ", "CIBZZAJJ"]  # realistic SWIFT BICs
ACCOUNT_POOL = [f"ACC{str(i).zfill(8)}" for i in range(1, 301)]
TRADE_REF_POOL = [f"TRD{str(i).zfill(7)}" for i in range(1, 501)]

start = datetime(2026, 5, 1)


def settlement_date(base: datetime) -> str:
    return (base + timedelta(days=random.choice([0, 1, 2]))).strftime("%Y-%m-%d")


def value_date(base: datetime) -> str:
    return base.strftime("%Y-%m-%d")


def generate_cash_records(n: int = 400):
    records = []
    for i in range(1, n + 1):
        txn_date = start + timedelta(days=random.randint(0, 29))
        amount = round(random.uniform(1000, 2_500_000), 2)
        currency = random.choice(CURRENCIES)
        account = random.choice(ACCOUNT_POOL)
        trade_ref = random.choice(TRADE_REF_POOL)
        records.append({
            "cash_id": f"CSH{str(i).zfill(8)}",
            "account_number": account,
            "trade_reference": trade_ref,
            "swift_bic": random.choice(BANKS),
            "transaction_date": txn_date.strftime("%Y-%m-%d"),
            "value_date": value_date(txn_date),
            "settlement_date": settlement_date(txn_date),
            "currency": currency,
            "amount": amount,
            "debit_credit": random.choice(["D", "C"]),
            "narrative": f"PAYMENT REF {trade_ref}",
            "status": "SETTLED",
        })
    return records


def generate_rada_records(cash_records: list, match_rate: float = 0.82):
    """
    RADA records are generated from Cash with controlled mismatches:
      - 82% perfect matches (same trade_ref, account, amount)
      - 8%  amount mismatches (rounding / FX difference)
      - 5%  missing from RADA (Cash has it, RADA doesn't → Unmatched)
      - 5%  new in RADA (RADA has it, Cash doesn't → New)
    """
    rada_records = []
    rada_id = 1

    for cash in cash_records:
        r = random.random()
        if r < (1 - match_rate - 0.13):
            continue  # Missing from RADA — Unmatched

        rada_amount = cash["amount"]
        if r > 0.95:  # Amount mismatch
            rada_amount = round(cash["amount"] + random.choice([-1, 1]) * random.uniform(0.01, 50.00), 2)

        rada_records.append({
            "rada_id": f"RADA{str(rada_id).zfill(8)}",
            "account_number": cash["account_number"],
            "trade_reference": cash["trade_reference"],
            "swift_bic": cash["swift_bic"],
            "transaction_date": cash["transaction_date"],
            "value_date": cash["value_date"],
            "settlement_date": cash["settlement_date"],
            "currency": cash["currency"],
            "amount": rada_amount,
            "debit_credit": cash["debit_credit"],
            "narrative": cash["narrative"],
            "status": "CONFIRMED",
        })
        rada_id += 1

    # New records in RADA not in Cash
    for i in range(25):
        txn_date = start + timedelta(days=random.randint(0, 29))
        trade_ref = f"TRD{str(9000 + i).zfill(7)}"
        rada_records.append({
            "rada_id": f"RADA{str(rada_id).zfill(8)}",
            "account_number": random.choice(ACCOUNT_POOL),
            "trade_reference": trade_ref,
            "swift_bic": random.choice(BANKS),
            "transaction_date": txn_date.strftime("%Y-%m-%d"),
            "value_date": value_date(txn_date),
            "settlement_date": settlement_date(txn_date),
            "currency": random.choice(CURRENCIES),
            "amount": round(random.uniform(5000, 500_000), 2),
            "debit_credit": random.choice(["D", "C"]),
            "narrative": f"PAYMENT REF {trade_ref}",
            "status": "CONFIRMED",
        })
        rada_id += 1

    return rada_records


def write_csv(path: Path, records: list):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=records[0].keys())
        w.writeheader()
        w.writerows(records)


if __name__ == "__main__":
    cash = generate_cash_records(400)
    rada = generate_rada_records(cash, match_rate=0.82)

    write_csv(OUT / "cash_transactions.csv", cash)
    write_csv(OUT / "rada_transactions.csv", rada)

    print(f"Cash records:  {len(cash)}")
    print(f"RADA records:  {len(rada)}")
    print(f"Saved to:      {OUT}")
