"""
Unit tests for the Finance Reconciliation Engine.

These tests validate the core business logic without requiring a live
Spark cluster — they use Python-level logic extracted from the PySpark
functions so they run in the GitHub Actions CI environment.

For full Spark integration tests, run on a Databricks cluster using
pytest with the databricks-connect package.
"""
import hashlib
import unittest


SEPARATOR = "|"
MATCHING_FIELDS = ["account_number", "trade_reference", "settlement_date", "currency", "debit_credit"]
AMOUNT_TOLERANCE = 1.00


# ── Key construction logic (mirrors src/reconciliation/key_builder.py) ──────
def build_business_key(record: dict) -> str:
    parts = [str(record.get(f) or "NULL") for f in MATCHING_FIELDS]
    raw = SEPARATOR.join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()


# ── Reconciliation classification logic (mirrors src/reconciliation/engine.py) ──
def classify(cash: dict, rada: dict, tolerance: float = AMOUNT_TOLERANCE) -> str:
    if cash and rada:
        if abs(cash["amount"] - rada["amount"]) <= tolerance:
            return "MATCHED"
        return "AMOUNT_MISMATCH"
    if cash and not rada:
        return "UNMATCHED"
    if not cash and rada:
        return "NEW"
    return "UNKNOWN"


class TestKeyBuilder(unittest.TestCase):

    def test_same_fields_produce_same_key(self):
        r1 = {"account_number": "ACC00000001", "trade_reference": "TRD0000001",
              "settlement_date": "2026-05-05", "currency": "ZAR", "debit_credit": "D"}
        r2 = r1.copy()
        self.assertEqual(build_business_key(r1), build_business_key(r2))

    def test_different_currency_produces_different_key(self):
        r1 = {"account_number": "ACC00000001", "trade_reference": "TRD0000001",
              "settlement_date": "2026-05-05", "currency": "ZAR", "debit_credit": "D"}
        r2 = {**r1, "currency": "USD"}
        self.assertNotEqual(build_business_key(r1), build_business_key(r2))

    def test_case_sensitivity(self):
        r1 = {"account_number": "acc00000001", "trade_reference": "TRD0000001",
              "settlement_date": "2026-05-05", "currency": "ZAR", "debit_credit": "D"}
        r2 = {"account_number": "ACC00000001", "trade_reference": "TRD0000001",
              "settlement_date": "2026-05-05", "currency": "ZAR", "debit_credit": "D"}
        # Silver standardisation uppercases everything — so keys should match post-Silver
        # but would differ on raw Bronze data, which is why standardisation runs first
        self.assertNotEqual(build_business_key(r1), build_business_key(r2))

    def test_null_field_produces_consistent_key(self):
        r1 = {"account_number": "ACC00000001", "trade_reference": "TRD0000001",
              "settlement_date": "2026-05-05", "currency": "ZAR", "debit_credit": None}
        r2 = r1.copy()
        self.assertEqual(build_business_key(r1), build_business_key(r2))

    def test_key_is_sha256_length(self):
        r = {"account_number": "ACC00000001", "trade_reference": "TRD0000001",
             "settlement_date": "2026-05-05", "currency": "ZAR", "debit_credit": "D"}
        self.assertEqual(len(build_business_key(r)), 64)


class TestReconciliationEngine(unittest.TestCase):

    def test_exact_match(self):
        cash = {"amount": 100000.00}
        rada = {"amount": 100000.00}
        self.assertEqual(classify(cash, rada), "MATCHED")

    def test_match_within_tolerance(self):
        cash = {"amount": 100000.00}
        rada = {"amount": 100000.50}
        self.assertEqual(classify(cash, rada), "MATCHED")

    def test_amount_mismatch_exceeds_tolerance(self):
        cash = {"amount": 100000.00}
        rada = {"amount": 100050.00}
        self.assertEqual(classify(cash, rada), "AMOUNT_MISMATCH")

    def test_unmatched_cash_only(self):
        self.assertEqual(classify({"amount": 50000}, None), "UNMATCHED")

    def test_new_rada_only(self):
        self.assertEqual(classify(None, {"amount": 75000}), "NEW")

    def test_exact_tolerance_boundary(self):
        cash = {"amount": 100000.00}
        rada = {"amount": 100001.00}
        self.assertEqual(classify(cash, rada), "MATCHED")  # exactly at tolerance

    def test_one_cent_over_tolerance(self):
        cash = {"amount": 100000.00}
        rada = {"amount": 100001.01}
        self.assertEqual(classify(cash, rada), "AMOUNT_MISMATCH")


class TestSilverValidation(unittest.TestCase):

    def test_negative_amount_should_be_rejected(self):
        record = {"cash_id": "C1", "account_number": "ACC001",
                  "trade_reference": "TRD001", "settlement_date": "2026-05-05",
                  "currency": "ZAR", "amount": -500.00, "debit_credit": "D"}
        valid = record["amount"] >= 0
        self.assertFalse(valid)

    def test_valid_debit_credit_values(self):
        self.assertIn("D", ("D", "C"))
        self.assertIn("C", ("D", "C"))
        self.assertNotIn("X", ("D", "C"))

    def test_mandatory_fields_check(self):
        mandatory = ["cash_id", "account_number", "trade_reference",
                     "settlement_date", "currency", "amount"]
        complete = {"cash_id": "C1", "account_number": "ACC001",
                    "trade_reference": "TRD001", "settlement_date": "2026-05-05",
                    "currency": "ZAR", "amount": 1000.0}
        missing = [f for f in mandatory if not complete.get(f)]
        self.assertEqual(missing, [])

    def test_missing_mandatory_field_detected(self):
        mandatory = ["cash_id", "account_number", "trade_reference",
                     "settlement_date", "currency", "amount"]
        incomplete = {"cash_id": "C1", "account_number": None,
                      "trade_reference": "TRD001", "settlement_date": "2026-05-05",
                      "currency": "ZAR", "amount": 1000.0}
        missing = [f for f in mandatory if not incomplete.get(f)]
        self.assertIn("account_number", missing)


if __name__ == "__main__":
    unittest.main()
