"""
Unit tests for the data quality and reconciliation modules. Run with:
    python -m unittest discover -s tests
(also discoverable by pytest, if installed, with no changes needed)
"""
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import data_quality
import reconcile


def _fresh_conn():
    conn = sqlite3.connect(":memory:")
    schema_path = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
    conn.executescript(schema_path.read_text())
    return conn


class TestDataQuality(unittest.TestCase):
    def setUp(self):
        self.conn = _fresh_conn()
        self.conn.executemany(
            "INSERT INTO stg_transactions VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("TXN1", "1001", "2026-05-01", 100.0, "DEBIT", "CORE_BANKING", "t"),
                ("TXN2", "1001", "2026-05-01", 200.0, "CREDIT", "CORE_BANKING", "t"),
                ("TXN3", "1001", "2026-05-01", None, "DEBIT", "CORE_BANKING", "t"),
            ],
        )
        self.conn.commit()

    def test_completeness_flags_null_amount(self):
        status, score = data_quality.check_completeness(self.conn)
        self.assertEqual(status, "FAIL")
        self.assertAlmostEqual(score, 2 / 3, places=3)

    def test_postings_coverage_with_no_postings_is_zero(self):
        status, score = data_quality.check_postings_coverage(self.conn)
        self.assertEqual(status, "FAIL")
        self.assertEqual(score, 0.0)


class TestReconciliation(unittest.TestCase):
    def setUp(self):
        self.conn = _fresh_conn()
        self.conn.executemany(
            "INSERT INTO stg_transactions VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("TXN1", "1001", "2026-05-01", 100.0, "DEBIT", "CORE_BANKING", "t"),
                ("TXN2", "1001", "2026-05-01", 200.0, "DEBIT", "CORE_BANKING", "t"),
                ("TXN3", "1001", "2026-05-01", 300.0, "DEBIT", "CORE_BANKING", "t"),
            ],
        )
        self.conn.executemany(
            "INSERT INTO stg_postings VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                # TXN1: clean match
                ("PST1", "TXN1", "1001", "2026-05-01", 100.0, "DEBIT", "t"),
                # TXN2: amount break (posted 205 vs transacted 200)
                ("PST2", "TXN2", "1001", "2026-05-01", 205.0, "DEBIT", "t"),
                # TXN3: deliberately has no posting -> MISSING_POSTING
            ],
        )
        self.conn.commit()

    def test_detects_missing_posting_and_amount_break(self):
        breaks = reconcile.reconcile_source_to_subledger(self.conn)
        self.assertEqual(breaks, 2)  # one missing posting, one amount break
        rows = self.conn.execute(
            "SELECT break_type FROM reconciliation_breaks ORDER BY break_type"
        ).fetchall()
        break_types = {r[0] for r in rows}
        self.assertIn("MISSING_POSTING", break_types)
        self.assertIn("AMOUNT_BREAK", break_types)

    def test_clean_transaction_produces_no_break(self):
        reconcile.reconcile_source_to_subledger(self.conn)
        rows = self.conn.execute(
            "SELECT * FROM reconciliation_breaks WHERE transaction_id = 'TXN1'"
        ).fetchall()
        self.assertEqual(len(rows), 0)


if __name__ == "__main__":
    unittest.main()
