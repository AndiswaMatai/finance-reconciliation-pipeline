"""
End-to-end orchestrator: source CSVs -> staging -> dimension/fact -> data
quality checks -> reconciliation -> printed control report.

Run:
    python src/generate_sample_data.py   # only needed once, or to refresh data
    python src/run_pipeline.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ingest
import transform
import data_quality
import reconcile
from db import get_connection


def main():
    print("=" * 60)
    print("FINANCE RECONCILIATION PIPELINE")
    print("=" * 60)

    print("\n[1/4] Ingesting source -> staging (Bronze)...")
    counts = ingest.run()
    for table, n in counts.items():
        print(f"   {table}: {n} rows loaded")

    print("\n[2/4] Transforming Bronze -> Silver/Gold...")
    t = transform.run()
    print(f"   dim_account changes: {t['dim_account_changes']}")
    print(f"   fact_transactions rows: {t['fact_transactions_rows']}")

    print("\n[3/4] Running data quality checks...")
    dq = data_quality.run()
    for name, (status, score) in dq.items():
        flag = "PASS" if status == "PASS" else "FAIL"
        print(f"   [{flag}] {name}: {score:.2%}")

    print("\n[4/4] Reconciling source -> sub-ledger -> GL...")
    rec = reconcile.run()
    print(f"   source-to-subledger breaks: {rec['source_to_subledger_breaks']}")
    print(f"   subledger-to-GL variances:  {rec['subledger_to_gl_variances']}")

    print("\n" + "=" * 60)
    print("CONTROL EVIDENCE SUMMARY")
    print("=" * 60)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT break_type, COUNT(*) FROM reconciliation_breaks GROUP BY break_type")
    for break_type, n in cur.fetchall():
        print(f"   {break_type}: {n}")
    cur.execute("SELECT COUNT(*) FROM data_quality_results WHERE status = 'FAIL'")
    failed_checks = cur.fetchone()[0]
    print(f"   data quality checks failed: {failed_checks}")
    conn.close()
    print("\nDone. Full detail available in data/warehouse.db "
          "(tables: reconciliation_breaks, data_quality_results).")


if __name__ == "__main__":
    main()
