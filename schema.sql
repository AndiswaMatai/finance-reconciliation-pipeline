-- Schema for the finance reconciliation pipeline.
-- Written for SQLite (used by run_pipeline.py) with comments showing the
-- equivalent SQL Server / Azure Synapse syntax where it differs.

-- ===================== STAGING (Bronze) =====================
CREATE TABLE IF NOT EXISTS stg_transactions (
    transaction_id   TEXT PRIMARY KEY,
    account_id       TEXT,
    txn_date         TEXT,
    amount           REAL,
    direction        TEXT,
    source_system    TEXT,
    load_ts          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_postings (
    posting_id       TEXT PRIMARY KEY,
    transaction_id   TEXT NOT NULL,
    account_id       TEXT NOT NULL,
    posting_date     TEXT NOT NULL,
    posted_amount    REAL NOT NULL,
    direction        TEXT NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    load_ts          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_gl_balances (
    account_id       TEXT NOT NULL,
    balance_date     TEXT NOT NULL,
    gl_balance       REAL NOT NULL,
    load_ts          TEXT NOT NULL,
    PRIMARY KEY (account_id, balance_date)
);

-- ===================== DIMENSION (Silver, SCD Type 2) =====================
-- SQL Server / Synapse: swap AUTOINCREMENT for IDENTITY(1,1), TEXT for VARCHAR(n).
CREATE TABLE IF NOT EXISTS dim_account (
    account_sk        INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id        TEXT NOT NULL,
    account_name      TEXT NOT NULL,
    account_type      TEXT NOT NULL,
    effective_from     TEXT NOT NULL,
    effective_to       TEXT,
    is_current        INTEGER NOT NULL DEFAULT 1
);

-- ===================== FACT (Gold) =====================
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id    TEXT PRIMARY KEY,
    account_sk        INTEGER NOT NULL REFERENCES dim_account(account_sk),
    txn_date          TEXT NOT NULL,
    amount            REAL NOT NULL,
    direction         TEXT NOT NULL,
    source_system     TEXT NOT NULL
);

-- ===================== RECONCILIATION OUTPUT =====================
CREATE TABLE IF NOT EXISTS reconciliation_breaks (
    break_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id    TEXT,
    account_id        TEXT NOT NULL,
    break_type        TEXT NOT NULL,   -- MISSING_POSTING | AMOUNT_BREAK | TIMING_BREAK | GL_VARIANCE
    detail            TEXT,
    detected_ts       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS data_quality_results (
    check_name        TEXT NOT NULL,
    table_name        TEXT NOT NULL,
    status            TEXT NOT NULL,  -- PASS | FAIL
    metric_value      REAL,
    threshold         REAL,
    run_ts            TEXT NOT NULL
);
