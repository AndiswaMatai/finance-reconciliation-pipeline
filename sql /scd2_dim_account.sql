-- Type 2 SCD merge pattern for dim_account.
-- SQLite has no MERGE statement, so run_pipeline.py implements this logic in
-- Python (see src/transform.py:apply_scd2). This file documents the same
-- logic as it would run in Azure Synapse / SQL Server, which is how it is
-- implemented in production-style dbt/SQL pipelines.

-- Step 1: close out any current row whose attributes have changed
UPDATE dim_account
SET effective_to = CAST(GETDATE() AS DATE),
    is_current    = 0
WHERE is_current = 1
  AND account_id IN (
      SELECT s.account_id
      FROM stg_account_reference s
      JOIN dim_account d
        ON d.account_id = s.account_id AND d.is_current = 1
      WHERE d.account_name <> s.account_name
         OR d.account_type <> s.account_type
  );

-- Step 2: insert new current rows for new accounts and for changed accounts
-- closed out above (anti-join on currently-open rows).
INSERT INTO dim_account (account_id, account_name, account_type, effective_from, effective_to, is_current)
SELECT
    s.account_id,
    s.account_name,
    s.account_type,
    CAST(GETDATE() AS DATE) AS effective_from,
    NULL AS effective_to,
    1    AS is_current
FROM stg_account_reference s
LEFT JOIN dim_account d
       ON d.account_id = s.account_id AND d.is_current = 1
WHERE d.account_id IS NULL;
