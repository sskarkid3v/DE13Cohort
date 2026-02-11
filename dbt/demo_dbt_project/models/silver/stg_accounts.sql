{{ config(materialized='view') }}

SELECT
  account_id::bigint      AS account_id,
  customer_id::int        AS customer_id,
  branch_id::int          AS branch_id,
  lower(account_type)     AS account_type,
  lower(status)           AS status,
  opened_at::timestamp    AS opened_at,
  NULLIF(closed_at::text,'')::timestamp AS closed_at,
  currency::text          AS currency,
  initial_balance::numeric(18,2) AS initial_balance
FROM {{ source('bronze','accounts') }}
WHERE account_id IS NOT NULL
