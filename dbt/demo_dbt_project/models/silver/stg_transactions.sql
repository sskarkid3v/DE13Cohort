{{ config(materialized='view') }}

SELECT
    transaction_id::bigint as transaction_id,
    account_id::bigint as account_id,
    customer_id::int as customer_id,
    branch_id::int as branch_id,
    txn_ts::timestamp as txn_ts,
    txn_ts::date as txn_date,
    lower(txn_type) as txn_type,
    lower(channel) as channel,
    amount::numeric(18,2) as amount,
    lower(direction) as direction
from {{ source('bronze', 'transactions') }}
where transaction_id is not null
