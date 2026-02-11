{{ config(
    materialized='table'
) }}

select
    transaction_id,
    account_id,
    customer_id,
    branch_id,
    txn_ts,
    txn_date,
    txn_type,
    channel,
    direction,
    amount
from {{ ref('stg_transactions') }}