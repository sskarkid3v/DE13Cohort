{{ config(materialized='table') }}

select
    account_id,
    customer_id,
    branch_id,
    account_type,
    status,
    opened_at,
    closed_at,
    currency,
    initial_balance
from {{ ref('stg_accounts') }}