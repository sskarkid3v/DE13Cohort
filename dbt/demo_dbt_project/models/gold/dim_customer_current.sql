{{ config(materialized='table') }}

select
    customer_id,
    first_name,
    last_name,
    dob,
    email,
    phone,
    address,
    city,
    state,
    postal_code,
    created_at
from {{ ref('stg_customers') }}