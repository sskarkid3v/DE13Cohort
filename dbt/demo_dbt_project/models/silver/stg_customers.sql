{{ config(materialized='view') }}

SELECT
    customer_id::int as customer_id,
    first_name::text as first_name,
    last_name::text as last_name,
    dob::date as dob,
    lower(email::text) as email,
    phone::text as phone,
    address::text as address,
    city::text as city,
    state::text as state,
    postal_code::text as postal_code,
    created_at::timestamp as created_at
from {{ source('bronze', 'customers') }}
where customer_id is not null
