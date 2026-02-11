{{ config(materialized='view') }}

SELECT
    branch_id::int as branch_id,
    branch_code::text as branch_code,
    name::text as branch_name,
    city::text as city,
    state::text as state,
    opened_on::date as opened_on
from {{ source('bronze', 'branches') }}
where branch_id is not null
