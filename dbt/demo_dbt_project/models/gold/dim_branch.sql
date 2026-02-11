{{ config(materialized='table') }}

select
    branch_id,
    branch_code,
    branch_name,
    city,
    state,
    opened_on
from {{ ref('stg_branches') }}