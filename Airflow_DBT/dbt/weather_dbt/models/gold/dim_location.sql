{{ config(materialized='table') }}

select DISTINCT
    latitude,
    longitude
from {{ ref('stg_weather') }}