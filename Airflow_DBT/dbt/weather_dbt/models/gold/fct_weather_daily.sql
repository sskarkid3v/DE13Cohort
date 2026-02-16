{{ config(materialized='table') }}

SELECT
    day as date_day,
    latitude,
    longitude,
    temp_max,
    temp_min,
    temp_max - temp_min as temp_range,
    precipitation_sum
FROM {{ ref('stg_weather') }}