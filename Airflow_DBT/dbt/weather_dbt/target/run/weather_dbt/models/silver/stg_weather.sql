
  create view "analytics"."silver"."stg_weather__dbt_tmp"
    
    
  as (
    

with ranked as (
    SELECT
        day::date as day,
        latitude::numeric as latitude,
        longitude::numeric as longitude,
        temp_max::numeric as temp_max,
        temp_min::numeric as temp_min,
        precipitation_sum::numeric as precipitation_sum,
        ingested_at::timestamp as ingested_at,
        row_number() over (partition by day order by ingested_at desc) as rn
    FROM "analytics"."bronze"."weather_raw"
)
select
    day,
    latitude,
    longitude,
    temp_max,
    temp_min,
    precipitation_sum,
    ingested_at
from ranked
where rn = 1
  );