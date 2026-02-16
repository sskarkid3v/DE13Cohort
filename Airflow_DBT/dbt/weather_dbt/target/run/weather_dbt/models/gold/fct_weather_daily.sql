
  
    

  create  table "analytics"."gold"."fct_weather_daily__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    day as date_day,
    latitude,
    longitude,
    temp_max,
    temp_min,
    temp_max - temp_min as temp_range,
    precipitation_sum
FROM "analytics"."silver"."stg_weather"
  );
  