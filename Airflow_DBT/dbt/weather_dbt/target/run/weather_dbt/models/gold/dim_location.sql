
  
    

  create  table "analytics"."gold"."dim_location__dbt_tmp"
  
  
    as
  
  (
    

select DISTINCT
    latitude,
    longitude
from "analytics"."silver"."stg_weather"
  );
  