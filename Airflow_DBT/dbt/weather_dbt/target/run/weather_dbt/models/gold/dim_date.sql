
  
    

  create  table "analytics"."gold"."dim_date__dbt_tmp"
  
  
    as
  
  (
    

with bounds as (
    SELECT
        min(day) as min_day,
        max(day) as max_day
    FROM "analytics"."silver"."stg_weather"    
),
dates as (
    select generate_series(min_day, max_day, interval '1 day')::date as date_day
    from bounds
)
SELECT
    date_day,
    extract(year from date_day)::int as year,
    extract(month from date_day)::int as month,
    to_char(date_day, 'Mon')  as month_name,
    extract(day from date_day) as day_of_month,
    extract(dow from date_day)::int as day_of_week,
    to_char(date_day, 'Dy') as day_name,
    case when extract(dow from date_day) in (0,6) then true else false end as is_weekend
FROM dates
  );
  