
    
    

select
    day as unique_field,
    count(*) as n_records

from "analytics"."silver"."stg_weather"
where day is not null
group by day
having count(*) > 1


