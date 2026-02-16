create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;


create table if not exists bronze.weather_raw (
    ingested_at TIMESTAMP not null,
    latitude numeric,
    longitude numeric,
    day date not null,
    temp_max numeric,
    temp_min numeric,
    precipitation_sum numeric
);