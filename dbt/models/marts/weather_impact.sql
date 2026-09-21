{{ config(materialized='table') }}

select
    pickup_date as date,
    weather_condition,
    precipitation,
    count(*) as trip_count,
    avg(trip_duration_seconds) as average_duration,
    avg(fare_amount) as average_fare
from {{ ref('fct_trips') }}
group by 1, 2, 3
