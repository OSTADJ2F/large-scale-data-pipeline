{{ config(materialized='table') }}

select
    pickup_date as date,
    pickup_hour as hour,
    count(*) as trip_count,
    avg(trip_duration_seconds) as average_trip_duration,
    avg(fare_amount) as average_fare
from {{ ref('fct_trips') }}
group by 1, 2
