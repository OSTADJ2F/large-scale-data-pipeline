{{ config(materialized='table') }}

select
    pickup_date as date,
    pickup_location_id,
    count(*) as trip_count,
    avg(trip_duration_seconds) as average_trip_duration,
    sum(trip_distance) as total_distance,
    sum(total_charge) as total_revenue,
    avg(fare_amount) as average_fare
from {{ ref('fct_trips') }}
group by 1, 2
