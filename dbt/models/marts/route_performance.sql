{{ config(materialized='table') }}

select
    pickup_location_id,
    dropoff_location_id,
    count(*) as trip_count,
    avg(trip_distance) as average_distance,
    avg(trip_duration_seconds) as average_duration,
    sum(total_charge) as total_revenue,
    avg(tip_amount) as average_tip
from {{ ref('fct_trips') }}
group by 1, 2
