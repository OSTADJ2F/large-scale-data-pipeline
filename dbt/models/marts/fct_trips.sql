{{ config(materialized='table') }}

select
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_date,
    t.pickup_hour,
    t.pickup_day_of_week,
    t.pickup_location_id,
    pu.zone as pickup_zone,
    pu.borough as pickup_borough,
    t.dropoff_location_id,
    doz.zone as dropoff_zone,
    doz.borough as dropoff_borough,
    t.passenger_count,
    t.trip_distance,
    t.trip_duration_seconds,
    t.average_speed,
    t.fare_amount,
    t.tip_amount,
    t.total_charge,
    t.payment_type,
    t.is_airport_trip,
    t.temperature,
    t.precipitation,
    t.wind_speed,
    t.weather_condition,
    t.weather_joined
from {{ ref('stg_trips') }} t
left join {{ ref('dim_zones') }} pu on t.pickup_location_id = pu.location_id
left join {{ ref('dim_zones') }} doz on t.dropoff_location_id = doz.location_id
