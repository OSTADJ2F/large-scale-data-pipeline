{{ config(materialized='view') }}

select
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(pickup_date as date) as pickup_date,
    cast(pickup_hour as integer) as pickup_hour,
    cast(pickup_day_of_week as integer) as pickup_day_of_week,
    cast(pickup_location_id as integer) as pickup_location_id,
    cast(dropoff_location_id as integer) as dropoff_location_id,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as double) as trip_distance,
    cast(trip_duration_seconds as double) as trip_duration_seconds,
    cast(average_speed as double) as average_speed,
    cast(fare_amount as double) as fare_amount,
    cast(tip_amount as double) as tip_amount,
    cast(total_charge as double) as total_charge,
    payment_type,
    is_airport_trip,
    cast(temperature as double) as temperature,
    cast(precipitation as double) as precipitation,
    cast(wind_speed as double) as wind_speed,
    weather_condition,
    weather_joined
from read_parquet('{{ var("curated_trips_glob") }}', hive_partitioning = false, union_by_name = true)
