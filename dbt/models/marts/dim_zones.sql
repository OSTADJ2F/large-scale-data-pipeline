{{ config(materialized='table') }}

select
    "LocationID" as location_id,
    "Borough" as borough,
    "Zone" as zone,
    "service_zone" as service_zone
from read_csv_auto('{{ var("zones_csv") }}')
