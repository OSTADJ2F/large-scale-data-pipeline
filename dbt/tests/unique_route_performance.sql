select pickup_location_id, dropoff_location_id, count(*) as n
from {{ ref('route_performance') }}
group by 1, 2
having count(*) > 1
