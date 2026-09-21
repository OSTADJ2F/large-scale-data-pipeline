select date, pickup_location_id, count(*) as n
from {{ ref('daily_demand') }}
group by 1, 2
having count(*) > 1
