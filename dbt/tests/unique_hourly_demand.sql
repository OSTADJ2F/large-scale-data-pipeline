select date, hour, count(*) as n
from {{ ref('hourly_demand') }}
group by 1, 2
having count(*) > 1
