select date, weather_condition, precipitation, count(*) as n
from {{ ref('weather_impact') }}
group by 1, 2, 3
having count(*) > 1
