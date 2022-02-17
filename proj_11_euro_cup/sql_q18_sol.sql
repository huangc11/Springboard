 select max(count) as max_count
from 
    (
    select match_no, count(*) count 
    from player_booked
    group by match_no
    )  p1
;

------------------ solution -------------------------
+-----------+
| max_count |
+-----------+
|        10 |
+-----------+