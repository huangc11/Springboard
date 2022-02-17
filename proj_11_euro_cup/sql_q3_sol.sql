

 select mm.match_no , play_date , goal_score   
from match_mast mm 
where stop1_sec=0 
;

--------------- solution -----------------
+----------+------------+------------+
| match_no | play_date  | goal_score |
+----------+------------+------------+
|        4 | 2016-06-12 | 01-Jan     |
+----------+------------+------------+