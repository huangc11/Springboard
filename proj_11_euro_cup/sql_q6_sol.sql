--  6. Write a SQL query to find the number of matches that were won by a single point, but
do not include matches decided by penalty shootout.

select count(*)
from 
(select distinct t1.match_no, t2.decided_by
from match_details t1, 
    match_details t2,
    match_mast mm
where t1.match_no = t2.match_no
 and abs(t1.goal_score-t2.goal_score)=1
  and t1.team_id <> t2.team_id
  and t1.match_no =mm.match_no
  and mm.decided_by='N'
) mch
 ;

-----------------  solution----------------------------
+----------+
| count(*) |
+----------+
|       22 |
+----------+
