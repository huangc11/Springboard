
select count(*)  
from ( 
select  ps.match_no 
from  penalty_shootout ps,   
           match_details md  
where ps.team_id = md.team_id and 
            ps.match_no = md.match_no and 
            md.win_lose ='W' 
group by ps.match_no 
) m1;

-------------------- Solution ----------------------------------
+----------+
| count(*) |
+----------+
|        3 |
+----------+