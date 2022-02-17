
select count(*)
from 
(
select distinct t2.player_id
from match_captain t1, 
	 player_mast t2,
     playing_position t3
where t2.player_id = t1.player_captain
 and t2.posi_to_play = t3.position_id
 and t3.position_desc  ='Goalkeepers'
 ) p1

-------------  Solution ---------------
+----------+
| count(*) |
+----------+
|        4 |
+----------+