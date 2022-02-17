
-- 11. Write a SQL query to find the players, their jersey number, and playing club who were
the goalkeepers for England in EURO Cup 2016.



select  distinct pm.player_name, pm.jersey_no, 
pm.playing_club 
from 
	 match_details md,
     soccer_country sc,
     player_mast pm 
where md.team_id = sc.country_id 
  and md.player_gk = pm.player_id 
  and sc.country_name ='England'
;


--------------  Solution -------------
+-------------+-----------+--------------+
| player_name | jersey_no | playing_club |
+-------------+-----------+--------------+
| Joe Hart    |         1 | Man. City    |
+-------------+-----------+--------------+