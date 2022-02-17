select distinct  pm.player_name, pm.jersey_no
from 
	 match_details md,
     soccer_country sc,
     player_mast pm
where md.team_id = sc.country_id 
  and md.player_gk = pm.player_id
  and md.play_stage='G'
  and sc.country_name ='Germany'
;

--------------  Solution -------------
+--------------+-----------+
| player_name  | jersey_no |
+--------------+-----------+
| Manuel Neuer |         1 |
+--------------+-----------+