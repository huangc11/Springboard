-- 20. Write a SQL query to find the substitute players who came into the field in the first
half of play, within a normal play schedule

select  pm.player_name,pm.playing_club 
from player_in_out pio,
     player_mast pm 
where play_half=1  
  and play_schedule='NT' 
  AND in_out ='I' 
  and pio.player_id = pm.player_id 
;


-----------------  solution----------------------------
+------------------------+--------------+
| player_name            | playing_club |
+------------------------+--------------+
| Bastian Schweinsteiger | Man. United  |
| Ricardo Quaresma       | Besiktas     |
| Erik Johansson         | Kobenhavn    |
+------------------------+--------------+

