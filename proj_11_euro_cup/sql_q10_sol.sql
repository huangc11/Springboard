select  pm.* 
from 
     soccer_country sc,
     player_mast pm 
where  pm.team_id = sc.country_id 
  and sc.country_name ='England'
  and pm.playing_club like '%Liverpool%'
;

--------------  Solution -------------
+-----------+---------+-----------+------------------+--------------+------------+------+--------------+
| player_id | team_id | jersey_no | player_name      | posi_to_play | dt_of_bir  | age  | playing_club |
+-----------+---------+-----------+------------------+--------------+------------+------+--------------+
|    160131 |    1206 |         4 | James Milner     | MF           | 1986-01-04 |   30 | Liverpool    |
|    160130 |    1206 |         8 | Adam Lallana     | MF           | 1988-05-10 |   28 | Liverpool    |
|    160121 |    1206 |        12 | Nathaniel Clyne  | DF           | 1991-04-05 |   25 | Liverpool    |
|    160129 |    1206 |        14 | Jordan Henderson | MF           | 1990-06-17 |   26 | Liverpool    |
|    160137 |    1206 |        15 | Daniel Sturridge | FD           | 1989-09-01 |   26 | Liverpool    |
+-----------+---------+-----------+------------------+--------------+------------+------+--------------+