
-- 12. Write a SQL query that returns the total number of goals scored by each position on
each country’s team. Do not include positions which scored no goals.


select pp.position_desc, sc.country_name , count(*) as count 
from goal_details gd, 
	 player_mast pm,
     playing_position pp,
     soccer_country sc 
where gd.player_id = pm.player_id
  and pm.posi_to_play = pp.position_id
  and gd.team_id = sc.country_id 
group by pp.position_desc, sc.country_name 
order by pp.position_desc, sc.country_name;



--------------  Solution -------------
+---------------+---------------------+-------+
| position_desc | country_name        | count |
+---------------+---------------------+-------+
| Defenders     | Albania             |     1 |
| Defenders     | Belgium             |     4 |
| Defenders     | Croatia             |     1 |
| Defenders     | Czech Republic      |     2 |
| Defenders     | England             |     3 |
| Defenders     | France              |     9 |
| Defenders     | Germany             |     4 |
| Defenders     | Hungary             |     4 |
| Defenders     | Iceland             |     6 |
| Defenders     | Italy               |     5 |
| Defenders     | Northern Ireland    |     3 |
| Defenders     | Poland              |     2 |
| Defenders     | Portugal            |     8 |
| Defenders     | Republic of Ireland |     1 |
| Defenders     | Romania             |     2 |
| Defenders     | Russia              |     1 |
| Defenders     | Spain               |     5 |
| Defenders     | Switzerland         |     2 |
| Defenders     | Turkey              |     1 |
| Defenders     | Wales               |     8 |
| Midfielders   | Austria             |     1 |
| Midfielders   | Belgium             |     5 |
| Midfielders   | Croatia             |     4 |
| Midfielders   | England             |     1 |
| Midfielders   | France              |     4 |
| Midfielders   | Germany             |     3 |
| Midfielders   | Hungary             |     1 |
| Midfielders   | Iceland             |     3 |
| Midfielders   | Italy               |     1 |
| Midfielders   | Poland              |     2 |
| Midfielders   | Portugal            |     1 |
| Midfielders   | Republic of Ireland |     3 |
| Midfielders   | Russia              |     1 |
| Midfielders   | Slovakia            |     3 |
| Midfielders   | Switzerland         |     1 |
| Midfielders   | Turkey              |     1 |
| Midfielders   | Wales               |     1 |
+---------------+---------------------+-------+
37 rows in set (0.00 sec)