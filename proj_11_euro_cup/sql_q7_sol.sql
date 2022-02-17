

select  distinct sv.*
 from penalty_shootout ps, 
           match_mast mm,
          soccer_venue sv 
where mm.venue_id = sv.venue_id and
	  ps.match_no = mm.match_no ;

------------------ solution --------------------------------------------------
+----------+-------------------------+---------+--------------+
| venue_id | venue_name              | city_id | aud_capacity |
+----------+-------------------------+---------+--------------+
|    20009 | Stade Geoffroy Guichard |   10009 |        42000 |
|    20005 | Stade VElodrome         |   10007 |        64354 |
|    20001 | Stade de Bordeaux       |   10003 |        42115 |