-- 14. Write a SQL query to find referees and the number of bookings they made for the
entire tournament. Sort your answer by the number of bookings in descending order.

select rm.referee_name, count(*) count 
from  player_booked pb,  
      match_mast mm,
      referee_mast rm 
where pb.match_no = mm.match_no and 
      mm.referee_id - rm.referee_id 
group by rm.referee_name  
order by count(*) desc
;

-----------------  solution----------------------------
+-------------------------+-------+
| referee_name            | count |
+-------------------------+-------+
| Clement Turpin          |   198 |
| William Collum          |   193 |
| Svein Oddvar Moen       |   193 |
| Martin Atkinson         |   192 |
| Felix Brych             |   192 |
| Ovidiu Hategan          |   192 |
| Szymon Marciniak        |   191 |
| Carlos Velasco Carballo |   191 |
| Cuneyt Cakir            |   190 |
| Jonas Eriksson          |   190 |
| Pavel Kralovec          |   190 |
| Damir Skomina           |   189 |
| Bjorn Kuipers           |   189 |
| Sergei Karasev          |   189 |
| Viktor Kassai           |   189 |
| Milorad Mazic           |   188 |
| Nicola Rizzoli          |   181 |
| Mark Clattenburg        |   180 |
+-------------------------+-------+
18 rows in set (0.01 sec)