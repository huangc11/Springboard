select rm.referee_name, t2.count
from 
    (select referee_id,  count(*) count, rank() over(order by count(*) desc) ranking
    from
    (
         select referee_id,  player_id
        from player_booked pb,
             match_mast mm
        where pb.match_no = mm.match_no
        group by referee_id,  player_id
    ) t1
    group by referee_id) t2,
    referee_mast rm 
 where t2.referee_id = rm.referee_id
   and t2.ranking =1
;

-----------------  solution----------------------------
+------------------+-------+
| referee_name     | count |
+------------------+-------+
| Mark Clattenburg |    21 |
+------------------+-------+