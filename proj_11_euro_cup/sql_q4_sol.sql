-- 4. Write a SQL query to compute a list showing the number of substitutions that
happened in various stages of play for the entire tournament.

select mm.play_stage, count(*) 
from player_in_out pio, 
     match_mast mm 
where pio.match_no= mm.match_no 
and in_out ='I'
group by mm.play_stage
;

-----------------  solution----------------------------
+------------+----------+
| play_stage | count(*) |
+------------+----------+
| G          |      208 |
| R          |       45 |
| Q          |       22 |
| S          |       12 |
| F          |        6 |
+------------+----------+
