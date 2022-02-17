select t1.country_name, t1.count
from
(
    select t1.country_id, t2.country_name, count(*) count, rank() over(order by count(*) desc) ranking
    from  asst_referee_mast t1,
          soccer_country t2
    where t1.country_id = t2.country_id
    group by t1.country_id, t2.country_name
 ) t1
 where  t1.ranking=1
;

-----------------------  solution ---------------------
+--------------+-------+
| country_name | count |
+--------------+-------+
| England      |     4 |
+--------------+-------+
