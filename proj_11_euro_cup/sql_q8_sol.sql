
select  md.match_no, md.team_id, sc.country_name
from
    match_details md,
    soccer_country sc
where
   md.team_id = sc.country_id and md.match_no in
(
    select match_no
    from
    (select p1.match_no,
         p1.count,
         rank() OVER(order by  p1.count desc ) as ranking
    from (select ps.match_no,  count(*) count
            from penalty_shootout ps
            group by ps.match_no) p1
            ) mchno
            where ranking=1
)
;

------- result --------------
+----------+---------+--------------+
| match_no | team_id | country_name |
+----------+---------+--------------+
|       47 |    1208 | Germany      |
|       47 |    1211 | Italy        |
+----------+---------+--------------+
