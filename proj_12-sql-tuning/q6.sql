USE springboardopt;

-- -------------------------------------
SET @v1 = 1612521;
SET @v2 = 1145072;
SET @v3 = 1828467;
SET @v4 = 'MGT382';
SET @v5 = 'Amber Hill';
SET @v6 = 'MGT';
SET @v7 = 'EE';			  
SET @v8 = 'MAT';


SET @v7 = 'EE';			  
SET @v8 = 'CS';


-- 6. List the names of students who have taken all courses offered by department v8 (deptId).
SELECT name FROM Student,
	(SELECT studId
	FROM Transcript
		WHERE crsCode IN
		(SELECT crsCode FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching))
		GROUP BY studId
		HAVING COUNT(*) = 
			(SELECT COUNT(*) FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching))
                     ) as alias
WHERE id = alias.studId;




************************************************
*  Solutions 
************************************************

1. Create indexes on 4 tables


CREATE INDEX  Student_id on Student(id);

CREATE INDEX  Transcript_crs on Transcript(crsCode);

CREATE INDEX    Course_deptId ON Course(deptId);

CREATE INDEX  Teaching_crsCode on Teaching(crsCode);


2.  As two subqueries are the same,  I tried rewriting the query using two  different appraches:

(1)  Using CTE to replace this subquery. New code:

with  Tmp_course  as 
(
   SELECT * FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching)
)
SELECT name FROM Student,
	(SELECT studId
	FROM Transcript
		WHERE crsCode IN
		(SELECT crsCode FROM Tmp_course)
		GROUP BY studId
		HAVING COUNT(*) = 
			(SELECT COUNT(*) FROM Tmp_course)
                     ) as alias
WHERE id = alias.studId;


(2) use a temp table to replace the query.  New code;

.create temp table
create table tmp_q6_course as  SELECT * FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching);

.create index on temp table 
create index  tmp_q6_course_crs on  tmp_q6_course(crscode);





************************************************
*  Explanations
************************************************

1. Without  indexes,  the origial query has 7 table scans in its execution plan. So I decided to create indexes to improve.

As an improvement measure,  4 indexes were created.  As an result, in  execution plan, all table scan on the tables are eliminated.  

2. As there are two subqueries that the same, I rewrote the query by using a CTE  or  temp table to replace this subquery. 
The excution plans generated  for following 3 scarios are quite similar.  The last one seems to have the lowest cost;
 index 
 index +  CTE
 index +temp table

.I noticed on some items, 
the one using CTE has has smaller cost, such as 'Aggregate using temporary table'.  On other items, the one using subquery(original query) has smaller cost.

Perhaps by experimenting with a large amount of data,  we will know which one works best.

--------------------------------------------------------------------------
- Old execution plan
(Note: it has  table scans on tables
----------------------------------------------------------------

| -> Nested loop inner join  (cost=1041.00 rows=0) (actual time=6.186..6.186 rows=0 loops=1)
    -> Filter: (student.id is not null)  (cost=41.00 rows=400) (actual time=0.072..0.340 rows=400 loops=1)
        -> Table scan on Student  (cost=41.00 rows=400) (actual time=0.071..0.304 rows=400 loops=1)
    -> Covering index lookup on alias using <auto_key0> (studId=student.id)  (actual time=0.000..0.000 rows=0 loops=400)
        -> Materialize  (cost=0.00..0.00 rows=0) (actual time=5.778..5.778 rows=0 loops=1)
            -> Filter: (count(0) = (select #5))  (actual time=5.651..5.651 rows=0 loops=1)
                -> Table scan on <temporary>  (actual time=0.000..0.002 rows=19 loops=1)
                    -> Aggregate using temporary table  (actual time=5.646..5.649 rows=19 loops=1)
                        -> Nested loop inner join  (cost=1020.25 rows=10000) (actual time=0.321..0.518 rows=19 loops=1)
                            -> Filter: (transcript.crsCode is not null)  (cost=10.25 rows=100) (actual time=0.013..0.115 rows=100 loops=1)
                                -> Table scan on Transcript  (cost=10.25 rows=100) (actual time=0.013..0.100 rows=100 loops=1)
                            -> Single-row index lookup on <subquery3> using <auto_distinct_key> (crsCode=transcript.crsCode)  (actual time=0.001..0.001 rows=0 loops=100)
                                -> Materialize with deduplication  (cost=120.52..120.52 rows=100) (actual time=0.376..0.380 rows=19 loops=1)
                                    -> Filter: (course.crsCode is not null)  (cost=110.52 rows=100) (actual time=0.148..0.286 rows=19 loops=1)
                                        -> Filter: (teaching.crsCode = course.crsCode)  (cost=110.52 rows=100) (actual time=0.148..0.283 rows=19 loops=1)
                                            -> Inner hash join (<hash>(teaching.crsCode)=<hash>(course.crsCode))  (cost=110.52 rows=100) (actual time=0.147..0.277 rows=19 loops=1)
                                                -> Table scan on Teaching  (cost=0.13 rows=100) (actual time=0.010..0.103 rows=100 loops=1)
                                                -> Hash
                                                    -> Filter: (course.deptId = <cache>(convert((@v8) using utf8mb4)))  (cost=10.25 rows=10) (actual time=0.019..0.112 rows=19 loops=1)
                                                        -> Table scan on Course  (cost=10.25 rows=100) (actual time=0.011..0.090 rows=100 loops=1)
                -> Select #5 (subquery in condition; uncacheable)
                    -> Aggregate: count(0)  (cost=211.25 rows=1000) (actual time=0.261..0.261 rows=1 loops=19)
                        -> Nested loop inner join  (cost=111.25 rows=1000) (actual time=0.137..0.258 rows=19 loops=19)
                            -> Filter: ((course.deptId = <cache>((@v8))) and (course.crsCode is not null))  (cost=10.25 rows=10) (actual time=0.006..0.104 rows=19 loops=19)
                                -> Table scan on Course  (cost=10.25 rows=100) (actual time=0.003..0.077 rows=100 loops=19)
                            -> Single-row index lookup on <subquery6> using <auto_distinct_key> (crsCode=course.crsCode)  (actual time=0.001..0.001 rows=1 loops=361)
                                -> Materialize with deduplication  (cost=20.25..20.25 rows=100) (actual time=0.144..0.148 rows=97 loops=19)
                                    -> Filter: (teaching.crsCode is not null)  (cost=10.25 rows=100) (actual time=0.002..0.086 rows=100 loops=19)
                                        -> Table scan on Teaching  (cost=10.25 rows=100) (actual time=0.002..0.073 rows=100 loops=19)
            -> Select #5 (subquery in projection; uncacheable)
                -> Aggregate: count(0)  (cost=211.25 rows=1000) (actual time=0.261..0.261 rows=1 loops=19)
                    -> Nested loop inner join  (cost=111.25 rows=1000) (actual time=0.137..0.258 rows=19 loops=19)
                        -> Filter: ((course.deptId = <cache>((@v8))) and (course.crsCode is not null))  (cost=10.25 rows=10) (actual time=0.006..0.104 rows=19 loops=19)
                            -> Table scan on Course  (cost=10.25 rows=100) (actual time=0.003..0.077 rows=100 loops=19)
                        -> Single-row index lookup on <subquery6> using <auto_distinct_key> (crsCode=course.crsCode)  (actual time=0.001..0.001 rows=1 loops=361)
                            -> Materialize with deduplication  (cost=20.25..20.25 rows=100) (actual time=0.144..0.148 rows=97 loops=19)
                                -> Filter: (teaching.crsCode is not null)  (cost=10.25 rows=100) (actual time=0.002..0.086 rows=100 loops=19)
                                    -> Table scan on Teaching  (cost=10.25 rows=100) (actual time=0.002..0.073 rows=100 loops=19)
 
----------------------------------------------------------------------
- 2. Execution plan after indexes being created
-----------------------------------------------------
-------------  after index
 -> Nested loop inner join  (cost=3.43 rows=2) (actual time=2.751..2.751 rows=0 loops=1)
    -> Filter: (alias.studId is not null)  (cost=1.36..2.73 rows=2) (actual time=2.749..2.749 rows=0 loops=1)
        -> Table scan on alias  (cost=2.50..2.50 rows=0) (actual time=0.000..0.000 rows=0 loops=1)
            -> Materialize  (cost=2.50..2.50 rows=0) (actual time=2.749..2.749 rows=0 loops=1)
                -> Filter: (count(0) = (select #5))  (actual time=2.740..2.740 rows=0 loops=1)
                    -> Table scan on <temporary>  (actual time=0.000..0.002 rows=23 loops=1)
                        -> Aggregate using temporary table  (actual time=2.732..2.736 rows=23 loops=1)
                            -> Nested loop inner join  (cost=12.80 rows=24) (actual time=0.245..0.372 rows=23 loops=1)
                                -> Filter: (`<subquery3>`.crsCode is not null)  (cost=13.17..4.42 rows=23) (actual time=0.221..0.231 rows=23 loops=1)
                                    -> Table scan on <subquery3>  (cost=0.12..2.79 rows=24) (actual time=0.001..0.005 rows=23 loops=1)
                                        -> Materialize with deduplication  (cost=13.68..16.35 rows=24) (actual time=0.220..0.227 rows=23 loops=1)
                                            -> Filter: (course.crsCode is not null)  (cost=11.19 rows=24) (actual time=0.055..0.200 rows=23 loops=1)
                                                -> Nested loop inner join  (cost=11.19 rows=24) (actual time=0.053..0.196 rows=23 loops=1)
                                                    -> Filter: (course.crsCode is not null)  (cost=3.05 rows=23) (actual time=0.039..0.094 rows=23 loops=1)
                                                        -> Index lookup on Course using Course_deptId (deptId=(@v8)), with index condition: (course.deptId = <cache>(convert((@v8) using utf8mb4)))  (cost=3.05 rows=23) (actual time=0.037..0.090 rows=23 loops=1)
                                                    -> Covering index lookup on Teaching using Teaching_crsCode (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.003..0.004 rows=1 loops=23)
                                -> Index lookup on Transcript using Transcript_crs (crsCode=`<subquery3>`.crsCode)  (cost=6.09 rows=1) (actual time=0.005..0.006 rows=1 loops=23)
                    -> Select #5 (subquery in condition; uncacheable)
                        -> Aggregate: count(0)  (cost=13.56 rows=24) (actual time=0.097..0.097 rows=1 loops=23)
                            -> Nested loop semijoin  (cost=11.19 rows=24) (actual time=0.007..0.094 rows=23 loops=23)
                                -> Filter: (course.crsCode is not null)  (cost=3.05 rows=23) (actual time=0.004..0.042 rows=23 loops=23)
                                    -> Index lookup on Course using Course_deptId (deptId=(@v8))  (cost=3.05 rows=23) (actual time=0.004..0.039 rows=23 loops=23)
                                -> Covering index lookup on Teaching using Teaching_crsCode (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.002..0.002 rows=1 loops=529)
                -> Select #5 (subquery in projection; uncacheable)
                    -> Aggregate: count(0)  (cost=13.56 rows=24) (actual time=0.097..0.097 rows=1 loops=23)
                        -> Nested loop semijoin  (cost=11.19 rows=24) (actual time=0.007..0.094 rows=23 loops=23)
                            -> Filter: (course.crsCode is not null)  (cost=3.05 rows=23) (actual time=0.004..0.042 rows=23 loops=23)
                                -> Index lookup on Course using Course_deptId (deptId=(@v8))  (cost=3.05 rows=23) (actual time=0.004..0.039 rows=23 loops=23)
                            -> Covering index lookup on Teaching using Teaching_crsCode (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.002..0.002 rows=1 loops=529)
    -> Index lookup on Student using Student_id (id=alias.studId)  (cost=0.30 rows=1) (never executed)
 

----------------------------------------------------------------------
- 3. Execution plan for index +  CTE introduced
------------------------------------------------------------------------
 -> Nested loop inner join  (cost=3.43 rows=2) (actual time=0.579..0.579 rows=0 loops=1)
    -> Filter: (alias.studId is not null)  (cost=1.36..2.73 rows=2) (actual time=0.578..0.578 rows=0 loops=1)
        -> Table scan on alias  (cost=2.50..2.50 rows=0) (actual time=0.000..0.000 rows=0 loops=1)
            -> Materialize  (cost=2.50..2.50 rows=0) (actual time=0.578..0.578 rows=0 loops=1)
                -> Filter: (count(0) = (select #6))  (actual time=0.569..0.569 rows=0 loops=1)
                    -> Table scan on <temporary>  (actual time=0.000..0.002 rows=23 loops=1)
                        -> Aggregate using temporary table  (actual time=0.561..0.566 rows=23 loops=1)
                            -> Nested loop inner join  (cost=12.80 rows=24) (actual time=0.180..0.309 rows=23 loops=1)
                                -> Filter: (`<subquery3>`.crsCode is not null)  (cost=18.10..4.42 rows=23) (actual time=0.171..0.180 rows=23 loops=1)
                                    -> Table scan on <subquery3>  (cost=0.12..2.79 rows=24) (actual time=0.000..0.003 rows=23 loops=1)
                                        -> Materialize with deduplication  (cost=18.84..21.51 rows=24) (actual time=0.171..0.176 rows=23 loops=1)
                                            -> Filter: (tmp_course.crsCode is not null)  (cost=13.68..16.35 rows=24) (actual time=0.152..0.160 rows=23 loops=1)
                                                -> Table scan on Tmp_course  (cost=0.12..2.79 rows=24) (actual time=0.002..0.004 rows=23 loops=1)
                                                    -> Materialize CTE tmp_course if needed  (cost=13.68..16.35 rows=24) (actual time=0.151..0.156 rows=23 loops=1)
                                                        -> Nested loop semijoin  (cost=11.19 rows=24) (actual time=0.033..0.127 rows=23 loops=1)
                                                            -> Filter: (course.crsCode is not null)  (cost=3.05 rows=23) (actual time=0.024..0.071 rows=23 loops=1)
                                                                -> Index lookup on Course using Course_deptId (deptId=(@v8))  (cost=3.05 rows=23) (actual time=0.023..0.066 rows=23 loops=1)
                                                            -> Covering index lookup on Teaching using Teaching_crsCode (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.002..0.002 rows=1 loops=23)
                                -> Index lookup on Transcript using Transcript_crs (crsCode=`<subquery3>`.crsCode)  (cost=6.09 rows=1) (actual time=0.004..0.005 rows=1 loops=23)
                    -> Select #6 (subquery in condition; uncacheable)
                        -> Aggregate: count(0)  (cost=13.78..18.72 rows=24) (actual time=0.008..0.008 rows=1 loops=23)
                            -> Table scan on Tmp_course  (cost=0.12..2.79 rows=24) (actual time=0.000..0.003 rows=23 loops=23)
                                -> Materialize CTE tmp_course if needed (query plan printed elsewhere)  (cost=13.68..16.35 rows=24) (never executed)
                -> Select #6 (subquery in projection; uncacheable)
                    -> Aggregate: count(0)  (cost=13.78..18.72 rows=24) (actual time=0.008..0.008 rows=1 loops=23)
                        -> Table scan on Tmp_course  (cost=0.12..2.79 rows=24) (actual time=0.000..0.003 rows=23 loops=23)
                            -> Materialize CTE tmp_course if needed (query plan printed elsewhere)  (cost=13.68..16.35 rows=24) (never executed)
    -> Index lookup on Student using Student_id (id=alias.studId)  (cost=0.30 rows=1) (never executed)
 |



----------------------------------------------------------------------
- 4. Execution plan for index +  temp table
------------------------------------------------------------------------
| -> Nested loop inner join  (cost=3.43 rows=2) (actual time=13.190..13.190 rows=0 loops=1)
    -> Filter: (alias.studId is not null)  (cost=1.36..2.73 rows=2) (actual time=13.185..13.185 rows=0 loops=1)
        -> Table scan on alias  (cost=2.50..2.50 rows=0) (actual time=0.002..0.002 rows=0 loops=1)
            -> Materialize  (cost=2.50..2.50 rows=0) (actual time=13.184..13.184 rows=0 loops=1)
                -> Filter: (count(0) = (select #4))  (actual time=12.954..12.954 rows=0 loops=1)
                    -> Table scan on <temporary>  (actual time=0.004..0.031 rows=23 loops=1)
                        -> Aggregate using temporary table  (actual time=2.061..2.098 rows=23 loops=1)
                            -> Nested loop inner join  (cost=11.62 rows=24) (actual time=1.431..1.944 rows=23 loops=1)
                                -> Remove duplicates from input sorted on tmp_q6_course_crs  (cost=3.24 rows=23) (actual time=0.568..0.777 rows=23 loops=1)
                                    -> Filter: (tmp_q6_course.crsCode is not null)  (cost=3.24 rows=23) (actual time=0.564..0.702 rows=23 loops=1)
                                        -> Covering index scan on tmp_q6_course using tmp_q6_course_crs  (cost=3.24 rows=23) (actual time=0.558..0.686 rows=23 loops=1)
                                -> Index lookup on Transcript using Transcript_crs (crsCode=tmp_q6_course.crsCode)  (cost=6.09 rows=1) (actual time=0.046..0.049 rows=1 loops=23)
                    -> Select #4 (subquery in condition; run only once)
                        -> Count rows in tmp_q6_course  (actual time=10.819..10.819 rows=1 loops=1)
    -> Index lookup on Student using Student_id (id=alias.studId)  (cost=0.30 rows=1) (never executed)