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

-- 5. List the names of students who have taken a course from department v6 (deptId), but not v7.
SELECT distinct * FROM Student, 
	(SELECT studId FROM Transcript, Course WHERE deptId = @v6 AND Course.crsCode = Transcript.crsCode
	AND studId NOT IN
	(SELECT studId FROM Transcript, Course WHERE deptId = @v7 AND Course.crsCode = Transcript.crsCode)) as alias
WHERE Student.id = alias.studId;


************************************************
*  Solutions 
************************************************
1.  create 3 indexes on 3 tables

create index Student_id on Student(id);
create index Course_deptid on Course(deptid);
create index transcript_crs on transcript(crsCode);

2.
select s1.*
from student s1, 
(
select a1.studid
from (SELECT studId FROM Transcript, Course WHERE deptId = @v6 AND Course.crsCode = Transcript.crsCode) a1
left join (SELECT studId FROM Transcript, Course WHERE deptId = @v7 AND Course.crsCode = Transcript.crsCode) a2
on a1.studid= a2.studid
where a2.studid is  NULL
) b1
where s1.id= b1.studid


************************************************
*  Explanations
************************************************

----------------------------------------------------
- Execution plan for oringal query when no indexes being created
----------------------------------------------------
(Note:  5 table scans)

| -> Filter: <i
n_optimizer>(transcript.studId,<exists>(select #3) is false)  (cost=4112.69 rows=4000) (actual time=0.430..5.133 rows=30 loops=1)
    -> Inner hash join (student.id = transcript.studId)  (cost=4112.69 rows=4000) (actual time=0.217..0.471 rows=30 loops=1)
        -> Table scan on Student  (cost=0.06 rows=400) (actual time=0.005..0.214 rows=400 loops=1)
        -> Hash
            -> Filter: (transcript.crsCode = course.crsCode)  (cost=110.52 rows=100) (actual time=0.119..0.195 rows=30 loops=1)
                -> Inner hash join (<hash>(transcript.crsCode)=<hash>(course.crsCode))  (cost=110.52 rows=100) (actual time=0.118..0.189 rows=30 loops=1)
                    -> Table scan on Transcript  (cost=0.13 rows=100) (actual time=0.005..0.055 rows=100 loops=1)
                    -> Hash
                        -> Filter: (course.deptId = <cache>(convert((@v6) using utf8mb4)))  (cost=10.25 rows=10) (actual time=0.025..0.094 rows=26 loops=1)
                            -> Table scan on Course  (cost=10.25 rows=100) (actual time=0.022..0.076 rows=100 loops=1)
    -> Select #3 (subquery in condition; dependent)
        -> Limit: 1 row(s)  (cost=110.52 rows=1) (actual time=0.153..0.153 rows=0 loops=30)
            -> Filter: <if>(outer_field_is_not_null, <is_not_null_test>(transcript.studId), true)  (cost=110.52 rows=100) (actual time=0.153..0.153 rows=0 loops=30)
                -> Filter: (<if>(outer_field_is_not_null, ((<cache>(transcript.studId) = transcript.studId) or (transcript.studId is null)), true) and (transcript.crsCode = course.crsCode))  (cost=110.52 rows=100) (actual time=0.153..0.153 rows=0 loops=30)
                    -> Inner hash join (<hash>(transcript.crsCode)=<hash>(course.crsCode))  (cost=110.52 rows=100) (actual time=0.081..0.149 rows=34 loops=30)
                        -> Table scan on Transcript  (cost=0.13 rows=100) (actual time=0.002..0.052 rows=100 loops=30)
                        -> Hash
                            -> Filter: (course.deptId = <cache>(convert((@v7) using utf8mb4)))  (cost=10.25 rows=10) (actual time=0.005..0.067 rows=32 loops=30)
                                -> Table scan on Course  (cost=10.25 rows=100) (actual time=0.002..0.054 rows=100 loops=30)
 
----------------------------------------------------
-  Execution plan after index being created
----------------------------------------------------
| -> Table scan on <temporary>  (cost=0.10..2.84 rows=27) (actual time=0.007..0.010 rows=26 loops=1)
    -> Temporary table with deduplication  (cost=25.12..27.85 rows=27) (actual time=12.544..12.549 rows=26 loops=1)
        -> Nested loop inner join  (cost=22.31 rows=27) (actual time=5.043..12.331 rows=30 loops=1)
            -> Nested loop inner join  (cost=12.83 rows=27) (actual time=2.549..2.748 rows=30 loops=1)
                -> Filter: (course.crsCode is not null)  (cost=3.35 rows=26) (actual time=0.530..0.590 rows=26 loops=1)
                    -> Index lookup on Course using Course_deptid (deptId=(@v6)), with index condition: (course.deptId = <cache>(convert((@v6) using utf8mb4)))  (cost=3.35 rows=26) (actual time=0.528..0.583 rows=26 loops=1)
                -> Filter: (transcript.studId is not null)  (cost=0.26 rows=1) (actual time=0.081..0.083 rows=1 loops=26)
                    -> Index lookup on Transcript using transcript_crs (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.081..0.082 rows=1 loops=26)
            -> Filter: <in_optimizer>(transcript.studId,<exists>(select #3) is false)  (cost=0.25 rows=1) (actual time=0.318..0.319 rows=1 loops=30)
                -> Index lookup on Student using Student_id (id=transcript.studId)  (cost=0.25 rows=1) (actual time=0.107..0.108 rows=1 loops=30)
                -> Select #3 (subquery in condition; dependent)
                    -> Limit: 1 row(s)  (cost=15.62 rows=1) (actual time=0.209..0.209 rows=0 loops=30)
                        -> Filter: <if>(outer_field_is_not_null, <is_not_null_test>(transcript.studId), true)  (cost=15.62 rows=33) (actual time=0.208..0.208 rows=0 loops=30)
                            -> Nested loop inner join  (cost=15.62 rows=33) (actual time=0.208..0.208 rows=0 loops=30)
                                -> Filter: (course.crsCode is not null)  (cost=3.95 rows=32) (actual time=0.005..0.062 rows=32 loops=30)
                                    -> Index lookup on Course using Course_deptid (deptId=(@v7)), with index condition: (course.deptId = <cache>(convert((@v7) using utf8mb4)))  (cost=3.95 rows=32) (actual time=0.005..0.057 rows=32 loops=30)
                                -> Filter: <if>(outer_field_is_not_null, ((<cache>(transcript.studId) = transcript.studId) or (transcript.studId is null)), true)  (cost=0.26 rows=1) (actual time=0.004..0.004 rows=0 loops=960)
                                    -> Index lookup on Transcript using transcript_crs (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.003..0.004 rows=1 loops=960)
 
---------------
-?
---------------
| -> Filter: (transcript.studId is null)  (cost=45.44 rows=28) (actual time=0.311..6.348 rows=30 loops=1)
    -> Nested loop left join  (cost=45.44 rows=28) (actual time=0.309..6.341 rows=30 loops=1)
        -> Nested loop inner join  (cost=22.31 rows=27) (actual time=0.045..0.337 rows=30 loops=1)
            -> Nested loop inner join  (cost=12.83 rows=27) (actual time=0.037..0.221 rows=30 loops=1)
                -> Filter: (course.crsCode is not null)  (cost=3.35 rows=26) (actual time=0.025..0.079 rows=26 loops=1)
                    -> Index lookup on Course using Course_deptid (deptId=(@v6)), with index condition: (course.deptId = <cache>(convert((@v6) using utf8mb4)))  (cost=3.35 rows=26) (actual time=0.024..0.072 rows=26 loops=1)
                -> Filter: (transcript.studId is not null)  (cost=0.26 rows=1) (actual time=0.004..0.005 rows=1 loops=26)
                    -> Index lookup on Transcript using transcript_crs (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.004..0.005 rows=1 loops=26)
            -> Index lookup on s1 using Student_id (id=transcript.studId)  (cost=0.25 rows=1) (actual time=0.003..0.004 rows=1 loops=30)
        -> Nested loop inner join  (cost=72.36 rows=1) (actual time=0.200..0.200 rows=0 loops=30)
            -> Filter: (course.deptId = <cache>(convert((@v7) using utf8mb4)))  (cost=0.79 rows=10) (actual time=0.003..0.058 rows=32 loops=30)
                -> Index lookup on Course using Course_deptid (deptId=(@v7))  (cost=0.79 rows=10) (actual time=0.003..0.050 rows=32 loops=30)
            -> Filter: (transcript.studId = transcript.studId)  (cost=0.26 rows=0) (actual time=0.004..0.004 rows=0 loops=960)
                -> Index lookup on Transcript using transcript_crs (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.003..0.004 rows=1 loops=960)
 |