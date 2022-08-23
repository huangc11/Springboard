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


-- 4. List the names of students who have taken a course taught by professor v5 (name).
SELECT name FROM Student,
	(SELECT studId FROM Transcript,
		(SELECT crsCode, semester FROM Professor
			JOIN Teaching
			WHERE Professor.name = @v5 AND Professor.id = Teaching.profId) as alias1
	WHERE Transcript.crsCode = alias1.crsCode AND Transcript.semester = alias1.semester) as alias2
WHERE Student.id = alias2.studId;

----------------------------------

************************************************
*  Solutions 
************************************************

1. Create indexes on 4 tables 

create  index  Teaching_profid on Teaching(profId);

create index Student_id on Student（id）;

create index professor_name_id on Professor(name, id);

create index  Transcript_crs_sms on Transcript(crsCode, semester);


2.  Create a temp table with an index；  rewrite the query to use the temp table instead of the subquery

drop TABLE tmp_q4_prof;

CREATE TEMPORARY TABLE tmp_q4_prof
SELECT crsCode, semester FROM Professor
			JOIN Teaching
			WHERE Professor.name = @v5 AND Professor.id = Teaching.profId;

CREATE INDEX  tmp_q4_prof_i1 on  tmp_q4_prof(crsCode, semester);

SELECT name FROM Student,
	(SELECT studId FROM Transcript,
		(SELECT crsCode, semester FROM  tmp_q4_prof ) as alias1
	WHERE Transcript.crsCode = alias1.crsCode AND Transcript.semester = alias1.semester) as alias2
WHERE Student.id = alias2.studId;


************************************************
*  Explanation
************************************************

1.  Without indexes, the original query has 4 table scans in its excution plan

2.  By creating 4 indexes on the 4 tables,  the  4 table scans are eliminated from the  excution plan

3.  By using temp table to replace the subquery,  we are able to create index on the temp table, which further reduce the cost. 

Here are  the old/execution plans to run the query before or after our improvement.  We can see, after enhancement, 
all the table scans are eliminlated and the total cost is much lower.

-----------------------------------------------------
- Old Execution  Plan 
--------------------------------
(Note:  it has 4 table scan s)
| -> Inner hash join (student.id = transcript.studId)  (cost=1313.72 rows=160) (actual time=3.537..5.559 rows=1 loops=1)
    -> Table scan on Student  (cost=0.03 rows=400) (actual time=0.013..2.039 rows=400 loops=1)
    -> Hash
        -> Inner hash join (professor.id = teaching.profId)  (cost=1144.90 rows=4) (actual time=2.129..3.216 rows=1 loops=1)
            -> Filter: (professor.`name` = <cache>(convert((@v5) using utf8mb4)))  (cost=0.95 rows=4) (actual time=1.491..2.578 rows=1 loops=1)
                -> Table scan on Professor  (cost=0.95 rows=400) (actual time=1.475..2.511 rows=400 loops=1)
            -> Hash
                -> Filter: ((teaching.semester = transcript.semester) and (teaching.crsCode = transcript.crsCode))  (cost=1010.70 rows=100) (actual time=0.552..0.604 rows=1 loops=1)
                    -> Inner hash join (<hash>(teaching.semester)=<hash>(transcript.semester)), (<hash>(teaching.crsCode)=<hash>(transcript.crsCode))  (cost=1010.70 rows=100) (actual time=0.549..0.600 rows=1 loops=1)
                        -> Table scan on Teaching  (cost=0.01 rows=100) (actual time=0.012..0.058 rows=100 loops=1)
                        -> Hash
                            -> Table scan on Transcript  (cost=10.25 rows=100) (actual time=0.038..0.088 rows=100 loops=1)
 |

-----------------------------------------------------
- New  Execution  Plan 
----------------------------------
Including two parts

-- execution plan for creating the temp table
| -> Nested loop inner join  (cost=1.45 rows=1) (actual time=0.028..0.039 rows=1 loops=1)
    -> Filter: (professor.id is not null)  (cost=1.10 rows=1) (actual time=0.014..0.015 rows=1 loops=1)
        -> Covering index lookup on Professor using professor_name_id (name=(@v5))  (cost=1.10 rows=1) (actual time=0.013..0.014 rows=1 loops=1)
    -> Index lookup on Teaching using Teaching_profid (profId=professor.id)  (cost=0.35 rows=1) (actual time=0.011..0.021 rows=1 loops=1)
 |


-- Execution  plan for running the query 
| -> Nested loop inner join  (cost=1.05 rows=1) (actual time=0.089..0.096 rows=1 loops=1)
    -> Nested loop inner join  (cost=0.70 rows=1) (actual time=0.076..0.079 rows=1 loops=1)
        -> Filter: ((tmp_q4_prof.crsCode is not null) and (tmp_q4_prof.semester is not null))  (cost=0.35 rows=1) (actual time=0.046..0.048 rows=1 loops=1)
            -> Covering index scan on tmp_q4_prof using tmp_q4_prof_i1  (cost=0.35 rows=1) (actual time=0.044..0.045 rows=1 loops=1)
        -> Filter: (transcript.studId is not null)  (cost=0.35 rows=1) (actual time=0.028..0.029 rows=1 loops=1)
            -> Index lookup on Transcript using Transcript_crs_sms (crsCode=tmp_q4_prof.crsCode, semester=tmp_q4_prof.semester)  (cost=0.35 rows=1) (actual time=0.028..0.029 rows=1 loops=1)
    -> Index lookup on Student using Student_id (id=transcript.studId)  (cost=0.35 rows=1) (actual time=0.012..0.016 rows=1 loops=1)
 |