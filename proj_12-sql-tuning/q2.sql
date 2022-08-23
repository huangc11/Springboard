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

-- 2. List the names of students with id in the range of v2 (id) to v3 (inclusive).
SELECT name FROM Student WHERE id BETWEEN @v2 AND @v3;




************************************************
*  Solutions 
************************************************

Create  index  as follows and run the original query:

create index student_name_id on student(name, id);


************************************************
*  Explanation
************************************************

The  original execution plan of the query  has 1 table scan

By creating the index, we eliminated the  table scans on table student in the execution plan.


-----------------------------
| old execution plan 
------------------------------------------------------------------------
| -> Filter: (student.id between <cache>((@v2)) and <cache>((@v3)))  (cost=5.44 rows=44) (actual time=0.042..0.390 rows=278 loops=1)
    -> Table scan on Student  (cost=5.44 rows=400) (actual time=0.039..0.329 rows=400 loops=1)

---------------------
| New execution plan 
----------------------------------------------
| -> Filter: (student.id between <cache>((@v2)) and <cache>((@v3)))  (cost=5.44 rows=44) (actual time=0.025..0.387 rows=278 loops=1)
    -> Covering index scan on Student using student_name_id  (cost=5.44 rows=400) (actual time=0.023..0.327 rows=400 loops=1)
