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

-- 1. List the name of the student with id equal to v1 (id).
SELECT name FROM Student WHERE id = @v1;



************************************************
*  Solutions 
************************************************
1. Create an index on student(id) as follows

 create index   student_id on student(id);


2. Run the original query


************************************************
*  Explanation
************************************************
When run the old query,  the explain plan has a full table scan, which is expensive. 

By creating the  index, in the explain plan,  the table scan is replaced by a  Index lookup, which is less costly. 

Here are  explain plans of query running without the index and with the index:
+-------------------------------------------------------------------------------------------------------------------+
| old  EXPLAIN  Plan                                                                                             
+-------------------------------------------------------------------------------------------------------------------+
| -> Filter: (student.id = <cache>((@v1)))  (cost=41.00 rows=40) (actual time=0.185..0.468 rows=1 loops=1)
    -> Table scan on Student  (cost=41.00 rows=400) (actual time=0.112..0.425 rows=400 loops=1)

+-------------------------------------------------------------------------------------------------------------------+
| New  EXPLAIN  Plan                                                                                             
+-------------------------------------------------------------------------------------------------------------------+
| -> Index lookup on Student using stu_id (id=(@v1))  (cost=0.35 rows=1) (actual time=0.030..0.033 rows=1 loops=1)
 