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

-- 3. List the names of students who have taken course v4 (crsCode).
SELECT name FROM Student WHERE id IN (SELECT studId FROM Transcript WHERE crsCode = @v4);


************************************************
*  Solutions 
************************************************

1.  Create index on table Transcript and student

 create index Transcript_crs on Transcript(crscode);
 create index Student_id on Student (id);

2.  Rewrite the query , such that 'where id in (...)'  is replaced by  table Student  joining  to a subquery

SELECT Student.name FROM Student , (SELECT studId FROM Transcript WHERE crsCode = @v4) a1
where Student.id= a1.studid



************************************************
*  Explanation
************************************************

The  original explain plan has 3 table scans, one for table student, one for two transcript  and one for subquery.

By creating indexes, we eliminated the two table scans on table student and transcript. 

By rewriting  the query,  we eliminate the table scan on the subquery. 

Here are the  execution plans of the query before and after our enhancement.  For the 
new execution plan,  we can see all the table scans are eliminated.


---------------------
| old execution plan 
--------------  
-> Inner hash join (student.id = `<subquery2>`.studId)  (cost=414.91 rows=400) (actual time=0.226..0.573 rows=2 loops=1)
    -> Table scan on Student  (cost=5.04 rows=400) (actual time=0.012..0.324 rows=400 loops=1)
    -> Hash
        -> Table scan on <subquery2>  (cost=0.26..2.62 rows=10) (actual time=0.001..0.001 rows=2 loops=1)
            -> Materialize with deduplication  (cost=11.51..13.88 rows=10) (actual time=0.141..0.142 rows=2 loops=1)
                -> Filter: (transcript.studId is not null)  (cost=10.25 rows=10) (actual time=0.062..0.133 rows=2 loops=1)
                    -> Filter: (transcript.crsCode = <cache>(convert((@v4) using utf8mb4)))  (cost=10.25 rows=10) (actual time=0.061..0.132 rows=2 loops=1)
                        -> Table scan on Transcript  (cost=10.25 rows=100) (actual time=0.030..0.111 rows=100 loops=1)
 
---------------------
| new execution plan 
--------------  
| -> Nested loop inner join  (cost=1.40 rows=2) (actual time=0.037..0.049 rows=2 loops=1)
    -> Filter: (transcript.studId is not null)  (cost=0.70 rows=2) (actual time=0.026..0.030 rows=2 loops=1)
        -> Index lookup on Transcript using Transcript_crs (crsCode=(@v4)), with index condition: (transcript.crsCode = <cache>(convert((@v4) using utf8mb4)))  (cost=0.70 rows=2) (actual time=0.024..0.028 rows=2 loops=1)
    -> Index lookup on Student using Student_id (id=transcript.studId)  (cost=0.30 rows=1) (actual time=0.006..0.008 rows=1 loops=2)
 |





