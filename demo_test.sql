-- 根据name进行分组统计年龄之和
-- 单个表ETL
create table student(`id`int primary key, `name`varchar(128), `age`int);
CREATE TABLE spend_report (`id`int primary key, `name`varchar(128), `age`int);

insert into student values(7,'paul',20);
insert into student values(8,'paul',20);
insert into student values(9,'paul',20);
insert into student values(10,'paul',20);
insert into student values(11,'paul',20);

update student set age=120 where id=11;
delete from student where id>5;

SELECT * from student;

-- 分表下的etl
CREATE table student_0 like student;
CREATE table student_1 like student;

insert into student_0 values(21, 'kate', 28);

update student_0 set age=120 where id=11;
delete from student_1 where id = 11;

SELECT * from student_1;

SELECT * from spend_report;

SELECT sum(age) FROM student_1 where name = 'kate';
