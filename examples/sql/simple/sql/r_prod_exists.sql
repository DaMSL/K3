create table R(a int, b int);

select a * b from R where exists (select a from R where a > 10);
