create table R(a int, b int);

select Y.d * Y.c from (select a as c, b as d from R) as Y;
