create table R(a int, b int, c int);

select b, sum(a), sum(a*c) from R group by b;
