create table R(a int, b int);

select b, sum(a) from R group by b;
