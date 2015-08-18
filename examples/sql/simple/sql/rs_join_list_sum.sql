create table R(a int, b int);
create table S(c int, d int);

select a, sum(d) from R, S where b = c group by a;
