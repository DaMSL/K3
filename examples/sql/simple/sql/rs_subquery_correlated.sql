create table R(a int, b int);
create table S(c int, d int);

select a from R where a > ( select sum(d) from S where b = c );
