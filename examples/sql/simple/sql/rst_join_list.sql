create table R(a int, b int);
create table S(c int, d int);
create table T(e int, f int);

select a * f from R, S, T where b = c and d = e;
