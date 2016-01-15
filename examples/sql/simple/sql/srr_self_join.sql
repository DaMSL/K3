create table R(a int, b int);
create table S(c int, d int);

select R1.b * S.d from S, R R1, R R2 where R1.a = R2.b and R2.b = S.c;
