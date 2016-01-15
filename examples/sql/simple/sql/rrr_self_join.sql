create table R(a int, b int);

select R1.b * R3.a from R R1, R R2, R R3 where R1.a = R2.a and R2.b = R3.b;
