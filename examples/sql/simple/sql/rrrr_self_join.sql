create table R(a int, b int);

select R1.b * R4.b from R R1, R R2, R R3, R R4 where R1.a = R2.a and R2.a = R3.a and R3.a = R4.a;
