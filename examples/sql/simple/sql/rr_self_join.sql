create table R(a int, b date);

select R1.b * R2.a from R R1, R R2 where R1.a = R2.b;
