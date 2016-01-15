create table R(a int, b date);

select y, sum(x) from ( select R1.b as y, R2.a as x from R R1, R R2 where R1.a = R2.b ) T group by y;
