create table R(a int, b int);

select a * b from R where b > (select sum(a) from R);
