create table R(a int, b int);

select a * b from R where b in (select a from R);
