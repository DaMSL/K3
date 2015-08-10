create table R(a int, b int);
create table S(c int, d int);

select a * d from R join S on ( b < c );
