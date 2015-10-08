create table R(a int, b int);
create table S(b int, c int);

select a * c from R join S using ( b );
