create table R(a int, b int);
create table S(c int, d int);
create table T(e int, f int);
create table U(g int, h int);

select a * h
  from
    (select a, d from R, S where b = c),
    (select e, h from T, U where f = g)
  where
    d = e;
