create table R(a int, b int);

select b, sum(a) from R
where b > 5
group by b
having sum(a) > 20;
