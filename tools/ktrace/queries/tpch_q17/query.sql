select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
        lineitem L,
        part,
        (select l_partkey, 0.2 * avg(l_quantity) as avg
         from lineitem
         group by l_partkey) T
where
        p_partkey = T.l_partkey
        and L.l_partkey = T.l_partkey
        and p_brand = 'Brand#23'
--xjk#        and p_container = 'MED BOX'
        and l_quantity < avg;
