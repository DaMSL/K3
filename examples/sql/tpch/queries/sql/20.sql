select
        s_name,
        s_address
from
        supplier,
        nation
where
        s_suppkey in (
                select
                        ps_suppkey
                from
                        partsupp
                where
                        ps_partkey in (
                                select
                                        p_partkey
                                from
                                        part
                                where
                                        p_name like 'forest%'
                        )
                        and ps_availqty > (
                                select
                                        0.5 * sum(l_quantity)
                                from
                                        lineitem
                                where
                                        l_partkey = ps_partkey
                                        and l_suppkey = ps_suppkey
                                        and l_shipdate >= date '1994-01-01'
                                        and l_shipdate < date '1995-01-01'
                        )
        )
        and s_nationkey = n_nationkey
        and n_name = 'CANADA'
order by
        s_name;

--select
--        s_name,
--        s_address
--from
--        nation,
--        supplier,
--        partsupp,
--        part P,
--        
--        (select l_partkey, l_suppkey, 0.5 * sum(l_quantity) as sum_quantity
--        from lineitem
--        group by l_partkey, l_suppkey
--        where l_shipdate >= '1994-01-01'
--          and l_shipdate < '1995-01-01') as L
--where
--       ps_partkey = P.p_partkey
--  and  ps_partkey = L.l_partkey
--  and  ps_suppkey = L.l_suppkey
--  and  ps_availqty > L.sum_quantity
--  and  s_suppkey = ps_suppkey
--  and  s_nationkey = n_nationkey
--  and  n_name = "CANADA"
--  and  p_partkey like 'forest%'
--
--order by s_name;
