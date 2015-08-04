select top 20
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
from
        customer,
        orders,
        lineitem,
        nation
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '1993-10-01'
        and o_orderdate < date '1994-01-01'
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
order by
        revenue desc


stage1 => cno
  (customer equijoin nation)
    DHJ
  (orders.filter valid_orderdate)

stage2 => cnol
  cno
    DHJ
  lineitem
    .filter valid_returnflag
    .groupBy
    (\l -> l.l_orderkey)
    (\a -> \e -> a + (e.l_extendedprice * (1 - e.l_discount)) )
    0.0

stage 3:
 cnol
  DGB

  `


