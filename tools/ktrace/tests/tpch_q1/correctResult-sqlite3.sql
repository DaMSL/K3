drop table if exists lineitem;
create table lineitem (
   l_orderkey      integer       not null,
   l_partkey       integer       not null,
   l_suppkey       integer       not null,
   l_linenumber    integer       not null,
   l_quantity      double precision not null,
   l_extendedprice double precision not null,
   l_discount      double precision not null,
   l_tax           double precision not null,
   l_returnflag    char(1)       not null,
   l_linestatus    char(1)       not null,
   l_shipdate      text          not null,
   l_commitdate    text          not null,
   l_receiptdate   text          not null,
   l_shipinstruct  char(25)      not null,
   l_shipmode      char(10)      not null,
   l_comment       varchar(44)   not null
);


.import tools/ktrace/data/tpch/lineitem0 lineitem
.import tools/ktrace/data/tpch/lineitem1 lineitem


drop table if exists CorrectResults;
create table CorrectResults as

select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        lineitem
where
        l_shipdate <= '1998-09-02'
group by
        l_returnflag,
        l_linestatus;
select * from CorrectResults;
