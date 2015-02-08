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


\copy lineitem from data/tpch/lineitem0 with delimiter '|';
\copy lineitem from data/tpch/lineitem1 with delimiter '|';

DELETE FROM CorrectResults;
INSERT INTO CorrectResults
select
        sum(l_extendedprice * l_discount) as revenue
from
        lineitem
where
        l_shipdate >=  '1994-01-01'
        and l_shipdate < '1995-01-01'
        and l_discount >= 0.05 
        and l_discount <= 0.07
        and l_quantity < 24
