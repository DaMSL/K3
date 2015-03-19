-- Usage: sqlite3 test.db -init tools/ktrace/data/tpch/tests/tpch_q18/correctResult-sqlite3.sql
.separator |

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

drop table if exists customer;
create table customer (
   c_custkey    integer       not null,
   c_name       varchar(25)   not null,
   c_address    varchar(40)   not null,
   c_nationkey  integer       not null,
   c_phone      char(15)      not null,
   c_acctbal    double precision not null,
   c_mktsegment char(10)      not null,
   c_comment    varchar(117)  not null
);

.import tools/ktrace/data/tpch/customer0 customer
.import tools/ktrace/data/tpch/customer1 customer

drop table if exists orders;
create table orders (
	   o_orderkey      integer       not null,
	   o_custkey       integer       not null,
	   o_orderstatus   char(1)       not null,
	   o_totalprice   double precision not null,
	   o_orderdate     text          not null,
	   o_orderpriority char(15)      not null,
	   o_clerk         char(15)      not null,
	   o_shippriority  integer       not null,
	   o_comment       varchar(79)   not null
);

.import tools/ktrace/data/tpch/orders0 orders
.import tools/ktrace/data/tpch/orders1 orders

create table CorrectResults (
  c_name varchar(25) not null,
  c_custkey integer not null,
  o_orderkey integer not null,
  o_orderdate text not null,
  o_totalprice double precision not null,
  total double precision not null
);

INSERT INTO CorrectResults
SELECT
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice,
        sum(l_quantity)
from
        customer,
        orders,
        lineitem
where
        o_orderkey in (
                select
                        l_orderkey
                from
                        lineitem
                group by
                        l_orderkey having
                                sum(l_quantity) > 300
        )
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
group by
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice;

select * from CorrectResults;