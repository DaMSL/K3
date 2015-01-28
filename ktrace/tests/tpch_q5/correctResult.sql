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

\copy customer from data/tpch/customer0 with delimiter '|';
\copy customer from data/tpch/customer1 with delimiter '|';

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

\copy orders from data/tpch/orders0 with delimiter '|';
\copy orders from data/tpch/orders1 with delimiter '|';

drop table if exists supplier;
create table supplier (
   s_suppkey   integer       not null,
   s_name      char(25)      not null,
   s_address   varchar(40)   not null,
   s_nationkey integer       not null,
   s_phone     char(15)      not null,
   s_acctbal   double precision not null,
   s_comment   varchar(101)  not null
);

\copy supplier from data/tpch/supplier0 with delimiter '|';
\copy supplier from data/tpch/supplier1 with delimiter '|';

drop table if exists nation;
create table nation (
   n_nationkey integer      not null,
   n_name      char(25)     not null,
   n_regionkey integer      not null,
   n_comment   varchar(152) not null
);

\copy nation from data/tpch/nation.tbl with delimiter '|';

drop table if exists region;
create table region (
   r_regionkey integer      not null,
   r_name      char(25)     not null,
   r_comment   varchar(152) not null
);

\copy region from data/tpch/region.tbl with delimiter '|';

DELETE FROM CorrectResults;
INSERT INTO CorrectResults

select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= '1994-01-01'
        and o_orderdate <  '1995-01-01'
group by
        n_name;
