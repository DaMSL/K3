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

drop table if exists nation;
create table nation (
   n_nationkey integer      not null,
   n_name      char(25)     not null,
   n_regionkey integer      not null,
   n_comment   varchar(152) not null
);


drop table if exists region;
create table region (
   r_regionkey integer      not null,
   r_name      char(25)     not null,
   r_comment   varchar(152) not null
);


drop table if exists partsupp;
create table partsupp (
	   ps_partkey    integer       not null,
	   ps_suppkey    integer       not null,
	   ps_availqty   integer       not null,
	   ps_supplycost double precision  not null,
	   ps_comment    varchar(199)  not null
);

DROP TABLE IF EXISTS rankings;
CREATE TABLE rankings (
  pageURL text,
  pageRank int,
  avgDuration int
);


DROP TABLE IF EXISTS uservisits;
CREATE TABLE uservisits (
  sourceIP text,
  destURL text,
  visitDate text,
  adRevenue double precision,
  userAgent text,
  countryCode text,
  languageCode text,
  searchWord text,
  duration int
);

