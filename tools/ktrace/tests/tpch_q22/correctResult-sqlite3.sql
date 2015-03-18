.separator |

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

drop table if exists CorrectResults;
create table CorrectResults as
select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
from
        (
                select
                        substr(c_phone,1,2) as cntrycode,
                        c_acctbal
                from
                        customer
                where
                        substr(c_phone,1,2) in
                                ('13', '31', '23', '29', '30', '18', '17')
                        and c_acctbal > (
                                select
                                        avg(c_acctbal)
                                from
                                        customer
                                where
                                        c_acctbal > 0.00
                                        and substr(c_phone,1,2) in
                                                ('13', '31', '23', '29', '30', '18', '17')
                        )
                        and not exists (
                                select
                                        *
                                from
                                        orders
                                where
                                        o_custkey = c_custkey
                        )
        ) as custsale
group by
cntrycode;

select * from CorrectResults;
