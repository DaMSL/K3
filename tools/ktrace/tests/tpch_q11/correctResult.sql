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

drop table if exists partsupp;
create table partsupp (
	   ps_partkey    integer       not null,
	   ps_suppkey    integer       not null,
	   ps_availqty   integer       not null,
	   ps_supplycost double precision  not null,
	   ps_comment    varchar(199)  not null
);

\copy partsupp from data/tpch/partsupp0 with delimiter '|';
\copy partsupp from data/tpch/partsupp1 with delimiter '|';

DELETE FROM CorrectResults;
INSERT INTO CorrectResults
select
        ps_partkey,
        sum(ps_supplycost * ps_availqty) as "value"
from
        partsupp,
        supplier,
        nation
where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
group by
        ps_partkey having
                sum(ps_supplycost * ps_availqty) > (
                        select
                                sum(ps_supplycost * ps_availqty) * 0.0001
                        from
                                partsupp,
                                supplier,
                                nation
                        where
                                ps_suppkey = s_suppkey
                                and s_nationkey = n_nationkey
                                and n_name = 'GERMANY'
                );
