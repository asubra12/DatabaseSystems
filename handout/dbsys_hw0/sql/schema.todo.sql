-- This script creates each of the TPCH tables using the SQL 'create table' command.
drop table if exists part;
drop table if exists supplier;
drop table if exists partsupp;
drop table if exists customer;
drop table if exists orders;
drop table if exists lineitem;
drop table if exists nation;
drop table if exists region;

-- Notes:
--   1) Use all lowercase letters for table and column identifiers.
--   2) Use only INTEGER/REAL/TEXT datatypes. Use TEXT for dates.
--   3) Do not specify any integrity contraints (e.g., PRIMARY KEY, FOREIGN KEY).

-- Students should fill in the followins statements:

create table part (
	p_partkey INTEGER,
	p_name TEXT,
	p_mfgr TEXT,
	p_brand TEXT,
	p_type TEXT,
	p_size INTEGER,
	p_container TEXT,
	p_retailprice REAL,
	p_comment TEXT

);

create table supplier (
	s_suppkey INTEGER,
	s_name TEXT,
	s_address TEXT,
	s_nationkey TEXT,
	s_phone TEXT,
	s_acctbal REAL,
	s_comment TEXT
);

create table partsupp (
	ps_partkey INTEGER,
	ps_suppkey INTEGER,
	ps_availqty INTEGER,
	ps_supplycost REAL,
	ps_comment TEXT

);

create table customer (
	c_custkey INTEGER,
	c_name TEXT,
	c_address TEXT,
	c_nationkey INTEGER,
	c_phone TEXT,
	c_acctbal REAL,
	c_mktsegment TEXT,
	c_comment TEXT

);

create table orders (
	o_orderkey INTEGER,
	o_custkey INTEGER,
	o_orderstatus TEXT,
	o_totalprice REAL,
	o_orderdate TEXT,
	o_orderpriority TEXT,
	o_clerk TEXT,
	o_shippriority INTEGER,
	o_comment TEXT

);

create table lineitem (
	l_orderkey INTEGER,
	l_partkey INTEGER,
	l_suppkey INTEGER,
	l_linenumber INTEGER,
	l_quantity REAL,
	l_extendedprice REAL,
	l_discount REAL,
	l_tax REAL,
	l_returnflag TEXT,
	l_linestatus REAL,
	l_shipdate TEXT,
	l_commitdate TEXT,
	l_receiptdate TEXT,
	l_shipinstruct TEXT,
	l_shipmode TEXT,
	l_comment TEXT

);

create table nation (
	n_nationkey INTEGER,
	n_name TEXT,
	n_regionkey INTEGER,
	n_comment TEXT

);

create table region (
	r_regionkey INTEGER,
	r_name TEXT,
	r_comment TEXT

);
