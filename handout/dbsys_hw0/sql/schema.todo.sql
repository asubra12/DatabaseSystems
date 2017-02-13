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

);

create table customer (

);

create table orders (

);

create table lineitem (

);

create table nation (

);

create table region (

);
