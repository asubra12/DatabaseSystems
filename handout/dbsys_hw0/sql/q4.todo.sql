-- For each of the top 5 nations with the greatest value (i.e., total price) of orders placed,
-- find the top 5 nations which supply these orders.
-- Output schema: (Order placer name, Order supplier name, value of orders placed)
-- Order by: (Order placer, Order supplier)

-- Notes
--  1) We are expecting exactly 25 results 

-- Student SQL code here:


DROP TABLE IF EXISTS cust_supp;
CREATE TABLE cust_supp (
	cust INTEGER,
	supp INTEGER,
	val REAL
);

INSERT INTO cust_supp (cust, supp, val)
	select n1.n_nationkey as cust, n2.n_nationkey as supp, sum(o.o_totalprice) as val
	FROM nation AS n1
		JOIN customer AS c on c.c_nationkey = n1.n_nationkey
		JOIN orders AS o on o.o_custkey = c.c_custkey
		JOIN lineitem AS l on l.l_orderkey = o.o_orderkey
		JOIN supplier AS s on s.s_suppkey = l.l_suppkey
		JOIN nation as n2 on n2.n_nationkey = s.s_nationkey
	GROUP BY cust, supp;

WITH top_five AS (
	select cust 
	from cust_supp
	group by cust
	order by sum(val) DESC
	limit 5),
	supp_order AS (
	select * from cust_supp
	order by cust asc, val desc)
select top_five.cust, supp_order.supp, supp_order.val
from top_five join supp_order on top_five.cust = supp_order.cust
where supp_order.supp IN (
	select s2.supp from supp_order s2
	where s2.cust = top_five.cust
	limit 5);