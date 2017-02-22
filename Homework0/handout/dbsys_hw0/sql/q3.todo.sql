-- Find the name of the most heavily ordered (i.e., highest quantity) part per nation.
-- Output schema: (nation key, nation name, part key, part name, quantity ordered)
-- Order by: (nation key, part key) SC

-- Notes
--   1) You may use a SQL 'WITH' clause for common table expressions.
--   2) A single nation may have more than 1 most-heavily-ordered-part.

-- Student SQL code here:

with temp as (
	select c.c_nationkey as nkey, n.n_name as nname, p.p_partkey as pkey, p.p_name as pname, sum(l.l_quantity) as partsum
	from customer c, nation n, part p, lineitem l, orders o
	where n.n_nationkey = c.c_nationkey and c.c_custkey = o.o_custkey and o.o_orderkey = l.l_orderkey and l.l_partkey = p.p_partkey
	group by nkey, pname),
temp2 as (
	select t1.nkey as nkey, max(t1.partsum) as partsum
	from temp t1
	group by t1.nkey)

select t1.nkey, t1.nname, t1.pkey, t1.pname, t1.partsum
from temp t1 join temp2 t2 on t1.nkey = t2.nkey
where t1.partsum = t2.partsum
order by t1.nkey asc;
