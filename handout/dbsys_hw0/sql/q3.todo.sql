-- Find the name of the most heavily ordered (i.e., highest quantity) part per nation.
-- Output schema: (nation key, nation name, part key, part name, quantity ordered)
-- Order by: (nation key, part key) SC

-- Notes
--   1) You may use a SQL 'WITH' clause for common table expressions.
--   2) A single nation may have more than 1 most-heavily-ordered-part.

-- Student SQL code here:

with temp as (
	select c.c_nationkey as nkey, n.n_name as nname, p.p_name as pname, sum(l.l_quantity) as partsum
	from customer c, nation n, part p, lineitem l, orders o
	where n.n_nationkey = c.c_nationkey and c.c_custkey = o.o_custkey and o.o_orderkey = l.l_orderkey and l.l_partkey = p.p_partkey
	group by nkey, pname
	limit 50)
select nkey, nname, pname, partsum
from (
	select nkey, nname, pname, partsum, max(partsum) as maxsum
	from temp
	group by nkey
) r
where partsum = r.maxsum
group by nkey;
--having partsum = max(partsum);

