-- Find the top 10 parts that with the highest quantity in returned orders. 
-- An order is returned if the returnflag field on any lineitem part is the character R.
-- Output schema: (part key, part name, quantity returned)
-- Order by: by quantity returned, descending.

-- Student SQL code here:

select l.l_partkey, p.p_name, sum(l.l_quantity)
from lineitem l, part p
where l_returnflag="R" and l.l_partkey=p.p_partkey
group by l.l_partkey
order by sum(l_quantity) desc
limit 10;
