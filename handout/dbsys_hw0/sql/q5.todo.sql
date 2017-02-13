-- Find the customer market segments where the yearly total number of orders declines 
-- in the last 2 years in the dataset. Note the database will have different date 
-- ranges per market segment, for example segment A records between 1990-1995, and 
-- segment B between 1992-1998. That is for segment A, we want the difference between 
-- 1995 and 1994.
-- Output schema: (market segment, last year for segment, difference in # orders)
-- Order by: market segment ASC 

-- Notes
--  1) Use the sqlite function strftime('%Y', <text>) to extract the year from a text field representing a date.
--  2) Use CAST(<text> as INTEGER) to convert text (e.g., a year) into an INTEGER.
--  3) You may use a SQL 'WITH' clause.

-- Student SQL code here:

with mkt_order as (
	select c_mktsegment, CAST(strftime('%Y', o_orderdate) as INTEGER) as yr, count(*) as count
	from customer, orders
	where o_custkey = c_custkey
	group by c_mktsegment, yr
	order by c_mktsegment asc, yr desc)
select m1.c_mktsegment, m1.yr, (m1.count - m2.count)
from mkt_order m1 join mkt_order m2 on (m1.yr-1)=m2.yr
where m1.c_mktsegment = m2.c_mktsegment and m1.count < m2.count
group by m1.c_mktsegment
having max(m1.yr)
order by m1.c_mktsegment asc;
