--  Find the 10 customers who spent the highest average number of days waiting for shipments.
-- A customer is waiting between a shipment's ship date and receipt date
-- Output schema: (customer key, customer name, average wait)
-- Order by: average wait DESC

-- Notes
--  1) Use the sqlite DATE(<text>) function to interpret a text field as a date.
--  2) Use subtraction to compute the duration between two dates (e.g., DATE(column1) - DATE(column2)).
--  3) Assume that a package cannot be received before it is shipped.

-- Student SQL code here:

select o.o_custkey, c.c_name, avg(DATE(l.l_receiptdate)-DATE(l.l_shipdate))
from orders o, customer c, lineitem l
where o.o_orderkey = l.l_orderkey and c.c_custkey = o.o_custkey
group by o.o_custkey
order by avg(DATE(l.l_receiptdate)-DATE(l.l_shipdate)) desc
limit 10;
