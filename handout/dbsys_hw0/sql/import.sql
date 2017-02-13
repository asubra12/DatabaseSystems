-- Populate each of the TPCH tables using the sqlite '.import' meta-command.
-- Students no not need to modify this file.

-- Notes:
--   1) The csv file for <table> is located at '~cs416/datasets/hw0/tpch-sf0.1/<table>.csv'.

.import ~/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/part.csv part
.import ~/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/supplier.csv supplier
.import ~/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/partsupp.csv partsupp
.import ~/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/customer.csv customer
.import ~/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/orders.csv orders
.import ~/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/lineitem.csv lineitem
.import ~/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/nation.csv nation
.import ~/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/region.csv region
