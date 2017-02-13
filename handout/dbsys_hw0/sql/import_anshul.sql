-- Populate each of the TPCH tables using the sqlite '.import' meta-command.
-- Students no not need to modify this file.

-- Notes:
--   1) The csv file for <table> is located at '~cs416/datasets/hw0/tpch-sf0.1/<table>.csv'.

.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/part.csv" part
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/supplier.csv" supplier
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/partsupp.csv" partsupp
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/customer.csv" customer
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/orders.csv" orders
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/lineitem.csv" lineitem
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/nation.csv" nation
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/HW0/tpch-sf0.1/region.csv" region
