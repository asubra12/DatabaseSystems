-- Populate each of the TPCH tables using the sqlite '.import' meta-command.
-- Students no not need to modify this file.

-- Notes:
--   1) The csv file for <table> is located at '~cs416/datasets/hw0/tpch-sf0.1/<table>.csv'.

.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/Code/dbsys-hw2/test/datasets/tpch-tiny/part.csv" part
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/Code/dbsys-hw2/test/datasets/tpch-tiny/supplier.csv" supplier
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/Code/dbsys-hw2/test/datasets/tpch-tiny/partsupp.csv" partsupp
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/Code/dbsys-hw2/test/datasets/tpch-tiny/customer.csv" customer
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/Code/dbsys-hw2/test/datasets/tpch-tiny/orders.csv" orders
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/Code/dbsys-hw2/test/datasets/tpch-tiny/lineitem.csv" lineitem
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/Code/dbsys-hw2/test/datasets/tpch-tiny/nation.csv" nation
.import "/Users/Anshul/Documents/Junior\ Year/Database\ Systems/Code/dbsys-hw2/test/datasets/tpch-tiny/region.csv" region
