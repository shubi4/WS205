import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

if len(sys.argv) < 2:
    print "Usage: %s <product_name>"
    sys.exit()

print "Querying the database for product %s" %sys.argv[1]    

sc = SparkContext("local")
sqlContext = SQLContext(sc)


#read in product summary

dfproductsummary = sqlContext.read.parquet("/user/w205/project/dfproductsummary.parquet")
dfproductsummary.registerTempTable("productsummary")

query = "SELECT * from productsummary where ProductName like '%{0}%' ORDER BY sumToxicity DESC".format(sys.argv[1])
res = sqlContext.sql(query)
res.show(10, False)

