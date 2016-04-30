import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

if len(sys.argv) < 2:
    print "Usage: %s <category_name>"
    sys.exit()

print "Querying the database for category %s" %sys.argv[1]

sc = SparkContext("local")
sqlContext = SQLContext(sc)


#read in chemicals

dfcategories = sqlContext.read.parquet("/user/w205/project/dfcategories.parquet")
dfcategories.registerTempTable("categories")

query = "SELECT * from categories where category like '%{0}%' ORDER BY sumToxicity DESC".format(sys.argv[1])
res = sqlContext.sql(query)
res.show(10, False)
