import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

if len(sys.argv) < 2:
    print "Usage: %s <chemical_name>"
    sys.exit()

print "Querying the database for chemical %s" %sys.argv[1]    

sc = SparkContext("local")
sqlContext = SQLContext(sc)


#read in chemicals

dfchemicals = sqlContext.read.parquet("/user/w205/project/dfchemicals.parquet")
dfchemicals.registerTempTable("chemicals")

query = "SELECT * from chemicals where Name like '%{0}%' ORDER BY ToxicityScore DESC, numProducts DESC".format(sys.argv[1])
res = sqlContext.sql(query)
res.show(10, False)

