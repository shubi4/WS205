#spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 cpcat_ELT2.py

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext("local")
sqlContext = SQLContext(sc)

#create a product summary

print "Processing product summaries"

dfchemicals = sqlContext.read.parquet("/user/w205/project/dfchemicals.parquet")

dfproducts = sqlContext.read.parquet("/user/w205/project/dfproducts.parquet")


dfproductsummary = dfproducts.join(dfchemicals, on="CASRN")
dfproductsummary = dfproductsummary.groupBy('ProductName').agg({'Name':'count', 'isSafe':'sum', 'isToxic':'sum', 'isUnknown':'sum', 'ToxicityScore':'sum',
                 'isCancer':'sum', 'isDevToxin':'sum', 'isrepToxin':'sum', 
                 'isEnvBioacc':'sum', 'isEnvDanger':'sum', 'isEnvGrnGas':'sum', 'isEnvOzone':'sum', 'isEnvPersist':'sum'})
dfproductsummary = dfproductsummary.withColumnRenamed('count(Name)', 'numChemicals')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(isToxic)', 'numToxic')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(isCancer)', 'numCancer')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(isEnvOzone)', 'numEnvOzone')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(isSafe)', 'numSafe')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(isDevToxin)', 'numDevToxin')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(isUnknown)', 'numUnknown')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(isEnvDanger)', 'numEnvDanger')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(isEnvBioacc)', 'numEnvBioacc')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(isrepToxin)', 'numRepToxin')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(isEnvGrnGas)', 'numEnvGrnGas')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(isEnvPersist)', 'numEnvPersist')
dfproductsummary = dfproductsummary.withColumnRenamed('sum(ToxicityScore)', 'sumToxicity')

print "saving product summaries"
dfproductsummary.coalesce(1).write.mode('overwrite').parquet("/user/w205/project/dfproductsummary.parquet")
