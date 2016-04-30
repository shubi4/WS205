#spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 sample_queries.py
#pyspark --packages com.databricks:spark-csv_2.10:1.2.0

import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext("local")
sqlContext = SQLContext(sc)


#read in chemicals

dfchemicals = sqlContext.read.parquet("/user/w205/project/dfchemicals.parquet")
dfchemicals.registerTempTable("chemicals")

dfchemcat = sqlContext.read.parquet("/user/w205/project/dfchemcat.parquet")
dfchemcat.registerTempTable("chemcat")

dfproducts = sqlContext.read.parquet("/user/w205/project/dfproducts.parquet")
dfproducts.registerTempTable("products")

dfcategories = sqlContext.read.parquet("/user/w205/project/dfcategories.parquet")
dfcategories.registerTempTable("categories")

dfproductsummary = sqlContext.read.parquet("/user/w205/project/dfproductsummary.parquet")
dfproductsummary.registerTempTable("productsummary")


#for the following queries, note: isToxic is a meta-cateogry indicating if the chemical is any kind of toxin (health or environment)

print "Top 10 cancer-causing chemicals ordered by number of categories they appear in"
res = sqlContext.sql("Select CASRN, Name, numCategories, numProducts, ToxicityScore from chemicals where isCancer= 1 ORDER BY numCategories DESC")
res.show(10, False)

print "top 10 reproductive toxins ordered by number of products they appear in"
res = sqlContext.sql("Select CASRN, Name, numCategories, numProducts, ToxicityScore from chemicals where isrepToxin= 1 ORDER BY numProducts DESC")
res.show(10, False)

print "top 10 chemicals ordered by toxicity score (most toxic first)"
res = sqlContext.sql("Select CASRN, Name, numCategories, numProducts, ToxicityScore from chemicals ORDER BY ToxicityScore DESC, numCategories DESC, numProducts DESC")
res.show(10, False)

print "top 10 categories containing the most number of  greenhouse gases"
res = sqlContext.sql("Select category, numEnvGrnGas from categories ORDER BY numEnvGrnGas DESC")
res.show(10, False)

print "top 10 categories containing the most number of safe chemicals"
res = sqlContext.sql("Select category, numSafe from categories  where numToxic = 0 ORDER BY numSafe DESC")
res.show(10, False)

print "top 10 products containing the most number of chemicals"
res = sqlContext.sql("Select ProductName, numChemicals, numToxic, numSafe, numUnknown from productsummary  ORDER BY numChemicals DESC, numToxic DESC")
res.show(10, False)

print "top 10 products by sumToxicity (sum of toxicities of its consituent chemicals)"
res = sqlContext.sql("Select ProductName, sumToxicity, numChemicals, numToxic, numSafe, numUnknown from productsummary  ORDER BY sumToxicity DESC")
res.show(10, False)

print "top 10 products by least sumToxicity (sum of toxicities of its consituent chemicals)"
res = sqlContext.sql("Select ProductName, sumToxicity, numChemicals, numToxic, numSafe, numUnknown from productsummary  ORDER BY sumToxicity ASC")
res.show(10, False)
