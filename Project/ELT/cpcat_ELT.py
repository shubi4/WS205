#pyspark --packages com.databricks:spark-csv_2.10:1.2.0
#spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 cpcat_ETL.py

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext("local", "weblog app")
sqlContext = SQLContext(sc)

######### LOADING the data, with headers ####################

####CPCAT chemicals list

print "Reading in chemicals"

#read from CSV
dfchemicals = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/chemicals.txt',
                           format='com.databricks.spark.csv',
                           delimiter='\t',
                           header='true',
                           inferSchema='true')

#remove spaces from CASRN column                           
dfchemicals = dfchemicals.withColumn('CASRN', ltrim(rtrim(dfchemicals.CASRN)))
dfchemicals = dfchemicals.withColumn('Name', lower(dfchemicals.Name))
#drop the code column
dfchemicals = dfchemicals.drop('CODE')

dfchemicals = dfchemicals.distinct()

print "Finished reading chemicals"


############ #CPCAT chemical categories

print "Reading in chemical categories..."

dfchemcat = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/cpcat_chemicals.txt',
                           format='com.databricks.spark.csv',
                           delimiter='\t',
                           header='true',
                           inferSchema='true')

#drop unwanted columns
dfchemcat = dfchemcat.select('CASRN', 'Description_CPCAT')

#trim whitespaces
dfchemcat = dfchemcat.withColumn('CASRN', ltrim(rtrim(dfchemcat.CASRN)))
dfchemcat = dfchemcat.withColumn('Description_CPCAT', ltrim(rtrim(dfchemcat.Description_CPCAT)))

#use the first word of the description as the category name - this reduces the number of overall categories from 1,200+ to 200+
dfchemcat = dfchemcat.withColumn('Description_CPCAT', split(dfchemcat.Description_CPCAT, " ")[0])

#get the unique rows
dfchemcat = dfchemcat.distinct()

#rename column for clarity
dfchemcat = dfchemcat.withColumnRenamed('Description_CPCAT', 'category')

print "Finished processing chemical categories"
#dfchemcat.printSchema()


################ Adding a numCategories column to dfchemicals

print("Adding numCategories column to chemicals")

#group by chemical to get the count of categories
temp = dfchemcat.groupby('CASRN').agg({'category': 'count'})

temp = temp.withColumnRenamed('count(category)', 'numCategories')

#required for outer join, to distingusih the 2 col names in the final result set
temp = temp.withColumnRenamed('CASRN', 'CAS')

#Add the numCategories column to chemicals list
dfchemicals = dfchemicals.join(temp, dfchemicals.CASRN == temp.CAS, how='left_outer')

dfchemicals = dfchemicals.drop('CAS')

print "Added numCategories column to chemicals"


############# products

print "Reading in products..."

dfproducts = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/products.txt',
                           format='com.databricks.spark.csv',
                           delimiter='\t',
                           header='true',
                           inferSchema='true')

#dfproducts.printSchema()                           

dfproducts = dfproducts.drop('CODE')
dfproducts = dfproducts.drop('ChemicalName')
dfproducts = dfproducts.drop('Source')

dfproducts = dfproducts.withColumn('CASRN', ltrim(rtrim(dfproducts.CASRN)))
dfproducts = dfproducts.withColumn('ProductName', lower(dfproducts.ProductName))
dfproducts = dfproducts.withColumn('Manufacturer', lower(dfproducts.Manufacturer))
print "Finished processing products"


############## Add numProducts column to chemicals

print "Adding numProducts column to chemicals"

temp = dfproducts.groupBy('CASRN').agg({'ProductName':'count'})
temp.dtypes

temp = temp.withColumnRenamed('count(ProductName)', 'numProducts')
#required for outer join, to distingusih the 2 col names in the final result set
temp = temp.withColumnRenamed('CASRN', 'CAS')

dfchemicals = dfchemicals.join(temp, dfchemicals.CASRN == temp.CAS , how='left_outer')
dfchemicals.dtypes

#not every chemical is in a listed product - in fact 30,000+ are not listed in any product
#dfchemicals.filter(dfchemicals.numProducts.isNull()).count()

#drop the extra col from the join.
dfchemicals = dfchemicals.drop('CAS')

print "Finished processing numProducts column"

############ safe chemicals
                           
print "Adding isSafe column to chemicals"
                           
dfsaferchemicals = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/safer_chemical_ingredients_list.csv',
                           format='com.databricks.spark.csv',
                           header='true',
                           inferSchema='true')

                           
#drop unwanted columns
dfsaferchemicals = dfsaferchemicals.select('CAS', 'List Call')       

dfsaferchemicals = dfsaferchemicals.withColumn('CAS', ltrim(rtrim(dfsaferchemicals.CAS)))

#data cleanup -retain only safe elements
dfsaferchemicals = dfsaferchemicals.filter((dfsaferchemicals['List Call'].startswith('Yellow')) |
                    (dfsaferchemicals['List Call'].startswith('Green')) |
                    (dfsaferchemicals['List Call'].startswith('Half Green')))


#make an integer "isSafe" column. This will be all 1's here.
dfsaferchemicals = dfsaferchemicals.withColumn('isSafe', (dfsaferchemicals['List Call'] != "").cast('integer'))
#this column is not needed now and we dont want it in the join
dfsaferchemicals = dfsaferchemicals.drop('List Call')                           


#now join with chemicals 
dfchemicals = dfchemicals.join(dfsaferchemicals, dfchemicals.CASRN == dfsaferchemicals.CAS, how='left_outer')                          
dfchemicals = dfchemicals.drop('CAS')

#not sure why, but the count is going up.
dfchemicals = dfchemicals.distinct()

print "Finished adding isSafe column to chemicals"


########## unsafe - cancer
print "adding isCancer column to chemicals"

dfdevtoxin = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/Scorecard_cancer_known.csv',
                           format='com.databricks.spark.csv',
                           header='true',
                           inferSchema='true')

dfdevtoxin = dfdevtoxin.withColumnRenamed('CAS Registry Number (or EDF Substance ID)', 'CAS')
dfdevtoxin = dfdevtoxin.select('CAS')                           
dfdevtoxin = dfdevtoxin.withColumn('CAS', ltrim(rtrim(dfdevtoxin.CAS)))
dfdevtoxin = dfdevtoxin.withColumn('isCancer', (dfdevtoxin.CAS != "").cast('integer'))

dfchemicals = dfchemicals.join(dfdevtoxin, dfchemicals.CASRN == dfdevtoxin.CAS, how='left_outer')                          
dfchemicals = dfchemicals.drop('CAS')

print "Finished adding isCancer column to chemicals"

######### unsafe - developmental toxin

print "adding isdevTox column to chemicals"

dfdevtoxin = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/scorecard_devtox_known.csv',
                           format='com.databricks.spark.csv',
                           header='true',
                           inferSchema='true')

dfdevtoxin = dfdevtoxin.withColumnRenamed('CAS Registry Number (or EDF Substance ID)', 'CAS')
dfdevtoxin = dfdevtoxin.select('CAS')                           
dfdevtoxin = dfdevtoxin.withColumn('CAS', ltrim(rtrim(dfdevtoxin.CAS)))
dfdevtoxin = dfdevtoxin.withColumn('isDevToxin', (dfdevtoxin.CAS != "").cast('integer'))

dfchemicals = dfchemicals.join(dfdevtoxin, dfchemicals.CASRN == dfdevtoxin.CAS, how='left_outer')                          
dfchemicals = dfchemicals.drop('CAS')

print "Finished adding isdevTox column to chemicals"
                   
######### unsafe - reproductive toxin

print "adding isrepTox column to chemicals"

dfreptoxin = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/scorecard_reptox_known.csv',
                           format='com.databricks.spark.csv',
                           header='true',
                           inferSchema='true')

dfreptoxin = dfreptoxin.withColumnRenamed('CAS Registry Number (or EDF Substance ID)', 'CAS')
dfreptoxin = dfreptoxin.select('CAS')                           
dfreptoxin = dfreptoxin.withColumn('CAS', ltrim(rtrim(dfreptoxin.CAS)))
dfreptoxin = dfreptoxin.withColumn('isrepToxin', (dfreptoxin.CAS != "").cast('integer'))

dfchemicals = dfchemicals.join(dfreptoxin, dfchemicals.CASRN == dfreptoxin.CAS, how='left_outer')                          
dfchemicals = dfchemicals.drop('CAS')

print "Finished adding isrepTox column to chemicals"

######### unsafe - environment bioaccumulative

print "adding isEnvBioacc column to chemicals"

dfenv_bioacc = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/scorecard_env_bioaccumulative.csv',
                           format='com.databricks.spark.csv',
                           header='true',
                           inferSchema='true')

dfenv_bioacc = dfenv_bioacc.withColumnRenamed('CAS Registry Number (or EDF Substance ID)', 'CAS')
dfenv_bioacc = dfenv_bioacc.select('CAS')                           
dfenv_bioacc = dfenv_bioacc.withColumn('CAS', ltrim(rtrim(dfenv_bioacc.CAS)))
dfenv_bioacc = dfenv_bioacc.withColumn('isEnvBioacc', (dfenv_bioacc.CAS != "").cast('integer'))

dfchemicals = dfchemicals.join(dfenv_bioacc, dfchemicals.CASRN == dfenv_bioacc.CAS, how='left_outer')                          
dfchemicals = dfchemicals.drop('CAS')

print "Finished adding isEnvBioacc column to chemicals"


######### unsafe - environment dangerous

print "adding isEnvDanger column to chemicals"

dfenv_danger = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/scorecard_env_dangerous.csv',
                           format='com.databricks.spark.csv',
                           header='true',
                           inferSchema='true')

dfenv_danger = dfenv_danger.withColumnRenamed('CAS Registry Number (or EDF Substance ID)', 'CAS')
dfenv_danger = dfenv_danger.select('CAS')                           
dfenv_danger = dfenv_danger.withColumn('CAS', ltrim(rtrim(dfenv_danger.CAS)))
dfenv_danger = dfenv_danger.withColumn('isEnvDanger', (dfenv_danger.CAS != "").cast('integer'))

dfchemicals = dfchemicals.join(dfenv_danger, dfchemicals.CASRN == dfenv_danger.CAS, how='left_outer')                          
dfchemicals = dfchemicals.drop('CAS')

print "Finished adding isEnvDanger column to chemicals"

######### unsafe - environment greenhouse gases

print "adding isEnvGreenhouseGas column to chemicals"

dfenv_gg = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/scorecard_env_greenhouse_gases.csv',
                           format='com.databricks.spark.csv',
                           header='true',
                           inferSchema='true')

dfenv_gg = dfenv_gg.withColumnRenamed('CAS Registry Number (or EDF Substance ID)', 'CAS')
dfenv_gg = dfenv_gg.select('CAS')                           
dfenv_gg = dfenv_gg.withColumn('CAS', ltrim(rtrim(dfenv_gg.CAS)))
dfenv_gg = dfenv_gg.withColumn('isEnvGrnGas', (dfenv_gg.CAS != "").cast('integer'))

dfchemicals = dfchemicals.join(dfenv_gg, dfchemicals.CASRN == dfenv_gg.CAS, how='left_outer')                          
dfchemicals = dfchemicals.drop('CAS')

print "Finished adding isEnvGreenhouseGas column to chemicals"

######### unsafe - environment ozone depleting

print "adding isEnvOzonedepleting column to chemicals"

dfenv_ozone = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/scorecard_env_ozone_dep.csv',
                           format='com.databricks.spark.csv',
                           header='true',
                           inferSchema='true')

dfenv_ozone = dfenv_ozone.withColumnRenamed('CAS Registry Number (or EDF Substance ID)', 'CAS')
dfenv_ozone = dfenv_ozone.select('CAS')                           
dfenv_ozone = dfenv_ozone.withColumn('CAS', ltrim(rtrim(dfenv_ozone.CAS)))
dfenv_ozone = dfenv_ozone.withColumn('isEnvOzone', (dfenv_ozone.CAS != "").cast('integer'))

dfchemicals = dfchemicals.join(dfenv_ozone, dfchemicals.CASRN == dfenv_ozone.CAS, how='left_outer')                          
dfchemicals = dfchemicals.drop('CAS')

print "Finished adding isEnvOzonedepleting column to chemicals"

######### unsafe - environment persistent bioaccumulative

print "adding isEnvPersistent column to chemicals"

dfenv_persist = sqlContext.read.load('file:///home/w205/project/data/WithHeaders/scorecard_env_pers_bioacc_toxic.csv',
                           format='com.databricks.spark.csv',
                           header='true',
                           inferSchema='true')

dfenv_persist = dfenv_persist.withColumnRenamed('CAS Registry Number (or EDF Substance ID)', 'CAS')
dfenv_persist = dfenv_persist.select('CAS')                           
dfenv_persist = dfenv_persist.withColumn('CAS', ltrim(rtrim(dfenv_persist.CAS)))
dfenv_persist = dfenv_persist.withColumn('isEnvPersist', (dfenv_persist.CAS != "").cast('integer'))

dfchemicals = dfchemicals.join(dfenv_persist, dfchemicals.CASRN == dfenv_persist.CAS, how='left_outer')                          
dfchemicals = dfchemicals.drop('CAS')

print "Finished adding isEnvPersistent column to chemicals"

######################

#calculate overall toxicity score

print 'Calculating Overall toxicity'

dfchemicals = dfchemicals.na.fill(0, ['isSafe', 'isCancer', 'isDevToxin', 'isrepToxin', 'isEnvBioacc', 'isEnvDanger', 'isEnvGrnGas', 'isEnvOzone', 'isEnvPersist'])
dfchemicals = dfchemicals.na.fill(0, ['numCategories', 'numProducts'])
dfchemicals = dfchemicals.withColumn('ToxicityScore', dfchemicals.isCancer + dfchemicals.isDevToxin + dfchemicals.isrepToxin + 
                dfchemicals.isEnvBioacc + dfchemicals.isEnvDanger + dfchemicals.isEnvGrnGas + dfchemicals.isEnvOzone + dfchemicals.isEnvPersist)
dfchemicals = dfchemicals.withColumn('isToxic', (dfchemicals.ToxicityScore > 0).cast('integer'))
dfchemicals = dfchemicals.withColumn('isUnknown', ((dfchemicals.ToxicityScore == 0) & (dfchemicals.isSafe == 0)).cast('integer')) 
                
print 'Finished Calculating Overall toxicity'

############## categories

print "processing categories"

dfcategories = dfchemcat.join(dfchemicals, on="CASRN")
dfcategories = dfcategories.groupBy('category').agg({'Name':'count', 'isSafe':'sum', 'isToxic':'sum', 'isUnknown':'sum', 'ToxicityScore':'sum',
                 'isCancer':'sum', 'isDevToxin':'sum', 'isrepToxin':'sum', 
                 'isEnvBioacc':'sum', 'isEnvDanger':'sum', 'isEnvGrnGas':'sum', 'isEnvOzone':'sum', 'isEnvPersist':'sum'})
dfcategories = dfcategories.withColumnRenamed('count(Name)', 'numChemicals')
dfcategories = dfcategories.withColumnRenamed('sum(isToxic)', 'numToxic')
dfcategories = dfcategories.withColumnRenamed('sum(isCancer)', 'numCancer')
dfcategories = dfcategories.withColumnRenamed('sum(isEnvOzone)', 'numEnvOzone')
dfcategories = dfcategories.withColumnRenamed('sum(isSafe)', 'numSafe')
dfcategories = dfcategories.withColumnRenamed('sum(isDevToxin)', 'numDevToxin')
dfcategories = dfcategories.withColumnRenamed('sum(isUnknown)', 'numUnknown')
dfcategories = dfcategories.withColumnRenamed('sum(isEnvDanger)', 'numEnvDanger')
dfcategories = dfcategories.withColumnRenamed('sum(isEnvBioacc)', 'numEnvBioacc')
dfcategories = dfcategories.withColumnRenamed('sum(isrepToxin)', 'numRepToxin')
dfcategories = dfcategories.withColumnRenamed('sum(isEnvGrnGas)', 'numEnvGrnGas')
dfcategories = dfcategories.withColumnRenamed('sum(isEnvPersist)', 'numEnvPersist')
dfcategories = dfcategories.withColumnRenamed('sum(ToxicityScore)', 'sumToxicity')

dfcategories = dfcategories.drop('CASRN')
#dfcategories.dtypes

print "Finished processed categories" 

                   
########### Saving to HDFS
#save chemicals
print "Saving chemicals"
dfchemicals.coalesce(1).write.mode('overwrite').parquet("/user/w205/project/test/dfchemicals.parquet")

#save chemical categories
print "saving chemicals and categories"
dfchemcat.coalesce(1).write.mode('overwrite').parquet("/user/w205/project/test/dfchemcat.parquet")

#save categories list
print "saving categories"
dfcategories.coalesce(1).write.mode('overwrite').parquet("/user/w205/project/test/dfcategories.parquet")

#save products list
print "saving products"
dfproducts.coalesce(1).write.mode('overwrite').parquet("/user/w205/project/test/dfproducts.parquet")


#dfchemicals.coalesce(1).write.mode('overwrite').options(header="true").format('com.databricks.spark.csv').save('/user/w205/project/dfchemicals.csv')
#dfchemcat.coalesce(1).write.mode('overwrite').options(header="true").format('com.databricks.spark.csv').save('/user/w205/project/dfchemcat.csv')
#dfcategories.coalesce(1).write.mode('overwrite').options(header="true").format('com.databricks.spark.csv').save('/user/w205/project/dfcategories.csv')
#dfproducts.coalesce(1).write.mode('overwrite').options(header="true").format('com.databricks.spark.csv').save('/user/w205/project/dfproducts.csv')
