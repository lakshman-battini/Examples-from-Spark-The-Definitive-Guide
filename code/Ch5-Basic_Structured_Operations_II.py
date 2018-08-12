# Databricks notebook source
# Creating the DataFrame using JSON Dataset.
# As seen in previous chapters, we can create DataFrames from raw datasources. Creating the DF from json dataset.

df = spark.read.format("json").load("dbfs:/data/flight-data/json/2015-summary.json")
df.createOrReplaceTempView("dfTable")

# COMMAND ----------

# We can also create DataFrames on the fly by taking a set of rows and converting them to a DataFrame.

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
# Creating the Schema Manually
myManualSchema = StructType([
  StructField("some", StringType(), True),
  StructField("col", StringType(), True),
  StructField("names", LongType(), False)
])
# Creating the Row object which represents the record in DataFrame.
myRow = Row("Hello", None, 1)

# Creating the DataFrame using Row Object and Schema info.
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()


# COMMAND ----------

# select and selectExpr allow you to do the DataFrame equivalent of SQL queries on a table of data:

df.select("DEST_COUNTRY_NAME").show(2)

# COMMAND ----------

# Querying using SQL query on the Registered DataFrame table using spark.sql.

spark.sql("SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2").show()

# COMMAND ----------

# You can select multiple columns by using the same style of query, just add more column name strings to your select method call:
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

# COMMAND ----------

spark.sql("SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2").show()

# COMMAND ----------

# expr is the most flexible reference that we can use to represent column. It can refer to a plain column or a string manipulation of a column.
from pyspark.sql.functions import expr
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

# COMMAND ----------

# Select followed by a series of expr is such a common pattern, so Spark has a shorthand for doing this efficiently: selectExpr.
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

# COMMAND ----------

# SelectExpr is a simple way to build up complex expressions that create new DataFrames. 
# In fact, we can add any valid non-aggregating SQL statement, and as long as the columns resolve, it will be valid!

# Here’s a simple example that adds a new column withinCountry to our DataFrame that specifies whether the destination and origin are the same:
df.selectExpr(
  "*", # all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
  .show(2)

# COMMAND ----------

# The same query can be written using spark.sql

spark.sql("""SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry
FROM dfTable
LIMIT 2""").show()

# COMMAND ----------

# With selectExpr, we can specify the aggregations over the entire DataFrame by taking advantage of the functions that we have in spark.

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

# COMMAND ----------

# usiing withColumn function, we can add the columns to the DataFrame.
# Adding the literal(1) as a column to teh Dataframe for demonstration.
from pyspark.sql.functions import lit
df.withColumn("numberOne", lit(1)).show(2)

# COMMAND ----------

# Adding the new column with Boolean flag for when the origin country is the same as the destination country:

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
  .show(2)

# COMMAND ----------

# We can rename the column using 'withColumnRenamed' function.
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").show(5)

# COMMAND ----------

# In spark we use backtick (`) characters to as an escaping character.

dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))

dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")\
  .show(2)

# COMMAND ----------

# By default Spark is case insensitive; however, you can make Spark case sensitive by setting the configuration:

spark.conf.set("spark.sql.caseSensitive", "true")

# COMMAND ----------

# To filter rows, we create an expression that evaluates to true or false. You then filter out the rows with an expression that is equal to false.

# There are two methods to perform the filter operations: you can use where or filter and they both will perform the same operation and accept the same argument types when used with DataFrames.

# Both the below operations returns the same result.
from pyspark.sql.functions import col
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)

# COMMAND ----------

# We can put multiple filter operations in the same expression.

df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
  .show(2)

# COMMAND ----------

# To extract the unique or disticnt values from the DataFrame, Spark provides the distinct() function.

# COunting the number of distinct origin and destination country values from the DataFrame.
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

# COMMAND ----------

# To sample some random records from your DataFrame, Spark provides sample method on a DataFrame.

# We can sample the records with or without replacement.
seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()

# COMMAND ----------

# Random splits is helpful when you need to break up your DataFrame into a random “splits” of the original DataFrame. 
# This is often used with machine learning algorithms to create training, validation, and test sets.

# RandomSplit function accepts weights as an arguement by which the DF need to be split.
# We can also specify the seed.
# It’s important to note that if you don’t specify a proportion for each DataFrame that adds up to one, they will be normalized so that they do:
seed = 5
dataFrames = df.randomSplit([0.25, 0.75], seed)
dataFrames[0].count() > dataFrames[1].count() # False

# COMMAND ----------

# To append to a DataFrame, you must union the original DataFrame along with the new DataFrame. This just concatenates the two DataFramess. 
# To union two DataFrames, you must be sure that they have the same schema and number of columns; otherwise, the union will fail.

from pyspark.sql import Row
schema = df.schema
# Creating the new Rows
newRows = [
  Row("New Country", "Other Country", 5L),
  Row("New Country 2", "Other Country 3", 1L)
]
# Creatign the RDD out of the list of Rows
parallelizedRows = spark.sparkContext.parallelize(newRows)
# Creating the Spark DataFrame from the RDD.
newDF = spark.createDataFrame(parallelizedRows, schema)

newDF.show()

# COMMAND ----------

# Appending the newDF to the existing DF. This will create the New DataFrame.

df.union(newDF)\
  .where("count = 1")\
  .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
  .show()

# COMMAND ----------

# There are two equivalent operations to do this sort and orderBy that work the exact same way. 
# They accept both column expressions and strings as well as multiple columns. 

# Sort the DF by count values in Ascending order.
df.sort("count").show(5)
# Sort the DF by count and Dest_country_name values in Ascending order.
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)

# COMMAND ----------

# To more explicitly specify sort direction, you need to use the asc and desc functions if operating on a column.
from pyspark.sql.functions import desc, asc
# Sorting the df by count values in Descending order.
df.orderBy(expr("count desc")).show(2)
# Sorting the df by count in Descending and dest_country_name values in Ascending order.
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

# COMMAND ----------

# Another important optimization opportunity is to partition the data according to some frequently filtered columns, which control the physical layout of data across the cluster including the partitioning scheme and the number of partitions.

# Repartition will incur a full shuffle of the data, regardless of whether one is necessary. 
# This means that you should typically only repartition when the future number of partitions is greater than your current number of partitions or when you are looking to partition by a set of columns:

# Get the number of Partitions
df.rdd.getNumPartitions()

# COMMAND ----------

# Repartition the DataFrame into 5 partitions
df = df.repartition(5)
# check the number of Partitions
df.rdd.getNumPartitions()

# COMMAND ----------

# If you know that you’re going to be filtering by a certain column often, it can be worth repartitioning based on that column:
df.repartition(col("DEST_COUNTRY_NAME"))

# COMMAND ----------

# We can optionally specify the number of partitions you would like, too:
# Repartition into 5 partitions by dest_country_name column.
df = df.repartition(5, col("DEST_COUNTRY_NAME"))
df.rdd.getNumPartitions()

# COMMAND ----------

# Coalesce will not incur a full shuffle and will try to combine partitions. 
# This operation will shuffle your data into five partitions based on the destination country name, and then coalesce them (without a full shuffle):

df = df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)
df.rdd.getNumPartitions()

# COMMAND ----------

# Spark maintains the state of the cluster in the driver. There are times when you’ll want to collect some of your data to the driver in order to manipulate it on your local machine.

# Collect gets all data from the entire DataFrame, take selects the first N rows, and show prints out a number of rows nicely.

collectDF = df.limit(10)
collectDF.take(5) # take works with an Integer count

# COMMAND ----------

collectDF.show(5, False) # Truncate option to False, to display full column values 
collectDF.collect() # collect the Rows to the Driver

# COMMAND ----------

# Congratulation!! You have completed the tutorial.
# This Notebook covered basic operations on DataFrames. You learned the simple concepts and tools that you will need to be successful with Spark DataFrames.
