# Databricks notebook source
# Aggregating is the act of collecting something together and is a cornerstone of big data analytics.
# In an aggregation, you will specify a key or grouping and an aggregation function that specifies how you should transform one or more columns.

# In addition to working with any type of values, Spark also allows us to create the following groupings types:

#1. The simplest grouping is to just summarize a complete DataFrame by performing an aggregation in a select statement.

#2. A “group by” allows you to specify one or more keys as well as one or more aggregation functions to transform the value columns.

#3. A “window” gives you the ability to specify one or more keys as well as one or more aggregation functions to transform the value columns. However, the rows input to the function are somehow related to the current row.

#4. A “grouping set,” which you can use to aggregate at multiple different levels. Grouping sets are available as a primitive in SQL and via rollups and cubes in DataFrames.

#5. A “rollup” makes it possible for you to specify one or more keys as well as one or more aggregation functions to transform the value columns, which will be summarized hierarchically.

#6. A “cube” allows you to specify one or more keys as well as one or more aggregation functions to transform the value columns, which will be summarized across all combinations of columns.

# Each grouping returns a RelationalGroupedDataset on which we specify our aggregations.

# COMMAND ----------

# Reading in retail data on purchases, repartitioning the data to have far fewer partitions and caching the results for rapid access:
df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("dbfs:/data/retail-data/all/*.csv")\
  .coalesce(5)
# Caching the dataframe
df.cache()
# Registering the DataFrame as Temporary view.
df.createOrReplaceTempView("dfTable")

# COMMAND ----------

# Aggregation Functions

# COMMAND ----------

# Count function is used to count the number of records.
# We can specify a specific column to count, or all the columns by using count(*) or count(1) to represent that we want to count every row as the literal one.

from pyspark.sql.functions import count
df.select(count("StockCode")).show()

# COMMAND ----------

# countDistinct is used to count the number of Unique records.
from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show()

# COMMAND ----------

# On very large datasets finding the exact distinct count can be time taking.
# There are times when an approximation to a certain degree of accuracy will work just fine, and for that, you can use the approx_count_distinct function:

# approx_count_distinct takes the another parameter with which you can specify the maximum estimation error allowed.
from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show()

# COMMAND ----------

# Using first and last we can get the first and last values from a DataFrame.
# This will be based on the rows in the DataFrame, not on the values in the DataFrame:
from pyspark.sql.functions import first, last
df.select(first("StockCode"), last("StockCode")).show()

# COMMAND ----------

# Using min and max functions we can extract the minimum and maximum values from a DataFrame:
from pyspark.sql.functions import min, max
df.select(min("Quantity"), max("Quantity")).show()

# COMMAND ----------

# Using sum function: to add all the values in a row using the sum function:
from pyspark.sql.functions import sum
df.select(sum("Quantity")).show()

# COMMAND ----------

# Using sumDistinct function: to get sum a distinct set of values:
from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Quantity")).show()

# COMMAND ----------

# Using avg function to get that value via the avg or mean functions:
from pyspark.sql.functions import sum, count, avg, expr

df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))\
  .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()

# COMMAND ----------

# The variance is the average of the squared differences from the mean, and the standard deviation is the square root of the variance.
# You can calculate these in Spark by using their respective functions.

from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"), stddev_samp("Quantity")).show()

# COMMAND ----------

# Skewness and kurtosis are both measurements of extreme points in your data. 
# Skewness measures the asymmetry of the values in your data around the mean. 
# Kurtosis is a measure of the tail of data.

from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()

# COMMAND ----------

# Correlation measures the Pearson correlation coefficient, which is scaled between –1 and +1. 
# The covariance is scaled according to the inputs in the data.

from pyspark.sql.functions import corr, covar_pop, covar_samp
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")).show()

# COMMAND ----------

# collect_list to collect all the values as a list.
# collect_set to collect all the values as a set.

from pyspark.sql.functions import collect_set, collect_list
df.agg(collect_set("Country"), collect_list("Country")).show()

# COMMAND ----------



# COMMAND ----------

# Performing the aggregations based on Groups of data.
# Group by each unique invoice number and get the count of items on that invoice. Note that this returns another DataFrame and is lazily performed.

# COMMAND ----------

# We do this grouping in two phases. First we specify the column(s) on which we would like to group, and then we specify the aggregation(s). The first step returns a RelationalGroupedDataset, and the second step returns a DataFrame.
df.groupBy("InvoiceNo", "CustomerId").count().show(5)

# COMMAND ----------

# Grouping with Expressions:
from pyspark.sql.functions import count

df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show(5)

# COMMAND ----------

# Grouping with Maps
# Sometimes, it can be easier to specify your transformations as a series of Maps for which the key is the column, and the value is the aggregation function (as a string) that you would like to perform. 
# You can reuse multiple column names if you specify them inline, as well:

df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)")).show(5)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


