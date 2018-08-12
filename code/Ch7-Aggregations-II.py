# Databricks notebook source
# You can use "window" functions to carry out some unique aggregations by either computing some aggregation on a specific “window” of data.
# A group-by takes data, and every row can go only into one grouping. 
# A window function calculates a return value for every input row of a table based on a group of rows, called a frame.
# Each row can fall into one or more frames.
# Spark supports three kinds of window functions: ranking functions, analytic functions, and aggregate functions.

# COMMAND ----------

# Loading the DataFrame from Retail-dataset.
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

# To demonstrate, adding a date column that will convert our invoice date into a column that contains only date information:
from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")
dfWithDate.show(3, False)

# COMMAND ----------

# The first step to a window function is to create a window specification. 
# Note that the "partition by" is unrelated to the partitioning scheme concept that we have covered thus far. t’s just a similar concept that describes how we will be breaking up our group. 
# The ordering determines the ordering within a given partition.
# Finally, the frame specification (the rowsBetween statement) states which rows will be included in the frame based on its reference to the current input row. 

# In the following example, we look at all previous rows up to the current row:
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
windowSpec = Window\
  .partitionBy("CustomerId", "date")\
  .orderBy(desc("Quantity"))\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# COMMAND ----------

# Using the aggreation function on the DataFrame.
# Lets find the maximum purchase quantity over all time. To find this, we use the "MAX" aggregation function. In addition, we indicate the window specification that defines to which frames of data this function will apply:
from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

# maxPurchaseQuantity, returns the Column.
# We can use this maxPurchaseQuantity value in a DataFrame select Statement.

# COMMAND ----------

# Creating the Purchase Quantity Rank, i.e which date had the maximum purchase quantity for every customer
# To do that, we use the "dense_rank" function instead of "rank" to avoid the gaps in the ranking sequence when there are tied values (or in our case, duplicate rows):

from pyspark.sql.functions import dense_rank, rank
# Dense Ranking over windowSpec
purchaseDenseRank = dense_rank().over(windowSpec)
# Ranking over windowSpec
purchaseRank = rank().over(windowSpec)

# This also returns the Column, we can use this in a DataFrame.

# COMMAND ----------

# Applying the above functions to DataFrame.

from pyspark.sql.functions import col

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()

# COMMAND ----------

# Grouping sets are a low-level tool for combining sets of aggregations together. 
# Grouping Sets give you the ability to create arbitrary aggregation in their group-by statements.

# COMMAND ----------

# Let’s work through an example to gain a better understanding. 
dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

# COMMAND ----------

# Calculating the total quantity of all stock codes and customers.
spark.sql("""SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode
ORDER BY CustomerId DESC, stockCode DESC""").show(3)

# COMMAND ----------

# You can do the exact same thing by using a grouping set:
spark.sql("""SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
ORDER BY CustomerId DESC, stockCode DESC""").show(3)

# COMMAND ----------

# Note:
# Grouping sets depend on null values for aggregation levels. If you do not filter-out null values, you will get incorrect results. This applies to cubes, rollups, and grouping sets.

# COMMAND ----------

# If you also want to include the total number of items, regardless of customer or stock code? 
# With a conventional group-by statement, this would be impossible. But, it’s simple with grouping sets: we simply specify that we would like to aggregate at that level, as well, in our grouping set. This is, effectively, the union of several different groupings together:

# COMMAND ----------

spark.sql("""SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode),())
ORDER BY CustomerId DESC, stockCode DESC""").show(3)

# COMMAND ----------

# Note:
# The GROUPING SETS operator is only available in SQL. To perform the same in DataFrames, you use the rollup and cube operators—which allow us to get the same results. Let’s go through those.

# COMMAND ----------

# A rollup is a multidimensional aggregation that performs a variety of group-by style calculations for us.
# Let’s create a rollup that looks across time (with our new Date column) and space (with the Country column) and creates a new DataFrame that includes the grand total over all dates, the grand total for each date in the DataFrame, and the subtotal for each country on each date in the DataFrame:

# COMMAND ----------

from pyspark.sql.functions import sum
rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum('Quantity')).selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity").orderBy("Date")
rolledUpDF.show(3)

# COMMAND ----------

# A cube takes the rollup to a level deeper. Rather than treating elements hierarchically, a cube does the same thing across all dimensions.

# With cube, you can find below information:
# The total across all dates and countries
# The total for each date across all countries
# The total for each country on each date
# The total for each country across all dates

# COMMAND ----------

from pyspark.sql.functions import sum

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))\
  .select("Date", "Country", "sum(Quantity)").orderBy("Date").show(5)

# This is a quick and easily accessible summary of nearly all of the information in our table, and it’s a great way to create a quick summary table that others can use later on.

# COMMAND ----------

# Sometimes when using cubes and rollups, you want to be able to query the aggregation levels so that you can easily filter them down accordingly. 
# We can do this by using the grouping_id, which gives us a column specifying the level of aggregation that we have in our result set. 
# The query in the example that follows returns four distinct grouping IDs.

from pyspark.sql.functions import grouping_id, sum, expr, col
dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity").alias("total_quantity")).orderBy(col("total_quantity").desc()).show(3)

# COMMAND ----------

# Pivots make it possible for you to convert a row into a column. For example, in our current data we have a Country column. 
# With a pivot, we can aggregate according to some function for each of those given countries and display them in an easy-to-query way:

pivoted = dfWithDate.groupBy("date").pivot("Country").sum()

# COMMAND ----------

# This DataFrame will now have a column for every combination of country, numeric variable, and a column specifying the date. 
# For example, for USA we have the following columns: USA_sum(Quantity), USA_sum(UnitPrice), USA_sum(CustomerID). 

# COMMAND ----------

# User-defined aggregation functions (UDAFs) are a way for users to define their own aggregation functions based on custom formulae or business rules. 
# You can use UDAFs to compute custom calculations over groups of input data (as opposed to single rows). 
# Spark maintains a single AggregationBuffer to store intermediate results for every group of input data.

# To create a UDAF, you must inherit from the UserDefinedAggregateFunction base class and implement the following methods:

#1. inputSchema represents input arguments as a StructType

#2. bufferSchema represents intermediate UDAF results as a StructType

#3. dataType represents the return DataType

#4. deterministic is a Boolean value that specifies whether this UDAF will return the same result for a given input

#5. initialize allows you to initialize values of an aggregation buffer

#6. update describes how you should update the internal buffer based on a given row

#7. merge describes how two aggregation buffers should be merged

#8. evaluate will generate the final result of the aggregation

# COMMAND ----------

# UDAFs are currently available only in Scala or Java. However, you will also be able to call Scala or Java UDFs and UDAFs by registering the function just as UDF.
