# Databricks notebook source
# Dates and times are a constant challenge in programming languages and databases.
# Spark does its best to keep things simple by focusing explicitly on two kinds of time-related information. 
# There are dates, which focus exclusively on calendar dates. 
# Timestamps, which include both date and time information.

# Note:
# In version 2.1 and before, Spark parsed according to the machine’s timezone if timezones are not explicitly specified in the value that you are parsing. 
# You can set a session local timezone if necessary by setting spark.conf.sessionLocalTimeZone in the SQL configurations.

# Spark’s TimestampType class supports only second-level precision, which means that if you’re going to be working with milliseconds or microseconds, you’ll need to work around this problem by potentially operating on them as longs.

# COMMAND ----------

# Let’s begin with the basics and get the current date and the current timestamps:

# current_date and current_timestamp functions return the current date and timestamps.
from pyspark.sql.functions import current_date, current_timestamp
# Creating dateDF using these functions.
dateDF = spark.range(10)\
  .withColumn("today", current_date())\
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")
dateDF.show(2, False)

# COMMAND ----------

# Printing the Schema
dateDF.printSchema()

# COMMAND ----------

# Now that we have a simple DataFrame to work with, let’s add and subtract five days from today. 
# These functions take a column and then the number of days to either add or subtract as the arguments:
from pyspark.sql.functions import col, date_add, date_sub

dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

# COMMAND ----------

# datediff function will return the number of days in between two dates.

from pyspark.sql.functions import datediff, months_between, to_date, lit
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
  .select(datediff(col("week_ago"), col("today"))).show(1)

# COMMAND ----------

# months_between function gives you the number of months between two dates:
dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
  .select(months_between(col("start"), col("end"))).show(1)

# COMMAND ----------

# to_date function allows you to convert a string to a date, optionally with a specified format. 
# We specify our format in the Java SimpleDateFormat which will be important to reference if you use this function:

from pyspark.sql.functions import to_date, lit
spark.range(5).withColumn("date", lit("2017-01-01"))\
  .select(to_date(col("date"))).show(1)

# Spark will not throw an error if it cannot parse the date; rather, it will just return null.

# COMMAND ----------

# We can specify the date_format according to the Java "SimpleDateFormat".
from pyspark.sql.functions import to_date
# Defining the date format.
dateFormat = "yyyy-dd-MM"
# Specifying the Date format in to_date method.
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")
cleanDateDF.show()

# COMMAND ----------

# to_timestamp, converts the string to Timestamp Type, it always requires the Format.
from pyspark.sql.functions import to_timestamp
dateFormat = "yyyy-dd-MM"
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

# COMMAND ----------

# comparing between them is actually quite easy.
# We just need to be sure to either use a date/timestamp type or specify our string according to the right format of yyyy-MM-dd if we’re comparing a date:
cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()

# We can also set this as a string, which Spark parses to a literal:
# cleanDateDF.filter(col("date2") > "'2017-12-12'").show()

# COMMAND ----------

# As a best practice, you should always use nulls to represent missing or empty data in your DataFrames. 
# Spark can optimize working with null values more than it can if you use empty strings or other values. 
# The primary way of interacting with null values, at DataFrame scale, is to use the .na subpackage on a DataFrame.

# Loading the DataFrame from the Retail Dataset to Demonstrate Null values.
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/data/retail-data/by-day/2010-12-01.csv")
df.createOrReplaceTempView("dfTable")

# COMMAND ----------

# Coalesce
# to select the first non-null value from a set of columns.
from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show(2, False)

# COMMAND ----------

# ifnull, nullIf, nvl, and nvl2 functions

# ifnull allows you to select the second value if the first is null, and defaults to the first. 
# nullif, which returns null if the two values are equal or else returns the second if they are not. 
# nvl returns the second value if the first is null, but defaults to the first. 
# nvl2 returns the second value if the first is not null; otherwise, it will return the last specified value (else_value in the following example):

spark.sql("""SELECT
  ifnull(null, 'return_value'),
  nullif('value', 'value'),
  nvl(null, 'return_value'),
  nvl2('not_null', 'return_value', "else_value")
FROM dfTable LIMIT 1""").show()

# COMMAND ----------

# drop, removes rows that contain nulls. The default is to drop any row in which any value is null:
df.na.drop()
# df.na.drop("any")

# COMMAND ----------

# Using “all” drops the row only if all values are null or NaN for that row:
df.na.drop("all", subset=["StockCode", "InvoiceNo"])

# COMMAND ----------

# fill, to fill one or more columns with a set of values.
df.na.fill("All Null values become this string")

# COMMAND ----------

# We could do the same for columns of type Integer by using df.na.fill(5:Integer), or for Doubles df.na.fill(5:Double). 
# To specify columns, we just pass in an array of column names like we did in the previous example:
fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals)

# COMMAND ----------

# replace, replace all values in a certain column according to their current value. The only requirement is that this value be the same type as the original value:
df.na.replace([""], ["UNKNOWN"], "Description")

# COMMAND ----------

# There are three kinds of complex types: structs, arrays, and maps.

# COMMAND ----------

# Structs:
# You can think of structs as DataFrames within DataFrames. 
from pyspark.sql.functions import struct
complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.show(3, False)
complexDF.createOrReplaceTempView("complexDF")

# COMMAND ----------

# We now have a DataFrame with a column complex. We can query it just as we might another DataFrame, the only difference is that we use a dot syntax to do so, or the column method getField:

complexDF.select(col("complex").getField("Description")).show(3)
#complexDF.select("complex.Description")

# COMMAND ----------

# Arrays

# Defining an array column "array_col" by Splitting the Description column by "" value. we use split function to do this.
from pyspark.sql.functions import split
df.select(split(col("Description"), " ").alias("array_col"))\
  .selectExpr("array_col[0]").show(2)

# COMMAND ----------

# Array Length
# We can determine the array’s length by querying for its size:

from pyspark.sql.functions import size
df.select(size(split(col("Description"), " "))).show(2)

# COMMAND ----------

# array_contains
# We can also see whether this array contains a value:
from pyspark.sql.functions import array_contains
df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

# COMMAND ----------

# explode
# The explode function takes a column that consists of arrays and creates one row (with the rest of the values duplicated) per value in the array.

from pyspark.sql.functions import split, explode

df.withColumn("splitted", split(col("Description"), " "))\
  .withColumn("exploded", explode(col("splitted")))\
  .select("Description", "InvoiceNo", "exploded").show(5, False)

# COMMAND ----------

# Maps
# Maps are created by using the map function and key-value pairs of columns. You then can select them just like you might select from an array:

from pyspark.sql.functions import create_map
# create_map function is used to define MapType from the Description and InvoiceNo columns
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
  .show(2, False)

# COMMAND ----------


