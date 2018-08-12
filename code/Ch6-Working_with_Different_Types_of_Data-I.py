# Databricks notebook source
# This notebook covers building expressions, which are the bread and butter of Spark’s structured operations. 
# We also review working with a variety of different kinds of data, including the following:

# Booleans
# Numbers
# Strings
# Dates and timestamps
# Handling null
# Complex types
# User-defined functions

# COMMAND ----------

# To begin, let’s read in the DataFrame from the retail-dateset that we’ll be using for this analysis:

df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("dbfs:/data/retail-data/by-day/2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")

# COMMAND ----------

# Converting to Spark Types

# The 'lit' function, converts a type in another language to its correspnding Spark representation.

from pyspark.sql.functions import lit
df.select(lit(5), lit("five"), lit(5.0)).printSchema()

# COMMAND ----------

# Booleans are essential when it comes to data analysis because they are the foundation for all filtering. 
# Boolean statements consist of four elements: and, or, true, and false.
# These statements are often used as conditional requirements for when a row of data must either pass the test (evaluate to true) or else it will be filtered out.

from pyspark.sql.functions import col

df.where(col("InvoiceNo") != 536365)\
  .select("InvoiceNo", "Description")\
  .show(5)

# Scala has some particular semantics regarding the use of == and ===. In Spark, if you want to filter by equality you should use === (equal) or =!= (not equal). You can also use the not function and the equalTo method.

# COMMAND ----------

# Another option is to specify the predicate as an expression in a string. This is valid for Python or Scala.

df.where("InvoiceNo = 536365").show(3, False)
  
# df.where("InvoiceNo <> 536365").show(5, False)

# COMMAND ----------

# Complex Boolean Statements

from pyspark.sql.functions import instr
# Defining the Price Filter based on Unit Price.
priceFilter = col("UnitPrice") > 600
# instr locates the position of the Given String in the column.
descripFilter = instr(df.Description, "POSTAGE") >= 1
# isin - A boolean expression that is evaluated to true if the value of this expression is contained by the evaluated values of the arguments.
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show(3)

# Corresponding SQL Statement:
# SELECT * FROM dfTable WHERE StockCode in ("DOT") AND(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)

# COMMAND ----------

# One “gotcha” that can come up is if you’re working with null data when creating Boolean expressions. 
# If there is a null in your data, you’ll need to treat things a bit differently. Here’s how you can ensure that you perform a null-safe equivalence test:

df.where(col("Description").eqNullSafe("hello"))

# COMMAND ----------

# Working with numerical data types.

# Let’s imagine that we found out that we mis-recorded the quantity in our retail dataset and the true quantity is equal to (the current quantity * the unit price)2 + 5
# We use pow function that raises a column to the expressed power:
from pyspark.sql.functions import expr, pow
# Expressing the equation using pow function.
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select("CustomerId", fabricatedQuantity.alias("realQuantity")).show(2)

# COMMAND ----------

# The same expression can be expressed as SQL query.
df.selectExpr(
  "CustomerId",
  "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)

# COMMAND ----------

# Rounding to the whole number.

# By default, the round function rounds up if you’re exactly in between two numbers. You can round down by using the bround:
from pyspark.sql.functions import lit, round, bround
df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

# COMMAND ----------

# Calculating the Correlation of two columns.

from pyspark.sql.functions import corr
# We can find correlation using df.stat (DataFrameStatFunctions).
df.stat.corr("Quantity", "UnitPrice")
# We can also find using corr function available in DataFrame.
df.select(corr("Quantity", "UnitPrice")).show()

# COMMAND ----------

# Compute summary statistics for a column or set of columns. We can use the describe method to achieve exactly this. 
# This will take all numeric columns and calculate the count, mean, standard deviation, min, and max.

df.describe().show()

# COMMAND ----------

# We can also find these statistics by computing them invidually.
from pyspark.sql.functions import count, mean, stddev_pop, min, max

# COMMAND ----------

# You also can use this to see a cross-tabulation or frequent item pairs (be careful, this output will be large and is omitted for this reason):
df.stat.crosstab("StockCode", "Quantity").show()
df.stat.freqItems(["StockCode", "Quantity"]).show()

# COMMAND ----------

# we can also add a unique ID to each row by using the function monotonically_increasing_id. This function generates a unique value for each row, starting with 0:

from pyspark.sql.functions import monotonically_increasing_id
df.select(monotonically_increasing_id()).show(2)

# COMMAND ----------

# Note:

# There are also a number of more advanced tasks like bloom filtering and sketching algorithms available in the stat package. 
# Be sure to search the API documentation for more information and functions.

# COMMAND ----------

# String manipulation shows up in nearly every data flow.


# COMMAND ----------

# The initcap function will capitalize every word in a given string when that word is separated from another by a space.
from pyspark.sql.functions import initcap
df.select(initcap(col("Description"))).show(5)

# COMMAND ----------

# You can cast strings in uppercase and lowercase, as well:
from pyspark.sql.functions import lower, upper
df.select(col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))).show(2)

# COMMAND ----------

# Regular Expressions
# Regular expressions give the user an ability to specify a set of rules to use to either extract values from a string or replace them with some other values.
# Spark takes advantage of the complete power of Java regular expressions.

# COMMAND ----------

# regexp_replace function to replace substitute color names in our description column:
from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(col("Description"),
  regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean")).show(2, False)

# COMMAND ----------

# Another task might be to replace given characters with other characters. Spark provides the translate function to replace these values.
from pyspark.sql.functions import translate
df.select(col("Description"), translate(col("Description"), "LEET", "1337")).show(2)

# COMMAND ----------

# Using regex_extract we can pull the matching Strings from the column values.
from pyspark.sql.functions import regexp_extract
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
     regexp_extract(col("Description"), extract_str, 1).alias("color_clean"), col("Description")).show(2, False)

# COMMAND ----------

# Contains function is to just simply check for the existence of the String in column value.

from pyspark.sql.functions import instr
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
  .where("hasSimpleColor")\
  .select("Description").show(3, False)

# COMMAND ----------

# The above is just with 2 arguements, let's walk through some rigorous example.

from pyspark.sql.functions import expr, locate
# colors to be identified.
simpleColors = ["black", "white", "red", "green", "blue"]
# function to locate the color in column, if exists returns "is_color" value
def color_locator(column, color_string):
  return locate(color_string.upper(), column)\
          .cast("boolean")\
          .alias("is_" + c)

selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*")) # has to a be Column type

df.select(*selectedColumns).where(expr("is_white OR is_red"))\
  .select("Description").show(3, False)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


