# Databricks notebook source
# Creating the DataFrame from the JSON data.
# The json dataset contains the Flight data from the Transportation statistics, you can find the dataset in Spark-The-Definitive-Guide git repo.
df = spark.read.format("json").load("dbfs:/data/flight-data/json/2015-summary.json")

# COMMAND ----------

# The schema of the DataFrame that is created above from the json dataset:
df.printSchema()

# COMMAND ----------

# A schema is a StructType made up of a number of fields, StructFields, that have a name, type, a Boolean flag which specifies whether that column can contain missing or null values, and, finally, users can optionally specify associated metadata with that column. The metadata is a way of storing information about this column.
# Schema can contain other StructTypes ( Spark's complex types).
df.schema

# COMMAND ----------

# We can also create the Schema and enforce the Schema on DataFrame.
# If the Types doesn't match the Schema, Spark will throw an error.

from pyspark.sql.types import StructField, StructType, StringType, LongType

manualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(manualSchema)\
  .load("dbfs:/data/flight-data/json/2015-summary.json")

# Printing the Schema information
df.schema

# COMMAND ----------

# Accessing the DataFrames columns

df.columns

# COMMAND ----------

# In Spark, each row in a DataFrame is a single record. Spark represents this record as an object of type Row.
# Row objects internally represent arrays of bytes. The byte array interface is never shown to users because we only use column expressions to manipulate them.

df.first()
# Notice the Return type is Row Type

# COMMAND ----------

# Creating Rows
from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)

# COMMAND ----------

# Accessing data in rows is equally as easy: you just specify the position that you would like.
myRow[0]
myRow[2]

# COMMAND ----------


