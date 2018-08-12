# Databricks notebook source
# One of the most powerful things that you can do in Spark is define your own functions. 
# These user-defined functions (UDFs) make it possible for you to write your own custom transformations using Python or Scala and even use external libraries. UDFs can take and return one or more columns as input. 

# Spark UDFs are incredibly powerful because you can write them in several different programming languages; you do not need to create them in an esoteric format or domain-specific language. They’re just functions that operate on the data, record by record. 
# By default, these functions are registered as temporary functions to be used in that specific SparkSession or Context.

# Although you can write UDFs in Scala, Python, or Java, there are performance considerations that you should be aware of. To illustrate this, we’re going to walk through exactly what happens when you create UDF, pass that into Spark, and then execute code using that UDF.

# COMMAND ----------

# Defining a power3 function that takes a number and raises it to a power of three:
def power3(double_value):
  return double_value ** 3
power3(2.0)

# COMMAND ----------

# Creating the DataFrame. 
udfExampleDF = spark.range(5).toDF("num")

# COMMAND ----------

# Registering the function as Spark DataFrame function:
from pyspark.sql.functions import udf
# Registering the function to make it available for DataFrame Function.
power3udf = udf(power3)

# COMMAND ----------

# Calling power3udf on the DataFrame
from pyspark.sql.functions import col
udfExampleDF.select(power3udf(col("num"))).show(2)

# COMMAND ----------

# Registering the function as UDF.
# Register this UDF as a Spark SQL function. This is valuable because it makes it simple to use this function within SQL as well as across languages.

from pyspark.sql.types import IntegerType, DoubleType, LongType
spark.udf.register("power3py", power3, LongType())

# COMMAND ----------

# If you specify the type that doesn’t align with the actual type returned by the function, Spark will not throw an error but will just return null to designate a failure.
udfExampleDF.selectExpr("power3py(num)").show(5)

# You can replace the return type to IntegerType / DoubleType and check the result.

# COMMAND ----------

# Java or Scala UDF's:

# If the function is written in Scala or Java, you can use it within the Java Virtual Machine (JVM). This means that there will be little performance penalty aside from the fact that you can’t take advantage of code generation capabilities that Spark has for built-in functions.

# COMMAND ----------

# Python or R UDF's:

# If the function is written in Python, something quite different happens. Spark starts a Python process on the worker, serializes all of the data to a format that Python can understand (remember, it was in the JVM earlier), executes the function row by row on that data in the Python process, and then finally returns the results of the row operations to the JVM and Spark.

# COMMAND ----------

# Conclusion:

# Starting this Python process is expensive, but the real cost is in serializing the data to Python. 
# This is costly for two reasons: 
# it is an expensive computation. 
# Also after the data enters Python, Spark cannot manage the memory of the worker. This means that you could potentially cause a worker to fail if it becomes resource constrained (because both the JVM and Python are competing for memory on the same machine). 

# We recommend that you write your UDFs in Scala or Java—the small amount of time it should take you to write the function in Scala will always yield significant speed ups, and on top of that, you can still use the function from Python!

# COMMAND ----------



# COMMAND ----------


