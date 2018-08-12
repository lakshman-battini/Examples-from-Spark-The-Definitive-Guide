# Databricks notebook source
# A join brings together two sets of data, the left and the right, by comparing the value of one or more keys of the left and right and evaluating the result of a join expression that determines whether Spark should bring together the left set of data with the right set of data.

# The join expression determines whether two rows should join, the join type determines what should be in the result set. There are a variety of different join types available in Spark for you to use:

# Inner joins (keep rows with keys that exist in the left and right datasets)

# Outer joins (keep rows with keys in either the left or right datasets)

# Left outer joins (keep rows with keys in the left dataset)

# Right outer joins (keep rows with keys in the right dataset)

# Left semi joins (keep the rows in the left, and only the left, dataset where the key appears in the right dataset)

# Left anti joins (keep the rows in the left, and only the left, dataset where they do not appear in the right dataset)

# Natural joins (perform a join by implicitly matching the columns between the two datasets with the same names)

# Cross (or Cartesian) joins (match every row in the left dataset with every row in the right dataset)

# COMMAND ----------

# Let’s create some simple datasets that we can use in our examples:
# Person DataFrame using lists.
person = spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Matei Zaharia", 1, [500, 250, 100]),
    (2, "Michael Armbrust", 1, [250, 100])])\
  .toDF("id", "name", "graduate_program", "spark_status")
# Graduate program DataFrame using lists.
graduateProgram = spark.createDataFrame([
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")])\
  .toDF("id", "degree", "department", "school")
# Status DataFrame using lists.
sparkStatus = spark.createDataFrame([
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")])\
  .toDF("id", "status")

# COMMAND ----------

# Let’s register these as tables so that we use them throughout this notebook:
person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

# COMMAND ----------

# Inner joins evaluate the keys in both of the DataFrames or tables and include (and join together) only the rows that evaluate to true.

# In the following example, we join the graduateProgram DataFrame with the person DataFrame to create a new DataFrame:

joinExpression = person["graduate_program"] == graduateProgram['id']

# Keys that do not exist in both DataFrames will not show in the resulting DataFrame.

# COMMAND ----------

# Inner joins are the default join, so we just need to specify our left DataFrame and join the right in the JOIN expression:

person.join(graduateProgram, joinExpression).show(5)

# COMMAND ----------

# We can also specify this explicitly by passing in a third parameter, the joinType:
joinType = "inner"

# COMMAND ----------

# Performing the join, specifying the joint Type. Default is "inner" join.
person.join(graduateProgram, joinExpression, joinType).show(5)

# COMMAND ----------

# Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins together) the rows that evaluate to true or false. 
# If there is no equivalent row in either the left or right DataFrame, Spark will insert null:

# COMMAND ----------

joinType = "outer"

# COMMAND ----------

# Joining two DataFrames by joinType
person.join(graduateProgram, joinExpression, joinType).show(5)

# COMMAND ----------

# Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the left DataFrame as well as any rows in the right DataFrame that have a match in the left DataFrame. If there is no equivalent row in the right DataFrame, Spark will insert null:

# COMMAND ----------

# Specifying the join type as 'left_outer'
joinType = "left_outer"

# COMMAND ----------

# Performing the Left outer join.
graduateProgram.join(person, joinExpression, joinType).show()
# Notice the null values in Person DataFrame.

# COMMAND ----------

# Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the right DataFrame as well as any rows in the left DataFrame that have a match in the right DataFrame. If there is no equivalent row in the left DataFrame, Spark will insert null:

# COMMAND ----------

# Specifying the join type as right_outer
joinType = "right_outer"

# COMMAND ----------

# Notice the null values for Graduate_program DataFrames.
person.join(graduateProgram, joinExpression, joinType).show()

# COMMAND ----------

# Semi joins are a bit of a departure from the other joins. They do not actually include any values from the right DataFrame. 
# They only compare values to see if the value exists in the second DataFrame. 
# If the value does exist, those rows will be kept in the result, even if there are duplicate keys in the left DataFrame. 
# Think of left semi joins as filters on a DataFrame, as opposed to the function of a conventional join:

# COMMAND ----------

# Specifying the "left_semi" join
joinType = "left_semi"

# COMMAND ----------

# Performing the Left_semi join
graduateProgram.join(person, joinExpression, joinType).show()

# Notice the result DataFrame contains only Left DataFrame ie "graduateProgram" values.

# COMMAND ----------

# Left anti joins are the opposite of left semi joins. 
# Like left semi joins, they do not actually include any values from the right DataFrame. They only compare values to see if the value exists in the second DataFrame. However, rather than keeping the values that exist in the second DataFrame, they keep only the values that do not have a corresponding key in the second DataFrame. 
# Think of anti joins as a NOT IN SQL-style filter:

# COMMAND ----------

# Specifying the "left_anti" join
joinType = "left_anti"

# COMMAND ----------

# Performing the "left_anti" join
graduateProgram.join(person, joinExpression, joinType).show()

# Notice the result DataFrame contains only Left DataFrame.

# COMMAND ----------

# Natural joins make implicit guesses at the columns on which you would like to join. It finds matching columns and returns the results. Left, right, and outer natural joins are all supported.

# COMMAND ----------

# Cross-joins in simplest terms are inner joins that do not specify a predicate. 
# Cross joins will join every single row in the left DataFrame to ever single row in the right DataFrame. 
# This will cause an absolute explosion in the number of rows contained in the resulting DataFrame. 
# If you have 1,000 rows in each DataFrame, the cross-join of these will result in 1,000,000 (1,000 x 1,000) rows. 

# COMMAND ----------

# Specifying the join type as 'cross'
joinType = "cross"
# Performing the "cross join".
graduateProgram.join(person, joinExpression, joinType).show()
# Be careful when you cross join on large datasets, it causes the explosion in the number of rows contained in result DF.

# COMMAND ----------

# When performing joins, there are some specific challenges and some common questions that arise. 
# The rest of the notebook will provide answers to these common questions and then explain how, at a high level, Spark performs joins.

# COMMAND ----------

# Even though this might seem like a challenge, it’s actually not. Any expression is a valid join expression, assuming that it returns a Boolean:
# For ex: Joining by the id from the person DF and spark_status array from the SparkStatus DF.
from pyspark.sql.functions import expr
# As spark_status is an array column, using an expression to check if the spark_status contains an id value.
person.withColumnRenamed("id", "personId")\
  .join(sparkStatus, expr("array_contains(spark_status, id)")).show()

# COMMAND ----------

# One of the tricky things that come up in joins is dealing with duplicate column names in your results DataFrame.
# In a DataFrame, each column has a unique ID within Spark’s SQL Engine, Catalyst. This unique ID is purely internal and not something that you can directly reference. 
# This makes it quite difficult to refer to a specific column when you have a DataFrame with duplicate column names.

# COMMAND ----------

# This can occur in two distinct situations:

# The join expression that you specify does not remove one key from one of the input DataFrames and the keys have the same column name
# Two columns on which you are not performing the join have the same name

# COMMAND ----------

# Let’s create a problem dataset that we can use to illustrate these problems:
gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")

# COMMAND ----------

# Joining expression
joinExpr = gradProgramDupe["graduate_program"] == person["graduate_program"]

# COMMAND ----------

# Joining using the above joinExpression. By default its equi-join.
person.join(gradProgramDupe, joinExpr).show()

# Not below that the result DF contains duplicate graduate_program columns. ( as both DataFrames contains the same column name)

# COMMAND ----------

# The problem arises when you try to access one of the graduate_program column

person.join(gradProgramDupe, joinExpr).select("graduate_program").show()

# COMMAND ----------

# APPROACH 1: DIFFERENT JOIN EXPRESSION

# When you have two keys that have the same name, probably the easiest fix is to change the join expression from a Boolean expression to a string or sequence. This automatically removes one of the columns for you during the join:

person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()

# COMMAND ----------

# APPROACH 2: DROPPING THE COLUMN AFTER THE JOIN
# Another approach is to drop the offending column after the join. When doing this, we need to refer to the column via the original source DataFrame.
# We can do this if the join uses the same key names or if the source DataFrames have columns that simply have the same name:

person.join(gradProgramDupe, joinExpr).drop(person["graduate_program"]).select("graduate_program").show()

# COMMAND ----------

# APPROACH 3: RENAMING A COLUMN BEFORE THE JOIN
# We can avoid this issue altogether if we rename one of our columns before the join:

# Renaming the grad_id column to id.
gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
joinExpr = person["graduate_program"] == gradProgram3["grad_id"]
# Joining the DataFrames.
person.join(gradProgram3, joinExpr).show()

# COMMAND ----------

# Analysing the JOIN plan

joinExpr = person["graduate_program"] == graduateProgram["id"]
person.join(graduateProgram, joinExpr).explain()

# Notice the Exchange Hashpartitioning stage for the join, which represents the all node-to-node communication.

# COMMAND ----------

# With the DataFrame API, we can explicitly give the optimizer a hint that we would like to use a broadcast join by using the correct function around the small DataFrame in question.

from pyspark.sql.functions import broadcast
joinExpr = person["graduate_program"] == graduateProgram["id"]
person.join(broadcast(graduateProgram), joinExpr).explain()

# Notice the BroadcastHashJoin, which represents the Broadcast join.

# COMMAND ----------

# Broadcast Join:
# When the table is small enough to fit into the memory of a single worker node, with some breathing room of course, we can optimize our join. 
# Spark Driver will replicate small DataFrame onto every worker node in the cluster (be it located on one machine or many). Now this sounds expensive. However, what this does is prevent us from performing the all-to-all communication during the entire join process. Instead, we perform it only once at the beginning and then let each individual worker node perform the work without having to wait or communicate with any other worker node.
# This greatly reduces the communication between node-node and optimizes the join.

# COMMAND ----------

# Also partition your data correctly prior to a join, so that you can end up with much more efficient execution because even if a shuffle is planned, if data from two different DataFrames is already located on the same machine, Spark can avoid the shuffle. 

# COMMAND ----------

# Congratulations!!

# You have learnt the different joins possible in Spark also you have learnt the different optimizations that can be done for Joins.
