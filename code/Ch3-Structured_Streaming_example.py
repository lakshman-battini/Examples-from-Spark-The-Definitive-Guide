# Databricks notebook source
# Structured Streaming is a high-level API for Stream Processing.
# The same operations in batch mode can run in a streaming fashion. This can reduce latency and allow for incremental processing.

# This example uses Retail dataset, it is time-series data, is arranged by-day, is provided in Spark Definitive guide git repo.
# Creating the static dataframe on the data and capture the Schema.

staticDataFrame = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("dbfs:/data/retail-data/by-day/*.csv")

# COMMAND ----------

# Registering the Dataframe as Table.
staticDataFrame.createOrReplaceTempView("retail_data")
# Assign the dataframe Schema to staticSchema variable.
staticSchema = staticDataFrame.schema
# Checking the Schema
staticSchema

# COMMAND ----------

# Limiting the number of partitions created after shuffle. by-default 200 partitions will be created.
spark.conf.set("spark.sql.shuffle.partitions", "5")

# COMMAND ----------

# Creating the Streaming DataFrame to process the streaming retail-dataset.

streamingDataFrame = spark.readStream\
    .schema(staticSchema)\
    .option("maxFilesPerTrigger", 1)\
    .format("csv")\
    .option("header", "true")\
    .load("dbfs:/data/retail-data/by-day/*.csv")

# Schema is manadatory to be specified for Streaming Dataframe.
# maxFilesPerTrigger is specified as 1, just to demonstrate the Streaming behavior to limit the number of files read at once. In production, you may not specify this option.

# COMMAND ----------

# Checking if the streamingDataFrame is a Streaming DF

streamingDataFrame.isStreaming

# COMMAND ----------

# Finding the customers who made the highest purchases during 1 day.
# This involves grouping the customers by InvoiceDate and calculating the purchase amount by quantity and unit price.
from pyspark.sql.functions import col, window

purchaseByCustomerPerDay = streamingDataFrame.selectExpr("CustomerId", "(Quantity * UnitPrice) as total_cost", "InvoiceDate")\
                          .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
                          .sum("total_cost")

# COMMAND ----------

# The above operation is a lazy operation, we need to call a streaming action to start the execution of Data flow.
# Streaming actions are different from our conventional static action because we’re going to be populating data somewhere instead of just calling something like count.
# calling an action to output the data to In-Memory table, that will get updated for each trigger.

purchaseByCustomerPerDay.writeStream\
    .format("memory")\
    .queryName("customer_purchases")\
    .outputMode("complete")\
    .start()

# COMMAND ----------

# The Stream is started and output data will be updated to 'customer_purchases' table.
# We can query the 'customer_purchases' table using spark.sql queries.

spark.sql("""
  SELECT *
  FROM customer_purchases
  ORDER BY `sum(total_cost)` DESC
  """).show()

# Notice the composition of table changes as we read in more data! With each file, the results might or might not be changing based on the data, because we’re grouping customers.
# You may see few records with null customerId, because the dataset contains the null values.

# COMMAND ----------

# In the above example, we have output the data to In-Memory table.Alternatively we can output the data to the console as well, as below:

#purchaseByCustomerPerHour.writeStream
#    .format("console")
#    .queryName("customer_purchases_2")
#    .outputMode("complete")
#    .start()
