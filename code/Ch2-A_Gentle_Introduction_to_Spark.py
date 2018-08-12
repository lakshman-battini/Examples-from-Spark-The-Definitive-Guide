# Databricks notebook source
# This notebook presents a gentle introduction to Spark, in which we will walk through the basic examples using Spark’s structured APIs, so that you can begin using Spark right away.

# Let's get started with some basic background information:

# The cluster of machines that Spark will use to execute tasks is managed by a cluster manager like Spark’s standalone cluster manager, YARN, or Mesos. 
# We then submit Spark Applications to these cluster managers, which will grant resources to our application so that we can complete our work.
# Spark Applications consist of a driver process and a set of executor processes. 
# The driver process runs your main() function, sits on a node in the cluster, and is responsible for three things: maintaining information about the Spark Application; responding to a user’s program or input; and analyzing, distributing, and scheduling work across the executors (discussed momentarily). 
# The driver process is absolutely essential—it’s the heart of a Spark Application and maintains all relevant information during the lifetime of the application.
# The executors are responsible for actually carrying out the work that the driver assigns them. 
# This means that each executor is responsible for only two things: executing code assigned to it by the driver, and reporting the state of the computation on that executor back to the driver node.

# COMMAND ----------

# This notebook is written in Databricks community edition, Databricks cluster is initialized with Spark.
# Thi notebook written in Python.
# Checking if the Spark Session is instantiated in Notebook successfully
spark
# It should show the SparkSession object with some hascode address.
# The SparkSession instance is the way Spark executes user-defined manipulations across the cluster. There is a one-to-one correspondence between a SparkSession and a Spark Application.

# COMMAND ----------

# Creating the Spark DataFrame from range of numbers
# spark.range creates a new RDD of int containing elements from start to end (exclusive). If called with a single argument, the argument is interpreted as end, and start is set to 0.
# toDF function is used to convert DataFrame from RDD.

myRange = spark.range(1000).toDF("number")

# A DataFrame is the most common Structured API and simply represents a table of data with rows and columns. The list that defines the columns and the types within those columns is called the schema.

# COMMAND ----------

# Transformations on the DataFrames.

# In Spark, the core data structures are immutable, meaning they cannot be changed after they’re created. This might seem like a strange concept at first: if you cannot change it, how are you supposed to use it? To “change” a DataFrame, you need to instruct Spark how you would like to modify it to do what you want. These instructions are called transformations.

divisBy2 = myRange.where("number % 2 = 0")

# Notice that the above return no output. This is because we specified only an abstract transformation, and Spark will not act on transformations until we call an action.

# COMMAND ----------

# Lazy Evaluation:

# Spark will wait until the very last moment to execute the graph of computation instructions. 
# In Spark, instead of modifying the data immediately when you express some operation, you build up a plan of transformations. 
# By waiting until the last minute to execute the code, Spark compiles this plan from your raw DataFrame transformations to a streamlined physical plan that will run as efficiently as possible across the cluster. 
# This provides immense benefits because Spark can optimize the entire data flow from end to end.

# COMMAND ----------

# Actions:

# An action instructs Spark to compute a result from a series of transformations
# The simplest action is count, which gives us the total number of records in the DataFrame:
divisBy2.count()

# COMMAND ----------

# In this example, We will use Spark to Analyse the Flight Dataset. 
# This Dataset is provided in the Spark-The-Definitive-Guide repo, uploaded the Dataset to Databricks File system at  
inputPath = "dbfs:/data/flight-data/csv/2015-summary.csv"
# Dataset is downloaded from the Spark-The_definitive-guide github.

# COMMAND ----------

# Creating the DataFrame from the above CSV files.
# Using a DataFrameReader to read the data that is associated SparkSession. In doing so, we will specify the file format as well as any options we want to specify.
# Specify inferSchema to True, to let Spark to infer the Schema from the Dataset.
# Specify header to True, as the data contains the header information as a first row in CSV files.
flightData2015 = spark\
  .read\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .csv(inputPath)

# Reading data is a transformation, Spark peaks at only a couple of rows of data and try to guess the schema of Dataframe.

# COMMAND ----------

# Calling an action on the Dataframe, count is to count the number of rows in Dataframe.

flightData2015.count()

# COMMAND ----------

# Spark will Build up a plan for how it executes across the cluster. We can view plan using explain()

flightData2015.sort("count").explain()

# Read the explain plan from bottom to top, top being the end result, and the bottom being the source(s) of data.
# In this case, take a look at the first keywords. You will see sort, exchange, and FileScan. That’s because the sort of our data is actually a wide transformation because rows will need to be compared with one another.

# COMMAND ----------

# By Default, spark creates 200 partitions when we shuffle the data.
# Let’s set this value to 5 to reduce the number of the output partitions from the shuffle:

spark.conf.set("spark.sql.shuffle.partitions", "5")

# COMMAND ----------

# Take / collect 2 rows from the Dataframe, sorted by count in ascending order.

flightData2015.sort("count").take(2)

# COMMAND ----------

# Spark can run the same transformations, regardless of the language, in the exact same way. 
# You can express your business logic in SQL or DataFrames (either in R, Python, Scala, or Java) and Spark will compile that logic down to an underlying plan (that you can see in the explain plan) before actually executing your code.
# With Spark SQL, you can register any DataFrame as a table or view (a temporary table) and query it using pure SQL. 
# There is no performance difference between writing SQL queries or writing DataFrame code, they both “compile” to the same underlying plan that we specify in DataFrame code.

flightData2015.createOrReplaceTempView("flight_data_2015")

# COMMAND ----------

# We can query the table using SQL, To do so, we’ll use the spark.sql that conveniently returns a new DataFrame.

# Counting the number of Dest_country_name values in the table.
sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

sqlWay.explain()

# COMMAND ----------

# Performing the same operation in DataFrame way.
dataFrameWay = flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .count()

dataFrameWay.explain()

# Observe the Physical plan for both SQL and Dataframe way operations are the same.

# COMMAND ----------

# Finding the maximum number of flights to and from to any given location.

spark.sql("SELECT max(count) from flight_data_2015").take(1)

# COMMAND ----------

# Lets find the top 5 Destincation countries in the dataset.

maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()

# COMMAND ----------

# Solving the same problem in DataFrame way

# Top 5 Destincation countries in the dataset.
from pyspark.sql.functions import desc

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .show()

# COMMAND ----------

# Congratulations !!

# You have learnt the basics of Apache Spark. This notebook explained you about Transformations and Actions, and how Spark executes a DAG of Transformations in order to optimize the execution plan on DataFrames.
