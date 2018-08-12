# Databricks notebook source
# This notebook demonstrates the Basic clustering on data using a Standard K-Means algorithm.
# Spark provides a sophisticated machine learning API for performing a variety of machine learning tasks, from classification to regression, and clustering to deep learning.

# Creating the static dataframe on the retail data

staticDataFrame = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("dbfs:/data/retail-data/by-day/*.csv")

# COMMAND ----------

# The schema of the above dataframe.

staticDataFrame.printSchema()

# COMMAND ----------

staticDataFrame.show(5)

# COMMAND ----------

# Machine Learning algorithms requires that data is represented as Numerical values. But out DF contains the values in different other formats.

# date_format is used to converts a date/timestamp/string to a value of string in the format specified in 2nd arguement.

from pyspark.sql.functions import date_format, col
preppedDataFrame = staticDataFrame\
  .na.fill(0)\
  .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))\
  .coalesce(5)

preppedDataFrame.show(5)

# COMMAND ----------

# Split the data into Training and Test sets. Because this is time-series data, split by arbitrary date.

trainDataFrame = preppedDataFrame.where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame.where("InvoiceDate >= '2011-07-01'")

# COMMAND ----------

# Checking the counts of the split datasets.
trainDataFrame.count()
testDataFrame.count()

# COMMAND ----------

# converting the column day_of_week string value into numerical values using StringIndexer.
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer()\
  .setInputCol("day_of_week")\
  .setOutputCol("day_of_week_index")

# StringIndexer might represents Saturday as 6, and Monday as 1. However, with this numbering scheme, we are implicitly stating that Saturday is greater than Monday. This is obviously incorrect. 

# To fix this, we therefore need to use a OneHotEncoder to encode each of these values as their own column.

# OneHotEncoder encodes each of the value into each column, where each column corresponds to one possible value of one feature.
from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder()\
  .setInputCol("day_of_week_index")\
  .setOutputCol("day_of_week_encoded")

# COMMAND ----------

# All ML algorithms in Spark take Vector type as an input, assembling the numerical values into Vector Type using VectorAssembler.

from pyspark.ml.feature import VectorAssembler

vectorAssembler = VectorAssembler()\
  .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])\
  .setOutputCol("features")

# 3 Features UnitPrice, Quantity and Day_of_week_encoded assemled as Vector.

# COMMAND ----------

# Set this up into a pipeline so that any future data we need to transform can go through the exact same process

from pyspark.ml import Pipeline

transformationPipeline = Pipeline()\
  .setStages([indexer, encoder, vectorAssembler])

# COMMAND ----------

# Fit the TransformationPipeline to the Training DataFrame.
fittedPipeline = transformationPipeline.fit(trainDataFrame)

#  FitterPipeline can now be used to transform our training DataFrame in a consistent and repeatable way.

transformedTraining = fittedPipeline.transform(trainDataFrame)

# COMMAND ----------

# Cache the TransformedTraining Dataframe into Memory, so that we can repeatedly access it at much lower cost.
transformedTraining.cache()
transformedTraining.show(5)

# COMMAND ----------

# Training the model
# In Spark, training machine learning models is a two-phase process. First, we initialize an untrained model, and then we train it.
from pyspark.ml.clustering import KMeans
kmeans = KMeans()\
  .setK(20)\
  .setSeed(1L)

kmModel = kmeans.fit(transformedTraining)

# COMMAND ----------

# Computing the cost according to metrics.
kmModel.computeCost(transformedTraining)

# COMMAND ----------

# Transforming the Test Dataset with the pipeline.
transformedTest = fittedPipeline.transform(testDataFrame)

# Computing the cost on Test Dataset.
kmModel.computeCost(transformedTest)

# The computed cost is high, we can continue to improve this model, layering more preprocessing as well as performing hyperparameter tuning.
