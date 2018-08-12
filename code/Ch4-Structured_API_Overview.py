# Databricks notebook source
# Internally spark uses an engine called Catalyst that maintains its own type information through the planning and processing of work.
# Within the Structured APIs, there are two more APIs, the “untyped” DataFrames and the “typed” Datasets.

# DataFrames:
# To say that DataFrames are untyped is aslightly inaccurate; they have types, but Spark maintains them completely and only checks whether those types line up to those specified in the schema at runtime.
# To Spark (in Scala), DataFrames are simply Datasets of Type Row. The “Row” type is Spark’s internal representation of its optimized in-memory format for computation. 
# Row format makes for highly specialized and efficient computation because rather than using JVM types, which can cause high garbage-collection and object instantiation costs, Spark can operate on its own internal format without incurring any of those costs.

# Datasets:
# Datasets check whether types conform to the specification at compile time.

# COMMAND ----------

# Working with Types.
from pyspark.sql.types import *
b = ByteType()
