# Examples from Spark: The Definitive Guide

Spark: The Definitive Guide is a comprehensive guide, written by the creators of the open-source cluster-computing framework. With an emphasis on improvements and new features in Spark 2.0, authors Bill Chambers and Matei Zaharia break down Spark topics into distinct sections, each with unique goals.

This repository contains the code examples and solutions for the problems from Spark: The Definitive Guide book.

This repository can be used as a Quick Reference for the Spark API's, examples are organized in the same way as in Book.

# Data used in the examples

Examples in this repo use the Data provided in the [Spark: The Definitive Guide Repo](https://github.com/databricks/Spark-The-Definitive-Guide/tree/master/data).

# Running the code

Currently this repo provides the examples in Python, you can import them to Notebook and experiment with different data & options. 

This repo also provides Notebooks in python, this can be used as a Quick Reference, the output for each API is also provided for reference.

Work in progress to provide the examples in other languages.

Databricks cloud platform is used to run the examples, you can run them on any cloud platform or in your local machine.

### Instructions for importing

1. Navigate to the notebook you would like to import

For instance, you might go to [this page](https://github.com/lakshman-battini/Examples-from-Spark-The-Definitive-Guide/blob/master/code/Ch2-A_Gentle_Introduction_to_Spark.py). Once you do that, you're going to need to navigate to the **RAW** version of the file and save that to your Desktop. You can do that by clicking the **Raw** button. *Alternatively, you could just clone the entire repository to your local desktop and navigate to the file on your computer*.

2. Upload the notebook to Databricks

Read [the instructions](https://docs.databricks.com/user-guide/notebooks/index.html#import-a-notebook) here. Simply open the Databricks workspace and go to import in a given directory. From there, navigate to the file on your computer to upload it.

3. You're almost ready to go!

Now you just need to simply run the notebooks! Attach the notebook to the cluster.

4. Replacing the data path in each notebook

Examples in the repo refer to the data at: `dbfs:/data/` folder. If you upload the data at different path, change the path in all the notebooks.


 

