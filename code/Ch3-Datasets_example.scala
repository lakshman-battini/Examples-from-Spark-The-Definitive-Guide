// Databricks notebook source
// Datasets are type-safe version of Structured API's for writing Statically typed code in Java and Scala.
// Datasets API is not available in Python and R, because these languages are Dynamically typed.
// The Dataset class is parameterized with the type of object contained inside: Dataset<T> in Java and Dataset[T] in Scala. For example, a Dataset[Person].

// Defining the Schema using case class.
case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)

// Loading the data from Databricks FS with dbfs: prefix. Spark.read will retun the DataFrame.
val flightsDF = spark.read
  .parquet("dbfs:/data/flight-data/parquet/")

// COMMAND ----------

// Converting the DataFrame to Dataset of Type Flight.
val flights = flightsDF.as[Flight]

// COMMAND ----------

// Collect or Take on Dataset returns the type of Objects not the Rows.
flights
  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
  .map(flight_row => flight_row)
  .take(5)

// COMMAND ----------

// Order of the operators matters. Take returns the Array of type Flight, filter and map on the Array type.
flights
  .take(5)
  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "United States")
  .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))
