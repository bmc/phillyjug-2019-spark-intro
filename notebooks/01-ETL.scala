// Databricks notebook source
// MAGIC %md
// MAGIC #![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) ETL
// MAGIC                     
// MAGIC We'll be using [Philadelphia crime data](https://www.opendataphilly.org/dataset/crime-incidents) from 2015, 2016 and (later) 2017. The data was downloaded for all forces, without outcomes or stop-and-search data.

// COMMAND ----------

// MAGIC %run ./Defs

// COMMAND ----------

display(
  dbutils.fs.ls(s"$RawCSVPath")
)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a quick look at the data. I'm using the Spark DataFrame API, which I will explain further as we go along. For now, just 
// MAGIC treat it as magic.

// COMMAND ----------

val df = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv(RawCSVPath)

// COMMAND ----------

// MAGIC %md
// MAGIC Here's an example of the data.

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC DataFrames have _schemas_. Think of the DataFrames API as a SQL-like _query_ API (which is, in fact, exactly
// MAGIC what it is.)

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC So, how did Spark know the schema of the CSV file?
// MAGIC 
// MAGIC Because I did this:
// MAGIC 
// MAGIC ```
// MAGIC spark.read.option("header", "true").option("inferSchema", "true")
// MAGIC ```

// COMMAND ----------

// DBTITLE 1,Let's do some ETL
// MAGIC %md
// MAGIC **Goal:** Convert this data into Parquet, which is more efficient. We'll also get rid of columns we don't need,
// MAGIC and we'll rename columns 
// MAGIC 
// MAGIC Note that we have some duplication information:
// MAGIC 
// MAGIC - `dispatch_date_time` consolidates the `dispatch_date` and `dispatch_time` values, so we don't need all three.
// MAGIC - `point_x` and `lng` are the same, as are `point_y` and `lat`. So we'll arbitrarily use `lat` and `lng`.
// MAGIC 
// MAGIC **Discussion point**: Why is Parquet more efficient?

// COMMAND ----------

val df2 = df
  // Select only the columns we need, and rename them.
  .select($"lng".as("longitude"),
          $"lat".as("latitude"),
          $"objectid".as("id"),
          $"dispatch_date_time".as("timestamp"),
          $"location_block".as("streetLocation"),
          $"ucr_general".as("ucrGeneralCode"),
          $"text_general_code".as("crimeType"))

// COMMAND ----------

// MAGIC %md
// MAGIC NOTE: The above is _lazy_. We haven't actually done anything yet.
// MAGIC 
// MAGIC At this point, we _could_ do this:
// MAGIC 
// MAGIC ```
// MAGIC df2.write.parquet(/path/to/parquet)
// MAGIC ```
// MAGIC But why not just do it all in one chain?

// COMMAND ----------

df
  // Select only the columns we need, and rename them.
  .select($"lng".as("longitude"),
          $"lat".as("latitude"),
          $"objectid".as("id"),
          $"dispatch_date_time".as("timestamp"),
          $"location_block".as("streetLocation"),
          $"ucr_general".as("ucrGeneralCode"),
          $"text_general_code".as("crimeType"))
  // Get a DataFrameWriter
  .write
  // Write to Parquet, overwriting the Parquet if it's already there.
  .mode("overwrite")
  .parquet(ParquetPath)

// COMMAND ----------

// MAGIC %md
// MAGIC Did we get the same number of rows? (If we didn't, Spark is broken...)

// COMMAND ----------

val dfParquet = spark.read.parquet(ParquetPath)
dfParquet.count()

// COMMAND ----------

df.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Note that the Parquet read was significantly faster.

// COMMAND ----------

// MAGIC %md
// MAGIC Congratulations: ETL complete.
// MAGIC 
// MAGIC Granted, that's a relatively simple ETL process, but look how clean and easy it was.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Let's move on...
// MAGIC 
// MAGIC ...to some [analysis]($./02-Analyze).
