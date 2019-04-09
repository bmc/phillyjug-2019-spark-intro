// Databricks notebook source
// MAGIC %md 
// MAGIC #![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Future Crime
// MAGIC 
// MAGIC ## Using linear regression to (try to) predict future crimes
// MAGIC 
// MAGIC The objective here is to predict the monthly occurrences of a particular type of crime in the Philadelphia area in 2018, based on the statistics from 2016 and 2017.
// MAGIC 
// MAGIC Let's start by reading in the data and removing all but one crime type.

// COMMAND ----------

// MAGIC %run ./Defs

// COMMAND ----------

display(spark.read.parquet(ParquetPath).select("crimeType").groupBy("crimeType").count.orderBy($"count".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use Thefts. It's the largest category that isn't "something else we didn't feel like codifying properly".

// COMMAND ----------

val CrimeType = "Thefts"

// COMMAND ----------

// MAGIC %md
// MAGIC We're going to create a prediction for 2018, based on 2016 and 2017. In slightly more formal terms:
// MAGIC 
// MAGIC - We'll use 2016 and 2017 data to _train_ our machine learning model. 
// MAGIC - We'll then run predications against 2018 data.
// MAGIC - Finally, we'll compare the predications against the actual 2018 data to see how well the model did.
// MAGIC 
// MAGIC A central question is: Do past crime trends as a reasonable predictor of future crime trends?

// COMMAND ----------

import org.apache.spark.sql.functions._

val df = spark.read.parquet(ParquetPath)
val rawTrainDF = df.filter((year($"timestamp") === 2016) || (year($"timestamp") === 2017))
  .filter($"crimeType" === CrimeType)

val rawTestDF = df.filter(year($"timestamp") === 2018)
  .filter($"crimeType" === CrimeType)


// COMMAND ----------

// Sanity checks
display(rawTrainDF.select(year($"timestamp")).distinct)

// COMMAND ----------

display(rawTestDF.select(year($"timestamp")).distinct)

// COMMAND ----------

// MAGIC %md When we build a machine learning model, we generally split our data into a training set and a test set. The test set mimics unseen data, and gives us an idea of how well our model will generalize to new data.
// MAGIC 
// MAGIC ![trainTest](http://curriculum-release.s3-website-us-west-2.amazonaws.com/images/301/TrainTestSplit.png)
// MAGIC 
// MAGIC In our case, though, we already have some good test data: The existing 2017 data. However, 2017 is only partial data, and we're using the month number as a bucket. The bucket sizes have to match. 2015 and 2016 will have 12 buckets. What about the 2017 data?

// COMMAND ----------

// MAGIC %md
// MAGIC Since we have all 12 months in our test set, we're fine. If we did not, we'd have to pad the test set out with zeros for the
// MAGIC missing months (or we'd get an error).
// MAGIC 
// MAGIC We're interested in the number of incidents (thefts) per month, so we can roll that up.

// COMMAND ----------

import org.apache.spark.sql.functions._

val trainDF = rawTrainDF
  .select(month($"timestamp").alias("month"), year($"timestamp").as("year"))
  .na.drop
  .groupBy("year", "month")
  .count

val testDF = (
  rawTestDF
    .select(month($"timestamp").alias("month"), year($"timestamp").as("year"))
    .na.drop
    .groupBy("year", "month")
    .count
)

display(trainDF.orderBy("year", "month"))

// COMMAND ----------

// MAGIC %md
// MAGIC What we're going to do is:
// MAGIC 
// MAGIC - Create a vector column from our data, consisting of [month, count] pairs. This combined vector column constitutes the _features_ on which we'll train our model.
// MAGIC   We're trying to predict counts for each month.

// COMMAND ----------

import org.apache.spark.sql.functions.{floor, translate, round}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}

val vector =  new VectorAssembler()
  .setInputCols(Array("year", "month"))
  .setOutputCol("features")

val lr = new LinearRegression()
  .setFeaturesCol("features")
  .setLabelCol("count")
  .setPredictionCol("predictedCount")

println(lr.explainParams)
println("-" * 80)

// COMMAND ----------

// MAGIC %md
// MAGIC There are a few different hyperparameters we could change. For example, we could set the regularization parameter to a non-negative value to guard against overfitting to our training data (though if it is too large, we could underfit).
// MAGIC 
// MAGIC We have two operations going on above:
// MAGIC 
// MAGIC - Creating a vector of the two columns we're using to predict.
// MAGIC - Creating a linear regression transformer, which will produce a linear regression model we can use to predict.
// MAGIC 
// MAGIC We need to "chain" them together. Spark has a _pipeline_ capability for that.

// COMMAND ----------

import org.apache.spark.ml.Pipeline

// Our pipeline runs the data through the specified stages, in order, transforming
// the data as it goes. The pipeline is an Estimator, in Spark terminology.
val pipeline = new Pipeline().setStages(Array(vector, lr))

// Calling fit() on an Estimator produces a model. In this case, it's a pipelined model.
// We're training it on 2016 and 2017 data.
val pipelineModel = pipeline.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC We can examine the resulting model—though we might need to do some down-casting.

// COMMAND ----------

pipelineModel.stages

// COMMAND ----------

val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]

println(lrModel.intercept)
println(lrModel.coefficients)

// COMMAND ----------

// MAGIC %md
// MAGIC Finally, we can run predictions against 2018, our training data set.
// MAGIC 
// MAGIC **Note that we already know the answers for 2018.** So, we can easily compare the predicted values with the actual values.

// COMMAND ----------

val predictions = pipelineModel.transform(testDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's evaluate how well our model performed. Normally, we would use a technique like [RMSE](https://en.wikipedia.org/wiki/Root-mean-square_deviation) to evaluate the model, but, with this data set, we can just eyeball a graph.

// COMMAND ----------

display(predictions)

// COMMAND ----------

// MAGIC %md
// MAGIC Okay, that's not so useful. Let's combine the data so we can see the predicted counts and the actual counts for 2018.
// MAGIC 
// MAGIC Some casting is in order.

// COMMAND ----------

display(predictions.select($"month", $"predictedCount").orderBy($"month"))

// COMMAND ----------

val combined = (
  testDF.select(lit("2018").as("year"), $"month", $"count")
  union
  predictions.select(lit("2018-predicted").as("year"), $"month", $"predictedCount".cast("integer").as("count"))
  union
  rawTrainDF.select(year($"timestamp").cast("string").as("year"), month($"timestamp").as("month")).groupBy("year", "month").count
)
display(combined.orderBy("year", "month"))

// COMMAND ----------

// MAGIC %md
// MAGIC For some months, the predictions look pretty good. For others, the predictions are a bit off.
// MAGIC 
// MAGIC Why?
// MAGIC 
// MAGIC - This machine learning pipeline is rather simplistic. There are more complicated things we could do (e.g., give it more data by not pre-selecting
// MAGIC   a crime type, use a time-series analysis, etc.).
// MAGIC - It's also possible that crime data just isn't all that predictive by month.
// MAGIC 
// MAGIC Regardless, this simple example at least gives you a quick overview of the Spark ML Pipeline API.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="width: 100%; text-align: center">
// MAGIC   <img style="align: center" src="https://s3.amazonaws.com/ardentex-spark/spark-philly-jug-2019/images/thats-all-folks.png"/>
// MAGIC </div>

// COMMAND ----------


