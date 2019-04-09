// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <h1 style="font-size: 72px">
// MAGIC   Intro to
// MAGIC   <img style="vertical-align: text-bottom; height: 100px" src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/Apache-Spark-Logo_TM_400px.png"/>
// MAGIC </h1>
// MAGIC 
// MAGIC <div style="float: right">
// MAGIC   <img src="https://s3.amazonaws.com/ardentex-spark/spark-philly-jug-2019/images/bmc.jpg" style="width: 200px"/>
// MAGIC   <br clear="all"/>
// MAGIC   <div style="text-align: right">
// MAGIC     Brian Clapper, Databricks<br/>
// MAGIC     _bmc@clapper.org_<br/>
// MAGIC     _bmc@databricks.com_<br/>
// MAGIC     @brianclapper
// MAGIC   </div>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Agenda
// MAGIC 
// MAGIC This talk is a high-level, usage-oriented overview of Apache® Spark™. We'll cover:
// MAGIC 
// MAGIC * Architecture (really briefly)
// MAGIC * [ETL (of Philadelphia crime data)]($./01-ETL)
// MAGIC * Some data analysis
// MAGIC * A bit of prediction
// MAGIC 
// MAGIC We'll skip more advanced features like Structured Streaming (mostly because of time constraints, and because those
// MAGIC topics take up entire talks all by themselves).
// MAGIC 
// MAGIC Throughout this workshop, I'll be using Databricks, a Spark as a Service product that also happens to help pay my bills.
// MAGIC 
// MAGIC You can import the DBC directly into Databricks' free Spark as a Service
// MAGIC product,
// MAGIC [Databricks Community Edition](https://databricks.com/ce). The DBC is checked into this
// MAGIC GitHub repository: <https://github.com/bmc/phillyjug-2019-spark-intro>
// MAGIC 
// MAGIC The data files used by the notebooks came from the OpenDataPhilly site: <https://www.opendataphilly.org/dataset/crime-incidents>.
// MAGIC 
// MAGIC * Download the incidents CSV file
// MAGIC * Copy it to an S3 bucket
// MAGIC * [Mount the S3 bucket to DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html#mounting-an-s3-bucket)
// MAGIC 
// MAGIC You'll need to change the paths in the notebooks to correspond to your mount point, but everything else should work as is. You should just
// MAGIC be able to edit the `Defs` notebook to fix all the paths.

// COMMAND ----------

// MAGIC %md
// MAGIC # Architecture
// MAGIC 
// MAGIC Before we dive in, let's talk briefly about the Spark architecture, since it'll help to understand what's going on.
// MAGIC 
// MAGIC ## Why Spark?
// MAGIC 
// MAGIC **Discussion point**: Why go to the trouble of using Spark, rather than running ETL or analysis on your laptop?

// COMMAND ----------

// MAGIC %md
// MAGIC # Spark Architecture (very briefly)
// MAGIC 
// MAGIC ## Two levels of parallelism
// MAGIC 
// MAGIC - Across _machines_
// MAGIC - Across cores _within_ a machine
// MAGIC 
// MAGIC <hr/>
// MAGIC 
// MAGIC ![](https://s3.amazonaws.com/ardentex-spark/spark-philly-jug-2019/images/spark-cluster-slots.png)
// MAGIC 
// MAGIC <hr/>
// MAGIC 
// MAGIC ![](https://s3.amazonaws.com/ardentex-spark/spark-philly-jug-2019/images/spark-cluster-tasks.png)

// COMMAND ----------

// MAGIC %md
// MAGIC Okay, let's do some [ETL]($./01-ETL)
