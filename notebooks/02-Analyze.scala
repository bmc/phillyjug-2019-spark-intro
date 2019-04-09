// Databricks notebook source
// MAGIC %md 
// MAGIC #![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Analysis and Visualization

// COMMAND ----------

// MAGIC %run ./Defs

// COMMAND ----------

// MAGIC %md
// MAGIC Let's open our Parquet file again.

// COMMAND ----------

val df = spark.read.parquet(ParquetPath)

// COMMAND ----------

// MAGIC %md
// MAGIC What span of time does the data cover?

// COMMAND ----------

import org.apache.spark.sql.functions._

display(df.select(min($"timestamp"), max($"timestamp")))

// COMMAND ----------

// MAGIC %md
// MAGIC Does it cover all months?
// MAGIC 
// MAGIC **Hint**: There are loads of useful functions that can help us.
// MAGIC <https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$>

// COMMAND ----------

display(
  df.select(date_format($"timestamp", "yyyy-MM").as("yearmonth")).distinct.orderBy($"yearMonth")
)

// COMMAND ----------

// MAGIC %md
// MAGIC Okay, looks pretty good. It might be a sampling, but it's good enough for us.
// MAGIC 
// MAGIC Let's take another took at some of the data.

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC If you prefer to work with a Scala type, rather than column names, you can cast the result to a `case class`.
// MAGIC 
// MAGIC There are some potential performance implications, which we can discuss.
// MAGIC 
// MAGIC * Casting to a case class allows you to use traditional lambdas with your data.
// MAGIC * But, because Spark stores the data internal in an efficient column-oriented form (Tungsten), it has to deserialize
// MAGIC   the data for each row into an object to pass to your lambda. (It does so with custom-built encoders and decoders.)
// MAGIC * If you never use a lambda, you never pay that penalty.
// MAGIC 
// MAGIC Let's do that, just for demonstration purposes. It helps to create the class from the schema.
// MAGIC 
// MAGIC (Think of a Scala `case class` as simple data container—roughly, the Scala equivalent of a Java Bean, but with a _lot_ less coding.)

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Column names are matched to class field names, so we have to make sure they're the same.

// COMMAND ----------

case class CrimeDataElement(longitude:           Double,
                            latitude:            Double,
                            id:                  Int,
                            timestamp:           java.sql.Timestamp,
                            streetLocation:      String,
                            ucrGeneralCode:      Int,
                            crimeType:           String)
// Technically, what we're creating here is a Dataset type.
val ds = df.as[CrimeDataElement]

// COMMAND ----------

display(ds)

// COMMAND ----------

// MAGIC %md
// MAGIC What kinds of crimes are recorded? Note that you can _still_ use this Dataset with the DataFrame API.

// COMMAND ----------

display(ds.select($"crimeType").groupBy("crimeType").count.orderBy($"crimeType"))

// COMMAND ----------

// MAGIC %md
// MAGIC Interesting. Some crimes aren't classified as anything. Do they have an FBI code, but no text?

// COMMAND ----------

display(ds.filter($"crimeType".isNull).select($"ucrGeneralCode").groupBy($"ucrGeneralCode").count)

// COMMAND ----------

// MAGIC %md
// MAGIC At this point, in a production environment, we might elect to go back and update our ETL process to eliminate these values.
// MAGIC 
// MAGIC For now, though, let's continue our analysis. We'll just ignore those rows by creating another DataFrame that filters them out.

// COMMAND ----------

val dfCleaner = df.filter(! ($"crimeType".isNull || $"ucrGeneralCode".isNull))
println(df.count() - dfCleaner.count())

// COMMAND ----------

// MAGIC %md
// MAGIC What's the category of crime that occurs most often?

// COMMAND ----------

display(
  dfCleaner.select($"crimeType").groupBy($"crimeType").count.orderBy($"count".desc)
)

// COMMAND ----------

// MAGIC %md
// MAGIC Interesting the "DRIVING UNDER THE INFLUENCE" is capitalized. Is that some kind of police data commentary?

// COMMAND ----------

// MAGIC %md
// MAGIC At the risk of offending some people (and piquing the interest of others?), let's analyze vice-related crimes, with the goal of graphing them on a map.
// MAGIC 
// MAGIC For most of us, this exercise can tell us what parts of the city to avoid, if we're not interested in being solicited.
// MAGIC 
// MAGIC (For anyone who isn't in "most of us": Dude, this isn't Nevada.)

// COMMAND ----------

display(
  dfCleaner.select($"ucrGeneralCode").filter($"crimeType" === "Prostitution and Commercialized Vice").distinct
)

// COMMAND ----------

// MAGIC %md
// MAGIC Okay, only one UCR code applies, so we can type less.

// COMMAND ----------

val dfVice = dfCleaner.filter($"ucrGeneralCode" === 1600)

// COMMAND ----------

// MAGIC %md
// MAGIC Is there consistency, year from year?

// COMMAND ----------

display(
  dfVice.select(year($"timestamp").as("year")).groupBy($"year").count.orderBy($"year")
)

// COMMAND ----------

// MAGIC %md
// MAGIC What about month by month? Do we see more of this activity in certain months? Let's use the last three
// MAGIC full years for which we have data (2016, 2017, 2018). We'll graph each one independently (i.e., separate cells.)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
// Helper function
def rollUpByMonth(df: DataFrame, theYear: Int) = {
  df
    .filter(year($"timestamp") === theYear)
    .select(date_format($"timestamp", "MM-MMM").as("month"))
    .groupBy("month")
    .count
    .orderBy($"month")
}

// COMMAND ----------

display(rollUpByMonth(dfVice, 2016))

// COMMAND ----------

display(rollUpByMonth(dfVice, 2017))

// COMMAND ----------

display(rollUpByMonth(dfVice, 2018))

// COMMAND ----------

// MAGIC %md
// MAGIC As we might have expected, there seem to be fewer of these crimes in colder months (though, to be really accurate, we'd have
// MAGIC to correlate this data with daily weather data.)
// MAGIC 
// MAGIC What about crime, in general? Does that vary by month? We'll use just 2018 here.

// COMMAND ----------

display(
  rollUpByMonth(dfCleaner, 2018)
)

// COMMAND ----------

// MAGIC %md
// MAGIC Less variation in overall crime, month to month—perhaps because most crimes don't require the perpetrator to drop his pants?
// MAGIC 
// MAGIC Let's visualize the vice data geographically, for 2018. Google Maps supports up to 400 geodata points. How many vice "incidents" do we have in 2018?
// MAGIC 
// MAGIC (We could look above, but I'm lazy.)

// COMMAND ----------

val dfVice2018 = dfVice.filter(year($"timestamp") === 2018)
val total = dfVice2018.count
println(s"\ntotal=$total\n")

// COMMAND ----------

// MAGIC %md
// MAGIC Too many. No month has more than 400, so we have two options:
// MAGIC 
// MAGIC - Choose the month with the largest number of incidents.
// MAGIC - Get a random sampling of 400 data points for the entire year.
// MAGIC 
// MAGIC I'm going with the second option, because I'd like as close to 400 as possible. The graph is more fun that way.
// MAGIC 
// MAGIC (But if you'd prefer to graph the data from just one month, here's a quick way to figure which month it is.)

// COMMAND ----------

case class LargestMonth(monthNum: Int, count: BigInt)
val biggest = dfVice2018
    .filter(! $"latitude".isNull)
    .filter(! $"longitude".isNull)
    .select(month($"timestamp").as("monthNum"))
    .groupBy("monthNum")
    .count
    .orderBy($"count".desc)
    .as[LargestMonth]
    .collect()
    .take(1)
    .head

// COMMAND ----------

val fraction = 400.0 / dfVice2018.count.toDouble
val sample = dfVice2018
  .filter(! $"latitude".isNull)
  .filter(! $"longitude".isNull)
  .sample(withReplacement = false, fraction = fraction, seed = 345678654334567l)
  .limit(400)
println(sample.count)

// COMMAND ----------

// MAGIC %md
// MAGIC Now we can pull the data back and graph it.

// COMMAND ----------

case class Location(latitude: Double, longitude: Double)
val locations = sample.select($"latitude", $"longitude").as[Location].collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Plot the ones we got.

// COMMAND ----------

val html = s"""
<html>
  <head>
  <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
  <script>
    google.charts.load('current', { 'packages': ['corechart', 'map'] });
    google.charts.setOnLoadCallback(drawMap);

    function drawMap() {
      var array = [
      ['Lat', 'Long', ''],
${locations.map { l => s"[${l.latitude}, ${l.longitude}, 'normal']" }.mkString(",\n") }      
    ];
      var data = google.visualization.arrayToDataTable(array);
/*
      var data = new google.visualization.DataTable();

      data.addColumn('number', 'Latitude');
      data.addColumn('number', 'Longitude');
      data.addColumn('string', 'Marker');
      data.addRows([
${locations.map { l => s"[${l.latitude}, ${l.longitude}, 'alt']" }.mkString(",\n") }
      ]);
*/
    var options = {
      showTooltip: true,
      showInfoWindow: true,
      zoomLevel: 12,
      height: 750
/*
      icons: {
        alt: {
          normal: 'http://icons.iconarchive.com/icons/icons8/windows-8/48/Maps-Marker-icon.png',
          selected: 'http://icons.iconarchive.com/icons/icons8/windows-8/48/Maps-Marker-icon.png'
        }
      }
*/      
    };

    var map = new google.visualization.Map(document.getElementById('chart_div'));

    map.draw(data, options);
  };
  </script>
  </head>
  <body>
    <div id="chart_div"></div>
  </body>
</html>
"""
displayHTML(html)
//println(html)

// COMMAND ----------

// MAGIC %md
// MAGIC How do the last three years compare, month by month? (More visualization.)

// COMMAND ----------

val dfVice2017 = dfVice.filter(year($"timestamp") === 2017)
val dfVice2016 = dfVice.filter(year($"timestamp") === 2016)

val combined = (
  dfVice2016
    .select(year($"timestamp").as("year"), month($"timestamp").as("monthNum"))
    .groupBy("year", "monthNum")
    .count
  union  
  dfVice2017
    .select(year($"timestamp").as("year"), month($"timestamp").as("monthNum"))
    .groupBy("year", "monthNum")
    .count
  union
  dfVice2018
    .select(year($"timestamp").as("year"), month($"timestamp").as("monthNum"))
    .groupBy("year", "monthNum")
    .count
)

display(combined.orderBy("year", "monthNum"))

// COMMAND ----------

// MAGIC %md
// MAGIC Obviously, we could do the same with any other crime.

// COMMAND ----------

// MAGIC %md
// MAGIC Next up: Some [predictions]($./03-Predict)
