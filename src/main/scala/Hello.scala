import javafx.scene.chart.ScatterChart
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types.{CalendarIntervalType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{asc, col, concat, collect_list, desc, expr, hour, lit, mean, second, stddev, to_date, unix_timestamp}
import org.apache.spark.sql.functions._
import org.jfree.chart.plot.Plot
import scalafx.application.JFXApp
import scalafx.scene.chart
import scalafx.scene.chart.ScatterChart
import org.apache.spark.sql.expressions.Window

object Hello {
  def main(args: Array[String]) = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local") //# Change it as per your cluster
      .appName("Spark CSV Reader")
      .getOrCreate;
    import spark.implicits._

    val sc = new StructType()
      .add(StructField("VendorID", IntegerType, false))
      .add(StructField("tpep_pickup_datetime", TimestampType, false))
      .add(StructField("tpep_dropoff_datetime", TimestampType, false))
      .add(StructField("passenger_count", IntegerType, false))
      .add(StructField("trip_distance", FloatType, false))
      .add(StructField("RatecodeID", IntegerType, false))
      .add(StructField("store_and_fwd_flag", StringType, false))
      .add(StructField("PULocationID", StringType, false))
      .add(StructField("DOLocationID", StringType, false))
      .add(StructField("payment_type", StringType, false))
      .add(StructField("fare_amount", StringType, false))
      .add(StructField("extra", StringType, false))
      .add(StructField("mta_tax", StringType, false))
      .add(StructField("tip_amount", StringType, false))
      .add(StructField("tolls_amount", StringType, false))
      .add(StructField("improvement_surcharge", StringType, false))
      .add(StructField("total_amount", StringType, false))
      .add(StructField("congestion_surcharge", StringType, false))


    val taxiDataFrame = spark.read.option("header", "true").schema(sc).csv("yellow_tripdata_2019-09.csv" /*"smaller.csv"*/)
    val tripDistanceDf = taxiDataFrame.select("trip_distance", "PULocationId", "DOLocationId", "total_amount")
    val tripDistanceStatsDf =
      tripDistanceDf
        .agg(
          mean("trip_distance").as("mean"),
          stddev("trip_distance").as("stddev")
        )
        .withColumn("UpperLimit", col("mean") + col("stddev") * 6)
        .withColumn("LowerLimit", col("mean") - col("stddev") * 6)

    val detectOutlier = (values: Column, upperLimit: Column, lowerLimit: Column) =>
      (values < lowerLimit) or (values > upperLimit)

    val outlierDF = tripDistanceDf.join(tripDistanceStatsDf)
      .withColumn("isOutlier", detectOutlier(col("trip_distance"), col("UpperLimit"), col("LowerLimit"))).
      filter(col("isOutlier"))

    val pairs = outlierDF.select(concat(col("PULocationId"), lit(","), col("DOLocationId")).as("PU-DO-pair"), col("PULocationId"), col("DOLocationId"), col("total_amount"))

    val coordinates = pairs.select("PULocationId", "DOLocationID").collect().map(r => (r.get(0).toString.toInt, r.get(1).toString.toInt))
    val coordSet = coordinates.toSet

    type K = (Int, Int)
    val directParent:Map[(Int, Int),Option[(Int, Int)]] = coordinates.map { case (x: Int, y: Int) =>
      val possibleParents = coordSet.intersect(Set((x - 1, y - 1), (x, y - 1), (x - 1, y)))
      val parent = if (possibleParents.isEmpty) None else Some(possibleParents.min)
      ((x, y), parent)
    }.toMap

    // skip unionFind if only concerned with direct neighbors
    def unionFind(key: (Int, Int), map:Map[(Int, Int),Option[(Int, Int)]]): (Int, Int) = {
      val mapValue = map.get(key)
      mapValue.map {
        case None => key
        case Some(parent) => unionFind(parent, map)
      }.getOrElse(key)
    }

    val canonicalUDF = udf((x: Int, y: Int) => unionFind((x, y), directParent))

    // group using the canonical element
    // create column "neighbors" based on x, y values in each group
    val avgDF = pairs.groupBy(canonicalUDF($"PULocationId", $"DOLocationID").alias("canonical")).agg(
      concat_ws("_", collect_list(concat(lit("("), $"PULocationId", lit(","), $"DOLocationID", lit(")")))).alias("neighbors"),
      avg($"total_amount")).drop("canonical")

    avgDF.orderBy("avg(total_amount)").take(10).foreach(println)

    spark.stop()
  }
}
