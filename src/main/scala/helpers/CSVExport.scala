package helpers
import org.apache.spark.sql.DataFrame
import java.io._
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DateType, StringType, TimestampType}
import org.apache.spark.sql.functions._

/**
    This object aims to convert a Dataframe in.
    It will add a Label column at the beginning of the original data.
    This column has boolean values
*/
object CSVExport {
    def export(data: DataFrame, outputPath: String) = {
        val spark = SparkSession.builder().getOrCreate()    
        import spark.implicits._

        val format = new java.text.SimpleDateFormat("dd-MM-yyyy_HH-mm")
        val date = format.format(new java.util.Date())
        val newData = data.select( "Label", "network", "appOrSite", "timestamp", "size", "os", "exchange", "bidfloor",
            "publisher", "media", "user", "interests", "type", "city", "impid")
          .withColumn("Label", data("Label").cast(StringType))
          .withColumn("network", data("network").cast(StringType))
        .withColumn("appOrSite", data("appOrSite").cast(StringType))
        .withColumn("timestamp", data("timestamp").cast(StringType))
        .withColumn("size", data("size").cast(StringType))
        .withColumn("os", data("os").cast(StringType))
        .withColumn("exchange", data("exchange").cast(StringType))
        .withColumn("bidfloor", data("bidfloor").cast(StringType))
        .withColumn("publisher", data("publisher").cast(StringType))
            .withColumn("media", data("media").cast(StringType))
            .withColumn("user", data("user").cast(StringType))
        .withColumn("interests", data("interests").cast(StringType))
        .withColumn("type", data("type").cast(StringType))
        .withColumn("city", data("city").cast(StringType))
        .withColumn("impid", data("impid").cast(StringType))
        newData.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("results/" + outputPath + date.toString())
    }
}
