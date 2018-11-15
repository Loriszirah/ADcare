package helpers
import org.apache.spark.sql.DataFrame
import java.io._
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.TimestampType
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

        data.coalesce(1).write.format("com.databricks.spark.csv").save(outputPath)
    }
}
