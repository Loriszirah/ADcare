package main
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
object CSVExport{

    def export(data: DataFrame, outputPath: String) = {
        val spark = SparkSession.builder().getOrCreate()    
        import spark.implicits._
        /*data.select("size").show()
        data.withColumn("size", concat(lit("["), concat_ws(",",$"size"),lit("]"))).repartition(1).write.format("com.databricks.spark.csv").save("myFile.csv")
        data.select("size").show()*/
        data.coalesce(1).write.format("com.databricks.spark.csv").save("myFile.csv_"+ new Date())
        /**val bw = new BufferedWriter(new FileWriter(new File(outputPath)))
        var string = new StringBuilder
        string ++= data.columns.toSeq.map(element => "\"" + element + "\"").mkString(",") + "\n"
        var cpt = 0
        def writeLinesInFiles(data: DataFrame, bw: new BufferedWriter, lines: Int): Unit = {

        }
        println(data.count)
        data.foreach(row => {
            cpt += 1
            if(cpt % 20 == 0){
                val toWrite = string.toString
                bw.write(toWrite)
                string = new StringBuilder
            }
            string ++= row.toSeq.map(element => "\"" + element + "\"").mkString(",") + "\n"
        })
        println(string.toString)
        bw.write(string.toString)
        bw.close()*/
    }

}
