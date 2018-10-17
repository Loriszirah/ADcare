package main
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import scala.tools.nsc.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.nio.file.{Paths, Files}

object ADcare extends App {

  Main()
  def Main() = {
    // Initialization of the spark environment
    // Enable only the log for the errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ADcare").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("ADcare")
        .config("spark.master", "local")
        .getOrCreate()

    if(!Files.exists(Paths.get("./data/jsonFormat.json"))){
      // Load the json file.
      val jsonFile = sc.textFile("./data/data-students.json")
      // read the json and format the lines to separate the line by a '\'. Return an MapPartitionRDD
      val jsonPre = jsonFile.map(line => line.replace("} ", "}\n"))

      val outputPath = "./data/jsonFormat.json"
      jsonPre.saveAsTextFile(outputPath + "-tmp")
      import org.apache.hadoop.fs.Path
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.rename(new Path(outputPath + "-tmp/part-00000"), new Path(outputPath))
      fs.delete(new Path(outputPath  + "-tmp"), true)

    }
    // Read the file formatted
    val jsonFile2 = spark.read.json("./data/jsonFormat.json")
    // Show some lines of the file
    jsonFile2.show(20)
    // Select distinct values of appOrSite column
    jsonFile2.select("appOrSite").distinct().show()
    // Counting the number of each iterations
    jsonFile2.groupBy("appOrSite").count().show()
    // Close the spark session
    sc.stop()
  }
}
