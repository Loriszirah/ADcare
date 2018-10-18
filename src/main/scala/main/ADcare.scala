package main
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import scala.tools.nsc.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.nio.file.{Paths, Files}
import org.apache.spark.mllib

object ADcare extends App {

  Main()
  def Main() = {
    val (sc, spark) = initSpark()

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
    val data = spark.read.json("./data/jsonFormat.json")

    // Operations on data
    data.show(20)
    data.select("appOrSite").distinct().show()
    data.groupBy("appOrSite").count().show()

    //Get list of interests
    var interestsList = data.select("interests").distinct().head.getString(0)split(",")

    //Calculate our matching with these interests
    var indexInterests = 0
    interestsList.foreach(int => indexInterests += checkInterest(int))

    //Render choice
    selectChoice(indexInterests)

    sc.stop()
  }


  /**
  Test if we match the interest of the user
  TODO : Evaluate the interests
  */
  def checkInterest(int: String): Int={
      int match {
          case "IAB1" => 1
          case "IAB2" => 2
          case "IAB5" => -1
          case _ => 0
      }

  }

  /**
  See if we have a small, big, or no interest to buy for this user
  TODO : Scale the index on the interests
  */
  def selectChoice(index: Int){
      if(index > 1){
          println("High price")
      }else if(index > 0){
          println("Small price")
      }else{
          println("No buy")
      }
  }
  /**
   Initialization of the Spark environment.
   Return the Spark Context and spark session.
  */
  def initSpark() = {
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
    (sc, spark)
  }
}
