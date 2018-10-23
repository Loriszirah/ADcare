package main
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.mllib
import org.apache.spark.ml.classification.LogisticRegression

import java.nio.file.{Paths, Files}

import scala.tools.nsc.io.File
import scala.collection.mutable.ListBuffer


object DataBuilder {

    def getData(): (DataFrame, SparkContext, SparkSession, SQLContext) = {
        val (sc, spark, sqlContext) = initSpark()
        if(!Files.exists(Paths.get("./data/jsonFormat.json"))){

            println("===== Creating json file")

            // Load the json file.
            val jsonFile = sc.textFile("./data/data-students.json")
            // read the json and format the lines to separate the line by a '\'. Return an MapPartitionRDD
            val jsonPre = jsonFile.map(line => line.replace("} ", "}\n"))

            val outputPath = "./data/jsonFormat.json"
            jsonPre.saveAsTextFile(outputPath + "-tmp")

            val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
            fs.rename(new Path(outputPath + "-tmp/part-00000"), new Path(outputPath))
            fs.delete(new Path(outputPath  + "-tmp"), true)
            fs.close()
        }
        val data = spark.read.json("./data/jsonFormat.json")

        // Clean data
        val dataCleaned = DataCleaner.cleanData(data)

        (dataCleaned, sc, spark, sqlContext)
    }


    // Init Spark session
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
        (sc, spark, sqlContext)
    }
}
