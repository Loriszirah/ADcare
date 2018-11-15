package helpers
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

    def getData(pathToDataJSON: String) : (DataFrame, SparkContext, SparkSession, SQLContext) = {
        val (sc, spark, sqlContext) = initSpark()
        try {
            val data = spark.read.json(pathToDataJSON)
            val dataCleaned = DataCleaner.cleanData(data)
            (dataCleaned, sc, spark, sqlContext)
        } catch {
            case e : Throwable => {
                sc.stop()
                spark.stop()
                throw e
            }
        }
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
