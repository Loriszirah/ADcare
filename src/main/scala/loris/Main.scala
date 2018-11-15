package loris

import loris.Regression
import main.{CSVExport, DataBuilder}
import main.Main.{dataCleaned, sc, spark, sqlContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Main extends App {
  var pathToDataJSON = "./data/data-students.json"
  val (dataCleaned, sc, spark, sqlContext) = DataBuilder.getData(pathToDataJSON)

  val myRDD : RDD[Row] = dataCleaned.rdd

  // Getting the pipeline
  val indexerPipeline = Regression.preProcessing(dataCleaned, sc, spark, sqlContext)
  val res = Regression.logisticRegression(indexerPipeline, dataCleaned, sc, spark, sqlContext)
  //CSVExport.export(res, "res.csv")

  sc.stop()
}
