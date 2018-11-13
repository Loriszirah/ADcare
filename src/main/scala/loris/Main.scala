package loris

import loris.LogisticRegression
import main.DataBuilder
import main.Main.{dataCleaned, sc, spark, sqlContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Main extends App {
  var pathToDataJSON = "./data/data-students.json"
  val (dataCleaned, sc, spark, sqlContext) = DataBuilder.getData(pathToDataJSON)

  val myRDD : RDD[Row] = dataCleaned.rdd
  LogisticRegression.logistiRegression(dataCleaned, sc, spark, sqlContext)

  sc.stop()
}
