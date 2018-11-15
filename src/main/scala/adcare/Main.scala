package adcare

import adcare.ADcareRandomForest
import adcare.ADcareLogisticRegression

import helpers.{CSVExport, DataBuilder, DataIndexer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Main extends App {
    var pathToDataJSON = "./data/data-students.json"

    // Parse command lines args
    if(args.length > 0) {
        if(args(0) == "help" || args(0) == "usage") {
            println("Usage: path/to/data.json")
            System.exit(0)
        }
        pathToDataJSON = args(0)
    }

    val (dataCleaned, sc, spark, sqlContext) = DataBuilder.getData(pathToDataJSON)
    val myRDD : RDD[Row] = dataCleaned.rdd

    // Getting the pipeline
    val indexerPipeline = DataIndexer.preProcessing(dataCleaned, sc, spark, sqlContext)

    // TODO: train or predict ? 
    val res = ADcareLogisticRegression.logisticRegression(indexerPipeline, dataCleaned, sc, spark, sqlContext)
    CSVExport.export(res, "res.csv")

    sc.stop()
}
