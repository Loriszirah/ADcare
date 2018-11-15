package adcare

import adcare.ADcareRandomForest
import adcare.ADcareLogisticRegression

import helpers.{CSVExport, DataBuilder, DataIndexer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Main extends App {

    var pathToDataJSON = "./data/data-students.json"
    var model = "randomForest"
    var task = "predict"
    var usage = "Usage: path/to/data.json [model] [task]"
    var defaultValues = "Default values (if called with no argument): " + pathToDataJSON + "  " + model + "  " + task
    var possibleValues = "Possibles values: \n[model]: logisticRegression or randomForest\n[task]: predict or train"

    // Parse command lines args
    if(args.length > 0) {
        if(args(0) == "help" || args(0) == "usage") {
            println("\n\n======================================")
            println("===== ADcare predicting program =====")
            println("======================================")
            println("\n"+usage)
            println(possibleValues)
            println("\n"+defaultValues)
            System.exit(0)
        }
        pathToDataJSON = args(0)
        model = args(1)
        task = args(2)
    }


    val (dataCleaned, sc, spark, sqlContext) = DataBuilder.getData(pathToDataJSON)
    val myRDD : RDD[Row] = dataCleaned.rdd

    // Getting the pipeline
    val indexerPipeline = DataIndexer.preProcessing(dataCleaned, sc, spark, sqlContext)

    // Execute program (train or predict, randomForest or logisticRegression)
    if(model == "logisticRegression") {
        if(task == "predict") {
            val resLogistic = ADcareLogisticRegression.logisticRegression(indexerPipeline, dataCleaned, sc, spark, sqlContext)
            CSVExport.export(resLogistic, "res-logistic-regression")            
        }
        if(task == "train") {
            ADcareLogisticRegression.train(dataCleaned, indexerPipeline)
        }
    }
    if(model == "randomForest") {
        if(task == "predict") {
            val resRandomForest = ADcareRandomForest.randomForest(indexerPipeline, dataCleaned, sc, spark, sqlContext)
            CSVExport.export(resRandomForest, "res-random-forest")
        }
        if(task == "train") {
            ADcareLogisticRegression.train(dataCleaned, indexerPipeline)
        }
    }
    sc.stop()
}
