package main

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}

object Main extends App {
    
    // Parse command lines args
    if(args.length > 0) {
        if(args(0) == "help" || args(0) == "usage") {
            println("Usage: path/to/data.json")
            System.exit(0)
        }
        pathToDataJSON = args(0)
    }
    var pathToDataJSON = "./data/data-students.json"
  

    val (dataCleaned, sc, spark, sqlContext) = DataBuilder.getData(pathToDataJSON)
    val myRDD : RDD[Row] = dataCleaned.rdd
    sc.stop()
    spark.stop()
}
