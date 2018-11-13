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

object Main extends App{
    val (dataCleaned, sc, spark, sqlContext) = DataBuilder.getData()
    val myRDD : RDD[Row] = dataCleaned.rdd
    sc.stop()
    spark.stop()
}
