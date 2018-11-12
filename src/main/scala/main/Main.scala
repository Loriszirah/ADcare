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
import scala.collection.mutable.ListBuffer

object Main extends App{
    val (dataCleaned, sc, spark, sqlContext) = DataBuilder.getData()

    /**
        Fetch data on interests
    */
    val myRDD : RDD[Row] = dataCleaned.rdd
    var rowsDF = interestsFrequecies(myRDD)

    sc.stop()
    spark.stop()



    

    /**
        Group the different lists of interests
        Count the number of users with these interests, and the number of users whio have clicked
        Calculate the freqeuncy of clicks depending of the interests list
    */
    def interestsFrequecies(rdd:RDD[Row]): List[Row]={
        val rowRDD = rdd.map(s => Row(s(5),label(s(6))))
        val datasetAll = rdd.map(_(5)).countByValue()
        val datasetTrue = rdd.filter(x => x(6) == true).map(_(5)).countByValue()

        val result: ListBuffer[(String,Double)] = ListBuffer()


        datasetAll.foreach(i => {
            datasetTrue.foreach(j => {
                if(i._1 != null && j._1 != null){
                    if(i._1.equals(j._1)){

                        val ratio: Double = j._2.toFloat / i._2
                        val resToAdd = (i._1.toString, ratio)
                        result += resToAdd

                    }
                }    
            })
        })


        result.map({x => Row(x._1,x._2)}).toList  
    }

    def label(s:Any): Double = s match{
        case "true" => 1
        case _ => 0 
    }
}
