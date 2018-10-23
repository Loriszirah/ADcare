package main

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext


object Main extends App {
    val (dataCleaned, sc, spark, sqlContext) = DataBuilder.getData()

    // Your code goes here...

    sc.stop()
}
