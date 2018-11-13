package loris

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object LogisticRegression {

  def logistiRegression(data: DataFrame, sc: SparkContext, spark: SparkSession, sqlContext: SQLContext): Unit = {
    // Creation of the udf for converting boolean to integer
    def bool2int(b:Boolean) = if (b) 1 else 0
    val booltoInt = udf(bool2int _)

    // Selection of the columns and application of the udf to the label column
    val preProcessedData = data.withColumn("label", booltoInt(data("label")))
      .withColumn("network", data("network"))
      .withColumn("appOrSite", data("appOrSite"))
      .withColumn("timestamp_disc", data("timestamp_disc"))
      .withColumn("os", data("os"))
      .withColumn("exchange", data("exchange"))
      .withColumn("publisher", data("publisher"))
      .withColumn("media", data("media"))
      .drop("user")
      .drop("bidfloor")
      .drop("type")
      .drop("city")
      .drop("impid")
        .drop("size")
        .drop("timestamp")

    preProcessedData.printSchema()

    // split of the data : 80% for the training and 20% for the test
    val dfsplits = preProcessedData.randomSplit(Array(0.8, 0.2))
    var dfTrain = dfsplits(0)
    var dfPredict = dfsplits(1)

    // Recuperation of the string indexers
    val networkIndexer = DataIndexer.networkIndexer;
    val appOrSiteIndexer = DataIndexer.appOrSiteIndexer;
    val timestampDiscIndexer = DataIndexer.timestampDiscIndexer;
    val osIndexer = DataIndexer.osIndexer;
    val exchangeIndexer = DataIndexer.exchangeIndexer;
    val publisherIndexer = DataIndexer.publisherIndexer;
    val mediaIndexer = DataIndexer.mediaIndexer;

    // Creation of the vector assembler grouping all the features
    val assemblerEncoder = new VectorAssembler()
      .setInputCols(Array("networkIndex", "appOrSiteIndex", "timestampDiscIndex", "osIndex",
      "exchangeIndex", "publisherIndex", "mediaIndex"))
      .setOutputCol("features")

    // Creation of the logistic regression
    val lr = new LogisticRegression().setMaxIter(100000)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setLabelCol("label")
      .setFeaturesCol("features")


    val pipeline = new Pipeline().setStages(Array( networkIndexer, appOrSiteIndexer, timestampDiscIndexer,
      osIndexer, exchangeIndexer, publisherIndexer, mediaIndexer, assemblerEncoder, lr))
    val lrModel = pipeline.fit(dfTrain)
    val prediction = lrModel.transform(dfPredict)
    val a = prediction.select ("label", "prediction", "rawPrediction")
    a.show()

    import spark.implicits._
    val truep = a.filter($"prediction" === 1.0).filter($"label" === $"prediction").count
    val truen = a.filter($"prediction" === 0.0).filter($"label" === $"prediction").count
    val falseN = a.filter($"prediction" === 0.0).filter(!($"label" === $"prediction")).count
    val falseP = a.filter($"prediction" === 1.0).filter(!($"label" === $"prediction")).count

    val recall = truep / (truep + falseN).toDouble
    val precision = truep / (truep + falseP).toDouble
    println(s"TP = $truep, TN = $truen, FN = $falseN, FP = $falseP")
    println(s"recall = $recall")
    println(s"precision = $precision")

    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC")
      .setRawPredictionCol("rawPrediction")
      .setLabelCol("label")

    val eval = evaluator.evaluate(prediction)
    println("Test set areaunderROC/accuracy = " + eval)

    // println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  }
}
