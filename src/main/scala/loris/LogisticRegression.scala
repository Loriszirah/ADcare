package loris

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.ml.feature.{ChiSqSelector, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object LogisticRegression {

  def balanceDataset(dataset: DataFrame): DataFrame = {
    val numNegatives = dataset.filter(dataset("label") === 0.0d).count
    val datasetSize = dataset.count
    val balancingRatio = (datasetSize - numNegatives).toDouble / datasetSize

    val calculateWeights = udf { d: Double =>
      if (d == 0.0) {
        1 * balancingRatio
      }
      else {
        1 * (1.0 - balancingRatio)
      }
    }

    dataset.withColumn("classWeightCol", calculateWeights(dataset("label")))
  }


  def logistiRegression(data: DataFrame, sc: SparkContext, spark: SparkSession, sqlContext: SQLContext): Unit = {
    // Creation of the udf for converting boolean to integer
    def bool2int(b:Boolean) = if (b) 1 else 0
    val booltoInt = udf(bool2int _)
    // Selection of the columns and application of the udf to the label column
    val preProcessedData = data.withColumn("label", booltoInt(data("label")))
      .drop("user")
      .drop("bidfloor")
        .drop("size")
        .drop("timestamp")
    preProcessedData.printSchema()

    // Recuperation of the string indexers
    val networkIndexer = DataIndexer.networkIndexer;
    val appOrSiteIndexer = DataIndexer.appOrSiteIndexer;
    val timestampDiscIndexer = DataIndexer.timestampDiscIndexer;
    val osIndexer = DataIndexer.osIndexer;
    val exchangeIndexer = DataIndexer.exchangeIndexer;
    val publisherIndexer = DataIndexer.publisherIndexer;
    val mediaIndexer = DataIndexer.mediaIndexer;
    val cityIndexer = DataIndexer.cityIndexer;
    val typeIndexer = DataIndexer.typeIndexer;

    val weightedData = balanceDataset(preProcessedData)

    val pipeline = new Pipeline().setStages(Array(networkIndexer, appOrSiteIndexer, timestampDiscIndexer, osIndexer,
      exchangeIndexer, publisherIndexer, mediaIndexer, cityIndexer, typeIndexer))
    val dataIndexed = pipeline.fit(weightedData).transform(weightedData)

    // Creation of the vector assembler grouping all the features
    val assemblerEncoder = new VectorAssembler()
      .setInputCols(Array("networkIndex", "appOrSiteIndex", "timestampDiscIndex", "osIndex", "exchangeIndex", "publisherIndex",
      "mediaIndex", "cityIndex", "typeIndex"))
      .setOutputCol("features")
    val dataAssembled = assemblerEncoder.transform(dataIndexed)

    // Selection of the relevant features
    val chisqSelector = new ChiSqSelector()
      .setFpr(0.05).setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")
    val selectedDataFeatures = chisqSelector.fit(dataAssembled).transform(dataAssembled)

    // Creation of the logistic regression
    val lr = new LogisticRegression().setMaxIter(10)
      .setLabelCol("label")
      .setFeaturesCol("features")

    // split of the data : 80% for the training and 20% for the test
    val dfsplits = dataAssembled.randomSplit(Array(0.8, 0.2))
    var dfTrain = dfsplits(0)
    var dfPredict = dfsplits(1)

    val lrModel = lr.fit(dfTrain)
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    val prediction = lrModel.transform(dfPredict)
    val predictionValues = prediction.select ("label", "prediction", "rawPrediction")
    predictionValues.show(20)

    import spark.implicits._
    val truep = predictionValues.filter($"prediction" === 1.0).filter($"label" === $"prediction").count
    val truen = predictionValues.filter($"prediction" === 0.0).filter($"label" === $"prediction").count
    val falseN = predictionValues.filter($"prediction" === 0.0).filter(!($"label" === $"prediction")).count
    val falseP = predictionValues.filter($"prediction" === 1.0).filter(!($"label" === $"prediction")).count

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

  }
}
