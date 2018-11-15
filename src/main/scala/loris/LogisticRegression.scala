package loris

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.util
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
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
    // Selection of the columns and application of the udf to the label column
    val preProcessedData = data
      .drop("user")
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
    val bidfloorIndexer = DataIndexer.bidfloodIndexer;
    val sizeIndexer = DataIndexer.sizeIndexer;

    // Creation of the vector assembler grouping all the features
    val assemblerEncoder = new VectorAssembler()
      .setInputCols(Array("networkIndex", "appOrSiteIndex", "timestampDiscIndex", "osIndex", "exchangeIndex",
      "publisherIndex", "mediaIndex", "cityIndex", "typeIndex", "bidfloorIndex", "sizeIndex"))
      .setOutputCol("features")

    //val dataIndexed = pipeline.fit(weightedData).transform(weightedData)
    //val dataAssembled = assemblerEncoder.transform(dataIndexed)

    // Selection of the relevant features
    val chisqSelector = new ChiSqSelector()
      .setFpr(0.05).setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")
    //val selectedDataFeatures = chisqSelector.fit(dataAssembled).transform(dataAssembled)

    // Creation of the logistic regression
    val lr = new LogisticRegression().setMaxIter(1000)
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(networkIndexer, appOrSiteIndexer, timestampDiscIndexer, osIndexer,
      exchangeIndexer, publisherIndexer, mediaIndexer, cityIndexer, typeIndexer, bidfloorIndexer, sizeIndexer,
      assemblerEncoder))

    // split of the data : 80% for the training and 20% for the test
    val dfsplits = preProcessedData.randomSplit(Array(0.8, 0.2))
    var dfTrain = dfsplits(0)
    var dfPredict = dfsplits(1)

    if(false){
      println("Indexing the training data...")
      val indexedDataTrain = pipeline.fit(dfTrain).transform(dfTrain)
      val pip2 = new Pipeline().setStages(Array(lr))
      println("Starting the training of the model...")
      val lrModel: PipelineModel = pip2.fit(indexedDataTrain)
      val lrm: LogisticRegressionModel = lrModel
        .stages
        .last.asInstanceOf[LogisticRegressionModel]
      println(s"Coefficients: ${lrm.coefficients} Intercept: ${lrm.intercept}")
      println("Saving the model...")
      lrModel.write.overwrite().save("linear-regression-model")
    }
    //println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    println("Indexing the data to predict...")
    val indexedDataPredict = pipeline.fit(dfPredict).transform(dfPredict)
    println("Loading the model...")
    val loadedModel: PipelineModel = PipelineModel.read.load("linear-regression-model")
    println("Predicting...")
    val prediction = loadedModel.transform(indexedDataPredict)
    val predictionValues = prediction.select ("label", "prediction", "rawPrediction")

    import spark.implicits._
    val truep = predictionValues.filter($"prediction" === 1.0).filter($"label" === $"prediction").count
    val truen = predictionValues.filter($"prediction" === 0.0).filter($"label" === $"prediction").count
    val falseN = predictionValues.filter($"prediction" === 0.0).filter(!($"label" === $"prediction")).count
    val falseP = predictionValues.filter($"prediction" === 1.0).filter(!($"label" === $"prediction")).count

    val recall = truep / (truep + falseN).toDouble
    val precision = truep / (truep + falseP).toDouble
    println(s"TP = $truep, TN = $truen")
    println(s"FN = $falseN, FP = $falseP")
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
