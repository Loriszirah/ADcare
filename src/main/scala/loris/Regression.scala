package loris

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.util
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.ml.feature.{ChiSqSelector, VectorAssembler}
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Regression {

  def preProcessing(data: DataFrame, sc: SparkContext, spark: SparkSession, sqlContext: SQLContext) = {
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
    val interestsIndexer = DataIndexer.interestsIndexer;

    // Creation of the vector assembler grouping all the features
    val assemblerEncoder = new VectorAssembler()
      .setInputCols(Array("appOrSiteIndex", "osIndex", "exchangeIndex",
         "mediaIndex", "typeIndex", "sizeIndex", "bidfloorIndex"))
      .setOutputCol("features")

    val indexerPipeline = new Pipeline().setStages(Array(appOrSiteIndexer, osIndexer,
      exchangeIndexer, mediaIndexer, typeIndexer, sizeIndexer, publisherIndexer, bidfloorIndexer, interestsIndexer,
      assemblerEncoder))

    indexerPipeline
  }

  def balanceDataset(dataset: DataFrame): DataFrame = {
    val numNegatives = dataset.filter(dataset("label") === 0.0d).count
    val datasetSize = dataset.count
    val balancingRatio = (datasetSize - numNegatives).toDouble / datasetSize

    val calculateWeights = udf { d: Int =>
      if (d == 0.0) {
        1 * balancingRatio
      }
      else {
        (1 * (1.0 - balancingRatio))
      }
    }

    dataset.withColumn("classWeightCol", calculateWeights(dataset("label")))
  }

  def logisticRegression(indexerPipeline: Pipeline, data: DataFrame, sc: SparkContext, spark: SparkSession, sqlContext: SQLContext): DataFrame = {

    // Creation of the logistic regression
    val lr = new LogisticRegression().setMaxIter(100000)
      .setLabelCol("label")
      .setFeaturesCol("features")

    data.printSchema()
    data.show(50)

    // split of the data : 80% for the training and 20% for the test
    val dfsplits = data.randomSplit(Array(0.8, 0.2))
    var dfTrain = dfsplits(0)
    var dfPredict = dfsplits(1)

    if(false){
      println("Indexing the training data...")
      val indexedDataTrain = indexerPipeline.fit(dfTrain).transform(dfTrain)
      val balancedDatasetTrain = balanceDataset(indexedDataTrain)
      balancedDatasetTrain.show(100)
      balancedDatasetTrain.printSchema()
      val pip2 = new Pipeline().setStages(Array(lr))
      println("Starting the training of the model...")
      val lrModel: PipelineModel = pip2.fit(balancedDatasetTrain)
      val lrm: LogisticRegressionModel = lrModel
        .stages
        .last.asInstanceOf[LogisticRegressionModel]
      println(s"Coefficients: ${lrm.coefficients} Intercept: ${lrm.intercept}")
      println("Saving the model...")
      lrModel.write.overwrite().save("linear-regression-model")
    }

    println("Indexing the data to predict...")
    val indexedDataPredict = indexerPipeline.fit(dfPredict).transform(dfPredict)
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

    prediction.withColumn("Label", prediction("prediction"))select("user","Label")
  }

  def randomForest(indexerPipeline: Pipeline, data: DataFrame, sc: SparkContext, spark: SparkSession, sqlContext: SQLContext): Unit = {

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
//      .setNumTrees(10)

    // split of the data : 80% for the training and 20% for the test
    val dfsplits = data.randomSplit(Array(0.8, 0.2))
    var dfTrain = dfsplits(0)
    var dfPredict = dfsplits(1)

    if(true){
      println("Indexing the training data...")
      val indexedDataTrain = indexerPipeline.fit(dfTrain).transform(dfTrain)
      val pip2 = new Pipeline().setStages(Array(rf))
      println("Starting the training of the model...")
      val rfModel: PipelineModel = pip2.fit(indexedDataTrain)
      println("Saving the model...")
      rfModel.write.overwrite().save("random-forest-model")
    }

    println("Indexing the data to predict...")
    val indexedDataPredict = indexerPipeline.fit(dfPredict).transform(dfPredict)
    println("Loading the model...")
    val loadedModel: PipelineModel = PipelineModel.read.load("random-forest-model")
    println("Predicting...")
    val predictions = loadedModel.transform(indexedDataPredict)
    val evaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator2.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")

    val rfModel = loadedModel.stages(0).asInstanceOf[RandomForestClassificationModel]
    println(s"Learned classification forest model:\n ${rfModel.toDebugString}")
    val predictionValues = predictions.select ("label", "prediction", "rawPrediction")

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

    val eval = evaluator.evaluate(predictions)
    println("Test set areaunderROC/accuracy = " + eval)
  }
}
