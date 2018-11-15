package adcare

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.util
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Dataset}
import org.apache.spark.ml.feature.{ChiSqSelector, VectorAssembler}
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object ADcareLogisticRegression {

    def time[R](block: => R): R = {
        val t0 = System.nanoTime()
        val result = block    // call-by-name
        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) + "ns")
        result
    }


    val logisticMaxIter = 100000

    // =========================================================== //
    // =================== LOGISTIC REGRESSION =================== //
    // =========================================================== //
    def logisticRegression (
        indexerPipeline: Pipeline, data: DataFrame, sc: SparkContext, 
        spark: SparkSession, sqlContext: SQLContext
    ): DataFrame = {

        println("Indexing the data to predict...")
        val indexedDataPredict = indexerPipeline.fit(data).transform(data)

        println("Loading the model...")
        val loadedModel: PipelineModel = PipelineModel.read.load("linear-regression-model")
        
        println("Predicting...")
        time {
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

            prediction.withColumn("Label", prediction("prediction"))
        }
    }

    /*
        Build a model and save it with the name "linear-regression-model".
    */
    def train(dfTrain: DataFrame, indexerPipeline: Pipeline): Unit = {
        val lr = new LogisticRegression().setMaxIter(logisticMaxIter)
            .setLabelCol("label")
            .setFeaturesCol("features")

        println("======== TRAINING LOGISTIC REGRESSION ========")
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
}
