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

object ADcareRandomForest {

    def time[R](block: => R): R = {
        println("Starting timer")
        val t0 = System.nanoTime()
        val result = block    // call-by-name
        val t1 = System.nanoTime()
        println("========")
        println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
        result
    }

    val logisticMaxIter = 100000

    // ===================================================== //
    // =================== RANDOM FOREST =================== //
    // ===================================================== //
    def randomForest(
        indexerPipeline: Pipeline, data: DataFrame, sc: SparkContext, 
        spark: SparkSession, sqlContext: SQLContext
    ): DataFrame = {         
        println("Indexing the data to predict...")
        val indexedDataPredict = indexerPipeline.fit(data).transform(data)

        println("Loading the model...")
        val loadedModel: PipelineModel = PipelineModel.read.load("random-forest-model")

        println("Predicting...")

        time {
            val prediction = loadedModel.transform(indexedDataPredict)
            val evaluator2 = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
            val accuracy = evaluator2.evaluate(prediction)
            println(s"Test Error = ${(1.0 - accuracy)}")

            val rfModel = loadedModel.stages(0).asInstanceOf[RandomForestClassificationModel]
            println(s"Learned classification forest model:\n ${rfModel.toDebugString}")
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
        Build a model and save it with the name "random-forest-model".
    */
    def train(dfTrain: DataFrame, indexerPipeline: Pipeline) = {
        val rf = new RandomForestClassifier()
            .setLabelCol("label")
            .setFeaturesCol("features")
            .setNumTrees(10)

        println("======== TRAINING RANDOM FOREST ========")
        println("Indexing the training data...")
        val indexedDataTrain = indexerPipeline.fit(dfTrain).transform(dfTrain)
        val pip2 = new Pipeline().setStages(Array(rf))
        println("Starting the training of the model...")
        val rfModel: PipelineModel = pip2.fit(indexedDataTrain)
        println("Saving the model...")
        rfModel.write.overwrite().save("random-forest-model")
    }
}
