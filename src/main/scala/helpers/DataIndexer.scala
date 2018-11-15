package helpers

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.SparkContext


object DataIndexer {

  def networkIndexer = {
    new StringIndexer()
    .setInputCol ("network")
    .setOutputCol ("networkIndex")
  }

  def appOrSiteIndexer = {
    new StringIndexer()
      .setInputCol ("appOrSite")
      .setOutputCol ("appOrSiteIndex")
  }

  def timestampDiscIndexer = {
    new StringIndexer()
      .setInputCol ("timestamp_disc")
      .setOutputCol ("timestampDiscIndex")
  }

  def typeIndexer = {
    new StringIndexer()
      .setInputCol ("type")
      .setOutputCol ("typeIndex")
  }

  def sizeIndexer = {
    new StringIndexer()
      .setInputCol ("size")
      .setOutputCol ("sizeIndex")
  }

  def cityIndexer = {
    new StringIndexer()
      .setInputCol ("city")
      .setOutputCol ("cityIndex")
  }

  def osIndexer = {
    new StringIndexer()
      .setInputCol ("os")
      .setOutputCol ("osIndex")
  }

  def exchangeIndexer = {
    new StringIndexer()
      .setInputCol ("exchange")
      .setOutputCol ("exchangeIndex")
  }

  def publisherIndexer = {
    new StringIndexer()
      .setInputCol ("publisher")
      .setOutputCol ("publisherIndex")
  }

  def mediaIndexer = {
    new StringIndexer()
      .setInputCol ("media")
      .setOutputCol ("mediaIndex")
  }

  def bidfloodIndexer = {
    new StringIndexer()
      .setInputCol ("bidfloor")
      .setOutputCol ("bidfloorIndex")
  }

  def interestsIndexer = {
    new StringIndexer()
      .setInputCol ("interests")
      .setOutputCol ("interestsIndex")
  }

  def interestsEncoder = {
    new OneHotEncoder()
      .setInputCol("interestsIndex")
      .setOutputCol("interestsEncoder")
  }

  def bidfloorEncoder = {
    new StringIndexer()
      .setInputCol("bidfloorIndex")
      .setOutputCol("bidfloorEncoder")
  }

  def preProcessing(data: DataFrame, sc: SparkContext, spark: SparkSession, sqlContext: SQLContext) = {
        // Selection of the columns and application of the udf to the label column
        val preProcessedData = data

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
}
