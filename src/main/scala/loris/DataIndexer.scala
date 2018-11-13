package loris

import org.apache.spark.ml.feature.StringIndexer

object DataIndexer {

  def networkIndexer = {
    new StringIndexer()
    .setHandleInvalid("skip")
    .setInputCol ("network")
    .setOutputCol ("networkIndex")
  }

  def appOrSiteIndexer = {
    new StringIndexer()
      .setHandleInvalid("skip")
      .setInputCol ("appOrSite")
      .setOutputCol ("appOrSiteIndex")
  }

  def timestampDiscIndexer = {
    new StringIndexer()
      .setHandleInvalid("skip")
      .setInputCol ("timestamp_disc")
      .setOutputCol ("timestampDiscIndex")
  }

  def sizeIndexer = {
    new StringIndexer()
      .setHandleInvalid("skip")
      .setInputCol ("size")
      .setOutputCol ("sizeIndex")
  }

  def osIndexer = {
    new StringIndexer()
      .setHandleInvalid("skip")
      .setInputCol ("os")
      .setOutputCol ("osIndex")
  }

  def exchangeIndexer = {
    new StringIndexer()
      .setHandleInvalid("skip")
      .setInputCol ("exchange")
      .setOutputCol ("exchangeIndex")
  }

  def publisherIndexer = {
    new StringIndexer()
      .setHandleInvalid("skip")
      .setInputCol ("publisher")
      .setOutputCol ("publisherIndex")
  }

  def mediaIndexer = {
    new StringIndexer()
      .setHandleInvalid("skip")
      .setInputCol ("media")
      .setOutputCol ("mediaIndex")
  }

}
