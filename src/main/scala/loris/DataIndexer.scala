package loris

import org.apache.spark.ml.feature.StringIndexer

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

}
