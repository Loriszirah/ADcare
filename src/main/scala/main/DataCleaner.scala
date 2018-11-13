package main
import org.apache.spark.sql.DataFrame
import scala.collection.immutable.List
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD


/*
    This object aims to clean the date in the dataFrame provided.
    It will create new columns or modify the existing ones ()
*/
object DataCleaner {

    // Main function used to clean the data. Returns the DataFrame cleaned.
    def cleanData(data: DataFrame): DataFrame = {
        cleanOS(discretizeTimestamp(data))
    }

    /*
        Discretize the timestamp in 3 periods : night, morning and afternoon.
        New column name : "timestamp_disc"
    */
    def discretizeTimestamp(data: DataFrame): DataFrame = {
        def discretize(hourOfTheDay: Int) : String = {
            hourOfTheDay match {
                case hourOfTheDay if hourOfTheDay >= 20 || hourOfTheDay < 4 => "night"
                case hourOfTheDay if hourOfTheDay >= 4 && hourOfTheDay < 12  => "morning"
                case hourOfTheDay if hourOfTheDay >= 12 && hourOfTheDay < 20  => "afternoon"
            }
        }
        def getCurrentHour(timestamp: Long): Int = {
            try {
                Integer.parseInt(new SimpleDateFormat("HH")format(new Date(timestamp*1000)))
            } catch {
                case _ => return 0
            }
        }
        def mapTimestamp(timestamp: String) : Option[String]  = {
            if(timestamp == null) null
            else{
                Some(discretize(getCurrentHour(timestamp.toLong)))
            }
        }
        val discret = udf[Option[String], String](mapTimestamp)
        val dataCleaned = data.withColumn("timestamp_disc", discret(data("timestamp")))
        dataCleaned
    }

    /*
        Clean the os (lower car, space...). If not listed, return the original one.
        No new column: modify "os" directly.
    */
    def cleanOS(data: DataFrame): DataFrame = {
        val osList: List[List[String]] = List(
            List("android"),
            List("bada"),
            List("blackberry", "black berry"),
            List("ios", "i os"),
            List("rim"),
            List("symbia"),
            List("unknown"),
            List("windows"),
            List("windows mobile", "windowsmobile"),
            List("windows phone", "windowsphone", "windows phone os", "windosphoneos")
        )

        def mapOs(os: String): Option[String] = {
            if(os == null) null
            else {
                val os_lower = os.toLowerCase()
                val list = osList.filter( l => {
                    l.contains(os_lower)
                })
                if(list.length > 0) Some(list(0)(0))
                else Some(os_lower)
            }
        }

        val udfMapOS = udf[Option[String], String](mapOs)
        data.withColumn("os", udfMapOS(data("os")))
    }

/*

    def getFirstInterest(data: DataFrame): DataFrame = {
        def mapInterest(interest: String): Option[String] = {
            if(interest == null) null
            else {
                val os_lower = os.toLowerCase()
                val list = osList.filter( l => {
                    l.contains(os_lower)
                })
                if(list.length > 0) Some(list(0)(0))
                else Some(os_lower)
            }
        }

        val udfMapOS = udf[Option[String], String](mapOs)
        data.withColumn("firstInterest", udfMapOS(data("interests")))
    }

    */



    // ============================================================ //
    // =================== NOT WORKING CLEANERS =================== //
    // ============================================================ //

    /*
        /!\ WORKING ON IT /!\
        Each interest has one column: 1 if the user got it, else 0.
    */
    def cleanInterests(data: DataFrame): DataFrame = {
        var currentDF = data.select("interests")
        var allInterests = List[String]()         

        currentDF.select("interests").foreach(row => {
            if(row.get(0) != null) {
                var currentInterests = row.get(0).toString().split(',')
                currentInterests.foreach(interest => {
                    if(!allInterests.contains(interest)){
                        allInterests = interest :: allInterests
                    }
                })
            }
        })

        println("================")
        println(allInterests.size)
        data
    }


    // =========== RESEARCH BY HUGO FAZIO =========== //
    /**
        Group the different lists of interests
        Count the number of users with these interests, and the number of users whio have clicked
        Calculate the freqeuncy of clicks depending of the interests list
    */
    def interestsFrequecies(rdd:RDD[Row]): List[Row]={
        val rowRDD = rdd.map(s => Row(s(5),label(s(6))))
        val datasetAll = rdd.map(_(5)).countByValue()
        val datasetTrue = rdd.filter(x => x(6) == true).map(_(5)).countByValue()

        val result: ListBuffer[(String,Double)] = ListBuffer()


        datasetAll.foreach(i => {
            datasetTrue.foreach(j => {
                if(i._1 != null && j._1 != null){
                    if(i._1.equals(j._1)){

                        val ratio: Double = j._2.toFloat / i._2
                        val resToAdd = (i._1.toString, ratio)
                        result += resToAdd

                    }
                }    
            })
        })


        result.map({x => Row(x._1,x._2)}).toList  
    }

    def label(s:Any): Double = s match{
        case "true" => 1
        case _ => 0 
    }

}
