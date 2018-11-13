package main
import org.apache.spark.sql.DataFrame
import scala.collection.immutable.List
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.functions._

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
}
