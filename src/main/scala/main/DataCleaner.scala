package main
import org.apache.spark.sql.DataFrame
import scala.collection.immutable.List
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.spark.sql.functions.udf

object DataCleaner {

    // Main function used to clean the data. Returns the DataFrame cleaned.
    def discretizeTimestamp(data: DataFrame): DataFrame = {
        def discretize(hourOfTheDay: Int) : String = {
            hourOfTheDay match {
                case hourOfTheDay if hourOfTheDay >= 20 || hourOfTheDay < 4 => "Night"
                case hourOfTheDay if hourOfTheDay >= 4 && hourOfTheDay < 12  => "Morning"
                case hourOfTheDay if hourOfTheDay >= 12 && hourOfTheDay < 20  => "Afternoon"
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
        val dataCleaned = data.withColumn("timestamp", discret(data("timestamp")))
        dataCleaned
    }

    def cleanData(data: DataFrame): DataFrame = {
        data
    }

    // Clean the os (lower car, space...). If not listed, return the original one.
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

}
