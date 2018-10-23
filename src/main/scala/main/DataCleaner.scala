package main
import org.apache.spark.sql.DataFrame
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.spark.sql.functions.udf

object DataCleaner {

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

}
