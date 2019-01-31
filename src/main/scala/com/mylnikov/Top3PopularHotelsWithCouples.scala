package com.mylnikov

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Calculates top 3 hotels with couples.
  * It takes csv as input and output would be printed in the console.
  */
object Top3PopularHotelsWithCouples {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      // Spark init
      val spark = org.apache.spark.sql.SparkSession.builder
        .master("local[*]")
        .appName("Top3PopularHotelsWithCouples")
        .getOrCreate

      // Query the csv to get result
      val top3PopularHotels = doQuery(spark, args(0))

      // Show the result
      top3PopularHotels.show()
      spark.stop()
    }

    println("You should specify filename")

  }

  /**
    * Queries the csv file for top3 hotels with couples.
    *
    * @param spark spark's session
    * @param file path to the csv file
    * @return 3 rows with top hotels with couples or empty array if there is now such bookings
    * @throws IllegalArgumentException in case invalid filename
    * @see Description #Top3PopularHotelsWithCouples
    */
  def doQuery(spark: SparkSession, file: String): Dataset[Row] = {
    if(file == null || file.isEmpty) {
      throw new IllegalArgumentException("Invalid filename: " + file)
    }
    val hotels = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("delimiter",",").load(file)

    hotels
      //select bookings only with couples
      .where("srch_adults_cnt = 2")
      //group by hotel
      .groupBy("hotel_continent", "hotel_country", "hotel_market")
      //count how many times couples books a hotel
      .count().as("n")
      // Get top 3
      .orderBy(desc("n.count")).limit(3)
  }

}
