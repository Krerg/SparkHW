package com.mylnikov

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.desc

/**
  * Calculates the most popular country where hotels and searched are from same country.
  * It takes csv as input and output would be printed in the console.
  */
object MostPopularCountryWhereHotelsAndSearchedFromSameCountry {

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      // Spark init
      val spark = org.apache.spark.sql.SparkSession.builder
        .master("local[*]")
        .appName("MostPopularCountryWhereHotelsAndSearchedFromSameCountry")
        .getOrCreate

      // Query the csv to get result
      val mostPopularHotel = doQuery(spark, args(0))

      // Show the result
      mostPopularHotel.show()

      spark.stop()
    } else {
      println("You should specify filename")
    }
  }

  /**
    * Queries the csv file for most popular country.
    *
    * @param spark spark's session
    * @param file path to the csv file
    * @return row with most popular country where hotels and searched from same country
    *         or empty array if there is now such booking
    * @throws IllegalArgumentException in case invalid filename
    * @see Description #MostPopularCountryWhereHotelsAndSearchedFromSameCountry
    */
  def doQuery(spark: SparkSession, file: String): Dataset[Row] = {
    if(file == null || file.isEmpty) {
      throw new IllegalArgumentException("Invalid filename: " + file)
    }
    val hotels = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("delimiter",",").load(file)
    hotels
      // Where booking is successful
      .where("is_booking = 1")
      // User's and hotel's country are same
      .where("hotel_country = user_location_country")
      // Group by country
      .groupBy("hotel_country")
      // Count such countries
      .count().as("n")
      // Get top 1
      .orderBy(desc("n.count")).limit(1)
  }

}
