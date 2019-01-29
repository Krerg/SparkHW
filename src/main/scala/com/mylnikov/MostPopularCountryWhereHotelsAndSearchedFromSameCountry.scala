package com.mylnikov

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.desc

object MostPopularCountryWhereHotelsAndSearchedFromSameCountry {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("You should specify filename")
      return
    }
    // Spark init
    val conf = new SparkConf().setAppName("Top3HotelsWherePeopleWithChildrenWereInterestedButNotBooked").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

    // Query the csv to get result
    val mostPopularHotel = doQuery(spark, args(0))

    // Show the result
    if (mostPopularHotel.isEmpty) {
      println("There is no such hotel")
    } else {
      println("Most popular hotel: " + mostPopularHotel(0))
    }
    sc.stop()
  }

  /**
    * Queries the csv file.
    *
    * @param spark spark's session
    * @param file path to the csv file
    * @return row with most popular country where hotels and searched from same country
    *         or empty array if there is now such booking
    */
  def doQuery(spark: SparkSession, file: String): Array[Row] = {
    if(file == null || file.isEmpty) {
      return null
    }
    val hotels = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("delimiter",",").load(file)
    val mostPopularHotel = hotels
      // Where booking is successful
      .where("is_booking = 1")
      // User's and hotel's country are same
      .where("hotel_country = user_location_country")
      // Group by country
      .groupBy("hotel_country")
      // Count such countries
      .count().as("n")
      // Show the result
      .orderBy(desc("n.count")).limit(1).collect()
    return mostPopularHotel
  }

}
