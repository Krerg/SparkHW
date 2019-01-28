package com.mylnikov

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.desc

object MostPopularCountryWhereHotelsAndSearchedFromSameCountry {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3HotelsWherePeopleWithChildrenWereInterestedButNotBooked").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate
    val hotels = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("delimiter",",").load(args(0))
    val mostPopularHotel = hotels.where("is_booking = 1").where("hotel_country = user_location_country").groupBy("hotel_country").count().as("n")
      .orderBy(desc("n.count")).limit(1).collect()
    if (mostPopularHotel.isEmpty) {
      println("There is no such hotel")
    } else {
      println("Most popular hotel: " + mostPopularHotel(0))
    }
    sc.stop()
  }

}
