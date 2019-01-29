package com.mylnikov

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.desc

object Top3HotelsWherePeopleWithChildrenWereInterestedButNotBooked {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("You should specify filename")
      return
    }
    val conf = new SparkConf().setAppName("Top3HotelsWherePeopleWithChildrenWereInterestedButNotBooked").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate
    val top3PopularHotels = doQuery(spark, args(0))
    println("Top 3 popular hotels are: ")
    for (hotel <- top3PopularHotels)
      println(hotel.get(0).toString + " " + hotel.get(1).toString + " " + hotel.get(2).toString)
    sc.stop()
  }

  def doQuery(spark: SparkSession, file: String): Array[Row] = {
    if(file == null || file.isEmpty) {
      return null
    }
    val hotels = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("delimiter",",").load(file)
    val top3PopularHotels = hotels.where("srch_children_cnt > 0").where("is_booking = 0").groupBy("hotel_continent", "hotel_country", "hotel_market").count().as("n")
      .orderBy(desc("n.count")).limit(3).collect()
    return top3PopularHotels
  }

}
