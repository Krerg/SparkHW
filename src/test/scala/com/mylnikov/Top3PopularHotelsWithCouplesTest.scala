package com.mylnikov

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class Top3PopularHotelsWithCouplesTest extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("Should calculate top 3 hotels"){
    val hotels = Top3PopularHotelsWithCouples.doQuery(sparkSession, "src/test/resources/Book2.csv")
    assert(hotels.length == 3)
    assert(hotels(0)(0) == "4");
  }

  test("Should process empty csv"){
    val country = Top3PopularHotelsWithCouples.doQuery(sparkSession, "src/test/resources/EmptyBook.csv")
    assert(country.length ==0 )
  }

  test("Should return null in case invalid fileName"){
    val country = Top3PopularHotelsWithCouples.doQuery(sparkSession, "")
    assert(country == null)
  }

  override def afterEach() {
    sparkSession.stop()
  }

}
