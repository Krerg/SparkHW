package com.mylnikov

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class MostPopularCountryWhereHotelsAndSearchedFromSameCountryTest extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _
  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("Should calculate most popelar country"){
    val country = MostPopularCountryWhereHotelsAndSearchedFromSameCountry.doQuery(sparkSession, "src/test/resources/Book2.csv")
    assert(country.length == 1)
    assert(country(0)(0) == "66");
  }

  test("Should calculate most popelar country2"){
    val country = MostPopularCountryWhereHotelsAndSearchedFromSameCountry.doQuery(sparkSession, "src/test/resources/EmptyBook.csv")
    assert(country.length ==0 )
  }

  test("Should return null in case invalid fileName"){
    val country = MostPopularCountryWhereHotelsAndSearchedFromSameCountry.doQuery(sparkSession, "")
    assert(country == null)
  }

  override def afterEach() {
    sparkSession.stop()
  }

}
