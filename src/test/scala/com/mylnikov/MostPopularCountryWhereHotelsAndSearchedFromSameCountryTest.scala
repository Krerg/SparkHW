package com.mylnikov

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class MostPopularCountryWhereHotelsAndSearchedFromSameCountryTest extends FunSuite with DataFrameSuiteBase {

  test("Should calculate most popelar country"){
    val country = MostPopularCountryWhereHotelsAndSearchedFromSameCountry.doQuery(spark, "src/test/resources/Book2.csv").collect()
    assert(country.length == 1)
    assert(country(0)(0) == "66")
  }

  test("Should calculate most popelar country2"){
    val country = MostPopularCountryWhereHotelsAndSearchedFromSameCountry.doQuery(spark, "src/test/resources/EmptyBook.csv").collect()
    assert(country.length == 0)
  }

  test("Should throw exception in case invalid fileName"){
    assertThrows[IllegalArgumentException] {
      MostPopularCountryWhereHotelsAndSearchedFromSameCountry.doQuery(spark, "").collect()
    }
  }

}
