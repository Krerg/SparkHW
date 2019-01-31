package com.mylnikov

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class Top3PopularHotelsWithCouplesTest extends FunSuite with DataFrameSuiteBase {

  test("Should calculate top 3 hotels"){
    val hotels = Top3PopularHotelsWithCouples.doQuery(spark, "src/test/resources/Book2.csv").collect()
    assert(hotels.length == 3)
    assert(hotels(0)(0) == "4")
  }

  test("Should process empty csv"){
    val country = Top3PopularHotelsWithCouples.doQuery(spark, "src/test/resources/EmptyBook.csv").collect()
    assert(country.length ==0 )
  }

  test("Should throw exception in case invalid fileName"){
    assertThrows[IllegalArgumentException] {
      Top3PopularHotelsWithCouples.doQuery(spark, "").collect()
    }
  }

}
