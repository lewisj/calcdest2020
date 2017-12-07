package org.ukho.ds

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.FunSuite
import org.ukho.ds.data_prep.AISFilterationByIHS.{filterAisByMmsis, getMmsisByShipTypeLevel2, getMmsisByGrossTonnage, ihsSchemaSubset, positionSchemaBefore201607}

/**
  * Created by Jonathan on 12/09/2017.
  */

class AISFilterationByIHSTests extends FunSuite with DataFrameSuiteBase {

  val nullAllowed = true

  def sameAs[A](c: Traversable[A], d: Traversable[A]): Boolean = {
    // See https://stackoverflow.com/a/7435236
    if (c.isEmpty) d.isEmpty
    else {
      val (e, f) = d span (c.head !=)
      if (f.isEmpty) false else sameAs(c.tail, e ++ f.tail)
    }
  }

  test("test getMmsisByGrossTonnage returns correctly when minGrossTonnage = 300"){

    val ihsDF = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(ihsSchemaSubset)
      .csv("src/test/resources/test_getMmsisByGrossTonnage.csv")

    val minGrossTonnage = 300

    val mmsis = getMmsisByGrossTonnage(spark, ihsDF, minGrossTonnage)

    val correctMmsis = Array("209499000", "671793000")

    assertTrue(sameAs(mmsis, correctMmsis))
  }

  test("test getMmsisByGrossTonnage returns correctly when minGrossTonnage = 2000"){

    val ihsDF = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(ihsSchemaSubset)
      .csv("src/test/resources/test_getMmsisByGrossTonnage.csv")

    val minGrossTonnage = 2000

    val mmsis = getMmsisByGrossTonnage(spark, ihsDF, minGrossTonnage)

    val correctMmsis = Array("671793000")

    assertTrue(sameAs(correctMmsis, mmsis))
  }

  test("test getMmsisByShipTypeLevel2 returns correctly when ShipTypeLevel2 = Fishing"){

    val ihsDF = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(ihsSchemaSubset)
      .csv("src/test/resources/test_getFishingMmsis.csv")

    val ShipTypeLevel2 = "Fishing"

    val mmsis = getMmsisByShipTypeLevel2(spark, ihsDF, ShipTypeLevel2)//, filteredIHSDF)

    val correctMmsis = Array("209499000", "671793000", "258301000")

    assertTrue(sameAs(correctMmsis, mmsis))
  }

  test("test getMmsisByShipTypeLevel2 returns correctly when ShipTypeLevel2 = Dry Cargo/Passenger"){

    val ihsDF = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(ihsSchemaSubset)
      .csv("src/test/resources/test_getShipTypeLevel2.csv")

    val ShipTypeLevel2 = "Dry Cargo/Passenger"

    val mmsis = getMmsisByShipTypeLevel2(spark, ihsDF, ShipTypeLevel2)//, filteredIHSDF)

    val correctMmsis = Array("111111111", "222222222")

    assertTrue(sameAs(correctMmsis, mmsis))
  }

  test("test getMmsisByShipTypeLevel2 returns correctly when ShipTypeLevel2 = Non-Merchant Ships"){

    val ihsDF = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(ihsSchemaSubset)
      .csv("src/test/resources/test_getShipTypeLevel2.csv")

    val ShipTypeLevel2 = "Non-Merchant Ships"

    val mmsis = getMmsisByShipTypeLevel2(spark, ihsDF, ShipTypeLevel2)//, filteredIHSDF)

    val correctMmsis = Array("333333333")

    assertTrue(sameAs(correctMmsis, mmsis))
  }

  test("test filterAisByMmsis returns correctly"){

    val aisDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaBefore201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("src/test/resources/test_filterToFishingType_ArkevistaBefore201607.txt")

    val mmsis = Array("209499000", "258292000")

    val aisFilteredDF = filterAisByMmsis(spark, aisDF, mmsis)

    val correctDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaBefore201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("src/test/resources/test_filterToFishingType_ArkevistaBefore201607__correct.txt")

    assertDataFrameEquals(aisFilteredDF, correctDF)
  }
}
