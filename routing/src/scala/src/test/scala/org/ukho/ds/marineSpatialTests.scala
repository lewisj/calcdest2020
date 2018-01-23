package org.ukho.ds


import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.ukho.ds.data_prep.MarineSpatialLimits.{appendPoly_marspat, readCellBoundariesIntoRTree_marspat}
import org.ukho.ds.data_prep.MarineSpatialLimits._
//import org.ukho.ds.data_prep.cellRecord

/**
  * Created by kari on 12/09/2017.
  */

class marineSpatialTests extends FunSuite with DatasetSuiteBase with DataFrameSuiteBase {

  val sncCoveragePath = "src/main/resources/snc_cat_scala.csv"
  val nullAllowed = true

  val aisSchema = StructType(
    Array(
      StructField("MMSI", StringType, nullAllowed),
      StructField("acquisition_time", TimestampType, nullAllowed),
      StructField("lon", DoubleType, nullAllowed),
      StructField("lat", DoubleType, nullAllowed)))

  val appendBand5Schema = StructType(
    Array(
      StructField("cellId", StringType, nullAllowed),
      StructField("mmsi", StringType, nullAllowed),
      StructField("acquisition_time", TimestampType, nullAllowed)))

  val getDestinationsSchema = StructType(
    Array(
      StructField("mmsi", StringType, nullAllowed),
      StructField("startCell", StringType, nullAllowed),
      StructField("endCell", StringType, nullAllowed),
      StructField("exitTimeOfStartCell", TimestampType, nullAllowed),
      StructField("entryTimeOfEndCell", TimestampType, nullAllowed)
    ))

  def sameAs[A](c: Traversable[A], d: Traversable[A]): Boolean = {
    // See https://stackoverflow.com/a/7435236
    if (c.isEmpty) d.isEmpty
    else {
      val (e, f) = d span (c.head !=)
      if (f.isEmpty) false else sameAs(c.tail, e ++ f.tail)
    }
  }



/*

  test("test correct number of band 5 cells"){
    val dataFilePath = sncCoveragePath
    val numCells = readCellBoundariesIntoRTree_marspat(spark, dataFilePath)._2
    val countOfBand5 = numCells.keys.size

    //    assert(countOfBand5 == 5920)  // fully contained cells NOT removed
    assert(countOfBand5 == 7040)  // fully contained cells removed
  }

  test("test no intersection with band 5"){

    val aisNoB5DF = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(aisSchema)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("src/test/resources/test_no_intersection_with_band_5.csv")

    import spark.implicits._
    val aisNoB5DS = aisNoB5DF.as[(String, Timestamp, Double, Double)]
    val (encCoverageIndex, cellCoverageMap) = readCellBoundariesIntoRTree_marspat(spark, sncCoveragePath)
    val aisPointsWithB5DS = appendBand5_marspat(spark, aisNoB5DS, encCoverageIndex, cellCoverageMap)
    assert(aisPointsWithB5DS.count == 0)
  }

  test("test 1 mmsi 1 intersection with band 5 and overlapping points"){

    val aisOneB5DF = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(aisSchema)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("src/test/resources/test_1_mmsi_1_intersection_with_band_5_overlapping_points.csv")

    val correctDF = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(appendBand5Schema)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("src/test/resources/test_1_mmsi_1_intersection_with_band_5_overlapping_points__correct.csv")

    import spark.implicits._
    val aisNoB5DS = aisOneB5DF.as[(String, Timestamp, Double, Double)]
    val correctDS = correctDF.as[(String, String, Timestamp)]

    val (encCoverageIndex, cellCoverageMap) = readCellBoundariesIntoRTree_marspat(spark, sncCoveragePath)
    val aisPointsWithB5DS = appendBand5_marspat(spark, aisNoB5DS, encCoverageIndex, cellCoverageMap)

    assertDatasetEquals(correctDS, aisPointsWithB5DS)
  }
*/

  test("test that union of before and after works successfully"){
    import spark.implicits._
    val aisBeforePath="/home/ubuntu/strozzapreti/routing/src/scala/src/test/resources/TEST_filterArkevistaBefore201607ToRegion_position.txt"
    val aisAfterPath="/home/ubuntu/strozzapreti/routing/src/scala/src/test/resources/TEST_filterArkevistaAfter201607ToRegion_position.txt"

    val aisBeforeDF = readArkevistaPositionDataBefore201607_marspat(spark, aisBeforePath)
    val aisAfterDF = readArkevistaPositionDataAfter201607_marspat(spark, aisAfterPath)
    val aisPointsDS = aisAfterDF.union(aisBeforeDF).as[(String, Timestamp, Double, Double)]

    val correctDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(aisSchema)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/home/ubuntu/strozzapreti/routing/src/scala/src/test/resources/test_union_before_and_after")

    val aisPointsDF = aisPointsDS.toDF()
    correctDF.show()
    aisPointsDF.show()
    assertDataFrameEquals(correctDF, aisPointsDF)

  }

  test("Test appending SNC chart detials onto ais"){

    import spark.implicits._
    val aisAfterPath="/home/ubuntu/strozzapreti/routing/src/scala/src/test/resources/skye_pos_yacht.csv"
    val outputPath = "/home/ubuntu/strozzapreti/routing/src/scala/src/main/resources/output.csv"
    val coveragePath = "/home/ubuntu/strozzapreti/routing/src/scala/src/main/resources/snc_cat_scala.csv"
    val aisafterDF=readArkevistaPositionDataAfter201607_marspat(spark, aisAfterPath)
    val aisPointsDS = aisafterDF.as[(String, Timestamp, Double, Double)]


    val (encCoverageIndex, cellCoverageMap) = readCellBoundariesIntoRTree_marspat(spark, coveragePath)


    val aisPointsWithChart = appendPoly_marspat(spark, aisPointsDS, encCoverageIndex, cellCoverageMap)

    aisPointsWithChart.show()

    aisPointsWithChart.write
      .csv(outputPath)
    val aisPointsWithChartDF = aisPointsWithChart.toDF()
    val correctDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(aisTempSchema)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/home/ubuntu/strozzapreti/routing/src/scala/src/test/resources/yacht_expected.csv")

    assertDataFrameEquals(aisPointsWithChartDF,correctDF)
    //checked in postgis
  }

/*
  test("test many mmsis many intersections with band 5"){

    val aisDF = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(aisSchema)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("src/test/resources/test_many_mmsis_many_intersections_with_band_5.csv")

    val correctDF = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(appendBand5Schema)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("src/test/resources/test_many_mmsis_many_intersections_with_band_5__correct.csv")

    import spark.implicits._
    val aisDS = aisDF.as[(String, Timestamp, Double, Double)]
    val correctDS = correctDF.as[(String, String, Timestamp)]

    val (encCoverageIndex, cellCoverageMap) = readCellBoundariesIntoRTree_marspat(spark, sncCoveragePath)
    val aisPointsWithB5DS = appendBand5_marspat(spark, aisDS, encCoverageIndex, cellCoverageMap)
    aisPointsWithB5DS.show(5)
    assertDatasetEquals(correctDS, aisPointsWithB5DS)
  }


*/






}
