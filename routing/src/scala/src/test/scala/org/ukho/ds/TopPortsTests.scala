package org.ukho.ds


import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.ukho.ds.data_prep.TopPorts.{appendBand5, readCellBoundariesIntoRTree, removeCellsFullyContainedInOthers}
import org.ukho.ds.data_prep.TopPorts._
//import org.ukho.ds.data_prep.cellRecord

/**
  * Created by kari on 12/09/2017.
  */

class TopPortsTests extends FunSuite with DatasetSuiteBase with DataFrameSuiteBase {

  val encCoveragePath = "src/main/resources/avcs_cat.csv"
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



  test("test removeCellsFullyContainedInOthers returns correctly"){
    var wktReader: WKTReader = null
    if (wktReader == null) wktReader = new WKTReader()

    val cellCoverageMapWithContainedCells: Map[Geometry, String] = Map(
      wktReader.read("POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))") -> "cellId_areaBase_keep",
      wktReader.read("POLYGON((0.3 0.3, 0.3 0.7, 0.7 0.7, 0.7 0.3, 0.3 0.3))") -> "cellId_100pcOverlapped_remove",
      wktReader.read("POLYGON((0 0.25, 0 1.25, 1 1.25, 1 0.25, 0 0.25))") -> "cellId_75pcOverlapped_keep",
      wktReader.read("POLYGON((0 0.5, 0 1.5, 1 1.5, 1 0.5, 0 0.5))") -> "cellId_50pcOverlapped_keep",
      wktReader.read("POLYGON((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))") -> "cellId_25pcOverlapped_keep",

      wktReader.read("POLYGON((10 10, 10 11, 11 11, 11 10, 10 10))") -> "cellId_noOverlap_keep")

    val cellCoverageMap = removeCellsFullyContainedInOthers(cellCoverageMapWithContainedCells)

    val correctMap: Map[Geometry, String] = Map(
      wktReader.read("POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))") -> "cellId_areaBase_keep",
      wktReader.read("POLYGON((0 0.25, 0 1.25, 1 1.25, 1 0.25, 0 0.25))") -> "cellId_75pcOverlapped_keep",
      wktReader.read("POLYGON((0 0.5, 0 1.5, 1 1.5, 1 0.5, 0 0.5))") -> "cellId_50pcOverlapped_keep",
      wktReader.read("POLYGON((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))") -> "cellId_25pcOverlapped_keep",

      wktReader.read("POLYGON((10 10, 10 11, 11 11, 11 10, 10 10))") -> "cellId_noOverlap_keep")

    assert(sameAs(cellCoverageMap, correctMap))
  }

  test("test correct number of band 5 cells"){
    val dataFilePath = "src/main/resources/avcs_cat.csv"
    val numCells = readCellBoundariesIntoRTree(spark, dataFilePath)._2
    val countOfBand5 = numCells.keys.size
    //    assert(countOfBand5 == 5920)  // fully contained cells NOT removed
    assert(countOfBand5 == 5644)  // fully contained cells removed
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
    val (encCoverageIndex, cellCoverageMap) = readCellBoundariesIntoRTree(spark, encCoveragePath)
    val aisPointsWithB5DS = appendBand5(spark, aisNoB5DS, encCoverageIndex, cellCoverageMap)
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

    val (encCoverageIndex, cellCoverageMap) = readCellBoundariesIntoRTree(spark, encCoveragePath)
    println(encCoverageIndex)
    val aisPointsWithB5DS = appendBand5(spark, aisNoB5DS, encCoverageIndex, cellCoverageMap)

    assertDatasetEquals(correctDS, aisPointsWithB5DS)
  }


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

    val (encCoverageIndex, cellCoverageMap) = readCellBoundariesIntoRTree(spark, encCoveragePath)
    val aisPointsWithB5DS = appendBand5(spark, aisDS, encCoverageIndex, cellCoverageMap)

    assertDatasetEquals(correctDS, aisPointsWithB5DS)
  }


  test("Test fake data to check function output"){
    val aisWithCellSchema = List(
      StructField("cellid",StringType,nullAllowed),
      StructField("mmsi", StringType, nullAllowed),
      StructField("acqTime", StringType, nullAllowed)
    )

    val aisWithCells = Seq(
      Row("GB50224H","220417000","2016-08-31 11:21:59"),//duplicate rows same mmsi
      Row("GB50224H","220417000","2016-09-31 12:21:59"),
      Row("GB50224H","220417000","2016-09-31 14:51:59"),
      Row("GB50224H","232640000","2016-03-20 07:01:51"),
      Row("GB50224H","538007097","2016-08-31 22:01:03"),
      Row("GB50224H","232647000","2016-03-20 07:01:53"),

      Row("GB502038","232647000","2016-03-20 07:01:53"),//same mmsi multiple cells
      Row("GB502038","232647000","2016-03-20 07:01:53"),
      Row("GB502038","538003321","2016-03-23 21:01:53"),
      Row("GB502038","220417000","2016-03-24 19:01:53"),

      Row("C1511661","352639000","2016-03-24 19:01:53"),
      Row("C1511661","229361000","2016-03-24 19:01:53"),
      Row("C1511661","477021200","2016-03-24 19:01:53"),
      Row("C1511661","232640000","2016-03-24 19:01:53"), //different cell same mmsi

      Row("AU5237X8","352639000","2016-03-24 19:01:53") // check that the order by ignores alphabetical



    )
    import spark.implicits._

    val aisWithbands = spark.createDataFrame(
      spark.sparkContext.parallelize(aisWithCells),
      StructType(aisWithCellSchema)
    )


    val aisWithCellsDS = aisWithbands.as[(String, String, Timestamp)]

    val aisDistinctCountDF = distinctCounter(spark, aisWithCellsDS)

    val correctDF = spark.sparkContext
      .parallelize(List[(String,Int)](("C1511661",4),("GB50224H",4),("GB502038",3),("AU5237X8",1)))
      .toDF(colNames = "cellId","uniqueCount")

    assertDatasetEquals(correctDF, aisDistinctCountDF)
  }






}
