package org.ukho.ds


import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.ukho.jl.nn.MarineSpatialNN._

/**
  * Created by kari on 12/09/2017.
  */

class marineSpatialNNTests extends FunSuite with DatasetSuiteBase with DataFrameSuiteBase {

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



  test("simple grid with 0.5 to 1 returns 2"){

    val gridString = getCellAndBorders(54.0434,20.2312,0.5,0.05)

    assert(gridString.size == 2 )

  }


  test("create grid id for point is working positive returns 3"){

    val gridString = getCellAndBorders(54.0434,20.2312,0.1,0.05)

    assert(gridString.size == 4 )

  }


  test("Edge case - its literally an edge case?!"){

    val gridString = getCellAndBorders(54.0,20.00,0.1,0.05)

    assert(gridString.size == 4 )

  }


  test("Negative values"){

    val gridString = getCellAndBorders(-54.01,-20.01,0.1,0.05)

    assert(gridString.size == 4 )

  }



  test("Negative 0.5"){

    val gridString = getCellAndBorders(-54.50,-20.50,0.5,0.001)

    assert(gridString.size == 4 )

  }


}
