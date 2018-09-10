package org.ukho.ds

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import org.apache.spark.sql.functions.split
import org.scalatest.FunSuite
import org.ukho.ds.AISDBSCANMMsiFirstTests
import org.ukho.jl.nn.AISFilterationByIHS.{getMmsisByGrossTonnage, ihsSchemaSubset}
import org.ukho.jl.nn.MarineSpatialNN.{positionSchemaAfter201607, positionSchemaBefore201607}
import org.ukho.jl.nn.dbscan.AISDBScanMMsiFirst.{getGroupedIter, getID, processCell}
import org.apache.spark.sql.expressions.Window

class AISDBSCANMMsiFirstTests  extends FunSuite with DatasetSuiteBase with DataFrameSuiteBase {

  def sameAs[A](c: Traversable[A], d: Traversable[A]): Boolean = {
    // See https://stackoverflow.com/a/7435236
    if (c.isEmpty) d.isEmpty
    else {
      val (e, f) = d span (c.head !=)
      if (f.isEmpty) false else sameAs(c.tail, e ++ f.tail)
    }
  }




  test("test to for no clusters formed"){

    import spark.implicits._

    val beforeDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaBefore201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/home/ubuntu/strozzapreti/routing/src/scala/src/main/resources/test_dbscan_before_ais_no_clusters.txt")
      .select("MMSI", "acquisition_time", "lon", "lat")


    val afterDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaAfter201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/home/ubuntu/strozzapreti/routing/src/scala/src/main/resources/test_dbscan_after_no_clusters.txt")
      .select("MMSI", "acquisition_time", "lon", "lat")

    val aisPointsDF = beforeDF.union(afterDF).toDF("mmsi","acquisition_time", "lon", "lat")

    val res = aisPointsDF
      .rdd
      .flatMap(x => getGroupedIter(x))
      .groupBy(x => getID(x))
      .map(x => processCell(x))

    val resultsAsDF = res.flatMap(y => y).toDF("temp","data")

    val dfcount = resultsAsDF.count()

    val testOutput =  resultsAsDF.withColumn("_tmp", split($"data", "\\,")).select(
      $"_tmp".getItem(0).as("lon"),
      $"_tmp".getItem(1).as("lat"),
      $"_tmp".getItem(2).as("cluster")
    ).drop("_tmp")
      .select($"cluster")
      .distinct()
        .rdd
      .map(r=> r(0)).collect()


    assertTrue(testOutput.last.toString==="0")
  }



  test("test for one cluster formed"){

    import spark.implicits._

    val beforeDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaBefore201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/home/ubuntu/strozzapreti/routing/src/scala/src/main/resources/test_dbscan_before_ais_no_clusters.txt")
      .select("MMSI", "acquisition_time", "lon", "lat")


    val afterDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaAfter201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/home/ubuntu/strozzapreti/routing/src/scala/src/main/resources/test_dbscan_after_one_clusters.txt")
      .select("MMSI", "acquisition_time", "lon", "lat")

    val aisPointsDF = beforeDF.union(afterDF).toDF("mmsi","acquisition_time", "lon", "lat")

    val res = aisPointsDF
      .rdd
      .flatMap(x => getGroupedIter(x))
      .groupBy(x => getID(x))
      .map(x => processCell(x))

    val resultsAsDF = res.flatMap(y => y).toDF("temp","data")

    val dfcount = resultsAsDF.count()

    val testOutput =  resultsAsDF.withColumn("_tmp", split($"data", "\\,")).select(
      $"_tmp".getItem(0).as("lon"),
      $"_tmp".getItem(1).as("lat"),
      $"_tmp".getItem(2).as("cluster")
    ).drop("_tmp")
      .select($"cluster")
      .distinct()
      .rdd
      .map(r=> r(0)).collect()

    testOutput.foreach(println)

    assertTrue(testOutput.length===2)
  }





  test("test for no cluster formed,making sure mmsi split works"){

    import spark.implicits._

    val beforeDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaBefore201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/home/ubuntu/strozzapreti/routing/src/scala/src/main/resources/test_dbscan_before_ais_no_clusters.txt")
      .select("MMSI", "acquisition_time", "lon", "lat")


    val afterDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaAfter201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("/home/ubuntu/strozzapreti/routing/src/scala/src/main/resources/test_dbscan_after_no_2_clusters.txt")
      .select("MMSI", "acquisition_time", "lon", "lat")

    val aisPointsDF = beforeDF.union(afterDF).toDF("mmsi","acquisition_time", "lon", "lat")

    val res = aisPointsDF
      .rdd
      .flatMap(x => getGroupedIter(x))
      .groupBy(x => getID(x))
      .map(x => processCell(x))

    val resultsAsDF = res.flatMap(y => y).toDF("temp","data")

    val dfcount = resultsAsDF.count()

    val testOutput =  resultsAsDF.withColumn("_tmp", split($"data", "\\,")).select(
      $"_tmp".getItem(0).as("lon"),
      $"_tmp".getItem(1).as("lat"),
      $"_tmp".getItem(2).as("cluster")
    ).drop("_tmp")
      .select($"cluster")
      .distinct()
      .rdd
      .map(r=> r(0)).collect()

    testOutput.foreach(println)

    assertTrue(testOutput.length===1)
  }




















}
