/**
  * Created by JP& JL on 22/01/2018.
  */
package org.ukho.jl.nn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import archery.Box
import archery.Entry
import archery.Point
import archery.RTree
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import java.io._
import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object AISGrid
{

  //   val r = scala.util.Random




  val nullAllowed = true
  val positionSchemaBefore201607 = StructType(
    Array(StructField("ArkPosID", StringType, nullAllowed),
      StructField("MMSI", StringType, nullAllowed),
      StructField("NavigationalStatus", StringType, nullAllowed),
      StructField("lon", DoubleType, nullAllowed),
      StructField("lat", DoubleType, nullAllowed),
      StructField("sog", StringType, nullAllowed),
      StructField("cog", StringType, nullAllowed),
      StructField("rot", StringType, nullAllowed),
      StructField("heading", StringType, nullAllowed),
      StructField("acquisition_time", TimestampType, nullAllowed),
      StructField("IPType", StringType, nullAllowed)))
  val positionSchemaAfter201607 = StructType(
    Array(StructField("ArkPosID", StringType, nullAllowed),
      StructField("MMSI", StringType, nullAllowed),
      StructField("acquisition_time", TimestampType, nullAllowed),
      StructField("lon", DoubleType, nullAllowed),
      StructField("lat", DoubleType, nullAllowed),
      StructField("vessel_class", StringType, nullAllowed),
      StructField("message_type_id", StringType, nullAllowed),
      StructField("navigational_status", StringType, nullAllowed),
      StructField("rot", StringType, nullAllowed),
      StructField("sog", StringType, nullAllowed),
      StructField("cog", StringType, nullAllowed),
      StructField("true_heading", StringType, nullAllowed),
      StructField("altitude", StringType, nullAllowed),
      StructField("special_manoeurve", StringType, nullAllowed),
      StructField("radio_status", StringType, nullAllowed),
      StructField("flags", StringType, nullAllowed)))

  val ihsSchemaSubsetNN = StructType(
    Array(StructField("ihsMMSI", StringType, nullAllowed),
      StructField("ihsShipTypeLevel5", StringType, nullAllowed),
      StructField("ihsShipTypeLevel2", StringType, nullAllowed),
      StructField("ihsUkhoVesselType", StringType, nullAllowed),
      StructField("ihsGrossTonnage", DoubleType, nullAllowed)
    ))




  def appendIHSandmonth(spark: SparkSession, aisPointsDF: DataFrame,ihsDF: DataFrame):  Dataset[(String, Timestamp, Double, Double,String,Double,Int)] = {

    import spark.implicits._
    val month_col = month(aisPointsDF("acquisition_time")).cast(IntegerType)

    val aisPointsFilteredIHS = aisPointsDF.join(broadcast(ihsDF),aisPointsDF.col("MMSI")===ihsDF.col("ihsMMSI"))
      .select($"MMSI",$"acquisition_time",$"lon",$"lat",$"ihsUkhoVesselType",$"ihsGrossTonnage")
      .filter($"ihsGrossTonnage" > 2000)
      .withColumn("month_int", month_col)
      .as[(String, Timestamp, Double, Double,String,Double,Int)]

    // TODO see if broadcast affects cluster


    aisPointsFilteredIHS
  }


  def getWKT(lon: Double,lat: Double,cellSize: Double):( String)=
  {

    val x5d = lon
    val y5d = lat

    val maxX = (roundUp(x5d/cellSize))*cellSize
    val minX = maxX-cellSize

    val maxY = (roundUp(y5d/cellSize))*cellSize
    val minY = (maxY - cellSize)

    val wkt = makeWKT(maxX,minX,maxY,minY)

    wkt
  }

def makeWKT(maxX: Double,minX: Double,maxY: Double,minY: Double):(String)={

  val maxXString = maxX.toString
  val minXString = minX.toString
  val maxYString = maxY.toString
  val minYString = minY.toString

  val wkt = "POLYGON(("+minXString+" "+maxYString+","+maxXString+" "+maxYString+","+maxXString+" "+minYString+"," +
    ""+minXString+" "+minYString+","+minXString+" "+maxYString+"))"
  wkt
}

  def roundUp(d: Double) =
  {
    math.ceil(d).toInt
  }


  def appendGrid(spark: SparkSession, AISFilteredDS:  Dataset[(String, Timestamp, Double, Double,String,Double,Int)],cellSize:Double): DataFrame = {


    import spark.implicits._


    val aisPointsWithPoly = AISFilteredDS.map { case (mmsi, acq_time, lon, lat,vtype,grosstonn,month_int) =>
      val cellId = getWKT(lon,lat,cellSize)
      (cellId, mmsi,lon,lat,vtype,acq_time,month_int)
    } filter { _._1 != "None"}
    aisPointsWithPoly.toDF("cellid","mmsi","lon","lat","vtype","acq_time","month_int")
  }

  def vesselTypeGrid(spark: SparkSession, AisPointsAll: DataFrame,monthsDF:DataFrame): DataFrame = {
    import spark.implicits._

    val distinctCount = AisPointsAll
      .select($"cellid",$"month_int",$"mmsi",$"vtype")
      .groupBy($"cellid",$"month_int",$"vtype")
        .agg('cellid, countDistinct('mmsi).alias("total_unique_count_month"))

    val avgVes = $"total_unique_count_month"/$"days"

    val AISAverage = distinctCount.join(broadcast(monthsDF),distinctCount.col("month_int")===monthsDF.col("month"))
      .select($"cellid",$"month_int",$"vtype",$"total_unique_count_month",$"days")
      .withColumn("avg_count",avgVes)
      .orderBy(desc("cellid"),asc("month_int"))



    val pivotCount = AISAverage
      .groupBy($"cellid",$"month_int")
      .pivot("vtype")
      .agg(first($"avg_count"))


//    val distinct_count_with_month = distinct_count.withColumn("month_int",lit(filler_month))
//    distinct_count_with_month.select($"_1",$"month_int", $"total_unique_count_month")

    pivotCount
  }




  def main(args: Array[String]): Unit =
  {
    require(args.length >= 1, "Specify data file")

    val beforeDataPath = args(0)
    val afterDataPath = args(1)
    val ihsPath = args(2)
    val cellSize = args(3).toDouble
    val outputPath = args(4)



    println(s"datapath= $beforeDataPath")

    val spark = SparkSession.builder()
      .appName("Nearest Neighbour - Its awesome")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val ihsDF = spark.read
      .option("delimiter", ",")
      .schema(ihsSchemaSubsetNN)
      .csv(ihsPath)


    val beforeDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaBefore201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(beforeDataPath)
      .select("MMSI", "acquisition_time", "lon", "lat")


    val afterDF = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaAfter201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(afterDataPath)
      .select("MMSI", "acquisition_time", "lon", "lat")


    val monthsDF = Map(1 -> 31, 2 -> 28, 3 -> 31,4 ->30,5->31,6->30,7->31,8->31,9->30,10->31,11->30,12->31).toSeq.toDF("month","days")

    val aisPointsDF = beforeDF.union(afterDF).toDF("mmsi","acquisition_time", "lon", "lat")
    //todo add in filter for lat lon

    val aisPointsDFwithExtras = appendIHSandmonth(spark, aisPointsDF,ihsDF)

    val aisWithGrid = appendGrid(spark,aisPointsDFwithExtras,cellSize)

    val aisCounts = vesselTypeGrid(spark,aisWithGrid,monthsDF)


    aisCounts.show()




    aisCounts
      .write
      .csv(outputPath)
  }
}
