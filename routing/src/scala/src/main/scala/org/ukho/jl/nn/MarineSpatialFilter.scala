package org.ukho.jl.nn

import java.sql.Timestamp

import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.broadcast

import scala.collection.mutable
/**
  * Created by lewisj on 21/09/17.
  */
object MarineSpatialFilter
{

  val nullAllowed = true

  // Before201607
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

  // After201607
  //WARNING - SOME TYPES DIFFER FROM ORIGINAL ARKEVISTA SPEC (changed some number type to string following problems like "\N" complain as "NumberFormatException")
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

  val aisTempSchema = StructType(
    Array(StructField("cellId",StringType,nullAllowed),
      StructField("MMSI",StringType,nullAllowed),
      StructField("acquisition_time", TimestampType,nullAllowed),
      StructField("lon", DoubleType, nullAllowed),
      StructField("lat", DoubleType, nullAllowed)))



  def readArkevistaPositionDataAfter201607_marspat(spark: SparkSession, dataPath: String) :DataFrame = {
    spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaAfter201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(dataPath)
      .select("MMSI", "acquisition_time", "lon", "lat")
  }

  val ihsSchemaSubset_marspat = StructType(
    Array(StructField("ihsMMSI", StringType, nullAllowed),
      StructField("ihsShipTypeLevel5", StringType, nullAllowed),
      StructField("ihsShipTypeLevel2", StringType, nullAllowed),
      StructField("ihsUkhoVesselType", StringType, nullAllowed),
      StructField("ihsGrossTonnage", DoubleType, nullAllowed)
    )) //TODO: This is not how the ihs data comes in, this pre-processing needs recording somewhere...







  def appendIHSfilter(spark: SparkSession, aisPointsDS: DataFrame,ukhoType: String ,ihsDF: DataFrame):DataFrame = {

    import spark.implicits._


    val filteredAIS = ukhoType match {
      case "Cargo" => aisPointsDS.join(broadcast(ihsDF),aisPointsDS.col("MMSI")===ihsDF.col("ihsMMSI"))
        .select($"MMSI",$"acquisition_time",$"lon",$"lat")
        .filter($"ihsGrossTonnage" > 2000).filter($"ihsMMSI" =!= "")
        .filter($"ihsUkhoVesselType" === "Bulker" || $"ihsUkhoVesselType" ==="Container" || $"ihsUkhoVesselType" ==="Roro"
          ||$"ihsUkhoVesselType" ==="Dry Cargo"||$"ihsUkhoVesselType" ==="Combination" ||$"ihsUkhoVesselType" ==="Reefer")
          .toDF()
      case _ => aisPointsDS.join(broadcast(ihsDF),aisPointsDS.col("MMSI")===ihsDF.col("ihsMMSI"))
        .select($"MMSI",$"acquisition_time",$"lon",$"lat")
        .filter($"ihsGrossTonnage" > 2000).filter($"ihsMMSI" =!= "")
        .filter($"ihsUkhoVesselType" === ukhoType)
          .toDF()

    }

    filteredAIS
  }


  def main(args: Array[String]): Unit = {


    require(args.length >= 1, "Specify data file")

    val spark = SparkSession.builder()
      .appName("calculateDestinations")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.SparkContext._





    val ukhoType = args(0)
    val aisDir = args(1)
    val ihsPath= args(2)
    val outputPath = args(3)



    val ihsDF = spark.read
      .option("delimiter", ",")
      .schema(ihsSchemaSubset_marspat)
      .csv(ihsPath)



    val ais2017 = readArkevistaPositionDataAfter201607_marspat(spark, aisDir)
    val aisPointsFiltered = appendIHSfilter(spark,ais2017,ukhoType,ihsDF)



    aisPointsFiltered.cache()

    aisPointsFiltered
      .write
      .csv(outputPath)
  }
}
