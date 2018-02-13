package org.ukho.JL.nn

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._


/**
  * Created by lewisj on 26/09/17.
  */
object AISFilterationByIHS {

  val nullAllowed = true

  // Before201607
  val positionSchemaBefore201607 = StructType(
    Array(StructField("aisArkPosID", StringType, nullAllowed),
      StructField("aisMMSI", StringType, nullAllowed),
      StructField("aisNavigationalStatus", StringType, nullAllowed),
      StructField("aisLon", DoubleType, nullAllowed),
      StructField("aisLat", DoubleType, nullAllowed),
      StructField("aisSog", StringType, nullAllowed),
      StructField("aisCog", StringType, nullAllowed),
      StructField("aisRot", StringType, nullAllowed),
      StructField("aisHeading", StringType, nullAllowed),
      StructField("aisAcquisition_time", TimestampType, nullAllowed),
      StructField("aisIPType", StringType, nullAllowed)))

  // After201607
  //WARNING - SOME TYPES DIFFER FROM ORIGINAL ARKEVISTA SPEC (changed some number type to string following problems like "\N" complain as "NumberFormatException")
  val positionSchemaAfter201607 = StructType(
    Array(StructField("aisArkPosID", StringType, nullAllowed),
      StructField("aisMMSI", StringType, nullAllowed),
      StructField("aisAcquisition_time", TimestampType, nullAllowed),
      StructField("aisLon", DoubleType, nullAllowed),
      StructField("aisLat", DoubleType, nullAllowed),
      StructField("aisVessel_class", StringType, nullAllowed),
      StructField("aisMessage_type_id", StringType, nullAllowed),
      StructField("aisNavigational_status", StringType, nullAllowed),
      StructField("aisRot", StringType, nullAllowed),
      StructField("aisSog", StringType, nullAllowed),
      StructField("aisCog", StringType, nullAllowed),
      StructField("aisTrue_heading", StringType, nullAllowed),
      StructField("aisAltitude", StringType, nullAllowed),
      StructField("aisSpecial_manoeurve", StringType, nullAllowed),
      StructField("aisRadio_status", StringType, nullAllowed),
      StructField("aisFlags", StringType, nullAllowed)))

  val ihsSchemaSubset = StructType(
    Array(StructField("ihsMMSI", StringType, nullAllowed),
      StructField("ihsShipTypeLevel5", StringType, nullAllowed),
      StructField("ihsShipTypeLevel2", StringType, nullAllowed),
      StructField("ihsUkhoVesselType", StringType, nullAllowed),
      StructField("ihsGrossTonnage", StringType, nullAllowed)
    )) //TODO: This is not how the ihs data comes in, this pre-processing needs recording somewhere...

  def readArkevistaPositionDataBefore201607(spark: SparkSession, dataPath: String) :DataFrame  = {
    spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaBefore201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(dataPath)
  }

  def readArkevistaPositionDataAfter201607(spark: SparkSession, dataPath: String) :DataFrame = {
    spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaAfter201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(dataPath)
  }

  def getMmsisByShipTypeLevel2(spark: SparkSession, ihsDF: DataFrame, ShipTypeLevel2: String): Array[String] = {

    import spark.implicits._

    val filteredMMSIs = ihsDF.filter($"ihsMMSI" =!= "")
      .filter($"ihsShipTypeLevel2" === ShipTypeLevel2)
      .select($"ihsMMSI").as[String].collect

    filteredMMSIs
  }


  def getMmsisByGrossTonnage(spark: SparkSession, ihsDF: DataFrame, minGrossTonnage: Int): Array[String] = {
    import spark.implicits._

    val filteredMMSIs = ihsDF.filter($"ihsMMSI" =!= "")
      .filter($"ihsGrossTonnage" > minGrossTonnage)
      .select($"ihsMMSI").as[String].collect

    filteredMMSIs
  }

  def filterAisByMmsis(spark: SparkSession, aisDF: DataFrame, mmsis: Array[String]): DataFrame = {
    import spark.implicits._

    aisDF.filter($"aisMMSI".isin(mmsis: _*))

  }

  def main(args: Array[String]): Unit = {

    require(args.length >= 1, "Specify data file")

    val spark = SparkSession.builder()
      .appName("calculateDestinations")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val aisType = args(0)
    val aisPath = args(1)
    val ihsPath = args(2)
    val filterType = args(3)
    val filterValue = args(4)
    val outputPath = args(5)


    val aisDF = if (aisType == "Before201607"){
      readArkevistaPositionDataBefore201607(spark, aisPath)

    } else if(aisType == "After201607"){
      readArkevistaPositionDataAfter201607(spark, aisPath)

    } else {
      println("\tFirst command line argument mush be 'Before201607' or 'After201607' or ...")
      System.exit(1)

    }.asInstanceOf[DataFrame]

    val ihsDF = spark.read
                  .option("delimiter", ",")
                  .schema(ihsSchemaSubset)
                  .csv(ihsPath)


    val mmsis = filterType match {
      case "ShipTypeLevel2" => getMmsisByShipTypeLevel2(spark, ihsDF, filterValue)
      case "GrossTonnage" => getMmsisByGrossTonnage(spark, ihsDF, filterValue.toInt)
    }

    val aisFilteredDF = filterAisByMmsis(spark, aisDF, mmsis)

    aisFilteredDF
      .write
      .option("header", "false")
      .option("delimiter", "\t")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(outputPath)
  }

}
