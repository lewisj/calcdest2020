package org.ukho.ds.data_prep

import java.sql.Timestamp

import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.ukho.ds.data_prep.MarineSpatialLimits.{appendIHS, readArkevistaPositionDataAfter201607_marspat, readArkevistaPositionDataBefore201607_marspat}

import scala.collection.mutable
/**
  * Created by lewisj on 21/09/17.
  */
object MarineSpatialNN {

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

  def readArkevistaPositionDataBefore201607_nn(spark: SparkSession, dataPath: String) :DataFrame  = {
    spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaBefore201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(dataPath)
      .select("MMSI", "acquisition_time", "lon", "lat")
  }

  def readArkevistaPositionDataAfter201607_nn(spark: SparkSession, dataPath: String) :DataFrame = {
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
      StructField("ihsGrossTonnage", StringType, nullAllowed)
    )) //TODO: This is not how the ihs data comes in, this pre-processing needs recording somewhere...













  def createGridId(xVar: Double,yVar: Double):(String)={

    val x3d = f"${xVar.toInt}%03d"
    val y3d = f"${yVar.toInt}%03d"

    val centreCord = x3d+"."+y3d

centreCord


  }


 def roundUp(d: Double) = math.ceil(d).toInt


  def getCellAndBorders(lon: Double,lat: Double,cellSize: Double,eps:Double):( Map[String,String]   )={
//array[String]
    //54.0434,20.2312

    var cellIds:Map[String,String]=  Map()

    var borderIds = Array.empty[String]
    //what a bodge, there is a better way to do this


    val mainCell = createGridId(roundUp((lon)/cellSize),roundUp((lat)/cellSize) )

    cellIds += mainCell -> "main"


    if ( createGridId(roundUp((lon - eps)/cellSize),roundUp((lat - eps)/cellSize) ) != mainCell ){

      var tempGridId= createGridId(roundUp((lon - eps)/cellSize),roundUp((lat - eps)/cellSize) )
      borderIds :+= tempGridId

    }

    if ( createGridId(roundUp((lon + eps)/cellSize),roundUp((lat + eps)/cellSize) ) != mainCell ){

      var tempGridId= createGridId(roundUp((lon + eps)/cellSize),roundUp((lat + eps)/cellSize) )
      borderIds :+= tempGridId

    }

    if ( createGridId(roundUp((lon - eps)/cellSize),roundUp((lat + eps)/cellSize) ) != mainCell ){

      var tempGridId= createGridId(roundUp((lon - eps)/cellSize),roundUp((lat + eps)/cellSize) )
      borderIds :+= tempGridId

    }

    if ( createGridId(roundUp((lon + eps)/cellSize),roundUp((lat - eps)/cellSize) ) != mainCell ){

      var tempGridId= createGridId(roundUp((lon + eps)/cellSize),roundUp((lat - eps)/cellSize) )
      borderIds :+= tempGridId

    }



  for (i <- borderIds){

    if(cellIds.exists(_==(i,"border"))){


    }

    else{
      cellIds += i -> "border"
    }
  }

    cellIds

  }


  def appendCellBorder(spark: SparkSession,aisPointsDS: Dataset[(String, Timestamp, Double, Double)],cellSize:Double,epsilon:
                      Double): DataFrame = {
    import spark.implicits._

    val aisPointsWithCellIds = aisPointsDS.map { case (mmsi, acq_time, lon, lat) =>
      val cellids = getCellAndBorders(lon,lat,cellSize,epsilon)
      (cellids, mmsi, acq_time,lon,lat)
    } filter { _._1 != "None"}


    val flataisDF = aisPointsWithCellIds.select($"_2",$"_3",$"_4",$"_5",explode($"_1") as Seq("cell", "type"))


  flataisDF

  }
  def appendIHSNN(spark: SparkSession, aisPointswithPoly: DataFrame,ihsDF: DataFrame): DataFrame = {

    import spark.implicits._

    val aisPointsWithPolyIHS = aisPointswithPoly.join(ihsDF,aisPointswithPoly.col("_2")===ihsDF.col("ihsMMSI")).select($"_2",$"_3"
      ,$"cell",$"type",$"_4",$"_5",$"ihsUkhoVesselType",$"ihsGrossTonnage")


    aisPointsWithPolyIHS
  }



  def cellCounter(spark: SparkSession, aisPointsComplete: DataFrame): DataFrame = {
    import spark.implicits._

    aisPointsComplete.show()

    val distinct_count = aisPointsComplete
      .select($"cell",$"_2",$"ihsUkhoVesselType")
        .filter($"type"=!="border")
      .groupBy($"cell",$"ihsUkhoVesselType")

      .agg('cell, count('_2).alias("total_count"))
      .orderBy(desc("cell"),desc("total_count"))

    distinct_count.select($"cell",$"ihsUkhoVesselType",$"total_count")

  }

  def partitionCreation(spark: SparkSession, aisPointsCompleteDF: DataFrame): DataFrame = {

    //  val point$"total_count"sByPartition = aisPointsComplete.groupBy("cell")
    import spark.implicits._

    //    val pointsByPartition = aisPointsComplete.repartition($"cell")

    //    pointsByPartition.mapPartitions(x => x.)
    aisPointsCompleteDF

    val aisPointsPartsDF = aisPointsCompleteDF
      .select("cell", "_4", "_5")
      .as[(String, Double, Double)]
      .groupByKey(_._1)
      .flatMapGroups { case (cell, lon, lat) =>
          println(cell,lat,lon)
          cell
    }
//


//    val pointsByPartition = aisPointsComplete.repartition($"total_count")
aisPointsComplete
//
//    val results= aisPointsComplete.map { case (mmsi, acq_time, lon, lat) =>
//      val cellids = getCellAndBorders(lon,lat,cellSize,epsilon)
//      (cellids, mmsi, acq_time,lon,lat)
//    } filter { _._1 != "None"}
//
   aisPointsComplete
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





    val coveragePath = args(0)
    val aisBeforeDir = args(1)
    val aisAfterDir = args(2)
    val ihsPath= args(3)
    val outputPath = args(4)
    val cellSizeSource = 1
    val epsSource = 0.1


    val ihsDF = spark.read
      .option("delimiter", ",")
      .schema(ihsSchemaSubset_marspat)
      .csv(ihsPath)

    val aisBeforeDF = readArkevistaPositionDataBefore201607_nn(spark, aisBeforeDir)


    val aisAfterDF = readArkevistaPositionDataAfter201607_nn(spark, aisAfterDir)




    val aisPointsDS = aisAfterDF.union(aisBeforeDF).as[(String, Timestamp, Double, Double)] //.toDF("mmsi","acq","lon","lat")

    val aisPointsDFWithCells = appendCellBorder(spark,aisPointsDS,cellSizeSource,epsSource)


    val aisPointsWithCellVtype = appendIHSNN(spark,aisPointsDFWithCells,ihsDF)

    val aisPointsCellCounts = cellCounter(spark,aisPointsWithCellVtype)

    aisPointsCellCounts.write.csv(outputPath)
  //val testCell= createCell(20.0,0.0,0.0)
  //val testGrid = createGrid(0.5,0,0)





    // val aisFilteredDF = filterAisByMmsis_marspat(spark, aisPointsWithPoly, mmsis)

    //val aisFilteredDS = aisFilteredDF.as[(String,String, Timestamp, Double, Double)]

    //val TankcountDF = distinctCounter_marspat(spark, aisFilteredDS)
    //val countDF = distinctCounter_marspat(spark, aisPointsWithPoly)

    //aisPointsWithPoly.show()
    //aisPointsWithPolyVtype.show()
    //aisPointsWithPolyVtypeMonth.show()





  }
}
