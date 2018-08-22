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
object AISCount
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

  def readArkevistaPositionDataBefore201607_marspat(spark: SparkSession, dataPath: String) :DataFrame  = {
    spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaBefore201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(dataPath)
      .select("MMSI", "acquisition_time", "lon", "lat")
  }

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

  def readCellBoundariesIntoRTree_marspat(spark: SparkSession, coveragePath: String): (STRtree, Map[Geometry, String]) = {

    @transient var wktReader: WKTReader = null

    def init() {
      if (wktReader == null) wktReader = new WKTReader()
    }

    // Read from HDFS
    val avcsCat = spark.read
      .option("delimiter", "\t")
      .csv(coveragePath)
      .collect()

    val cellCoverageMap = avcsCat.map(row => {
      init()  // Use existing wktReader, otherwise create

      val cellId = row.getString(0)
      val geometry = wktReader.read(row.getString(1))
      (geometry, cellId)
    }).toMap
    val encCoverageIndex = new STRtree()

    // Create index
    cellCoverageMap.foreach(cell => {
      encCoverageIndex.insert(cell._1.getEnvelopeInternal, cell._1)
    })

    (encCoverageIndex, cellCoverageMap)
  }

  def getIntersectionAisPoint_marspat(point: Point, encCoverageIndex: STRtree, cellCoverageMap:
  Map[Geometry, String]): Array[(String)] = {

    val geometries = encCoverageIndex.query(point.getEnvelopeInternal).toArray(new Array[Geometry](0)) //rough search
    var cellId = Array.empty[String]
    if (geometries.nonEmpty){
      val containingGeometries = geometries.filter(cell => cell.contains(point)) //exact search
      if (containingGeometries.nonEmpty) {

        val containingCellIdsWithDistance = containingGeometries.map(g => {
          val cellId = cellCoverageMap(g)
          val distToGeomCentroid = point.distance(g.getCentroid)
          (cellId)
        })
        cellId = containingCellIdsWithDistance



      }
    }
    cellId
  }



  def appendIHSandmonth(spark: SparkSession, aisPointsDS: DataFrame,ihsDF: DataFrame): Dataset[(String, Timestamp, Double, Double,String,Double,Int)] = {

    import spark.implicits._
    val month_col = month(aisPointsDS("acquisition_time"))

    val aisPointsFilteredIHS = aisPointsDS.join(broadcast(ihsDF),aisPointsDS.col("MMSI")===ihsDF.col("ihsMMSI"))
      .select($"MMSI",$"acquisition_time",$"lon",$"lat",$"ihsUkhoVesselType",$"ihsGrossTonnage")
      .filter($"ihsGrossTonnage" > 2000)
      .withColumn("month_int", month_col)
      .as[(String, Timestamp, Double, Double,String,Double,Int)]
        // TODO see if broadcast affects cluster



    aisPointsFilteredIHS
  }



//  def appendmonth(spark: SparkSession, aisPointswithPolyVtype: DataFrame): DataFrame = {
//
//    import spark.implicits._
//
//    val month_col = month(aisPointswithPolyVtype("_3"))
//    val aisPointsWithAll = aisPointswithPolyVtype.withColumn()
//
//
//
//    aisPointsWithAll
//  }

  def appendPoly_marspat(spark: SparkSession, AISFilteredDF: Dataset[(String, Timestamp, Double, Double,String,Double,Int)], encCoverageIndex: STRtree,
                         cellCoverageMap: Map[Geometry, String]): DataFrame = {

    val gf = new GeometryFactory()

    import spark.implicits._


    val aisPointsWithPoly = AISFilteredDF.map { case (mmsi, acq_time, lon, lat,vtype,gt,month_int) =>
      val cellId = getIntersectionAisPoint_marspat(gf.createPoint(new Coordinate(lon, lat)), encCoverageIndex, cellCoverageMap)
      (cellId, mmsi,vtype,month_int)
    } filter { _._1 != "None"}



    val flataisDF = aisPointsWithPoly.withColumn("_1", explode($"_1"))



    flataisDF
  }


  def yearCounter(spark: SparkSession, AisPointsAll: DataFrame): DataFrame = {
    import spark.implicits._

    val distinct_count = AisPointsAll
      .select($"_1",$"_2",$"_4",$"_3")//TODO remove columns not needed at the beginning
      .groupBy($"_1",$"_3")
      .agg('_1, countDistinct('_2).alias("total__unique_count_month"))
      .orderBy(desc("_1"),desc("total__unique_count_month"))

    val filler_month = "0"

    val distinct_count_with_month = distinct_count.withColumn("month_int",lit(filler_month))
    distinct_count_with_month.select($"_1",$"_3",$"month_int", $"total__unique_count_month")

  }


def monthCounter(spark: SparkSession, AisPointsAll: DataFrame): DataFrame = {
    import spark.implicits._


    val distinct_count = AisPointsAll
      .select($"_1",$"_2",$"_4",$"_3")
      .groupBy($"_1",$"_4",$"_3")
      .agg('_1, countDistinct('_2).alias("month_unique_count_month"))
      .orderBy(desc("_1"),asc("_4"),desc("month_unique_count_month"))

    distinct_count.select($"_1",$"_3",$"_4",$"month_unique_count_month")

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



    val ihsDF = spark.read
      .option("delimiter", ",")
      .schema(ihsSchemaSubset_marspat)
      .csv(ihsPath)

    val (encCoverageIndex, cellCoverageMap) = readCellBoundariesIntoRTree_marspat(spark, coveragePath)
    val aisBeforeDF = readArkevistaPositionDataBefore201607_marspat(spark, aisBeforeDir)


    val aisAfterDF = readArkevistaPositionDataAfter201607_marspat(spark, aisAfterDir)


    val aisPointsDF = aisAfterDF.union(aisBeforeDF).toDF()



    val aisPointsFilteredWithMonth = appendIHSandmonth(spark,aisPointsDF,ihsDF) // TODO move up and filter gross tonnage

    val aisPointsWithPolyWithMonth = appendPoly_marspat(spark, aisPointsFilteredWithMonth, encCoverageIndex, cellCoverageMap)




    aisPointsWithPolyWithMonth.cache()

    val monthCount = monthCounter(spark,aisPointsWithPolyWithMonth)
    val yearCount = yearCounter(spark,aisPointsWithPolyWithMonth)





    val totalCounts = monthCount.union(yearCount).toDF()



    totalCounts
      .write
      .csv(outputPath)
  }
}
