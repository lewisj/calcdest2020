package org.ukho.jl.nn

import java.sql.Timestamp

import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable



/**
  * Created by kari on 12/09/2017.
  */

object CalculateDestinations {

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
      .select("MMSI", "acquisition_time", "lon", "lat")
  }

  def readArkevistaPositionDataAfter201607(spark: SparkSession, dataPath: String) :DataFrame = {
    spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(positionSchemaAfter201607)
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(dataPath)
      .select("MMSI", "acquisition_time", "lon", "lat")
  }



  def removeCellsFullyContainedInOthers(cellCoverageMapWithContainedCells: Map[Geometry, String]): Map[Geometry, String] = {


    val cellIdsToRemoveList = cellCoverageMapWithContainedCells.flatMap(geom1 => {
      for (geom2 <- cellCoverageMapWithContainedCells.keys) yield {
        if (geom2.contains(geom1._1) && geom2 != geom1._1) {
          geom1._2
        }
      }
    })

    val cellCoverageMapsMutable = new mutable.HashMap[Geometry, String]()

    cellCoverageMapWithContainedCells.foreach(cell => {
      cellCoverageMapsMutable.put(cell._1, cell._2)
    })

    for (cellIdToRemove <- cellIdsToRemoveList){
      cellCoverageMapsMutable.foreach(cell => {
        if (cell._2.equals(cellIdToRemove.toString)) {
          cellCoverageMapsMutable.remove(cell._1)
        }
      })
    }

    cellCoverageMapsMutable.toMap
  }

  def readCellBoundariesIntoRTree(spark: SparkSession, coveragePath: String): (STRtree, Map[Geometry, String]) = {

    @transient var wktReader: WKTReader = null

    def init() {
      if (wktReader == null) wktReader = new WKTReader()
    }

    // Read from HDFS
    val avcsCat = spark.read
      .option("delimiter", "\t")
      .csv(coveragePath)
      .collect()

    val cellCoverageMapWithContainedCells = avcsCat.map(row => {
      init()  // Use existing wktReader, otherwise create

      val cellId = row.getString(0)
      val geometry = wktReader.read(row.getString(1))
        (geometry, cellId)
      }).toMap

    val cellCoverageMap = removeCellsFullyContainedInOthers(cellCoverageMapWithContainedCells)

    val encCoverageIndex = new STRtree()

    // Create index
    cellCoverageMap.foreach(cell => {
      encCoverageIndex.insert(cell._1.getEnvelopeInternal, cell._1)
    })

    (encCoverageIndex, cellCoverageMap)
  }


  def getCellIdIntersectionAisPoint(point: Point, encCoverageIndex: STRtree, cellCoverageMap: Map[Geometry, String]): String = {

    val geometries = encCoverageIndex.query(point.getEnvelopeInternal).toArray(new Array[Geometry](0)) //rough search
    var cellId = "None"
    if (geometries.nonEmpty){
      val containingGeometries = geometries.filter(cell => cell.contains(point)) //exact search
        if (containingGeometries.nonEmpty) {
          val containingCellIdsWithDistance = containingGeometries.map(g => {
            val cellId = cellCoverageMap(g)
            val distToGeomCentroid = point.distance(g.getCentroid)
            (cellId, distToGeomCentroid)
          })
          cellId = containingCellIdsWithDistance.minBy(_._2)._1
        }
    }
    cellId
  }

  def appendBand5(spark: SparkSession, aisPointsDS: Dataset[(String, Timestamp, Double, Double)], encCoverageIndex: STRtree,
                  cellCoverageMap: Map[Geometry, String]): Dataset[(String, String, Timestamp)] = {

    val gf = new GeometryFactory()

    import spark.implicits._

    val aisPointsWithB5DS = aisPointsDS.map { case (mmsi, acq_time, lon, lat) =>
      val cellId = getCellIdIntersectionAisPoint(gf.createPoint(new Coordinate(lon, lat)), encCoverageIndex, cellCoverageMap)
      (cellId, mmsi, acq_time)
    } filter { _._1 != "None"}

    aisPointsWithB5DS
  }

  def getDestinations(spark: SparkSession, aisPointsWithB5DS: Dataset[(String, String, Timestamp)]):
  DataFrame ={
    import spark.implicits._

    val byTimeWindow = Window.partitionBy("mmsi").orderBy("acq_time")

    val destinationsDF =  aisPointsWithB5DS.toDF("cellId", "mmsi", "acq_time")
      .withColumn("endCell", lead($"cellId",1).over(byTimeWindow))
      .withColumn("entryTimeOfEndCell", lead($"acq_time",1).over(byTimeWindow))
      .filter($"cellId" =!= $"endCell").filter($"endCell".isNotNull).toDF

    //TODO revisit this to do programmatically? below just re-orders and aliases columns

    destinationsDF.createOrReplaceTempView("destinationsDF")
    val destinationsReorderedDF = spark.sql("SELECT mmsi, cellId AS startCell, endCell, acq_time AS " +
      "exitTimeOfStartCell, entryTimeOfEndCell FROM destinationsDF ORDER BY mmsi, exitTimeOfStartCell")

    destinationsReorderedDF
  }


  def main(args: Array[String]): Unit = {

    require(args.length >= 1, "Specify data file")

    val spark = SparkSession.builder()
      .appName("calculateDestinations")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val coveragePath = args(0)
    val aisBeforeDir = args(1)
    val aisAfterDir = args(2)
    val outputPath = args(3)

    val (encCoverageIndex, cellCoverageMap) = readCellBoundariesIntoRTree(spark, coveragePath)
    val aisBeforeDF = readArkevistaPositionDataBefore201607(spark, aisBeforeDir)
    val aisAfterDF = readArkevistaPositionDataAfter201607(spark, aisAfterDir)
    val aisPointsDS = aisAfterDF.union(aisBeforeDF).as[(String, Timestamp, Double, Double)]

    val aisPointsWithB5DS = appendBand5(spark, aisPointsDS, encCoverageIndex, cellCoverageMap)

    val mmsiStartEndLocationsDF = getDestinations(spark, aisPointsWithB5DS)

    mmsiStartEndLocationsDF
      .write
      .csv(outputPath)
  }
}