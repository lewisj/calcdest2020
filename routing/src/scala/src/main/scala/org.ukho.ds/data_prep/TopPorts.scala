package org.ukho.ds.data_prep

import java.sql.Timestamp

import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable
/**
  * Created by lewisj on 21/09/17.
  */
object TopPorts {

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
      .option("delimiter", ",")
      .csv(coveragePath)
      .collect()

    val cellCoverageMapWithContainedCells = avcsCat.map(row => {
      init()  // Use existing wktReader, otherwise create

      val cellId = row.getString(0)
      val geometry = wktReader.read(row.getString(1))
      (geometry, cellId)
    }).filter(
      _._2.charAt(2).toString.equals("5")  //complains but only this seems to work == "5" does not
    ).toMap

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

  def distinctCounter(spark: SparkSession, cellMMSIDS: Dataset[(String, String, Timestamp)]): DataFrame = {
    import spark.implicits._

    val distinct_count = cellMMSIDS.toDF("cellId", "mmsi", "acq_time")
      .select($"cellId", $"mmsi")
      .groupBy("cellId")
      .agg('cellId, countDistinct('mmsi).alias("UniqueCount"))
      .orderBy(desc("UniqueCount"))

    distinct_count.select($"cellId", $"UniqueCount")

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
    val aisPath = args(1)
    val aisType = args(2)
    val outputPath = args(3)

    val (encCoverageIndex, cellCoverageMap) = readCellBoundariesIntoRTree(spark, coveragePath)
    val aisDF = if (aisType == "Before201607"){
     readArkevistaPositionDataBefore201607(spark, aisPath)

    } else if(aisType == "After201607"){
      readArkevistaPositionDataAfter201607(spark, aisPath)

    } else {
      println("\tThird command line argument mush be 'Before201607' or 'After201607")
      System.exit(1)

    }.asInstanceOf[DataFrame]


    val aisPointsDS = aisDF.as[(String, Timestamp, Double, Double)]

    val aisPointsWithB5DS = appendBand5(spark, aisPointsDS, encCoverageIndex, cellCoverageMap)

    val countDF = distinctCounter(spark, aisPointsWithB5DS)

    countDF
      .write
      .csv(outputPath)
  }
}
