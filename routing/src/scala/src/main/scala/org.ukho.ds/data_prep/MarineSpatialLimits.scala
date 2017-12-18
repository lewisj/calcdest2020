package org.ukho.ds.data_prep

import java.sql.Timestamp

import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._

import scala.collection.mutable
/**
  * Created by lewisj on 21/09/17.
  */
object MarineSpatialLimits {

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
      StructField("ihsGrossTonnage", StringType, nullAllowed)
    )) //TODO: This is not how the ihs data comes in, this pre-processing needs recording somewhere...

  def readCellBoundariesIntoRTree_marspat(spark: SparkSession, coveragePath: String): (STRtree, Map[Geometry, String]) = {

    @transient var wktReader: WKTReader = null

    def init() {
      if (wktReader == null) wktReader = new WKTReader()
    }

    // Read from HDFS
    val avcsCat = spark.read
      .option("delimiter", ",")
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

  def appendPoly_marspat(spark: SparkSession, aisPointsDS: Dataset[(String, Timestamp, Double, Double)], encCoverageIndex: STRtree,
                  cellCoverageMap: Map[Geometry, String]): Dataset[(String, String, Timestamp,Double,Double)] = {

    val gf = new GeometryFactory()

    import spark.implicits._

    val aisPointsWithB5DS = aisPointsDS.map { case (mmsi, acq_time, lon, lat) =>
      val cellId = getIntersectionAisPoint_marspat(gf.createPoint(new Coordinate(lon, lat)), encCoverageIndex, cellCoverageMap)
      (cellId, mmsi, acq_time,lon,lat)
    } filter { _._1 != "None"}



    val flataisPointswithchart = aisPointsWithB5DS.withColumn("_1", explode($"_1"))

    val flataisDS =flataisPointswithchart.as[(String,String, Timestamp, Double, Double)]

    flataisDS.orderBy("_2").orderBy("_3")
  }

  def getMmsisByShipTypeLevel2_marspat(spark: SparkSession, ihsDF: DataFrame, ShipTypeLevel2: String): Array[String] = {

    import spark.implicits._

    val filteredMMSIs = ihsDF.filter($"ihsMMSI" =!= "")
      .filter($"ihsShipTypeLevel2" === ShipTypeLevel2)
      .select($"ihsMMSI").as[String].collect

    filteredMMSIs
  }


  def getMmsisByGrossTonnage_marspat(spark: SparkSession, ihsDF: DataFrame, minGrossTonnage: Int): Array[String] = {
    import spark.implicits._

    val filteredMMSIs = ihsDF.filter($"ihsMMSI" =!= "")
      .filter($"ihsGrossTonnage" > minGrossTonnage)
      .select($"ihsMMSI").as[String].collect

    filteredMMSIs
  }

  def filterAisByMmsis_marspat(spark: SparkSession, aisDF: Dataset[(String, String, Timestamp,Double,Double)], mmsis: Array[String]): Dataset[(String, String, Timestamp,Double,Double)] = {
    import spark.implicits._

    aisDF.filter($"_2".isin(mmsis: _*))

  }

  def distinctCounter_marspat(spark: SparkSession, cellMMSIDS: Dataset[(String, String, Timestamp,Double,Double)]): DataFrame = {
    import spark.implicits._

    val distinct_count = cellMMSIDS.toDF("_1", "_2", "_3","_4","_5")
      .select($"_1", $"_2")
      .groupBy("_1")
      .agg('_1, countDistinct('_2).alias("total_year_unique_count"))
      .orderBy(desc("total_year_unique_count"))

    distinct_count.select($"_1", $"total_year_unique_count")

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


    val aisPointsDS = aisAfterDF.union(aisBeforeDF).as[(String, Timestamp, Double, Double)]


    val aisPointsWithB5DS = appendPoly_marspat(spark, aisPointsDS, encCoverageIndex, cellCoverageMap)

    val countDF = distinctCounter_marspat(spark, aisPointsWithB5DS)

    val mmsis = getMmsisByShipTypeLevel2_marspat(spark, ihsDF, "Tankers")

    val aisFilteredDF = filterAisByMmsis_marspat(spark, aisPointsWithB5DS, mmsis)

    val aisFilteredDS = aisFilteredDF.as[(String,String, Timestamp, Double, Double)]

    val TankcountDF = distinctCounter_marspat(spark, aisFilteredDS)

    val aisPointsWithChartDF = aisPointsWithB5DS.toDF("chart","mmsi","acq_time","lon","lat")


  
    val aisPointsWithChartvtypeDF = aisPointsWithChartDF.join(ihsDF,aisPointsWithChartDF.col("mmsi")===ihsDF.col("ihsMMSI")).select($"chart",$"mmsi",$"acq_time"
      ,$"lon",$"lat",$"ihsShipTypeLevel2",$"ihsGrossTonnage")

    aisPointsWithChartvtypeDF.show()
    TankcountDF.show()
    countDF
      .write
      .csv(outputPath)
  }
}
