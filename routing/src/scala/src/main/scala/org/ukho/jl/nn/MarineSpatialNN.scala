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


object MarineSpatialNN
{

//   val r = scala.util.Random
   val cellSize = 0.1
   val eps      = 0.005
   val minDistanceSquared = eps * eps



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
      StructField("ihsGrossTonnage", StringType, nullAllowed)
    ))



  def getMmsisByUkhoVtypeNN(spark: SparkSession, ihsDF: DataFrame, ukhoType: String): Array[String] = {

    import spark.implicits._

    val filteredMMSIs = ukhoType match {
      case "Cargo" => ihsDF.filter($"ihsMMSI" =!= "")
        .filter($"ihsUkhoVesselType" === "Bulker" || $"ihsUkhoVesselType" ==="Container" || $"ihsUkhoVesselType" ==="Roro"
          ||$"ihsUkhoVesselType" ==="Dry Cargo"||$"ihsUkhoVesselType" ==="Combination" ||$"ihsUkhoVesselType" ==="Reefer")
        .select($"ihsMMSI").as[String].collect
      case _ => ihsDF.filter($"ihsMMSI" =!= "")
        .filter($"ihsUkhoVesselType" === ukhoType)
        .select($"ihsMMSI").as[String].collect
    }



    filteredMMSIs
  }


  def getMmsisByGrossTonnageNN(spark: SparkSession, ihsDF: DataFrame, minGrossTonnage: Int): Array[String] = {
    import spark.implicits._

    val filteredMMSIs = ihsDF.filter($"ihsMMSI" =!= "")
      .filter($"ihsGrossTonnage" > minGrossTonnage)
      .select($"ihsMMSI").as[String].collect

    filteredMMSIs
  }

  def filterAisByMmsisNN(spark: SparkSession, aisDF: DataFrame, mmsis: Array[String]): DataFrame = {
    import spark.implicits._

    aisDF.filter($"MMSI".isin(mmsis: _*))

  }


   def getCellAndBorders(lon: Double,lat: Double,cellSize: Double,eps:Double):( Map[String,String]   )=
   {
      var cellIds:Map[String,String] =  Map()
      var borderIds = Array.empty[String]

      val mainCell = createGridId(roundUp((lon)/cellSize),roundUp((lat)/cellSize) )
      cellIds += mainCell -> "main"

      if ( createGridId(roundUp((lon - eps)/cellSize),roundUp((lat - eps)/cellSize) ) != mainCell )
      {
         var tempGridId = createGridId(roundUp((lon - eps)/cellSize),roundUp((lat - eps)/cellSize) )
         borderIds :+= tempGridId
      }

      if ( createGridId(roundUp((lon + eps)/cellSize),roundUp((lat + eps)/cellSize) ) != mainCell )
      {
         var tempGridId = createGridId(roundUp((lon + eps)/cellSize),roundUp((lat + eps)/cellSize) )
         borderIds :+= tempGridId
      }

      if ( createGridId(roundUp((lon - eps)/cellSize),roundUp((lat + eps)/cellSize) ) != mainCell )
      {
         var tempGridId = createGridId(roundUp((lon - eps)/cellSize),roundUp((lat + eps)/cellSize) )
         borderIds :+= tempGridId
      }

      if ( createGridId(roundUp((lon + eps)/cellSize),roundUp((lat - eps)/cellSize) ) != mainCell )
      {
         var tempGridId = createGridId(roundUp((lon + eps)/cellSize),roundUp((lat - eps)/cellSize) )
         borderIds :+= tempGridId
      }

      for (i <- borderIds)
      {
         if(cellIds.exists( _== (i,"border")))
         {

         }
         else
         {
            cellIds += i -> "border"
         }
      }
      cellIds
   }

   def createGridId(xVar: Double,yVar: Double):(String)=
   {
      val x3d = f"${xVar.toInt}%03d"
      val y3d = f"${yVar.toInt}%03d"
      x3d+"."+y3d
   }

   def roundUp(d: Double) =
   {
         math.ceil(d).toInt
   }

   def getGroupedIter(r:Row):Seq[Row] =
   {
      //println(r)
      // call function to get cell id....
      // if border then add another Row to the list....
      val lon = r(2).toString.toDouble
      val lat = r(3).toString.toDouble
      var cellIDs = getCellAndBorders(lon,lat, cellSize,eps)

      var ret = Seq[Row]()
      for ((k,v) <- cellIDs)
      {
         val cellID = k
         val ptType = v.toString()


        val newPoint = new AISPoint(Vectors.dense(lon,lat))
        val newRow = Row(cellID,ptType,newPoint )

         //println(s"-> $newRow")
         ret = ret :+ newRow
      }
      ret
   }





  def processCell(x:(String,Iterable[Row])):Array[String] =
  {
    val id = x._1
    val it = x._2
    val outPut = makeNearestNeighbours(id,it)
    outPut
  }

  def getID(r:Row):String =
  {
    r(0).toString
  }



   def makeNearestNeighbours(cellId:String,nnRow:Iterable[Row]):Array[String]={
//println(cellId+"this is a cellID")


//     println("Completed id ----=========================="+cellId+"=====================---")
     val tree = nnRow.foldLeft(RTree[AISLabeledPoint]())(
       (tempTree, nnPointFromRow) =>
         tempTree.insert(
           Entry(Point(nnPointFromRow(2).asInstanceOf[AISPoint].x.toFloat, nnPointFromRow(2).asInstanceOf[AISPoint].y.toFloat),
             new AISLabeledPoint(nnPointFromRow(2).asInstanceOf[AISPoint]
           ))))


     var resultsArray = Array.empty[String]

     tree.entries.foreach(entry => {
       val point = entry.value
       if (!point.visited) {

         point.visited = true
         val neighbors = tree.search(toBoundingBox(point), inRange(point))
         point.size = neighbors.size
         if(createGridId(roundUp(point.x/cellSize),roundUp(point.y/cellSize)) == cellId){

        resultsArray :+= point.x+","+point.y+","+point.size}
       }
     })
     resultsArray
   }

  def using[T <: Closeable, R](resource: T)(block: T => R): R = {
    try { block(resource) }
    finally { resource.close() }
  }

  def toBoundingBox(pointRow:AISPoint): Box = {
    Box(
      (pointRow.x.toString.toDouble - eps).toFloat,
      (pointRow.y.toString.toDouble - eps).toFloat,
      (pointRow.x.toString.toDouble + eps).toFloat,
      (pointRow.y.toString.toDouble + eps).toFloat)
  }

  def inRange(point: AISLabeledPoint)(entry: Entry[AISLabeledPoint]): Boolean = {
    entry.value.distanceSquared(point) <= minDistanceSquared
  }





   def main(args: Array[String]): Unit =
   {
    require(args.length >= 1, "Specify data file")

    val beforeDataPath = args(0)
    val afterDataPath = args(1)
    val ihsPath = args(2)
    val outputPath = args(3)

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


    val aisPointsDF = beforeDF.union(afterDF).toDF("mmsi","acquisition_time", "lon", "lat")

     val vtypeMMSIs = getMmsisByUkhoVtypeNN(spark, ihsDF, "Pass./Ferry")
     val gtMMSIs = getMmsisByGrossTonnageNN(spark, ihsDF, 2000)

     val aisFilteredDFGT = filterAisByMmsisNN(spark, aisPointsDF, gtMMSIs)
     val aisFilteredDF2 = filterAisByMmsisNN(spark, aisFilteredDFGT, vtypeMMSIs)


     //todo make a variable density dbscan grider
     aisFilteredDF2.show(5)
    val res = aisFilteredDF2
         .rdd
         .flatMap(x => getGroupedIter(x))
         .groupBy(x => getID(x))
         .map(x => processCell(x))

     print(res.take(5))

    val output = res.flatMap(y => y).toDF()

    output.show(5)


    output.write
        .csv(outputPath)

   }
}
