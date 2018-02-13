/**
  * Created by JP& JL on 22/01/2018.
  */
package org.ukho.JL.nn
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

object MarineSpatialNN
{

   val r = scala.util.Random
   val cellSize = 1
   val eps      = 0.01
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



   def getCellAndBorders(lon: Double,lat: Double,cellSize: Double,eps:Double):( Map[String,String]   )=
   {
      var cellIds:Map[String,String]=  Map()
      var borderIds = Array.empty[String]

      val mainCell = createGridId(roundUp((lon)/cellSize),roundUp((lat)/cellSize) )
      cellIds += mainCell -> "main"

      if ( createGridId(roundUp((lon - eps)/cellSize),roundUp((lat - eps)/cellSize) ) != mainCell )
      {
         var tempGridId= createGridId(roundUp((lon - eps)/cellSize),roundUp((lat - eps)/cellSize) )
         borderIds :+= tempGridId
      }

      if ( createGridId(roundUp((lon + eps)/cellSize),roundUp((lat + eps)/cellSize) ) != mainCell )
      {
         var tempGridId= createGridId(roundUp((lon + eps)/cellSize),roundUp((lat + eps)/cellSize) )
         borderIds :+= tempGridId
      }

      if ( createGridId(roundUp((lon - eps)/cellSize),roundUp((lat + eps)/cellSize) ) != mainCell )
      {
         var tempGridId= createGridId(roundUp((lon - eps)/cellSize),roundUp((lat + eps)/cellSize) )
         borderIds :+= tempGridId
      }

      if ( createGridId(roundUp((lon + eps)/cellSize),roundUp((lat - eps)/cellSize) ) != mainCell )
      {
         var tempGridId= createGridId(roundUp((lon + eps)/cellSize),roundUp((lat - eps)/cellSize) )
         borderIds :+= tempGridId
      }

      for (i <- borderIds)
      {
         if(cellIds.exists(_==(i,"border")))
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
         val cellid = k
         val pttype = v.toString()


        val newPoint = new AISPoint(Vectors.dense(lon,lat))
        val newrow = Row(cellid,pttype,newPoint )

         //println(s"-> $newrow")
         ret = ret :+ newrow
      }
      ret
   }





  def processCell(x:(String,Iterable[Row])):String =
  {
    val id = x._1
    val it = x._2
    makeNearestNeighbours(id,it)

    println("Competed id ----=========================="+id+"=====================---")
    id
  }

  def getID(r:Row):String =
  {
    r(0).toString
  }



   def makeNearestNeighbours(cellId:String,nnRow:Iterable[Row]):Integer ={
println(cellId+"this is a cellID")


     val tree = nnRow.foldLeft(RTree[AISLabeledPoint]())(
       (tempTree, nnPointFromRow) =>
         tempTree.insert(
           Entry(Point(nnPointFromRow(2).asInstanceOf[AISPoint].x.toFloat, nnPointFromRow(2).asInstanceOf[AISPoint].y.toFloat), new AISLabeledPoint(
             nnPointFromRow(2).asInstanceOf[AISPoint]
           ))))


     var resultsArray = Array.empty[String]

     tree.entries.foreach(entry => {
       val point = entry.value
       if (!point.visited) {
         point.visited = true

         val neighbors = tree.search(toBoundingBox(point), inRange(point))
         point.size = neighbors.size

        //stops the duplication of points from overlap, could be better if AISPoint had the flag for border ToDO
         if(createGridId(roundUp(point.x),roundUp(point.y)) == cellId){

        resultsArray :+= point.x+","+point.y+","+point.size}
       }

     })


       using(new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/home/ubuntu/output/"+cellId+"out.csv")))) {
         writer =>
           for (x <- resultsArray) {
            println(x)
             writer.write(x + "\n")
           }
       }

     resultsArray.size
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



      println(s"datapath=$beforeDataPath")

     val conf = new SparkConf().setAppName("spkTest")

     val sc = new SparkContext(conf)
      
      val spark = SparkSession.builder()
         .appName("calculateDestinations")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

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

     aisPointsDF.show(5)
     val res = aisPointsDF
           .rdd
           .flatMap(x=>getGroupedIter(x))
           .groupBy(x=>getID(x))
           .map(x=>processCell(x))
       .collect()



   }
}
