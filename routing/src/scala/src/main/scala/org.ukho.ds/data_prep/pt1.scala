/**
  * Created by JP on 22/01/2018.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object pt1
{

   case class pt(lat:Double,lon:Double,var id:Int=0,var ctype:String = "")
   val r = scala.util.Random
   val cellSize = 1
   val eps      = 0.1

   val nullAllowed = true
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

   def setID(r:Row):Seq[Row] =
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
         val newrow = Row(cellid,pttype,lon,lat)
         //println(s"-> $newrow")
         ret = ret :+ newrow
      }
      ret
   }

   def makeNearestNeighbours(cellId:String,points:Iterable[Row]):String = 
   {
      // write code to work out NN
      val total = r.nextInt(100)
      
      /*
            here is the code for working out nearest neighbours
       */
      
      cellId + "," + total.toString
   }
   
   def p(x:Row) =
   {
      println(x)
   }
   def processCell(x:(String,Iterable[Row])) =
   {
      val id = x._1
      val it = x._2
      it.foreach(x=>makeNearestNeighbours(id,it))
   }

   def getID(r:Row):String =
   {
      r(0).toString
   }

   def main(args: Array[String]): Unit =
   {
      require(args.length >= 1, "Specify data file")

      val dataPath = args(1)

      println(s"datapath=$dataPath")

      val conf = new SparkConf().setAppName("spkTest")
      conf.setMaster(args(0))
      val sc = new SparkContext(conf)
      
      val spark = SparkSession.builder()
         .appName("calculateDestinations")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      val df = spark.read
            .option("header", "false")
            .option("delimiter", "\t")
            .schema(positionSchemaAfter201607)
            .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
            .csv(dataPath)
            .select("MMSI", "acquisition_time", "lon", "lat")

      df.show(5)
      
      val res = df.
         rdd.
         flatMap(x=>setID(x)).
         groupBy(x=>getID(x)).
         map(x=>processCell(x)).
         collect()

      println("hello world");

   }
}
