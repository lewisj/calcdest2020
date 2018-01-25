/**
  * Created by JP on 22/01/2018.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import archery.Box
import archery.Entry
import archery.Point
import archery.RTree
import org.apache.spark.mllib.linalg.{Vector, Vectors}


object pt1
{

   val r = scala.util.Random
   val cellSize = 1
   val eps      = 0.1
   val minDistanceSquared = eps * eps


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


        val newPoint = new AISPoint(Vectors.dense(lon,lat))
        val newrow = Row(cellid,pttype,newPoint )

         //println(s"-> $newrow")
         ret = ret :+ newrow
      }
      ret
   }


  case class AISPoint(val vector: Vector) {

    def x = vector(0)
    def y = vector(1)

    def distanceSquared(other: AISPoint): Double = {
      val dx = other.x - x
      val dy = other.y - y
      (dx * dx) + (dy * dy)
    }

  }

  object AISLabeledPoint {

    val Unknown = 0

    object Flag extends Enumeration {
      type Flag = Value
      val Border, Core, Noise, NotFlagged = Value
    }

  }


  class AISLabeledPoint(vector: Vector) extends AISPoint(vector) {

    def this(point: AISPoint) = this(point.vector)

    var flag = AISLabeledPoint.Flag.NotFlagged
    var size = AISLabeledPoint.Unknown
    var visited = false

    override def toString(): String = {
      s"$vector,$size,$flag"
    }

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

   def makeNearestNeighbours(cellId:String,aisPoints:Iterable[Row]):String =
   {
      // write code to work out NN
      val total = r.nextInt(100)
     aisPoints.foreach(p)

     val tree = aisPoints.foldLeft(RTree[AISLabeledPoint]())(
       (tempTree, aisPoints) =>
         tempTree.insert(
           Entry(Point(aisPoints(2).asInstanceOf[AISPoint].x.toFloat, aisPoints(2).asInstanceOf[AISPoint].y.toFloat), new AISLabeledPoint(
             aisPoints(2).asInstanceOf[AISPoint]
           ))))


     tree.entries.foreach(entry => {

       val point = entry.value

       if (!point.visited) {
         point.visited = true

         val neighbors = tree.search(toBoundingBox(point), inRange(point))


         point.size = neighbors.size
         println(neighbors.size)

       }

     })


      cellId + "," + total.toString
   }
   
   def p(x:Row) =
   {
     val testval = x(2).asInstanceOf[AISPoint].x
      println( testval )
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
