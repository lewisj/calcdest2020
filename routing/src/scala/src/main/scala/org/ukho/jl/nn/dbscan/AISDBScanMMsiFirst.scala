/**
  * Created by JP& JL on 22/01/2018.
  */
package org.ukho.jl.nn.dbscan

import java.io._

import archery.{Box, Entry, Point, RTree}

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql._
import org.ukho.jl.nn.MarineSpatialNN._
import org.ukho.jl.nn.dbscan.DBSCANLabeledPoint
import org.ukho.jl.nn.dbscan.DBSCANLabeledPoint.Flag
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._

import scala.collection.mutable.Queue


object AISDBScanMMsiFirst
{

//   val r = scala.util.Random

   val eps      = 0.008
   val minDistanceSquared = eps * eps
    val minimumPoints = 3


   def getGroupedIter(r:Row):Seq[Row] =
   {

      var mmsiGrp = r(0).toString
      var acqTime = r(1)
      val lon = r(2).toString.toDouble
      val lat = r(3).toString.toDouble

     var ret = Seq[Row]()

     val newPoint = new DBSCANLabeledPoint(Vectors.dense(lon,lat))
     val newRow = Row(mmsiGrp,acqTime,newPoint)

         //println(s"-> $newRow")
     ret = ret :+ newRow

      ret
   }

  def getID(r:Row):String =
  {
    r(0).toString
  }

  def processCell(x:(String,Iterable[Row])):Map[String,String] =
  {
    val id = x._1
    val it = x._2
    val outPut = makeNearestNeighbours(id,it,minimumPoints)
    outPut
  }





   def makeNearestNeighbours(cellId:String,nnRow:Iterable[Row],minPoints:Int):Map[String,String]={

     val tree = nnRow.foldLeft(RTree[DBSCANLabeledPoint]())(
       (tempTree, nnPointFromRow) =>
         tempTree.insert(
           Entry(
             Point(nnPointFromRow(2).asInstanceOf[DBSCANLabeledPoint].x.toFloat,nnPointFromRow(2).asInstanceOf[DBSCANLabeledPoint].y.toFloat),
             new DBSCANLabeledPoint(nnPointFromRow(2).asInstanceOf[DBSCANLabeledPoint])
           )))

     var cluster = DBSCANLabeledPoint.Unknown

     tree.entries.foreach(entry => {

       val point = entry.value

       if (!point.visited) {
         point.visited = true

         val neighbors = tree.search(toBoundingBox(point), inRange(point))

         if (neighbors.size < minimumPoints) {
           point.flag = Flag.Noise
         } else {
           cluster += 1
           expandCluster(point, neighbors, tree, cluster,minimumPoints)
         }

       }

     })

     println( tree.entries.map(_.value).toIterable)

     val treeMap = tree.entries.map(_.value).toIterable.map(
      f=> ("row",f.x.toString+","+f.y.toString+","+(f.cluster.toString))
     ).toMap

     treeMap
   }

  def using[T <: Closeable, R](resource: T)(block: T => R): R = {
    try { block(resource) }
    finally { resource.close() }
  }

  def toBoundingBox(pointRow:DBSCANPoint): Box = {
    Box(
      (pointRow.x.toString.toDouble - eps).toFloat,
      (pointRow.y.toString.toDouble - eps).toFloat,
      (pointRow.x.toString.toDouble + eps).toFloat,
      (pointRow.y.toString.toDouble + eps).toFloat)
  }

  def inRange(point: DBSCANLabeledPoint)(entry: Entry[DBSCANLabeledPoint]): Boolean = {
    entry.value.distanceSquared(point) <= minDistanceSquared
  }


  def expandCluster(
                     point: DBSCANLabeledPoint,
                     neighbors: Seq[Entry[DBSCANLabeledPoint]],
                     tree: RTree[DBSCANLabeledPoint],
                     cluster: Int,
                     minPoints:Int): Unit = {

    point.flag = Flag.Core
    point.cluster = cluster

    val left = Queue(neighbors)

    while (left.nonEmpty) {

      left.dequeue().foreach(neighborEntry => {

        val neighbor = neighborEntry.value

        if (!neighbor.visited) {

          neighbor.visited = true
          neighbor.cluster = cluster

          val neighborNeighbors = tree.search(toBoundingBox(neighbor), inRange(neighbor))

          if (neighborNeighbors.size >= minPoints) {
            neighbor.flag = Flag.Core
            left.enqueue(neighborNeighbors)
          } else {
            neighbor.flag = Flag.Border
          }
        }

        if (neighbor.cluster == DBSCANLabeledPoint.Unknown) {
          neighbor.cluster = cluster
          neighbor.flag = Flag.Border
        }

      })

    }

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

//     val vtypeMMSIs = getMmsisByUkhoVtypeNN(spark, ihsDF, "Cargo")
//     val gtMMSIs = getMmsisByGrossTonnageNN(spark, ihsDF, 300)
//
//     val aisFilteredDFGT = filterAisByMmsisNN(spark, aisPointsDF, gtMMSIs)
//     val aisFilteredDF2 = filterAisByMmsisNN(spark, aisFilteredDFGT, vtypeMMSIs)
//     aisFilteredDF2.show(5)



    val res = aisPointsDF
         .rdd
         .flatMap(x => getGroupedIter(x))
         .groupBy(x => getID(x))
         .map(x => processCell(x))



    val resultsAsDF = res.flatMap(y => y).toDF("temp","data")
     resultsAsDF.show()


    val output =  resultsAsDF.withColumn("_tmp", split($"data", "\\,")).select(
       $"_tmp".getItem(0).as("lon"),
       $"_tmp".getItem(1).as("lat"),
       $"_tmp".getItem(2).as("cluster")
     ).drop("_tmp")


     output.write
       .csv(outputPath)



   }
}
