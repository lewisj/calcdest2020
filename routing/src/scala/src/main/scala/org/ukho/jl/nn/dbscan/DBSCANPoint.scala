package org.ukho.jl.nn.dbscan

import org.apache.spark.mllib.linalg.Vector

case class DBSCANPoint(val vector: Vector) {

  def x = vector(0)
  def y = vector(1)

  def distanceSquared(other: DBSCANPoint): Double = {
    val dx = other.x - x
    val dy = other.y - y
    (dx * dx) + (dy * dy)
  }

}

