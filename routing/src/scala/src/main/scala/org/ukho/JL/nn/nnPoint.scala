package org.ukho.JL.nn

import org.apache.spark.mllib.linalg.Vector

case class nnPoint(val vector: Vector) {

  def x = vector(0)
  def y = vector(1)

  def distanceSquared(other: nnPoint): Double = {
    val dx = other.x - x
    val dy = other.y - y
    (dx * dx) + (dy * dy)
  }

}