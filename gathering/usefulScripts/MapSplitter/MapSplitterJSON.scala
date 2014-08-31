#!/bin/sh
exec scala "$0" "$@"
!#

import java.io.{BufferedWriter, FileWriter}

/**
 * User: Vasily
 * Date: 21.03.14
 * Time: 16:46
 */

//latitude = широта = угол от экватора
object MapSplitterJSON extends App {

  case class Coordinate(lat: Double, lon: Double) {
    val getLon = Math.PI * lon / 180
    val getLat = Math.PI * lat / 180
  }

  def convert(angle: Double) = 180 * angle / Math.PI


  val writer = new BufferedWriter(new FileWriter(args(5)))
  var result = List[String]()
  def save(lat: Double, lon: Double) {
    result = f"($lat, $lon)" :: result
  }

  val radius = 6378
  val sideSize = args(0).toDouble
  val (left, right) = (Coordinate(args(1).toDouble, args(2).toDouble), Coordinate(args(3).toDouble, args(4).toDouble))

  val dy = sideSize / (2 * Math.PI * radius)
  val steps = Math.floor(Math.abs(right.getLat - left.getLat) / dy).toInt

  for (i <- 0 to steps) {
    val lat = left.getLat + dy * i
    val dx = sideSize / radius * Math.cos(lat)
    val dxSteps = Math.floor(Math.abs(right.getLon - left.getLon) / dx).toInt
    for (j <- 0 to dxSteps) {
      val lon = left.getLon + dx * j
      val centerLat = convert(lat + dy / 2)
      val centerLon = convert(lon + dx / 2)
      save(centerLat, centerLon)
    }
  }

  val resultStr = result.mkString(",")
  writer.write(f"[$resultStr]")
  writer.flush()
  writer.close()
}

MapSplitterJSON.main(args)