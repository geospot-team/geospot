package Foursquare.Counters

import java.io.{BufferedWriter, FileWriter}
import java.util.{Calendar, Date}

import GeoHash.Impl.GridGeoHashBuilder
import Foursquare.{FoursquareObject,FoursquareLoader}

import scala.collection.parallel._
import scala.io.Source

/*
first arg — file with tweets
second arg — file with grid
third arg — result file
then bounding box
 */
object Timer {
  var startTime = System.currentTimeMillis()

  def start(): Unit = {
    startTime = System.currentTimeMillis()
  }

  def stop() = System.currentTimeMillis() - startTime

  def stop(msg: String) = {
    val time = System.currentTimeMillis() - startTime
    println(f"$msg in time $time ms")
    time
  }
}

object CountersCLI extends App {


  case class Query(lat: Double, lon: Double, radius: Double)

  case class BoundingBox(minLat: Double, minLon: Double, maxLat: Double, maxLon: Double) {
    def in(pointLat: Double, pointLon: Double) = pointLat > minLat && pointLon > minLon && pointLat < maxLat && pointLon < maxLon
  }

  val region = BoundingBox(args(3).toDouble, args(4).toDouble, args(5).toDouble, args(6).toDouble)

  Timer.start()
  val fsqObjects = FoursquareLoader(args(0)).filter(fsqObj => {
    region.in(fsqObj.lat, fsqObj.lon)
  }).toArray.zipWithIndex


  Timer.stop("read fsq objects")
  Timer.start()
  val locationsFile = args(1)
  val queries = Source.fromFile(locationsFile).getLines().map(query => {
    val (lat: Double, lon: Double, radius: Double) = query.split("\t") match {
      case Array(lat: String, lon: String, r: String) => (lat.toDouble, lon.toDouble, r.toDouble)
      case Array(lat: String, lon: String) => (lat.toDouble, lon.toDouble, 0.5)
      case _ => (0, 0, 0) // default
    }
    Query(lat, lon, radius)
  }).toParArray

  Timer.stop("read queries")
  Timer.start()


  val boundingBox = queries.foldLeft(region)({ case (box, query: Query) => {
    BoundingBox(if (box.minLat < query.lat) box.minLat else query.lat,
      if (box.minLon < query.lon) box.minLon else query.lon,
      if (box.maxLat > query.lat) box.maxLat else query.lat,
      if (box.maxLon > query.lon) box.maxLon else query.lon)
  }
  })

  //(double  minLat, double minLon, double maxLat, double maxLon, int bins)
  val geoHashBuilder = new GridGeoHashBuilder(boundingBox.minLat, boundingBox.minLon, boundingBox.maxLat, boundingBox.maxLon, 200)
  fsqObjects.foreach(entry => {
    val fsqObj = entry._1
    geoHashBuilder.add(entry._2, fsqObj.lat, fsqObj.lon)
  })
  val geoHash = geoHashBuilder.build()

  Timer.stop("Build hash")

  Timer.start()

  val result = queries.par.map({
    case (query: Query) => {
      val fsqInArea = geoHash.near(query.lat, query.lon, query.radius).map(id => fsqObjects(id.toInt)._1)
      val sumAge = fsqInArea.foldLeft[Long](0)((prevSum: Long, obj: FoursquareObject) => prevSum + (obj.timestamp.getTime() - obj.createdAt.getTime()))
      sumAge/fsqInArea.size
    }
  })
  Timer.stop("proceeded queries")
//  Timer.start()
//  val writer = new BufferedWriter(new FileWriter(args(2)))
//  writer.write(filters.map(filter => {
//    filter._1 + "\tunique_" + filter._1
//  }).mkString("\t") + "\n")
//  for (entry <- result) {
//    writer.write(entry.mkString("\t") + "\n")
//  }
//  writer.flush()
//  writer.close()
//  Timer.stop("Wrote to file")
}

