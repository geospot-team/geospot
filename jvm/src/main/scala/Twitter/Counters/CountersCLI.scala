package Twitter.Counters


import java.io.{BufferedWriter, FileWriter}
import java.util.{Calendar, Date}

import GeoHash.Impl.GridGeoHashBuilder
import Twitter.{Tweet, TwitterLoader}

import scala.collection.parallel._
import scala.io.Source

/**
 * User: Noxoomo
 * Date: 15.11.14
 * Time: 20:53
 */

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
    def contains(pointLat: Double, pointLon: Double) = pointLat > minLat && pointLon > minLon && pointLat < maxLat && pointLon < maxLon
  }

  val region = BoundingBox(args(3).toDouble, args(4).toDouble, args(5).toDouble, args(6).toDouble)

  Timer.start()
  val tweets = TwitterLoader(args(0)).filter(tweet => {
    region.contains(tweet.lat, tweet.lon)
  }).toArray.zipWithIndex


  Timer.stop("read tweets")
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
  tweets.foreach(entry => {
    val tweet = entry._1
    geoHashBuilder.add(entry._2, tweet.lat, tweet.lon)
  })
  val geoHash = geoHashBuilder.build()

  Timer.stop("Build hash")

  Timer.start()
  val sources = List("Apple" -> Set("iPhone", "iPad"), "Android" -> Set("Android"),
    "All" -> Set("Web", "iPhone", "iPad", "Android", "instagram", "foursquare", "other"),
    "4sq" -> Set("foursquare"),
    "instagram" -> Set("instagram"),
    "Mobile" -> Set("iPhone", "iPad", "Android"),
    "Social" -> Set("foursquare", "instagram"))

  val times = List("Evening" -> Timestamps.Evening, "Morning" -> Timestamps.Morning, "Day" -> Timestamps.Day, "Night" -> Timestamps.Night, "AllDay" -> Timestamps.All)

  val filters: List[(String, Tweet => Boolean)] = for (time <- times; source <- sources) yield (time._1 + "_and_" + source._1, { tweet: Tweet => {
    time._2(tweet.timestamp) && source._2(tweet.source)
  }
  })

  val result = queries.par.map({
    case (query: Query) => {
      val tweetsInArea = geoHash.near(query.lat, query.lon, query.radius).map(id => tweets(id.toInt)._1)
      filters.flatMap({
        case (filter) => {
          val filteredTweets = tweetsInArea.filter(filter._2)
          List(filteredTweets.length, filteredTweets.foldLeft(Set[Long]())(_ + _.userId).size)
        }
      })
    }
  })
  Timer.stop("proceeded queries")
  Timer.start()
  val writer = new BufferedWriter(new FileWriter(args(2)))
  writer.write(filters.map(filter => {
    filter._1 + "\tunique_" + filter._1
  }).mkString("\t") + "\n")
  for (entry <- result) {
    writer.write(entry.mkString("\t") + "\n")
  }
  writer.flush()
  writer.close()
  Timer.stop("Wrote to file")
}


class TimestampFilter(val timeFilter: Date => Boolean) extends (Tweet => Boolean) {
  override def apply(tweet: Tweet): Boolean = timeFilter(tweet.timestamp)
}

object TimestampFilter {
  def apply(timeFilter: Date => Boolean) = new TimestampFilter(timeFilter)
}


class SourceFilter(val sources: Set[String]) extends (Tweet => Boolean) {
  override def apply(tweet: Tweet): Boolean = sources contains tweet.source
}

object SourceFilter {
  def apply(sources: Set[String]) = new SourceFilter(sources)
}

object Timestamps {
  val Day = { date: Date => {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    val hours = calendar.get(Calendar.HOUR_OF_DAY)
    12 <= hours && hours < 19
  }
  }

  val Night = { date: Date => {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    val hours = calendar.get(Calendar.HOUR_OF_DAY)
    hours >= 23 || hours < 6
  }
  }

  val Evening = { date: Date => {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    val hours = calendar.get(Calendar.HOUR_OF_DAY)
    hours >= 19 && hours < 23
  }
  }

  val Morning = { date: Date => {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    val hours = calendar.get(Calendar.HOUR_OF_DAY)
    6 <= hours && hours < 12
  }
  }
  val All = { date: Date => {
    true
  }
  }
}
