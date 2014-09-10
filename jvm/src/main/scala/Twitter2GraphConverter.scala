/**
 * User: Vasily
 * Date: 10.09.14
 * Time: 20:10
 */

import java.io._
import java.util.Date

import com.fasterxml.jackson.core.{JsonParser, JsonFactory, JsonToken}
import de.undercouch.bson4jackson.{BsonFactory, BsonParser}
;

case class Tweet(lat: Double, lon: Double, source: String, userId: Long, timestamp: Long) {
  override def toString = f"Tweet wrote by $userId from $source in ($lat, $lon) on $timestamp"
}

class TweetBuilder {
  var lat = 0.0
  var lon = 0.0
  var source = "other"
  var userId = -1L
  var timestamp = -1L
  var exactGps = false

  def build(): Tweet = Tweet(lat, lon, source, userId, timestamp)

  def clear(): Unit = {
    lat = 0.0
    lon = 0.0
    source = "other"
    userId = -1L
    timestamp = -1L
  }
}

class TwitterData {
  var tweets = List[Tweet]()

  val builder = new TweetBuilder()

  def add(): Unit = {
    if (builder.exactGps) {
      tweets = builder.build() :: tweets
    }
    if (tweets.size != 0)
      println(tweets.head.toString)
//    builder.clear()
  }
}


object Twitter2GraphConverter extends App() {
  val bsonFactory = new BsonFactory()
//  bsonFactory.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH)
  val input = new FileInputStream(new File(args(0)))
  var parser = bsonFactory.createParser(input)
  parser.isExpectedStartArrayToken

  val data = new TwitterData

  def parseGps(): Unit = {
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      val fieldName = parser.getCurrentName
      parser.nextToken()
      fieldName match {
        case "coordinates" => {
          parser.nextToken()
          data.builder.lon = parser.getDoubleValue
          parser.nextToken()
          data.builder.lat = parser.getDoubleValue
          parser.nextToken()
        }
        case _ => {
          parser.skipChildren()
        }
      }
    }
  }

  def parseUser(): Unit = {
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      val fieldName = parser.getCurrentName
      parser.nextToken()
      fieldName match {
        case "id" => {
          data.builder.userId = parser.getLongValue
        }
        case _ => {
          parser.skipChildren()
        }
      }
    }
  }

  parser.nextToken()
  while (parser.hasCurrentToken) {
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      val fieldName = parser.getCurrentName
      parser.nextToken()
      fieldName match {
        case "created_at" => {
          data.builder.timestamp = parser.getEmbeddedObject.asInstanceOf[Date].getTime
        }
        case "source" => {
          data.builder.source = parser.getText
        }
        case "user" => {
          parseUser()
        }
        case "certain_coords" => {
          data.builder.exactGps = parser.getIntValue == 1
        }
        case "geo" => {
          parseGps()
          data.add()
        }
        case _ => {
          parser.skipChildren()
        }
      }
    }
    parser.clearCurrentToken()
//    parser = bsonFactory.createParser(input)
    parser.nextToken()
  }

  parser.close()
  print("done")
}