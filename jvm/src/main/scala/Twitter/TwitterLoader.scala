package Twitter

import java.io.{File, FileInputStream}
import java.util.Date

import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.JsonFactory

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 20:33
 */


object TwitterLoader extends (String => List[Tweet]) {

  class TweetBuilder {
    var tweetId = 0L
    var lat = 0.0
    var lon = 0.0
    var source = "other"
    var userId = -1L
    var timestamp = new Date()
    var exactGps = false

    def build(): Tweet = Tweet(lat, lon, source, userId, timestamp,tweetId)

    def clear(): Unit = {
      lat = 0.0
      lon = 0.0
      source = "other"
      userId = -1L
      tweetId = 0
      timestamp = new Date()
    }
  }

  var tweets = List[Tweet]()
  val builder = new TweetBuilder()

  private def add(): Unit = {
    if (builder.exactGps) {
      tweets = builder.build() :: tweets
    }
    //    builder.clear()
  }

  private def loadTweetsFromFile(filename: String): Unit = {
    val jsonFactory = new JsonFactory()
    val input = new FileInputStream(new File(filename))
    val parser = jsonFactory.createParser(input)
    parser.isExpectedStartArrayToken

    def parseGps(): Unit = {
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        val fieldName = parser.getCurrentName
        parser.nextToken()
        fieldName match {
          case "coordinates" => {
            parser.nextToken()
            builder.lon = parser.getDoubleValue
            parser.nextToken()
            builder.lat = parser.getDoubleValue
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
            builder.userId = parser.getLongValue
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
            builder.timestamp = new Date(parser.getLongValue)
          }
          case "source" => {
            builder.source = parser.getText
          }
          case "_id"  => {
            builder.tweetId = parser.getLongValue
          }
          case "user" => {
            parseUser()
          }
          case "certain_coords" => {
            builder.exactGps = parser.getIntValue == 1
          }
          case "geo" => {
            parseGps()
            add()
          }
          case _ => {
            parser.skipChildren()
          }
        }
      }
      parser.clearCurrentToken()
      parser.nextToken()
    }
    parser.close()
  }

  override def apply(filename: String): List[Tweet] = {
    loadTweetsFromFile(filename)
    tweets
  }
}
