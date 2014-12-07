package Foursquare

import java.io.{File, FileInputStream}
import java.util.Date
import Array._

import com.fasterxml.jackson.core.{JsonFactory,JsonToken}
import Foursquare.FoursquareObject

object FoursquareLoader extends (String => List[FoursquareObject]) {

  class FoursquareObjectBuilder {
    var id = ""
    var lat = 0.0
    var lon = 0.0
    var categories: Array[String] = Array()
    var timestamp = new Date()
    var createdAt = new Date()

    def build(): FoursquareObject = FoursquareObject(id, lat, lon, categories, timestamp, createdAt)

    def clear(): Unit = {
      id = ""
      lat = 0.0
      lon = 0.0
      categories = empty[String]
      timestamp = new Date()
      createdAt = new Date()
    }
  }

  var fObjects = List[FoursquareObject]()
  val builder = new FoursquareObjectBuilder()

  private def add(): Unit = {
    fObjects +:= builder.build()
    builder.clear()
  }

  private def loadFoursquareObjectsFromFile(filename: String): Unit = {
    val jsonFactory = new JsonFactory()
    val input = new FileInputStream(new File(filename))
    val parser = jsonFactory.createParser(input)
    parser.isExpectedStartArrayToken

    parser.nextToken()
    while (parser.hasCurrentToken) {
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        val fieldName = parser.getCurrentName

        fieldName match {
          case "createdAt" => {
            parser.nextToken()
            builder.createdAt = new Date(parser.getLongValue*1000)
          }
          case "_categoryIds" => {
            parser.nextToken()
            parser.nextToken()
            while (parser.nextToken() != JsonToken.END_ARRAY) {
              builder.categories :+= parser.getText
            }
            parser.nextToken()
            parser.nextToken()
          }
          case "_id"  => {
            parser.nextToken()
            builder.id = parser.getText
          }
          case "_geo" => {
            parser.nextToken()
            builder.lon = parser.getDoubleValue
            parser.nextToken()
            builder.lat = parser.getDoubleValue
            parser.nextToken()
          }
          case "_timestamp" => {
            parser.nextToken()
            builder.timestamp = new Date ((math round parser.getDoubleValue)*1000)
          }
          case _ => {
            parser.skipChildren()
          }
        }
      }
      add()
      parser.clearCurrentToken()
      parser.nextToken()
    }
    parser.close()
  }

  override def apply(filename: String): List[FoursquareObject] = {
    loadFoursquareObjectsFromFile(filename)
    fObjects
  }

 }
