package Foursquare

import java.util.Date


case class FoursquareObject(idv: String, latv: Double, lonv: Double, categoriesv: Array[String], timestampv: Date, createdAtv: Date) {
  var id: String  = idv
  var lat: Double = latv
  var lon: Double = lonv
  var categories: Array[String] = categoriesv
  var timestamp: Date = timestampv
  var createdAt: Date = createdAtv

  override def toString: String = id + " is located at " + lat +","+lon + " created at: "+createdAt

}


