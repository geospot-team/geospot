/**
 * User: Vasily
 * Date: 15.03.14
 * Time: 15:15
 */


import java.io.{BufferedWriter, FileWriter}
import scala.xml.pull.{EvElemEnd, EvElemStart, XMLEventReader}

class Status

case class Good() extends Status()

case class This(list: Set[String]) extends Status()

case class Ignore(list: Set[String]) extends Status()


val tagsList = List(
   "id",
  "lat",
  "lon",
  "name",
  "amenity",
  "building",
  "url",
  "atm",
  "craft",
  "emergency",
  "highway",
  "historic",
  "leisure",
  "office",
  "public_transport",
  "transport",
  "railway",
  "network",
  "operator",
  "station",
  "shop",
  "sport",
  "tourism",
  "internet_access",
  "opening_hours",
  "charge",
  "phone",
  "website"
)

class Node(val lon: String, val lat: String, val id: String) {
  private val tags = Map(
     "id" -> 0,
    "lat" -> 1,
    "lon" -> 2,
    "name" -> 3,
    "amenity" -> 4,
    "building" -> 5,
    "url" -> 6,
    "atm" -> 7,
    "craft" -> 8,
    "emergency" -> 9,
    "highway" -> 10,
    "historic" -> 11,
    "leisure" -> 12,
    "office" -> 13,
    "public_transport" -> 14,
    "transport" -> 15,
    "railway" -> 16,
    "network" -> 17,
    "operator" -> 18,
    "station" -> 19,
    "shop" -> 20,
    "sport" -> 21,
    "tourism" -> 22,
    "internet_access" -> 23,
    "opening_hours" -> 24,
    "charge" -> 25,
    "phone" -> 26,
    "website" -> 27
  )

  private val accepted = Map(
    "name" -> Good(),
    "amenity" -> Ignore(Set("waste_disposal", "waste_basket", "bench", "playground",
      "register_office",
      "beauty",
      "studio",
      "trolley_stop",
      "collection office",
      "biergarten",
      "fitness_center",
      "educational_centre",
      "grave_yard",
      "printing_establishment",
      "bank;atm",
      "internet",
      "clockshop",
      "waste_transfer_station",
      "bank,bureau_de_change",
      "business",
      "languages_school",
      "beauty_salon",
      "soccer_club",
      "notary",
      "vehicle_inspection",
      "senior_house",
      "bus_stop  k100 k18\\n",
      "exchange",
      "internet_cafe",
      "compressed_air",
      "dance",
      "mending",
      "mortuary",
      "casino",
      "spa",
      "exhibition_center",
      "credit",
      "language_school",
      "stripclub",
      "offices")),
    "building" -> Ignore(Set("entrance",
      "gate", "toilets", "apartments", "public", "industrial", "dormitory")),

    "craft" -> Ignore(Set(
      "hvac",
      "shoemaker;key_cutter;electronics",
      "beekeeper",
      "basket_maker",
      "glasiery",
      "optician",
      "shoemaker; tailor; key_cutter",
      "computer",
      "shoemaker; dressmaker; key_cutter",
      "carpenter",
      "gardener",
      "furniture",
      "scrapbooking",
      "stamp",
      "shoemaker;tailor;key_cutter",
      "teilor",
      "tailor; key_cutter",
      "locksmith"
    )),
    "emergency" -> This(Set("phone")),
    "highway" -> This(Set("bus_stop")),
    "historic" -> Ignore(Set(
      "ruins", "milestone", "wayside_cross", "manor", "yes"
    )),
    "leisure" -> Ignore(Set(
      "water_park",
      "pitch",
      "track",
      "swimming_pool",
      "stadium",
      "slipway",
      "Детская площадка ",
      "marina",
      "ice_rink",
      "park",
      "sauna",
      "ice_cream"
    )),
    "office" -> Good(),
    "public_transport" -> Good(),
    "transport" -> Good(),
    "railway" -> This(Set("platform", "station", "subway_entrance", "tram_stop", "subway")),
    "network" -> Ignore(Set(
      "Кронверк Синема", "Торговая сеть «Жемчужина Японии»"
    )),
    "operator" -> Good(),
    "station" -> Good(),
    "shop" -> Good(),
    "sport" -> Good(),
    "tourism" -> Ignore(Set(
      "caravan_site",
      "gallery",
      "picnic_site",
      "guest_house",
      "museum arhiv",
      "motel",
      "yes",
      "camp_site"
    )),
    "internet_access" -> Good(),
    "opening_hours" -> Good(),
    //"charge" -> Good(),
    "phone" -> Good(),
    "website" -> Good(),
    "url" -> Good(),
    "atm" -> Good()
  )


  private val data = Array.fill[String](tags.keys.size)("NA")

  data(2) = "\"" + lon + "\""
  data(1) = "\"" + lat + "\""
  data(0) = "\"" + id + "\""
  def setTag(tag: String, value: String): Boolean = {
    if (tags contains tag)
      accepted(tag) match {
        case Good() => {
          data(tags(tag)) = "\"" + value + "\""
          true
        }
        case This(list) => {
          if (list contains value) {
            data(tags(tag)) = "\"" + value + "\""
            true
          } else false
        }
        case Ignore(list) => {
          if (!(list contains value)) {
            data(tags(tag)) = "\"" + value + "\""
            true
          } else false
        }
        case _ => false
      }
    else false
  }


  override def toString = {
    data.mkString(",") + "\n"
  }

}

object Node {
  def apply(lon: String, lat: String,id: String) = new Node(lon, lat, id)
}

val writer = new BufferedWriter(new FileWriter(args(1)))
writer.write(tagsList.mkString(",") + "\n")
var node: Node = null
var updated = false
val xml = new XMLEventReader(scala.io.Source.fromFile(args(0)))

for (event <- xml) {
  event match {
    case EvElemStart(_, "node", attr, _) => {
      node = Node(attr.get("lon").get.toString(), attr.get("lat").get.toString(),attr.get("id").get.toString())
      updated = false
    }
    case EvElemStart(_, "tag", attr, _) => {
      val key = attr.get("k").get.toString()
      val value = attr.get("v").get.toString()
      if (node setTag(key, value)) updated = true
    }
    case EvElemEnd(_, "node") => {
      if (updated) {
        writer.write(node.toString)
      }
    }
    case _ =>
  }
}


writer.flush()
writer.close()



