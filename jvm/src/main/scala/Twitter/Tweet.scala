package Twitter

import java.util.Date

import Graph.Vertex

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 21:05
 */
case class Tweet(lat: Double, lon: Double, source: String, userId: Long, timestamp: Date) extends Vertex {
  val scale = 1.5
  override val vertex = (scale * lat * 1000).toLong + (scale * lon * 1000).toLong * 10000000

  override def toString = f"Tweet wrote by $userId from $source in ($lat, $lon) on $timestamp"

  override val weight: Double = 1.0
}
