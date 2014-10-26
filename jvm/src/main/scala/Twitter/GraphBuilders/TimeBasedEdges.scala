package Twitter.GraphBuilders

import java.io.{FileWriter, BufferedWriter}

import Graph._
import Twitter.Tweet

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 21:11
 */
trait TimeBasedEdges extends EdgeExtractor[Seq[Tweet]] {
  val minMoveInterval: Long = 1000 * 60 * 5
  val maxMoveInterval: Long = 1000 * 60 * 60 * 12

  override def extract(tweets: Seq[Tweet]): List[Edge] = {
    val timeSortedTweets = tweets.sortWith(_.timestamp.getTime < _.timestamp.getTime)
    var edges = List[Edge]()
    def edgeFromTweets(first: Tweet, second: Tweet): Tweet = {
      val diff = second.timestamp.getTime - first.timestamp.getTime
      if (first.vertex != second.vertex && diff < maxMoveInterval && diff > minMoveInterval) {
        edges = EdgeImp(first, second, 1.0) :: edges
      }
      second
    }
    timeSortedTweets.tail.foldLeft(timeSortedTweets.head)(edgeFromTweets)
    edges
  }
}




trait SumEdgeWeightGraph extends Factory[Graph] {

  class SumEdgeWeightGraphImp(val edges: List[Edge] = List[Edge]()) extends Graph {
    override def add(vertex: Vertex): Graph = {
      this
    }

    override def save(filename: String): Unit = {
      val writer = new BufferedWriter(new FileWriter(filename))
      edges.groupBy(edge => (edge.from.vertex,edge.to.vertex)).foreach( {
        case ((fromId,toId), duplicates) =>
          val weight = duplicates.foldLeft(0.0)(_ + _.weight)
          writer.write(f"$fromId\t$toId\t$weight\n")
      })
      writer.flush()
      writer.close()
    }

    override def add(edge: Edge): Graph = new SumEdgeWeightGraphImp(edge :: edges)
  }

  override def create(): Graph = new SumEdgeWeightGraphImp(List())
}


