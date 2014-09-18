package Twitter.GraphBuilders

import java.io.{BufferedWriter, FileWriter}

import Graph._
import Twitter.Tweet

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 21:11
 */
trait MeanMovingTimeEdges extends EdgeExtractor[Seq[Tweet]] {
  val minMoveInterval: Long = 1000 * 60 * 5
  val maxMoveInterval: Long = 1000 * 60 * 60 * 3

  override def extract(tweets: Seq[Tweet]): List[Edge] = {
    val timeSortedTweets = tweets.sortWith(_.timestamp.getTime < _.timestamp.getTime)
    var edges = List[Edge]()
    def edgeFromTweets(first: Tweet, second: Tweet): Tweet = {
      val diff = Math.abs(second.timestamp.getTime - first.timestamp.getTime)
      if (first.vertex != second.vertex && diff < maxMoveInterval && diff > minMoveInterval) {
        edges = EdgeImp(first, second, diff) :: edges
      }
      second
    }
    timeSortedTweets.tail.foldLeft(timeSortedTweets.head)(edgeFromTweets)
    edges
  }
}


trait MeanEdgeWeightGraph extends Factory[Graph] {

  class GraphMeanWeightImp(val edges: List[Edge] = List[Edge]()) extends Graph {
    override def add(vertex: Vertex): Graph = {
      this
    }

    override def save(filename: String): Unit = {
      val writer = new BufferedWriter(new FileWriter(filename))
      edges.groupBy(_.from).foreach({ case (from, vertexEdges) => {
        vertexEdges.groupBy(_.to).foreach({ case (to, duplicates) => {
          val fromId = from.vertex
          val toId = to.vertex
          val weight = duplicates.foldLeft(0.0)(_ + _.weight) * 1.0 / duplicates.size
          writer.write(f"$fromId\t$toId\t$weight\n")
        }
        })
      }
      })
      writer.flush()
      writer.close()
    }

    override def add(edge: Edge): Graph = new GraphMeanWeightImp(edge :: edges)
  }

  override def create(): Graph = new GraphMeanWeightImp(List())
}





