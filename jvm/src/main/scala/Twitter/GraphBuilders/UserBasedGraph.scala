package Twitter.GraphBuilders

import java.io.{BufferedWriter, FileWriter}

import Graph._
import Twitter.Tweet

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 21:11
 */
object UserBasedGraph extends GraphExtractor[List[Tweet]] {
  override def apply(tweets: List[Tweet]): Graph = {
    val groupedByUsersTweets = tweets.groupBy(_.userId)
    var graph: Graph = new GraphImp()
    for (edge <- groupedByUsersTweets.flatMap({ case (id, userTweets) => TimeBasedEdges(userTweets)})) {
      graph = graph.add(edge)
    }
    graph
  }
}

class GraphImp(val edges: List[Edge] = List[Edge]()) extends Graph {
  override def add(vertex: Vertex): Graph = {
    this
  }

  override def save(filename: String): Unit = {
    val writer = new BufferedWriter(new FileWriter(filename))
    edges.groupBy(_.from).foreach({ case (from, vertexEdges) => {
      vertexEdges.groupBy(_.to).foreach({ case (to, duplicates) => {
        val fromId = from.vertex
        val toId = to.vertex
        val weight = duplicates.foldLeft(0.0)(_ + _.weight)
        writer.write(f"$fromId\t$toId\t$weight\n")
      }
      })
    }
    })
    writer.flush()
    writer.close()
  }

  override def add(edge: Edge): Graph = new GraphImp(edge :: edges)

}


