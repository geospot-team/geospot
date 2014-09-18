package Twitter.GraphBuilders

import java.io.{BufferedWriter, FileWriter}

import Graph._
import Twitter.Tweet

import scala.collection.AbstractSeq

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 21:11
 */
trait UserBasedGraph[TweetsSeq <: Seq[Tweet]] extends GraphExtractor[TweetsSeq ] with EdgeExtractor[Seq[Tweet]]  with Factory[Graph]{
  override def apply(tweets:  TweetsSeq): Graph = {
    val groupedByUsersTweets = tweets.groupBy(_.userId)
    var graph: Graph = create()
    for (edge <- groupedByUsersTweets.flatMap({ case (id, userTweets) => extract(userTweets)})) {
      graph = graph.add(edge)
    }
    graph
  }
}

