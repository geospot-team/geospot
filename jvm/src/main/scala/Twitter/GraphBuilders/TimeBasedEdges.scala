package Twitter.GraphBuilders

import Graph.{Edge, EdgeExtractor, EdgeImp}
import Twitter.Tweet

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 21:11
 */
object TimeBasedEdges extends EdgeExtractor[List[Tweet]] {
  val minMoveInterval: Long = 1000 * 60 * 5
  //5 minutes
  val maxMoveInterval: Long = 1000 * 60 * 60 * 8 //one hour

  override def apply(tweets: List[Tweet]): List[Edge] = {
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
