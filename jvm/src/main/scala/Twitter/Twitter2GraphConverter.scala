package Twitter

import Graph.GraphExtractor
import Twitter.GraphBuilders._

/**
 * User: Vasily
 * Date: 10.09.14
 * Time: 20:10
 */


object Twitter2GraphConverter extends App() {
  val startTime = System.currentTimeMillis()
  val tweets = TwitterLoader(args(0))
  val workTime = (System.currentTimeMillis() - startTime) / 1000
  val totalTweets = tweets.size
  println(f"Parsed $totalTweets in $workTime seconds")

  println(f"Making graph, based on twitter user ids and edges with all movements to different area in interval from 5 minutes to 8 hours")
  val userBasedGraphFilename = "user_based_graph.csv"
  val userBasedGraphFilenameMeanMoving = "user_based_graph_mean_moving_edges.csv"
  (new GraphExtractor[List[Tweet]] with UserBasedGraph[List[Tweet]]
    with TimeBasedEdges with SumEdgeWeightGraph)(tweets).save(userBasedGraphFilename)
  println(f"Saved to $userBasedGraphFilename")
  println(f"Making graph, based on twitter user ids and edges with moving time weight in interval from 5 minutes to 3 hours")

  (new GraphExtractor[List[Tweet]] with UserBasedGraph[List[Tweet]]
    with MeanMovingTimeEdges with MeanEdgeWeightGraph)(tweets).save(userBasedGraphFilenameMeanMoving)
  println(f"Saved to $userBasedGraphFilenameMeanMoving")

}
