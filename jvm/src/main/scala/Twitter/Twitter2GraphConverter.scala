package Twitter

import Twitter.GraphBuilders.UserBasedGraph

/**
 * User: Vasily
 * Date: 10.09.14
 * Time: 20:10
 */


object Twitter2GraphConverter extends App() {
  val startTime = System.currentTimeMillis()
  val tweets =  TwitterLoader(args(0))
  val workTime = (System.currentTimeMillis() - startTime) / 1000
  val totalTweets = tweets.size
  println(f"Parsed $totalTweets in $workTime seconds")

  println(f"Making graph, based on twitter user ids and edges with all movements to different area in interval from 5 minutes to 8 hours")
  val userBasedGraphFilename = "user_based_graph.csv"
  UserBasedGraph(tweets).save(userBasedGraphFilename)
  println(f"Saved to $userBasedGraphFilename")

}
