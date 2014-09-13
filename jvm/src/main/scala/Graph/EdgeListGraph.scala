package Graph

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 20:59
 */

trait EdgeListGraph extends Graph {
  val edgeList: List[(Long, Long, Double)]
}
