package Graph

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 21:12
 */
trait Graph {
  def add(vertex: Vertex): Graph

  def add(edge: Edge): Graph


  def save(filename: String)
}
