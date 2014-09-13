package Graph

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 21:07
 */
trait Edge {
  val from: Vertex
  val to: Vertex
  val weight: Double = 1.0

  override def toString = f"$from\t$to\t$weight"

  override def equals(other: Any) = {
    other match {
      case otherEdge: Edge => {
        otherEdge.from.equals(from) && otherEdge.to.equals(to) && otherEdge.weight == weight
      }
      case _ =>
        false
    }

  }

}

case class EdgeImp(from: Vertex, to: Vertex,override val weight: Double) extends Edge


