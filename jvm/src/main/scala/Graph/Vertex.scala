package Graph

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 21:07
 */
trait Vertex {
  val vertex: Long
  val weight: Double = 1.0

  override def toString = f"$vertex\t$weight"

  override def equals(other: Any) = {
    other match {
      case otherVertex: Vertex =>
        otherVertex.vertex == this.vertex && otherVertex.weight == weight
      case _ =>
        false
    }

  }
}

case class VertexImp(vertex: Long) extends Vertex
