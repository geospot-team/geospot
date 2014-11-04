package Graph

/**
 * Created by noxoomo on 18/09/14.
 */
trait Factory[T] {
  def create(): T
}
