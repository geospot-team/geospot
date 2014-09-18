package Graph

/**
 * User: Vasily
 * Date: 13.09.14
 * Time: 21:02
 */

trait EdgeExtractor[Source]  {
  def extract(source : Source) : List[Edge]
}
