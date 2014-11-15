package Twitter.Counters

import Twitter.Tweet

/**
 * User: Noxoomo
 * Date: 15.11.14
 * Time: 21:51
 */
trait Filters extends (Tweet => Boolean) {
}