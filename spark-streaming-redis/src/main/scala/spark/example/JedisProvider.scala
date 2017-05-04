package spark.example

import redis.clients.jedis.{Jedis, JedisPool}

/**
 * Created by brobrien on 10/25/16.
 */
object JedisProvider {
  val pool = new JedisPool("localhost")

  def exec(jedisFunc: Jedis => Unit) = {
    val jedis = pool.getResource
    var broken = false
    if (jedis != null) {
      try {
        jedisFunc(jedis)
      } catch {
        case exp: Exception => {
          broken = true
        }
      } finally {
        if (jedis != null)
          if (broken) {
            pool returnBrokenResource jedis
          } else {
            pool returnResource jedis
          }
      }
    }
  }
}
