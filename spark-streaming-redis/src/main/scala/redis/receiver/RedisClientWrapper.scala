package redis.receiver

/**
 * From https://github.com/Anchormen/spark-redis-connector
 */

import redis.clients.jedis.{Jedis, JedisCluster}

trait JedisClientWrapper {
  def spop(k: String): String

  def lpop(k: String): String

  def close(): Unit
}

class JedisWrapper(jedis: Jedis) extends JedisClientWrapper {
  override def spop(k: String): String = jedis.spop(k)

  override def lpop(k: String): String = jedis.lpop(k)

  override def close(): Unit = jedis.close()
}

class JedisClusterWrapper(jc: JedisCluster) extends JedisClientWrapper {
  override def spop(k: String): String = jc.spop(k)

  override def lpop(k: String): String = jc.lpop(k)

  override def close(): Unit = jc.close()
}