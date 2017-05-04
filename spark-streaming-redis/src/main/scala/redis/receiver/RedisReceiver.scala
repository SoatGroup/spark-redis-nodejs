package redis.receiver

/**
 * From https://github.com/Anchormen/spark-redis-connector
 */

import java.util
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}
import scala.util.{Failure, Success, Try}

/**
 * Created by elsioufy on 16-1-16.
 */

abstract class RedisReceiver(params: Map[String, String], keySet: Set[String], storageLevel: StorageLevel)
  extends Receiver[(String, String)](storageLevel) with Logging {
  /*supported params*/
  private val host: String = params.get("host").getOrElse("localhost")
  private val port: Int = Integer.parseInt(params.get("port").getOrElse("6379"))
  private val timeout: Int = Integer.parseInt(params.get("timeout").getOrElse("200"))
  private val cluster: Boolean = Try(params.get("cluster").get.toBoolean).getOrElse(false)
  private val struct: String = params.get("struct").getOrElse("list")

  /** ************************************************************************************************/

  override def onStart(): Unit = {
    //implicit val akkaSystem = akka.actor.ActorSystem()
    val t = if (cluster) getRedisClusterConnection else getRedisConnection
    t match {
      case Success(j) => log.info("onStart, Connecting to Redis [" + struct + "] API")
        new Thread("Redis List Receiver") {
          override def run() {
            receive(j)
          }
        }.start()
      case Failure(f) => log.error("Could not connect"); restart("Could not connect", f)
    }
  }

  def receive(j: JedisClientWrapper) = {
    try {
      log.info("Accepting messages from Redis")
      /*keeps running until the streaming application isStopped*/
      while (!isStopped()) {
        var allNull = true
        keySet.iterator.foreach(k => {
          val res = getData(j, k)
          if (res != null) {
            allNull = false
            log.info("received data from key: " + k)
            /*we don't implement a reliable receiver since Redis doesnt support message acknoledgements*/
            store((k, res))
          }
        })
        /*in case there isn't any data, maybe u want chill abit !*/
        if (allNull)
          Thread.sleep(timeout)
      }
    }
    /*In case any failure occurs; log the failure and try to restart the receiver*/
    catch {
      case e: Throwable => {
        log.error("Got this exception: ", e)
        restart("Trying to connect again")
      }
    }
    /*closing the redis connection*/
    finally {
      log.info("The receiver has been stopped - Terminating Redis Connection")
      try {
        j.close()
      } catch {
        case _: Throwable => log.error("error on close connection, ignoring")
      }
    }
  }

  def getData(j: JedisClientWrapper, k: String): String

  override def onStop(): Unit = {
    log.info("onStop ...nothing to do!")
  }

  private def getRedisConnection: Try[JedisClientWrapper] = {
    log.info("Redis host connection string: " + host + ":" + port)
    log.info("Creating Redis Connection")
    Try(new JedisWrapper(new Jedis(host, port)))
  }

  private def getRedisClusterConnection: Try[JedisClientWrapper] = {
    log.info("Redis cluster host connection string: " + host + ":" + port)
    log.info("[J]edis will attempt to discover the remaining cluster nodes automatically")
    log.info("Creating RedisCluster Connection")
    val jedisClusterNodes = new util.HashSet[HostAndPort]()
    jedisClusterNodes.add(new HostAndPort(host, port))
    Try(new JedisClusterWrapper(new JedisCluster(jedisClusterNodes)))
  }
}

class RedisListReceiver(params: Map[String, String], keySet: Set[String], storageLevel: StorageLevel)
  extends RedisReceiver(params, keySet, storageLevel) {
  override def getData(j: JedisClientWrapper, k: String): String = j.lpop(k)
}

class RedisSetReceiver(params: Map[String, String], keySet: Set[String], storageLevel: StorageLevel)
  extends RedisReceiver(params, keySet, storageLevel) {
  override def getData(j: JedisClientWrapper, k: String): String = j.spop(k)
}